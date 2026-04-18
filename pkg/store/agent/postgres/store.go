// Package postgres provides a PostgreSQL-backed implementation of agent.Store.
// Metadata indices live in Postgres; message payloads remain in S3 (matching
// the AWS backend).
package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	cid "github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/storacha/go-libstoracha/jobqueue"
	"github.com/storacha/go-ucanto/core/car"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/message"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/sprue/pkg/internal/ipldutil"
	"github.com/storacha/sprue/pkg/store/agent"
)

const (
	writeConcurrency = 25
	writeTimeout     = time.Second * 30
)

type writeJob struct {
	s3Put     *s3.PutObjectInput
	indexTask cid.Cid
	indexKind string
	indexVal  string
	callback  func(error)
}

type Store struct {
	writeQueue *jobqueue.JobQueue[writeJob]
	pool       *pgxpool.Pool
	s3         *s3.Client
	bucketName string
}

var _ agent.Store = (*Store)(nil)

func New(pool *pgxpool.Pool, s3Client *s3.Client, bucketName string) *Store {
	s := &Store{
		pool:       pool,
		s3:         s3Client,
		bucketName: bucketName,
	}
	handler := jobqueue.JobHandler(func(ctx context.Context, job writeJob) error {
		var err error
		if job.s3Put != nil {
			_, err = s3Client.PutObject(ctx, job.s3Put)
		}
		if err == nil && job.indexKind != "" {
			_, err = pool.Exec(ctx, `
				INSERT INTO agent_index (task, kind, identifier)
				VALUES ($1, $2, $3)
				ON CONFLICT (task, kind) DO UPDATE SET identifier = EXCLUDED.identifier
			`, job.indexTask.String(), job.indexKind, job.indexVal)
		}
		job.callback(err)
		return nil
	})
	q := jobqueue.NewJobQueue[writeJob](
		handler,
		jobqueue.WithConcurrency(writeConcurrency),
		jobqueue.WithJobTimeout(writeTimeout),
	)
	q.Startup()
	s.writeQueue = q
	return s
}

func (s *Store) Initialize(ctx context.Context) error {
	if _, err := s.s3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &s.bucketName}); err != nil {
		if _, err := s.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &s.bucketName}); err != nil {
			return fmt.Errorf("creating S3 bucket %q: %w", s.bucketName, err)
		}
	}
	return nil
}

func (s *Store) Shutdown(ctx context.Context) error {
	return s.writeQueue.Shutdown(ctx)
}

func (s *Store) GetInvocation(ctx context.Context, task cid.Cid) (invocation.Invocation, error) {
	root, bs, err := s.getByTask(ctx, task, "in")
	if err != nil {
		return nil, fmt.Errorf("getting invocation for task %s: %w", task, err)
	}
	return invocation.NewInvocationView(cidlink.Link{Cid: root}, bs)
}

func (s *Store) GetReceipt(ctx context.Context, task cid.Cid) (receipt.AnyReceipt, error) {
	root, bs, err := s.getByTask(ctx, task, "out")
	if err != nil {
		return nil, fmt.Errorf("getting receipt for task %s: %w", task, err)
	}
	return receipt.NewAnyReceipt(cidlink.Link{Cid: root}, bs)
}

func (s *Store) getByTask(ctx context.Context, task cid.Cid, kind string) (cid.Cid, blockstore.BlockReader, error) {
	var identifier string
	err := s.pool.QueryRow(ctx, `
		SELECT identifier FROM agent_index WHERE task = $1 AND kind = $2
	`, task.String(), kind).Scan(&identifier)
	if errors.Is(err, pgx.ErrNoRows) {
		if kind == "in" {
			return cid.Undef, nil, agent.ErrInvocationNotFound
		}
		return cid.Undef, nil, agent.ErrReceiptNotFound
	}
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("querying agent_index: %w", err)
	}
	parts := strings.SplitN(identifier, "@", 2)
	if len(parts) != 2 {
		return cid.Undef, nil, fmt.Errorf("invalid identifier format: %s", identifier)
	}
	root, err := cid.Parse(parts[0])
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("parsing root CID: %w", err)
	}
	msgRoot, err := cid.Parse(parts[1])
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("parsing message root CID: %w", err)
	}

	out, err := s.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucketName,
		Key:    aws.String(toMessagePath(msgRoot)),
	})
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("getting message from S3: %w", err)
	}
	defer out.Body.Close()

	_, blocks, err := car.Decode(out.Body)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("decoding CAR: %w", err)
	}
	bs, err := blockstore.NewBlockStore(blockstore.WithBlocksIterator(blocks))
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("creating blockstore: %w", err)
	}
	return root, bs, nil
}

func (s *Store) Write(ctx context.Context, msg message.AgentMessage, index []agent.IndexEntry, source []byte) error {
	var wg sync.WaitGroup
	var writeErrMutex sync.Mutex
	var writeErr error

	msgRoot, err := ipldutil.ToCID(msg.Root().Link())
	if err != nil {
		return fmt.Errorf("converting message root link to CID: %w", err)
	}

	callback := func(err error) {
		if err != nil {
			writeErrMutex.Lock()
			writeErr = errors.Join(writeErr, err)
			writeErrMutex.Unlock()
		}
		wg.Done()
	}

	wg.Add(1)
	if err := s.writeQueue.Queue(ctx, writeJob{
		s3Put: &s3.PutObjectInput{
			Bucket: &s.bucketName,
			Key:    aws.String(toMessagePath(msgRoot)),
			Body:   bytes.NewReader(source),
		},
		callback: callback,
	}); err != nil {
		return fmt.Errorf("adding message to write queue: %w", err)
	}

	for _, entry := range index {
		if entry.Invocation != nil {
			invRoot, err := ipldutil.ToCID(entry.Invocation.Invocation.Link())
			if err != nil {
				return fmt.Errorf("converting invocation root link to CID: %w", err)
			}
			wg.Add(1)
			if err := s.writeQueue.Queue(ctx, writeJob{
				indexTask: entry.Invocation.Task,
				indexKind: "in",
				indexVal:  fmt.Sprintf("%s@%s", invRoot, msgRoot),
				callback:  callback,
			}); err != nil {
				return fmt.Errorf("adding invocation index to write queue: %w", err)
			}
		}
		if entry.Receipt != nil {
			rcptRoot, err := ipldutil.ToCID(entry.Receipt.Receipt.Root().Link())
			if err != nil {
				return fmt.Errorf("converting receipt root link to CID: %w", err)
			}
			wg.Add(1)
			if err := s.writeQueue.Queue(ctx, writeJob{
				indexTask: entry.Receipt.Task,
				indexKind: "out",
				indexVal:  fmt.Sprintf("%s@%s", rcptRoot, msgRoot),
				callback:  callback,
			}); err != nil {
				return fmt.Errorf("adding receipt index to write queue: %w", err)
			}
		}
	}

	wg.Wait()
	return writeErr
}

func toMessagePath(msg cid.Cid) string {
	return fmt.Sprintf("%s/%s", msg, msg)
}
