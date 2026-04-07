package aws

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ipfs/go-cid"
	multihash "github.com/multiformats/go-multihash"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-libstoracha/digestutil"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/sprue/pkg/internal/timeutil"
	"github.com/storacha/sprue/pkg/store"
	blobregistry "github.com/storacha/sprue/pkg/store/blob_registry"
)

var DynamoBlobRegistryTableProps = struct {
	KeySchema  []types.KeySchemaElement
	Attributes []types.AttributeDefinition
	GSI        []types.GlobalSecondaryIndex
}{
	KeySchema: []types.KeySchemaElement{
		{
			AttributeName: aws.String("space"),
			KeyType:       types.KeyTypeHash,
		},
		{
			AttributeName: aws.String("digest"),
			KeyType:       types.KeyTypeRange,
		},
	},
	Attributes: []types.AttributeDefinition{
		{
			AttributeName: aws.String("space"),
			AttributeType: types.ScalarAttributeTypeS,
		},
		{
			AttributeName: aws.String("digest"),
			AttributeType: types.ScalarAttributeTypeS,
		},
	},
	GSI: []types.GlobalSecondaryIndex{
		{
			IndexName: aws.String("digest"),
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String("digest"),
					KeyType:       types.KeyTypeHash,
				},
				{
					AttributeName: aws.String("space"),
					KeyType:       types.KeyTypeRange,
				},
			},
			Projection: &types.Projection{
				ProjectionType: types.ProjectionTypeAll,
			},
		},
	},
}

type Store struct {
	Dynamo    *dynamodb.Client
	TableName string
}

var _ blobregistry.Store = (*Store)(nil)

func New(dynamo *dynamodb.Client, tableName string) *Store {
	return &Store{
		Dynamo:    dynamo,
		TableName: tableName,
	}
}

// Initialize creates the DynamoDB table if it does not already exist.
func (s *Store) Initialize(ctx context.Context) error {
	if _, err := s.Dynamo.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(s.TableName),
	}); err == nil {
		return nil
	}
	if _, err := s.Dynamo.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:              aws.String(s.TableName),
		KeySchema:              DynamoBlobRegistryTableProps.KeySchema,
		AttributeDefinitions:   DynamoBlobRegistryTableProps.Attributes,
		GlobalSecondaryIndexes: DynamoBlobRegistryTableProps.GSI,
		BillingMode:            types.BillingModePayPerRequest,
	}); err != nil {
		return fmt.Errorf("creating DynamoDB table %q: %w", s.TableName, err)
	}
	return nil
}

func (s *Store) Get(ctx context.Context, space did.DID, digest multihash.Multihash) (blobregistry.Record, error) {
	out, err := s.Dynamo.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.TableName),
		Key: map[string]types.AttributeValue{
			"space":  &types.AttributeValueMemberS{Value: space.String()},
			"digest": &types.AttributeValueMemberS{Value: digestutil.Format(digest)},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return blobregistry.Record{}, fmt.Errorf("getting blob registry entry: %w", err)
	}
	if len(out.Item) == 0 {
		return blobregistry.Record{}, blobregistry.ErrEntryNotFound
	}
	return ItemToRecord(out.Item)
}

func (s *Store) Add(ctx context.Context, space did.DID, blob captypes.Blob, cause cid.Cid) error {
	now := time.Now().UTC().Format(timeutil.SimplifiedISO8601)
	_, err := s.Dynamo.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.TableName),
		Item: map[string]types.AttributeValue{
			"space":      &types.AttributeValueMemberS{Value: space.String()},
			"digest":     &types.AttributeValueMemberS{Value: digestutil.Format(blob.Digest)},
			"size":       &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", blob.Size)},
			"cause":      &types.AttributeValueMemberS{Value: cause.String()},
			"insertedAt": &types.AttributeValueMemberS{Value: now},
		},
		ConditionExpression: aws.String("attribute_not_exists(#space)"),
		ExpressionAttributeNames: map[string]string{
			"#space": "space",
		},
	})
	if err != nil {
		var condErr *types.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			return blobregistry.ErrEntryExists
		}
		return fmt.Errorf("registering blob: %w", err)
	}
	return nil
}

// TransactAdd returns a TransactWriteItem for adding a blob.
// This is used by the AWS service implementation to build atomic transactions.
func (s *Store) TransactAdd(space did.DID, blob captypes.Blob, cause cid.Cid) types.TransactWriteItem {
	now := time.Now().UTC().Format(timeutil.SimplifiedISO8601)
	return types.TransactWriteItem{
		Put: &types.Put{
			TableName: aws.String(s.TableName),
			Item: map[string]types.AttributeValue{
				"space":      &types.AttributeValueMemberS{Value: space.String()},
				"digest":     &types.AttributeValueMemberS{Value: digestutil.Format(blob.Digest)},
				"size":       &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", blob.Size)},
				"cause":      &types.AttributeValueMemberS{Value: cause.String()},
				"insertedAt": &types.AttributeValueMemberS{Value: now},
			},
			ConditionExpression: aws.String("attribute_not_exists(#space)"),
			ExpressionAttributeNames: map[string]string{
				"#space": "space",
			},
		},
	}
}

func (s *Store) Remove(ctx context.Context, space did.DID, digest multihash.Multihash) error {
	_, err := s.Dynamo.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.TableName),
		Key: map[string]types.AttributeValue{
			"space":  &types.AttributeValueMemberS{Value: space.String()},
			"digest": &types.AttributeValueMemberS{Value: digestutil.Format(digest)},
		},
		ConditionExpression: aws.String("attribute_exists(#space)"),
		ExpressionAttributeNames: map[string]string{
			"#space": "space",
		},
	})
	if err != nil {
		var condErr *types.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			return blobregistry.ErrEntryNotFound
		}
		return fmt.Errorf("deregistering blob: %w", err)
	}
	return nil
}

// TransactRemove returns a TransactWriteItem for removing a blob.
// This is used by the AWS service implementation to build atomic transactions.
func (s *Store) TransactRemove(space did.DID, digest multihash.Multihash) types.TransactWriteItem {
	return types.TransactWriteItem{
		Delete: &types.Delete{
			TableName: aws.String(s.TableName),
			Key: map[string]types.AttributeValue{
				"space":  &types.AttributeValueMemberS{Value: space.String()},
				"digest": &types.AttributeValueMemberS{Value: digestutil.Format(digest)},
			},
			ConditionExpression: aws.String("attribute_exists(#space)"),
			ExpressionAttributeNames: map[string]string{
				"#space": "space",
			},
		},
	}
}

func (s *Store) List(ctx context.Context, space did.DID, options ...blobregistry.ListOption) (store.Page[blobregistry.Record], error) {
	cfg := blobregistry.ListConfig{}
	for _, opt := range options {
		opt(&cfg)
	}

	input := &dynamodb.QueryInput{
		TableName:              aws.String(s.TableName),
		KeyConditionExpression: aws.String("#space = :space"),
		ExpressionAttributeNames: map[string]string{
			"#space": "space",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":space": &types.AttributeValueMemberS{Value: space.String()},
		},
		ScanIndexForward: aws.Bool(true),
	}

	if cfg.Limit != nil {
		input.Limit = aws.Int32(int32(*cfg.Limit))
	}

	if cfg.Cursor != nil {
		input.ExclusiveStartKey = map[string]types.AttributeValue{
			"space":  &types.AttributeValueMemberS{Value: space.String()},
			"digest": &types.AttributeValueMemberS{Value: *cfg.Cursor},
		}
	}

	out, err := s.Dynamo.Query(ctx, input)
	if err != nil {
		return store.Page[blobregistry.Record]{}, fmt.Errorf("listing blob registry entries: %w", err)
	}

	records := make([]blobregistry.Record, 0, len(out.Items))
	for _, item := range out.Items {
		rec, err := ItemToRecord(item)
		if err != nil {
			return store.Page[blobregistry.Record]{}, err
		}
		records = append(records, rec)
	}

	var cursor *string
	if out.LastEvaluatedKey != nil {
		if digestAttr, ok := out.LastEvaluatedKey["digest"].(*types.AttributeValueMemberS); ok {
			cursor = &digestAttr.Value
		}
	}

	return store.Page[blobregistry.Record]{Results: records, Cursor: cursor}, nil
}

func ItemToRecord(item map[string]types.AttributeValue) (blobregistry.Record, error) {
	spaceAttr, ok := item["space"].(*types.AttributeValueMemberS)
	if !ok {
		return blobregistry.Record{}, fmt.Errorf("missing or invalid space attribute")
	}
	space, err := did.Parse(spaceAttr.Value)
	if err != nil {
		return blobregistry.Record{}, fmt.Errorf("parsing space DID: %w", err)
	}

	digestAttr, ok := item["digest"].(*types.AttributeValueMemberS)
	if !ok {
		return blobregistry.Record{}, fmt.Errorf("missing or invalid digest attribute")
	}
	digest, err := digestutil.Parse(digestAttr.Value)
	if err != nil {
		return blobregistry.Record{}, fmt.Errorf("decoding digest: %w", err)
	}

	sizeAttr, ok := item["size"].(*types.AttributeValueMemberN)
	if !ok {
		return blobregistry.Record{}, fmt.Errorf("missing or invalid size attribute")
	}
	var size uint64
	if _, err := fmt.Sscanf(sizeAttr.Value, "%d", &size); err != nil {
		return blobregistry.Record{}, fmt.Errorf("parsing size: %w", err)
	}

	causeAttr, ok := item["cause"].(*types.AttributeValueMemberS)
	if !ok {
		return blobregistry.Record{}, fmt.Errorf("missing or invalid cause attribute")
	}
	cause, err := cid.Parse(causeAttr.Value)
	if err != nil {
		return blobregistry.Record{}, fmt.Errorf("parsing cause CID: %w", err)
	}

	var insertedAt time.Time
	if v, ok := item["insertedAt"].(*types.AttributeValueMemberS); ok {
		insertedAt, _ = time.Parse(timeutil.SimplifiedISO8601, v.Value)
	}

	return blobregistry.Record{
		Space: space,
		Blob: captypes.Blob{
			Digest: digest,
			Size:   size,
		},
		Cause:      cause,
		InsertedAt: insertedAt,
	}, nil
}
