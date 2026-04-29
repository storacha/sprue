// Package blockstore provides a SQLite-backed implementation of the
// go-ipld-cbor IpldBlockstore interface, used to persist MST nodes and
// object manifests as content-addressed blocks.
package blockstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
)

// Schema is the DDL for the blocks table. Vanilla SQL so it works on SQLite,
// Postgres, etc.
const Schema = `
CREATE TABLE IF NOT EXISTS blocks (
    cid  BLOB PRIMARY KEY,
    data BLOB NOT NULL
);
`

// Store is a SQLite-backed IPLD blockstore.
type Store struct {
	db *sql.DB
}

// New wraps an open *sql.DB and ensures the schema exists.
func New(db *sql.DB) (*Store, error) {
	if _, err := db.Exec(Schema); err != nil {
		return nil, fmt.Errorf("blockstore: ensure schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Get fetches a block by CID. Returns ErrNotFound if absent.
func (s *Store) Get(ctx context.Context, c cid.Cid) (block.Block, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT data FROM blocks WHERE cid = ?`,
		c.Bytes()).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("blockstore: get %s: %w", c, err)
	}
	return block.NewBlockWithCid(data, c)
}

// Put writes a block. Idempotent: re-inserting the same CID is a no-op.
func (s *Store) Put(ctx context.Context, b block.Block) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO blocks (cid, data) VALUES (?, ?) ON CONFLICT (cid) DO NOTHING`,
		b.Cid().Bytes(), b.RawData())
	if err != nil {
		return fmt.Errorf("blockstore: put %s: %w", b.Cid(), err)
	}
	return nil
}

// ErrNotFound is returned by Get when the requested CID is absent.
var ErrNotFound = errors.New("blockstore: block not found")

// Compile-time assertion: *Store implements cbor.IpldBlockstore.
var _ cbor.IpldBlockstore = (*Store)(nil)
