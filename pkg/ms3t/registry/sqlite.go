package registry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"
)

// Schema is the DDL for the buckets table. Vanilla SQL.
const Schema = `
CREATE TABLE IF NOT EXISTS buckets (
    name           TEXT PRIMARY KEY,
    root_cid       BLOB,
    forge_root_cid BLOB,
    created_at     INTEGER NOT NULL
);
`

// addForgeRootColumn brings older schemas (without forge_root_cid)
// forward in place. Idempotent: if the column exists, the ALTER
// errors and we treat that as already-migrated.
const addForgeRootColumn = `ALTER TABLE buckets ADD COLUMN forge_root_cid BLOB`

// SQL is a database/sql-backed Registry. Works with any SQL driver that
// supports the byte-blob and integer types used here.
type SQL struct {
	db *sql.DB
}

// NewSQL wraps an open *sql.DB and ensures the schema exists.
func NewSQL(db *sql.DB) (*SQL, error) {
	if _, err := db.Exec(Schema); err != nil {
		return nil, fmt.Errorf("registry: ensure schema: %w", err)
	}
	// Best-effort migration for older databases. The error case is the
	// column already existing (driver-specific message), which is fine.
	if _, err := db.Exec(addForgeRootColumn); err != nil {
		if !strings.Contains(err.Error(), "duplicate column") {
			return nil, fmt.Errorf("registry: add forge_root_cid: %w", err)
		}
	}
	return &SQL{db: db}, nil
}

func (r *SQL) Create(ctx context.Context, name string, createdAt int64) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO buckets (name, root_cid, created_at) VALUES (?, NULL, ?)`,
		name, createdAt)
	if err != nil {
		// Cheap, portable detection: a second Create with the same name will
		// trip the PK. Different drivers wrap this error differently, so
		// fall back to Get to distinguish.
		if existing, gerr := r.Get(ctx, name); gerr == nil && existing != nil {
			return ErrExists
		}
		return fmt.Errorf("registry: create %q: %w", name, err)
	}
	return nil
}

func (r *SQL) Get(ctx context.Context, name string) (*State, error) {
	var rootBytes, forgeBytes []byte
	var createdAt int64
	err := r.db.QueryRowContext(ctx,
		`SELECT root_cid, forge_root_cid, created_at FROM buckets WHERE name = ?`, name).
		Scan(&rootBytes, &forgeBytes, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("registry: get %q: %w", name, err)
	}

	st := &State{Name: name, CreatedAt: createdAt}
	if err := setCid(&st.Root, rootBytes, name, "root_cid"); err != nil {
		return nil, err
	}
	if err := setCid(&st.ForgeRoot, forgeBytes, name, "forge_root_cid"); err != nil {
		return nil, err
	}
	return st, nil
}

func (r *SQL) List(ctx context.Context) ([]*State, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT name, root_cid, forge_root_cid, created_at FROM buckets ORDER BY name ASC`)
	if err != nil {
		return nil, fmt.Errorf("registry: list: %w", err)
	}
	defer rows.Close()

	var out []*State
	for rows.Next() {
		var name string
		var rootBytes, forgeBytes []byte
		var createdAt int64
		if err := rows.Scan(&name, &rootBytes, &forgeBytes, &createdAt); err != nil {
			return nil, fmt.Errorf("registry: list scan: %w", err)
		}
		st := &State{Name: name, CreatedAt: createdAt}
		if err := setCid(&st.Root, rootBytes, name, "root_cid"); err != nil {
			return nil, err
		}
		if err := setCid(&st.ForgeRoot, forgeBytes, name, "forge_root_cid"); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: list rows: %w", err)
	}
	return out, nil
}

func setCid(dst *cid.Cid, raw []byte, name, field string) error {
	if len(raw) == 0 {
		*dst = cid.Undef
		return nil
	}
	c, err := cid.Cast(raw)
	if err != nil {
		return fmt.Errorf("registry: bad %s for %q: %w", field, name, err)
	}
	*dst = c
	return nil
}

func (r *SQL) Delete(ctx context.Context, name string) error {
	res, err := r.db.ExecContext(ctx,
		`DELETE FROM buckets WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("registry: delete %q: %w", name, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("registry: delete rows: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *SQL) CASRoot(ctx context.Context, name string, expect, next cid.Cid) error {
	var (
		expectBytes []byte
		nextBytes   []byte
	)
	if expect.Defined() {
		expectBytes = expect.Bytes()
	}
	if next.Defined() {
		nextBytes = next.Bytes()
	}

	var (
		res sql.Result
		err error
	)
	if expectBytes == nil {
		res, err = r.db.ExecContext(ctx,
			`UPDATE buckets SET root_cid = ? WHERE name = ? AND root_cid IS NULL`,
			nextBytes, name)
	} else {
		res, err = r.db.ExecContext(ctx,
			`UPDATE buckets SET root_cid = ? WHERE name = ? AND root_cid = ?`,
			nextBytes, name, expectBytes)
	}
	if err != nil {
		return fmt.Errorf("registry: cas %q: %w", name, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("registry: cas rows: %w", err)
	}
	if n == 0 {
		// Either the bucket doesn't exist or the expected root didn't match.
		if _, gerr := r.Get(ctx, name); errors.Is(gerr, ErrNotFound) {
			return ErrNotFound
		}
		return ErrConflict
	}
	return nil
}

func (r *SQL) SetForgeRoot(ctx context.Context, name string, root cid.Cid) error {
	var rootBytes []byte
	if root.Defined() {
		rootBytes = root.Bytes()
	}
	res, err := r.db.ExecContext(ctx,
		`UPDATE buckets SET forge_root_cid = ? WHERE name = ?`,
		rootBytes, name)
	if err != nil {
		return fmt.Errorf("registry: set forge root %q: %w", name, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("registry: set forge root rows: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// Compile-time assertion.
var _ Registry = (*SQL)(nil)
