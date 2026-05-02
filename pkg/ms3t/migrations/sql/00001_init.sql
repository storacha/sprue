-- +goose Up
-- ms3t bucket registry. Mirrors the columns of the previous SQLite
-- schema (pkg/ms3t/registry/sqlite.go's `buckets` table) but in the
-- `ms3t` schema so the same Postgres database can host both sprue's
-- and ms3t's tables without collision.
--
-- name           — S3 bucket name (PK)
-- root_cid       — current MST root CID, bytes form; NULL for empty bucket
-- forge_root_cid — last MST root whose DAG has been shipped to Forge
-- created_at     — unix seconds at create time

CREATE TABLE ms3t.buckets (
    name           TEXT   PRIMARY KEY,
    root_cid       BYTEA,
    forge_root_cid BYTEA,
    created_at     BIGINT NOT NULL
);

-- +goose Down
DROP TABLE ms3t.buckets;
