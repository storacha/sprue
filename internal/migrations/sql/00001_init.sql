-- +goose Up
-- +goose StatementBegin
-- Sprue Postgres schema — tables are added by per-store migrations.
-- This file exists so the embedded FS is non-empty and goose has a baseline.
SELECT 1;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 1;
-- +goose StatementEnd
