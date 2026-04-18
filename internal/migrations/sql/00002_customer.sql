-- +goose Up
-- +goose StatementBegin
CREATE TABLE customer (
    customer          TEXT        PRIMARY KEY,
    account           TEXT,
    product           TEXT        NOT NULL,
    details           JSONB,
    reserved_capacity BIGINT,
    inserted_at       TIMESTAMPTZ NOT NULL,
    updated_at        TIMESTAMPTZ
);

CREATE INDEX customer_account_idx ON customer (account) WHERE account IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS customer;
-- +goose StatementEnd
