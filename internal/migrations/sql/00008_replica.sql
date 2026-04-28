-- +goose Up
-- +goose StatementBegin
CREATE TABLE replica (
    space       TEXT        NOT NULL,
    digest      TEXT        NOT NULL,
    provider    TEXT        NOT NULL,
    status      TEXT        NOT NULL,
    cause       TEXT        NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (space, digest, provider)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS replica;
-- +goose StatementEnd
