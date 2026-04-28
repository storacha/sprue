-- +goose Up
-- +goose StatementBegin
CREATE TABLE blob_registry (
    space       TEXT        NOT NULL,
    digest      TEXT        NOT NULL,
    size        BIGINT      NOT NULL,
    cause       TEXT        NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (space, digest)
);

CREATE INDEX blob_registry_digest_idx ON blob_registry (digest, space);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS blob_registry;
-- +goose StatementEnd
