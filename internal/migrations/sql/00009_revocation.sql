-- +goose Up
-- +goose StatementBegin
CREATE TABLE revocation (
    revoke      TEXT        NOT NULL,
    scope       TEXT        NOT NULL,
    cause       TEXT        NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (revoke, scope)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS revocation;
-- +goose StatementEnd
