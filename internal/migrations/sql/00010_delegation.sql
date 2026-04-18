-- +goose Up
-- +goose StatementBegin
CREATE TABLE delegation (
    link        TEXT        PRIMARY KEY,
    audience    TEXT        NOT NULL,
    issuer      TEXT        NOT NULL,
    cause       TEXT,
    expiration  BIGINT,
    inserted_at TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX delegation_audience_idx ON delegation (audience, link);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS delegation;
-- +goose StatementEnd
