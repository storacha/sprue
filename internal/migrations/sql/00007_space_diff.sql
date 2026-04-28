-- +goose Up
-- +goose StatementBegin
CREATE TABLE space_diff (
    provider     TEXT        NOT NULL,
    space        TEXT        NOT NULL,
    receipt_at   TIMESTAMPTZ NOT NULL,
    cause        TEXT        NOT NULL,
    subscription TEXT        NOT NULL,
    delta        BIGINT      NOT NULL,
    inserted_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (provider, space, receipt_at, cause)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS space_diff;
-- +goose StatementEnd
