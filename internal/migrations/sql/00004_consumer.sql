-- +goose Up
-- +goose StatementBegin
CREATE TABLE consumer (
    subscription TEXT        NOT NULL,
    provider     TEXT        NOT NULL,
    consumer     TEXT        NOT NULL,
    customer     TEXT        NOT NULL,
    cause        TEXT        NOT NULL,
    inserted_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (subscription, provider)
);

CREATE INDEX consumer_space_idx ON consumer (consumer, provider, subscription);
CREATE INDEX consumer_customer_idx ON consumer (customer, subscription, provider);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS consumer;
-- +goose StatementEnd
