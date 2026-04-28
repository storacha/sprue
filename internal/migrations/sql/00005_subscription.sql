-- +goose Up
-- +goose StatementBegin
CREATE TABLE subscription (
    subscription TEXT        NOT NULL,
    provider     TEXT        NOT NULL,
    customer     TEXT        NOT NULL,
    cause        TEXT        NOT NULL,
    inserted_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (subscription, provider)
);

CREATE INDEX subscription_customer_provider_idx
    ON subscription (customer, provider, subscription);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS subscription;
-- +goose StatementEnd
