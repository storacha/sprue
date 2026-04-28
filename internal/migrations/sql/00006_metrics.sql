-- +goose Up
-- +goose StatementBegin
CREATE TABLE admin_metrics (
    name  TEXT   PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE space_metrics (
    space TEXT   NOT NULL,
    name  TEXT   NOT NULL,
    value BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (space, name)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS space_metrics;
DROP TABLE IF EXISTS admin_metrics;
-- +goose StatementEnd
