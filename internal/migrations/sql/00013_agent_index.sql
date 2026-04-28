-- +goose Up
-- +goose StatementBegin
-- agent_index stores the "task + kind -> (root CID @ message CID)" mapping.
-- "kind" is either "in" (an invocation) or "out" (a receipt).
CREATE TABLE agent_index (
    task       TEXT NOT NULL,
    kind       TEXT NOT NULL,
    identifier TEXT NOT NULL,
    PRIMARY KEY (task, kind)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS agent_index;
-- +goose StatementEnd
