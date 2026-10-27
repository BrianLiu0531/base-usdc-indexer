-- Block headers the indexer has accepted as canonical. Every unfinalized block is kept so a reorg
-- can be traced back to the common ancestor; below the finalized height only sparse checkpoints remain.
CREATE TABLE blocks (
    chain_id    BIGINT  NOT NULL,
    number      BIGINT  NOT NULL,
    hash        TEXT    NOT NULL,
    parent_hash TEXT    NOT NULL,
    time        BIGINT  NOT NULL,
    PRIMARY KEY (chain_id, number)
);

-- status: 0 = pending (latest), 1 = safe, 2 = finalized
CREATE TABLE transfers (
    id           BIGSERIAL    PRIMARY KEY,
    chain_id     BIGINT       NOT NULL,
    block_number BIGINT       NOT NULL,
    block_hash   TEXT         NOT NULL,
    block_time   BIGINT       NOT NULL,
    tx_hash      TEXT         NOT NULL,
    log_index    INTEGER      NOT NULL,
    from_address TEXT         NOT NULL,
    to_address   TEXT         NOT NULL,
    value        NUMERIC(78)  NOT NULL CHECK (value >= 0),
    status       SMALLINT     NOT NULL CHECK (status BETWEEN 0 AND 2),
    confirmed_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- The database, not application code, guarantees a log is recorded once
    UNIQUE (chain_id, tx_hash, log_index)
);
CREATE INDEX transfers_block_idx ON transfers (chain_id, block_number);
CREATE INDEX transfers_from_idx ON transfers (chain_id, from_address, block_number DESC);
CREATE INDEX transfers_to_idx ON transfers (chain_id, to_address, block_number DESC);
CREATE INDEX transfers_unconfirmed_idx ON transfers (chain_id, status) WHERE confirmed_at IS NULL;

-- Audit trail of transfers removed by reorgs
CREATE TABLE orphaned_transfers (
    id           BIGSERIAL    PRIMARY KEY,
    chain_id     BIGINT       NOT NULL,
    block_number BIGINT       NOT NULL,
    block_hash   TEXT         NOT NULL,
    tx_hash      TEXT         NOT NULL,
    log_index    INTEGER      NOT NULL,
    from_address TEXT         NOT NULL,
    to_address   TEXT         NOT NULL,
    value        NUMERIC(78)  NOT NULL,
    was_confirmed BOOLEAN     NOT NULL,
    orphaned_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE sync_state (
    chain_id        BIGINT PRIMARY KEY,
    safe_block      BIGINT NOT NULL DEFAULT 0,
    finalized_block BIGINT NOT NULL DEFAULT 0,
    latest_seen     BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox: events are written in the same transaction as the transfers they describe,
-- then relayed to the message bus. event_id is deterministic, so consumers can deduplicate.
CREATE TABLE outbox (
    id           BIGSERIAL   PRIMARY KEY,
    event_id     TEXT        NOT NULL UNIQUE,
    type         TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- balanceOf at the first reconciliation fixes the opening balance (on-chain minus indexed net flow).
CREATE TABLE reconcile_baselines (
    chain_id BIGINT      NOT NULL,
    address  TEXT        NOT NULL,
    block    BIGINT      NOT NULL,
    offset_value NUMERIC(78) NOT NULL,
    PRIMARY KEY (chain_id, address)
);

CREATE TABLE reconciliations (
    id         BIGSERIAL   PRIMARY KEY,
    chain_id   BIGINT      NOT NULL,
    address    TEXT        NOT NULL,
    block      BIGINT      NOT NULL,
    onchain    NUMERIC(78) NOT NULL,
    expected   NUMERIC(78) NOT NULL,
    ok         BOOLEAN     NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX reconciliations_address_idx ON reconciliations (chain_id, address, id DESC);
