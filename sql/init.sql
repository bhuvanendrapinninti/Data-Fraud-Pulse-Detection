CREATE TABLE IF NOT EXISTS transactions (
 transaction_id TEXT PRIMARY KEY,
 customer_id TEXT NOT NULL,
 amount NUMERIC(14,2) NOT NULL CHECK (amount >= 0),
 currency CHAR(3) NOT NULL DEFAULT 'INR',
 merchant_id TEXT NOT NULL,
 event_type TEXT NOT NULL DEFAULT 'payment',
 event_time TIMESTAMPTZ NOT NULL,
 received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 is_suspicious BOOLEAN NOT NULL DEFAULT FALSE,
 risk_reasons TEXT[] NOT NULL DEFAULT '{}',
 source_topic TEXT,
 source_partition INTEGER,
 source_offset BIGINT
);
CREATE INDEX IF NOT EXISTS idx_txn_event_time ON transactions(event_time DESC);
CREATE INDEX IF NOT EXISTS idx_txn_customer_time ON transactions(customer_id,event_time DESC);
CREATE TABLE IF NOT EXISTS fraud_alerts (
 alert_id BIGSERIAL PRIMARY KEY,
 transaction_id TEXT NOT NULL UNIQUE REFERENCES transactions(transaction_id) ON DELETE CASCADE,
 customer_id TEXT NOT NULL,
 amount NUMERIC(14,2) NOT NULL,
 event_time TIMESTAMPTZ NOT NULL,
 reasons TEXT[] NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','REVIEWING','CLOSED'))
);
