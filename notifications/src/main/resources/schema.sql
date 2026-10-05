CREATE TABLE IF NOT EXISTS outbox (
 sequence BIGSERIAL UNIQUE, event_id UUID PRIMARY KEY, topic TEXT NOT NULL,
 event_key TEXT NOT NULL, payload TEXT NOT NULL, published_at TIMESTAMPTZ);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(sequence) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS processed_events (consumer TEXT NOT NULL,event_id UUID NOT NULL,processed_at TIMESTAMPTZ DEFAULT now(),PRIMARY KEY(consumer,event_id));
CREATE TABLE IF NOT EXISTS order_projection (order_id UUID PRIMARY KEY,sku TEXT NOT NULL,quantity INTEGER NOT NULL,status TEXT NOT NULL,created_at TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS minute_metrics (minute TIMESTAMPTZ PRIMARY KEY,orders BIGINT NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS stock_projection (sku TEXT PRIMARY KEY,available INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS notifications (order_id UUID PRIMARY KEY,status TEXT NOT NULL,created_at TIMESTAMPTZ DEFAULT now());
