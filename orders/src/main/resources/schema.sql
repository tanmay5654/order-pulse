CREATE TABLE IF NOT EXISTS outbox (
 sequence BIGSERIAL UNIQUE, event_id UUID PRIMARY KEY, topic TEXT NOT NULL,
 event_key TEXT NOT NULL, payload TEXT NOT NULL, published_at TIMESTAMPTZ);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(sequence) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS processed_events (consumer TEXT NOT NULL,event_id UUID NOT NULL,processed_at TIMESTAMPTZ DEFAULT now(),PRIMARY KEY(consumer,event_id));
CREATE TABLE IF NOT EXISTS orders (id UUID PRIMARY KEY,idempotency_key TEXT UNIQUE NOT NULL,sku TEXT NOT NULL,quantity INTEGER NOT NULL CHECK(quantity>0),created_at TIMESTAMPTZ NOT NULL DEFAULT now());
