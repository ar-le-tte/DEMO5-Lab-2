CREATE DATABASE ecommerce_streaming;
CREATE SCHEMA IF NOT EXISTS rt;

CREATE TABLE IF NOT EXISTS rt.ecommerce_events (
  event_id      TEXT PRIMARY KEY,
  event_time    TIMESTAMPTZ NOT NULL,
  user_id       INT NOT NULL,
  session_id    TEXT NOT NULL,
  event_type    TEXT NOT NULL CHECK (event_type IN ('view', 'purchase')),
  product_id    INT NOT NULL,
  product_name  TEXT NOT NULL,
  category      TEXT NOT NULL,
  price         NUMERIC(10,2) NOT NULL,
  quantity      INT NOT NULL,
  total_amount  NUMERIC(12,2) NOT NULL,
  ingest_time   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_events_event_time ON rt.ecommerce_events(event_time);
CREATE INDEX IF NOT EXISTS idx_events_user_id    ON rt.ecommerce_events(user_id);

