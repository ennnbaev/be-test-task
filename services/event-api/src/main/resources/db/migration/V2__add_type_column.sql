ALTER TABLE events ADD COLUMN IF NOT EXISTS type VARCHAR(100);

CREATE INDEX IF NOT EXISTS idx_events_type ON events (type);
CREATE INDEX IF NOT EXISTS idx_events_type_created_at ON events (type, created_at);
