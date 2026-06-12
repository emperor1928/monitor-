-- Adds recording column to live_call_events and cdr tables.

ALTER TABLE live_call_events ADD COLUMN IF NOT EXISTS recording BOOLEAN DEFAULT FALSE;
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS recording BOOLEAN DEFAULT FALSE;
