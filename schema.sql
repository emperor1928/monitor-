-- PostgreSQL schema for FreeSWITCH call monitoring





-- Live call events history table (CDR-style)
-- Stores ONE record per call/leg (on hangup), not every event
CREATE TABLE IF NOT EXISTS live_call_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    uuid VARCHAR(100) NOT NULL UNIQUE,
    b_uuid VARCHAR(100),
    event_type VARCHAR(50) NOT NULL,  -- Final call status: answered, no_answer, busy, failed, declined
    event_ts BIGINT NOT NULL,  -- Hangup timestamp
    caller VARCHAR(100),
    callee VARCHAR(100),
    customer_id VARCHAR(50),
    dest_type VARCHAR(50),
    dest_value VARCHAR(100),
    status_code VARCHAR(50),  -- Hangup cause code
    call_type VARCHAR(50),  -- INBOUND | OUTBOUND | DID_FORWARD
    outbound_caller_id VARCHAR(100),  -- For outbound + DID forward B-leg
    originating_extension VARCHAR(100),  -- Extension that originated outbound call
    originating_leg_uuid VARCHAR(100),  -- Reference to A-leg for DID forwards (B-leg only)
    ingress_trunk VARCHAR(100),  -- Source trunk/gateway
    egress_trunk VARCHAR(100),  -- Destination trunk/gateway
    duration INTEGER DEFAULT 0,  -- Total call duration in seconds (from create to hangup)
    billsec INTEGER DEFAULT 0,  -- Billable duration in seconds (from answer to hangup)
    recording BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Keep only the UNIQUE constraint index on uuid for hot write performance.
-- Additional secondary indexes can be added later based on confirmed query needs.

-- Sample data for testing
-- INSERT INTO "TollFreeNumbers" (number, config) VALUES 
--     ('100601', '{"subscriptionId": "550e8400-e29b-41d4-a716-446655440000"}'),
--     ('+15551234567', '{"subscriptionId": "550e8400-e29b-41d4-a716-446655440001"}');
-- 
-- INSERT INTO subscriptions ("subscriptionId", "customerId") VALUES 
--     ('550e8400-e29b-41d4-a716-446655440000', '123e4567-e89b-12d3-a456-426614174000'),
--     ('550e8400-e29b-41d4-a716-446655440001', '123e4567-e89b-12d3-a456-426614174001');
