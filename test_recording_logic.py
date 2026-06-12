#!/usr/bin/env python3
"""Test script for verifying recording handlers and CDR generation logic without dependencies."""
import sys
import logging
from unittest.mock import MagicMock, patch

# Mock system-level gevent, greenswitch, redis, and psycopg2 modules
sys.modules['gevent'] = MagicMock()
sys.modules['gevent.monkey'] = MagicMock()
sys.modules['gevent.pool'] = MagicMock()
sys.modules['greenswitch'] = MagicMock()
sys.modules['redis'] = MagicMock()
sys.modules['psycopg2'] = MagicMock()
sys.modules['psycopg2.extras'] = MagicMock()
sys.modules['psycopg2.pool'] = MagicMock()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_recording")

def run_test():
    logger.info("Initializing tests...")

    # Mock DB connection methods before importing handlers
    with patch("connections.get_redis") as mock_get_redis, \
         patch("connections.get_lookup_redis") as mock_get_lookup, \
         patch("connections.get_pg_connection") as mock_get_pg:

        # Mock Redis client
        mock_redis = MagicMock()
        mock_get_redis.return_value = mock_redis
        mock_get_lookup.return_value = MagicMock()
        
        # Now import handlers and verify
        import handlers
        
        # Setup mock active call in Redis
        call_uuid = "test-uuid-12345"
        mock_redis_store = {
            "uuid": call_uuid,
            "caller": "1001",
            "callee": "1002",
            "customer_id": "test_cust_999",
            "call_status": "ringing",
            "start_ts": "1718200000"
        }
        
        def hgetall_mock(key):
            if key == f"call:{call_uuid}":
                return mock_redis_store
            return {}
            
        def hset_mock(key, mapping=None, **kwargs):
            if key == f"call:{call_uuid}" and mapping:
                mock_redis_store.update(mapping)
            return 1

        mock_redis.hgetall.side_effect = hgetall_mock
        mock_redis.hset.side_effect = hset_mock
        mock_redis.pipeline.return_value = mock_redis
        mock_redis.execute.return_value = [True]

        # 1. Simulate RECORD_START event
        record_start_event = {
            "Event-Name": "RECORD_START",
            "Unique-ID": call_uuid,
            "Record-File-Path": "/var/lib/freeswitch/recordings/test.mp3"
        }
        
        logger.info("Triggering handle_record_start...")
        handlers.handle_record_start(record_start_event)
        
        # Verify recording flag is set to "true" in Redis store
        assert mock_redis_store.get("recording") == "true", "Failed: Recording flag should be 'true' in Redis after RECORD_START."
        logger.info("Success: handle_record_start updated Redis successfully.")

        # 2. Simulate CHANNEL_HANGUP_COMPLETE event
        hangup_event = {
            "Event-Name": "CHANNEL_HANGUP_COMPLETE",
            "Unique-ID": call_uuid,
            "Hangup-Cause": "NORMAL_CLEARING",
            "variable_duration": "42",
            "variable_billsec": "38",
            "Caller-Channel-Hangup-Time": "1718200042000000"
        }
        
        # Intercept call to save_cdr_to_postgres
        saved_cdr = {}
        def mock_save_cdr(cdr):
            nonlocal saved_cdr
            saved_cdr = cdr
            return True
            
        handlers.save_cdr_to_postgres = mock_save_cdr
        
        logger.info("Triggering handle_hangup_complete...")
        handlers.handle_hangup_complete(hangup_event)
        
        # Verify CDR payload
        assert saved_cdr.get("uuid") == call_uuid, "Failed: UUID mismatch in saved CDR."
        assert saved_cdr.get("recording") is True, "Failed: Recording flag should be True (boolean) in final CDR."
        assert saved_cdr.get("duration") == 42, "Failed: Call duration mismatch."
        assert saved_cdr.get("billsec") == 38, "Failed: Billsec mismatch."
        
        logger.info("Success: handle_hangup_complete finalized CDR with recording=True.")
        
        # 3. Simulate call without RECORD_START (unanswered call)
        call_uuid_no_rec = "test-uuid-no-rec"
        mock_redis_store_no_rec = {
            "uuid": call_uuid_no_rec,
            "caller": "1001",
            "callee": "1002",
            "customer_id": "test_cust_999",
            "call_status": "ringing",
            "start_ts": "1718200000"
        }
        
        def hgetall_mock_v2(key):
            if key == f"call:{call_uuid_no_rec}":
                return mock_redis_store_no_rec
            return {}
            
        mock_redis.hgetall.side_effect = hgetall_mock_v2
        
        hangup_event_no_rec = {
            "Event-Name": "CHANNEL_HANGUP_COMPLETE",
            "Unique-ID": call_uuid_no_rec,
            "Hangup-Cause": "NO_ANSWER",
            "variable_duration": "20",
            "variable_billsec": "0",
            "Caller-Channel-Hangup-Time": "1718200020000000"
        }
        
        logger.info("Triggering handle_hangup_complete for unanswered call...")
        handlers.handle_hangup_complete(hangup_event_no_rec)
        
        assert saved_cdr.get("uuid") == call_uuid_no_rec
        assert saved_cdr.get("recording") is False, "Failed: Recording flag should be False for unanswered calls."
        
        logger.info("Success: handle_hangup_complete finalized unanswered CDR with recording=False.")
        logger.info("All tests passed successfully!")

if __name__ == "__main__":
    run_test()
