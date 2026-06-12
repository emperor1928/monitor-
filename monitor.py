#!/usr/bin/env python3
"""
FreeSWITCH ESL Real-time Call Monitoring System
================================================
Production-optimized for 24/7 continuous operation with 100+ concurrent calls.

Features:
- Modular architecture with separate config, connections, handlers
- Gevent worker pool for parallel event processing
- Auto-reconnect for ESL, Redis, PostgreSQL
- Connection health monitoring with metrics
- Log rotation (prevents disk fill)
- Memory-efficient LRU caching
- Graceful shutdown handling
- Heartbeat logging with statistics
"""

import logging
import signal
import sys
import gc
import time
from logging.handlers import RotatingFileHandler

# Gevent MUST be patched before any other imports
from gevent import monkey
monkey.patch_all()

import gevent
from gevent.pool import Pool
import greenswitch

# Local imports - work both as package and direct run
try:
    from . import config
    from .connections import init_connections, close_connections, health_check, redis_manager, lookup_redis_manager, postgres_manager
    from .handlers import (
        process_event,
        stats,
        customer_cache,
        start_cdr_workers,
        get_cdr_queue_size,
        clear_customer_lookup_cache,
        get_call_from_redis,
        recover_orphan_call,
    )
except ImportError:
    import config
    from connections import init_connections, close_connections, health_check, redis_manager, lookup_redis_manager, postgres_manager
    from handlers import (
        process_event,
        stats,
        customer_cache,
        start_cdr_workers,
        get_cdr_queue_size,
        clear_customer_lookup_cache,
        get_call_from_redis,
        recover_orphan_call,
    )


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging():
    """Configure logging with rotation."""
    file_handler = RotatingFileHandler(
        config.LOG_FILE,
        maxBytes=config.LOG_MAX_BYTES,
        backupCount=config.LOG_BACKUP_COUNT,
        encoding="utf-8"
    )
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    ))
    
    # Root logger for all monitor modules
    root_logger = logging.getLogger("monitor")
    root_logger.setLevel(getattr(logging, config.LOG_LEVEL))
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    root_logger.propagate = False
    
    return logging.getLogger("monitor.main")


logger = setup_logging()


# =============================================================================
# GLOBAL STATE
# =============================================================================

shutdown_requested = False
worker_pool: Pool = None


# =============================================================================
# SIGNAL HANDLERS
# =============================================================================

def signal_handler(signum, frame):
    global shutdown_requested
    shutdown_requested = True
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")


# =============================================================================
# WORKER POOL EVENT PROCESSING
# =============================================================================

def process_event_async(event):
    """Process event in worker pool."""
    try:
        process_event(event)
    except Exception as e:
        logger.error(f"Worker error: {e}")


def reconcile_orphan_calls(reason: str) -> int:
    """Force-finalize stale calls from active_calls and clean Redis/cache.

    Returns number of calls recovered in this cycle.
    """
    if not getattr(config, "ORPHAN_REAPER_ENABLED", True):
        return 0

    redis_client = redis_manager.client
    if not redis_client:
        return 0

    recovered = 0
    scanned = 0
    now = int(time.time())
    threshold = max(1, int(getattr(config, "ORPHAN_REAPER_AGE_SECONDS", 1800)))
    max_per_cycle = max(1, int(getattr(config, "ORPHAN_REAPER_MAX_PER_CYCLE", 500)))

    try:
        active_uuids = list(redis_client.smembers("active_calls") or [])
    except Exception as e:
        logger.error(f"Orphan reconcile read failed ({reason}): {e}")
        return 0

    for uuid in active_uuids:
        if scanned >= max_per_cycle:
            break

        scanned += 1
        try:
            call_data = get_call_from_redis(uuid)
            if not call_data:
                # Orphan set member with missing hash; still force-write marker CDR.
                if recover_orphan_call(uuid, reason=f"{reason}:missing_hash"):
                    recovered += 1
                continue

            start_ts_raw = call_data.get("start_ts") or "0"
            try:
                start_ts = int(start_ts_raw)
            except Exception:
                start_ts = 0

            age = (now - start_ts) if start_ts > 0 else threshold
            if age >= threshold:
                if recover_orphan_call(uuid, reason=f"{reason}:age_{age}"):
                    recovered += 1
        except Exception as e:
            logger.error(f"Orphan reconcile item failed uuid={uuid[:8]}... ({reason}): {e}")

    if scanned and recovered:
        logger.warning(
            f"Orphan reconcile ({reason}) recovered={recovered} scanned={scanned} "
            f"threshold={threshold}s"
        )

    return recovered


# =============================================================================
# HEALTH CHECK & MONITORING
# =============================================================================

def health_check_loop():
    """Background health monitoring."""
    global shutdown_requested
    last_heartbeat = time.time()
    last_gc = time.time()
    
    while not shutdown_requested:
        try:
            now = time.time()
            
            # Heartbeat with stats
            if now - last_heartbeat >= config.HEARTBEAT_INTERVAL:
                redis_client = redis_manager.client
                active = redis_client.scard("active_calls") if redis_client else 0
                health = health_check()

                recovered = reconcile_orphan_calls("heartbeat")
                if recovered:
                    # Refresh active after reconciliation for accurate heartbeat metric.
                    active = redis_client.scard("active_calls") if redis_client else 0
                
                logger.info(
                    f"♥ Heartbeat: events={stats.count} errors={stats.errors} "
                    f"active={active} cache={customer_cache.size()} "
                    f"cdr_queue={get_cdr_queue_size()} "
                    f"redis={'✓' if health['redis'] else '✗'} "
                    f"lookup_redis={'✓' if health['lookup_redis'] else '✗'} "
                    f"pg={'✓' if health['postgres'] else '✗'}"
                )
                last_heartbeat = now
            
            # Periodic garbage collection (every 5 minutes)
            if now - last_gc >= 300:
                gc.collect()
                last_gc = now
            
            # Ensure connections
            redis_manager.ensure_connected()
            lookup_redis_manager.ensure_connected()
            postgres_manager.ensure_connected()
            
            gevent.sleep(config.HEALTH_CHECK_INTERVAL)
            
        except Exception as e:
            logger.error(f"Health check error: {e}")
            gevent.sleep(5)


# =============================================================================
# ESL LISTENER
# =============================================================================

def run_esl_listener():
    """Connect to FreeSWITCH ESL and listen for events."""
    global shutdown_requested, worker_pool
    
    # Create worker pool for parallel event processing
    worker_pool = Pool(size=config.EVENT_WORKER_POOL_SIZE)
    
    while not shutdown_requested:
        fs = None
        try:
            logger.info(f"Connecting to FreeSWITCH ESL {config.FS_ESL_HOST}:{config.FS_ESL_PORT}...")
            
            fs = greenswitch.InboundESL(
                host=config.FS_ESL_HOST,
                port=config.FS_ESL_PORT,
                password=config.FS_ESL_PASSWORD
            )
            fs.connect()
            logger.info("ESL connected")
            
            # Event handler that dispatches to worker pool
            def pooled_handler(event):
                if not shutdown_requested:
                    worker_pool.spawn(process_event_async, event)
            
            fs.register_handle("*", pooled_handler)
            
            # Subscribe to events
            # Using CHANNEL_HANGUP_COMPLETE for accurate CDR (not CHANNEL_HANGUP)
            events = "CHANNEL_CREATE CHANNEL_PROGRESS CHANNEL_ANSWER CHANNEL_BRIDGE CHANNEL_HANGUP_COMPLETE RECORD_START RECORD_STOP"
            fs.send(f"event plain {events}")
            logger.info(f"Subscribed to events (worker pool size: {config.EVENT_WORKER_POOL_SIZE})")
            
            # Start event processing (CRITICAL - must be called for callbacks to work)
            if hasattr(fs, 'start'):
                fs.start()
            
            logger.info("Listening for events...")
            
            # Keep connection alive and let callbacks handle events
            while not shutdown_requested:
                try:
                    # Send keepalive to check connection
                    resp = fs.send("api status")
                    if resp is None:
                        logger.warning("ESL keepalive failed")
                        break
                    gevent.sleep(config.ESL_KEEPALIVE_INTERVAL)
                except Exception as e:
                    logger.warning(f"ESL connection lost: {e}")
                    break
            
        except Exception as e:
            logger.error(f"ESL error: {e}")
        finally:
            if fs:
                try:
                    fs.stop()
                except:
                    pass
        
        if not shutdown_requested:
            logger.info(f"Reconnecting in {config.ESL_RECONNECT_DELAY}s...")
            gevent.sleep(config.ESL_RECONNECT_DELAY)
    
    # Cleanup worker pool
    if worker_pool:
        worker_pool.kill()


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main entry point."""
    global shutdown_requested
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info("=" * 70)
    logger.info("FreeSWITCH ESL Monitor - Production v2.0")
    logger.info("=" * 70)
    logger.info(f"ESL:        {config.FS_ESL_HOST}:{config.FS_ESL_PORT}")
    logger.info(f"Redis:      {config.REDIS_HOST}:{config.REDIS_PORT}")
    logger.info(f"PostgreSQL: {config.PG_HOST}:{config.PG_PORT}/{config.PG_DATABASE}")
    logger.info(f"Workers:    {config.EVENT_WORKER_POOL_SIZE}")
    logger.info(f"CDR Workers: {config.CDR_QUEUE_WORKERS}")
    logger.info(f"Log Level:  {config.LOG_LEVEL}")
    logger.info("=" * 70)
    
    # Initialize connections
    if not init_connections():
        logger.error("Failed to initialize connections")
        return 1
    
    # Clear old cached lookups to use fresh customer lookup logic
    clear_customer_lookup_cache()
    
    # Start CDR background workers
    start_cdr_workers()

    # Startup reconciliation for stale active calls after restart/crash.
    reconcile_orphan_calls("startup")
    
    # Start health check greenlet
    health_greenlet = gevent.spawn(health_check_loop)
    
    # Run ESL listener
    try:
        run_esl_listener()
    except KeyboardInterrupt:
        pass
    finally:
        shutdown_requested = True
        logger.info("Shutting down...")
        
        # Stop health check
        health_greenlet.kill()
        
        # Close connections
        close_connections()
        
        logger.info(f"Monitor stopped. Total events: {stats.count}, Errors: {stats.errors}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
