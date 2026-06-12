#!/usr/bin/env python3
"""
Event Handlers Module for ESL Monitor
======================================
Optimized event handlers for FreeSWITCH ESL events.
Uses connection pools and efficient Redis operations.
"""
import json
import logging
import time
import queue
import threading
import sqlite3
import re
from typing import Dict, Optional, Any
from functools import lru_cache

# Local imports - work both as package and direct run
try:
    from . import config
    from .connections import get_redis, get_lookup_redis, get_pg_connection
except ImportError:
    import config
    from connections import get_redis, get_lookup_redis, get_pg_connection

logger = logging.getLogger("monitor.handlers")

# Internal extension formats supported:
# - legacy numeric: 3-6 digits (e.g. 602001)
# - prefixed: one letter + 6 digits (e.g. U004101, C000601)
INTERNAL_PREFIXED_EXT_RE = re.compile(r"^[A-Za-z]\d{6}$")


# =============================================================================
# CUSTOMER ID CACHE
# =============================================================================

class CustomerCache:
    """LRU cache for UUID to customer_id mapping."""
    
    def __init__(self, max_size: int = 10000):
        self._cache: Dict[str, str] = {}
        self._max_size = max_size
        self._access_order: list = []
    
    def get(self, uuid: str) -> Optional[str]:
        """Get cached customer_id."""
        if uuid in self._cache:
            # Move to end (most recently used)
            if uuid in self._access_order:
                self._access_order.remove(uuid)
            self._access_order.append(uuid)
            return self._cache[uuid]
        return None
    
    def set(self, uuid: str, customer_id: str):
        """Cache UUID to customer_id mapping."""
        if len(self._cache) >= self._max_size:
            # Remove oldest entries
            to_remove = self._access_order[:self._max_size // 4]
            for key in to_remove:
                self._cache.pop(key, None)
            self._access_order = self._access_order[self._max_size // 4:]
        
        self._cache[uuid] = customer_id
        self._access_order.append(uuid)
    
    def remove(self, uuid: str):
        """Remove UUID from cache."""
        self._cache.pop(uuid, None)
        if uuid in self._access_order:
            self._access_order.remove(uuid)
    
    def size(self) -> int:
        """Get cache size."""
        return len(self._cache)


# Global cache instance
customer_cache = CustomerCache(config.CUSTOMER_CACHE_MAX_SIZE)


# =============================================================================
# NUMBER TO CUSTOMER LOOKUP CACHE
# =============================================================================

class SimpleTTLCache:
    """A simple TTL cache implementation."""
    def __init__(self, ttl_seconds: int, maxsize: int = 1000):
        self.ttl = ttl_seconds
        self.maxsize = maxsize
        self._cache = {}
        self._access = []
        
    def get(self, key):
        if key in self._cache:
            value, timestamp = self._cache[key]
            if time.time() - timestamp <= self.ttl:
                if key in self._access:
                    self._access.remove(key)
                self._access.append(key)
                return value
            else:
                self.remove(key)
        return None
        
    def set(self, key, value):
        if key in self._cache:
            self.remove(key)
        elif len(self._cache) >= self.maxsize:
            if self._access:
                oldest = self._access.pop(0)
                self._cache.pop(oldest, None)
        
        self._cache[key] = (value, time.time())
        self._access.append(key)
        
    def remove(self, key):
        self._cache.pop(key, None)
        if key in self._access:
            self._access.remove(key)
            
    def clear(self):
        self._cache.clear()
        self._access.clear()

_customer_lookup_cache = SimpleTTLCache(
    ttl_seconds=getattr(config, 'CUSTOMER_LOOKUP_CACHE_TTL', 300),
    maxsize=1000
)

_extension_lookup_cache = SimpleTTLCache(
    ttl_seconds=getattr(config, 'CUSTOMER_LOOKUP_CACHE_TTL', 300),
    maxsize=1000
)

def get_customer_id_from_number(number: str) -> Optional[str]:
    """Get customer_id from lookup Redis number key."""
    if not number:
        return None

    cached = _customer_lookup_cache.get(number)
    if cached is not None:
        return cached if cached != "NONE" else None

    lookup_redis = get_lookup_redis()
    if not lookup_redis:
        return None

    try:
        digits = ''.join(c for c in number if c.isdigit())
        if not digits:
            return None

        # Redis lookup uses local number format (e.g. 8005550100), not +1/1 prefixes.
        normalized = digits[1:] if len(digits) == 11 and digits.startswith('1') else digits
        key = f"{config.LOOKUP_REDIS_NUMBER_KEY_PREFIX}:{normalized}"
        customer_id = lookup_redis.get(key)
        
        _customer_lookup_cache.set(number, customer_id if customer_id else "NONE")
        
        if customer_id:
            logger.debug(f"Lookup Redis number matched {number} as {normalized}: {customer_id}")
            return customer_id
        return None
    except Exception as e:
        logger.error(f"Lookup Redis number error for {number}: {e}")
        return None


def get_customer_id_from_extension(extension: str) -> Optional[str]:
    """Get customer_id from lookup Redis extension-prefix key."""
    if not extension:
        return None

    cached = _extension_lookup_cache.get(extension)
    if cached is not None:
        return cached if cached != "NONE" else None

    lookup_redis = get_lookup_redis()
    if not lookup_redis:
        return None

    try:
        token = (extension or "").strip().upper()
        prefix_len = max(1, int(getattr(config, 'LOOKUP_EXTENSION_PREFIX_LEN', 5)))

        # Preferred format: prefixed extension (e.g. U004101 -> prefix U0041)
        if INTERNAL_PREFIXED_EXT_RE.fullmatch(token):
            prefixed_key = token[:min(len(token), prefix_len)]
            key = f"{config.LOOKUP_REDIS_EXT_KEY_PREFIX}:{prefixed_key}"
            customer_id = lookup_redis.get(key)
            if customer_id:
                _extension_lookup_cache.set(extension, customer_id)
                logger.debug(f"Lookup Redis extension matched {extension} as {prefixed_key}: {customer_id}")
                return customer_id

        # Backward-compatible fallback: numeric-only extension keys.
        digits = ''.join(c for c in token if c.isdigit())
        if not digits:
            _extension_lookup_cache.set(extension, "NONE")
            return None

        numeric_prefix = digits[:prefix_len]
        key = f"{config.LOOKUP_REDIS_EXT_KEY_PREFIX}:{numeric_prefix}"
        customer_id = lookup_redis.get(key)
        
        _extension_lookup_cache.set(extension, customer_id if customer_id else "NONE")
        
        if customer_id:
            logger.debug(f"Lookup Redis extension matched {extension} as {numeric_prefix}: {customer_id}")
            return customer_id
        return None
    except Exception as e:
        logger.error(f"Lookup Redis extension error for {extension}: {e}")
        return None


def clear_customer_lookup_cache():
    """Clear the customer lookup cache (call at startup to clear stale entries)."""
    _customer_lookup_cache.clear()
    _extension_lookup_cache.clear()
    logger.info("Customer lookup cache cleared")


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def is_internal(number: str) -> bool:
    """Check if number is internal extension vs external/trunk."""
    if not number:
        return False
    token = (number or "").strip()
    # Legacy internal extension: 3-6 digits.
    if token.isdigit():
        return 3 <= len(token) <= 6
    # New prefixed extension format: U004101 / C000601.
    return bool(INTERNAL_PREFIXED_EXT_RE.fullmatch(token))


def determine_destination(event_data: Dict[str, str]) -> tuple:
    """Determine destination type and value from event data."""
    bridge_to = (
        event_data.get("Caller-Callee-ID-Number") or
        event_data.get("Other-Leg-Destination-Number") or
        event_data.get("variable_bridge_to") or 
        event_data.get("variable_sip_req_user") or
        event_data.get("variable_sip_to_user") or
        event_data.get("variable_dialed_extension") or
        event_data.get("Caller-Destination-Number") or ""
    )
    
    if bridge_to:
        token = bridge_to.strip()
        if is_internal(token):
            # Preserve queue/ivr detection for legacy numeric extensions.
            if token.isdigit() and token.startswith('5'):
                return "queue", token
            if token.isdigit() and token.startswith('6'):
                return "ivr", token
            return "extension", token
        return "external", token
    return "unknown", ""


def determine_call_direction(event_dict: Dict[str, str]) -> str:
    """Determine call direction (legacy behavior)."""
    return (
        event_dict.get("Call-Direction") or
        event_dict.get("Caller-Direction") or
        ""
    ).lower()


ROUTE_KEY_RE = re.compile(r"^(ext|rg|ivr|fwd|sched)\d+_[A-Za-z0-9]+(?:__\d+)?$", re.IGNORECASE)
ROUTE_PREFIX_TO_TYPE = {
    "ext": "extension",
    "rg": "ring_group",
    "ivr": "ivr",
    "fwd": "did_forward",
    "sched": "schedule",
}


def _extract_route_key(value: str) -> str:
    """Return normalized route key when value matches configured dialplan formats."""
    if not value:
        return ""
    token = value.strip().lower()
    if ROUTE_KEY_RE.match(token):
        return token
    return ""


def _route_type_from_key(route_key: str) -> str:
    """Map route key prefix to route family label."""
    if not route_key:
        return ""
    prefix = route_key.split("_", 1)[0]
    family = re.match(r"^[a-zA-Z]+", prefix)
    if not family:
        return ""
    return ROUTE_PREFIX_TO_TYPE.get(family.group(0).lower(), "")


def _resolve_entry_route(event_dict: Dict[str, str], fallback: str = "") -> tuple:
    """Resolve first-known route key/type from event fields."""
    candidates = [
        event_dict.get("variable_route_key", ""),
        event_dict.get("variable_inbound_did", ""),
        event_dict.get("Caller-Destination-Number", ""),
        event_dict.get("Other-Leg-Destination-Number", ""),
        fallback,
    ]
    for candidate in candidates:
        key = _extract_route_key(candidate or "")
        if key:
            return key, _route_type_from_key(key)
    return "", ""


def _resolve_route_type_for_cdr(a_leg_data: Dict[str, str]) -> str:
    """Resolve route family for final CDR labeling."""
    explicit = (a_leg_data.get("entry_route_type") or "").strip().lower()
    if explicit:
        return explicit

    candidates = [
        a_leg_data.get("entry_route_key", ""),
        a_leg_data.get("route_key", ""),
        a_leg_data.get("inbound_did", ""),
        a_leg_data.get("forwarded_to", ""),
        a_leg_data.get("original_did", ""),
    ]
    for candidate in candidates:
        key = _extract_route_key(candidate or "")
        if key:
            return _route_type_from_key(key)
    return ""


def classify_call_type(a_leg_data: Dict[str, str]) -> tuple:
    """
    Determine call type and return tuple:
    (call_type, dest_type, dest_value, egress_trunk)
    
    Call Type Rules:
    - INBOUND: External caller -> Internal extension (no forwarding)
    - DID_FORWARD: External caller -> External destination (forwarded call, has RDNIS)
    - OUTBOUND: Internal extension -> External destination (user-initiated)
    - INTERNAL: Internal extension -> Internal extension
    """
    b_uuid = a_leg_data.get("b_uuid")
    forwarded_to = a_leg_data.get("forwarded_to", "")
    original_callee = a_leg_data.get("callee", "")
    caller = a_leg_data.get("caller", "")
    has_rdnis = a_leg_data.get("has_rdnis", "false") == "true"
    route_type = _resolve_route_type_for_cdr(a_leg_data)
    
    caller_internal = is_internal(caller)
    dest_internal = is_internal(forwarded_to)
    
    # No B-leg - simple call with no bridge
    if not b_uuid:
        if not caller_internal:  # External caller, no bridge = INBOUND
            return "INBOUND", (route_type or "extension"), original_callee, ""
        # Outbound attempt can fail before B-leg is created; keep dialed destination.
        fallback_dest = forwarded_to or original_callee
        fallback_type = "outbound" if fallback_dest else "unknown"
        return "OUTBOUND", fallback_type, fallback_dest, fallback_dest
    
    # === DID_FORWARD Detection ===
    # External caller -> External destination + RDNIS present = forwarded call
    if not caller_internal and not dest_internal and has_rdnis:
        return "DID_FORWARD", (route_type or "external"), forwarded_to, forwarded_to
    
    # === INBOUND Detection ===
    # External caller -> Internal extension = answered internally
    if not caller_internal and dest_internal:
        return "INBOUND", (route_type or "extension"), forwarded_to, ""
    
    # === OUTBOUND Detection ===
    # Internal extension -> External destination (user-initiated outbound)
    if caller_internal and not dest_internal:
        return "OUTBOUND", (route_type or "external"), forwarded_to, forwarded_to
    
    # === INTERNAL Detection ===
    # Internal extension -> Internal extension
    if caller_internal and dest_internal:
        return "INTERNAL", (route_type or "extension"), forwarded_to, ""
    
    # Fallback
    dest_type = route_type or ("extension" if dest_internal else "external")
    return "OUTBOUND", dest_type, forwarded_to, forwarded_to


def _normalize_did_value(value: str) -> str:
    """Normalize DID-like values by extracting digits when possible."""
    if not value:
        return ""
    digits = ''.join(c for c in value if c.isdigit())
    if len(digits) >= 7:
        return digits
    return value


def _preferred_cdr_callee(redis_data: Dict[str, str]) -> str:
    """Pick stable callee value (prefer original DID, fallback to initial callee)."""
    original_did = redis_data.get("original_did", "") or ""
    callee = redis_data.get("callee", "") or ""

    normalized_original = _normalize_did_value(original_did)
    normalized_callee = _normalize_did_value(callee)

    # Keep true DID when available; avoid routing labels like sched1_0006/ext2_0006.
    if normalized_original and normalized_original.isdigit() and len(normalized_original) >= 7:
        return normalized_original
    if normalized_callee and normalized_callee.isdigit() and len(normalized_callee) >= 7:
        return normalized_callee

    return callee or original_did


# =============================================================================
# REDIS OPERATIONS
# =============================================================================


def _primary_call_key(uuid: str) -> str:
    """Stable primary Redis key for a call."""
    return f"call:{uuid}"


def _customer_call_key(customer_id: str, uuid: str) -> str:
    """Compatibility Redis key used by existing downstream consumers."""
    return f"customer:{customer_id}:call:{uuid}"

def store_call_in_redis(call_data: Dict[str, Any]) -> bool:
    """Store call data in Redis with optimized pipeline."""
    redis_client = get_redis()
    if not redis_client:
        return False
    
    try:
        uuid = call_data.get("uuid")
        if not uuid:
            return False

        customer_id = call_data.get("customer_id") or "unknown"
        call_data["customer_id"] = customer_id
        primary_key = _primary_call_key(uuid)
        customer_key = _customer_call_key(customer_id, uuid)
        new_status = call_data.get("call_status")
        
        # Prevent status downgrade
        if new_status == "ringing":
            existing = redis_client.hget(primary_key, "call_status")
            if existing in ["answered", "connected"]:
                call_data["call_status"] = existing
        
        # Pipeline for atomic operations
        mapping = {k: str(v) if v is not None else "" for k, v in call_data.items()}
        pipe = redis_client.pipeline(transaction=False)
        # Primary source of truth
        pipe.hset(primary_key, mapping=mapping)
        pipe.expire(primary_key, config.REDIS_CALL_TTL)
        # Compatibility key for consumers expecting customer:<id>:call:<uuid>
        pipe.hset(customer_key, mapping=mapping)
        pipe.expire(customer_key, config.REDIS_CALL_TTL)
        
        status = call_data.get("call_status")
        if status in ["new", "ringing", "answered", "connected"]:
            pipe.sadd("active_calls", uuid)
        
        pipe.publish("calls_stream", json.dumps(call_data))
        results = pipe.execute()

        # Basic sanity check: hash key should exist after write.
        if getattr(config, "LOG_DEBUG_EVENTS", False):
            primary_exists = redis_client.exists(primary_key)
            customer_exists = redis_client.exists(customer_key)
            logger.debug(
                f"Redis upsert ok uuid={uuid[:8]}... customer={customer_id} "
                f"primary_exists={bool(primary_exists)} customer_key_exists={bool(customer_exists)} results={results}"
            )
        
        customer_cache.set(uuid, customer_id)
        return True
        
    except Exception as e:
        logger.error(f"Redis store error: {e}")
        return False


def get_call_from_redis(uuid: str) -> Optional[Dict[str, str]]:
    """Get call data from Redis using cache."""
    redis_client = get_redis()
    if not redis_client:
        return None
    
    try:
        # Primary lookup (stable key)
        primary_key = _primary_call_key(uuid)
        primary_data = redis_client.hgetall(primary_key)
        if primary_data:
            found_customer = primary_data.get("customer_id")
            if found_customer:
                customer_cache.set(uuid, found_customer)
            return primary_data

        # Try cache first
        customer_id = customer_cache.get(uuid)
        if customer_id:
            key = _customer_call_key(customer_id, uuid)
            data = redis_client.hgetall(key)
            if data:
                # Backfill primary key for future reads
                redis_client.hset(primary_key, mapping={k: str(v) if v is not None else "" for k, v in data.items()})
                redis_client.expire(primary_key, config.REDIS_CALL_TTL)
                return data

        # Fallback scan for compatibility with existing in-flight keys.
        pattern = f"customer:*:call:{uuid}"
        for key in redis_client.scan_iter(match=pattern, count=100):
            data = redis_client.hgetall(key)
            if data:
                found_customer = key.split(":", 2)[1]
                customer_cache.set(uuid, found_customer)
                # Backfill primary key for future reads
                redis_client.hset(primary_key, mapping={k: str(v) if v is not None else "" for k, v in data.items()})
                redis_client.expire(primary_key, config.REDIS_CALL_TTL)
                return data
        return None
        
    except Exception as e:
        logger.error(f"Redis get error: {e}")
        return None


def remove_call_from_redis(uuid: str, customer_id: str):
    """Remove call from Redis."""
    redis_client = get_redis()
    if not redis_client:
        return
    
    try:
        pipe = redis_client.pipeline(transaction=False)
        key_customer = customer_cache.get(uuid) or customer_id or "unknown"
        # Remove primary key
        pipe.delete(_primary_call_key(uuid))
        # Remove current known compatibility key
        pipe.delete(_customer_call_key(key_customer, uuid))
        # Remove any stale compatibility keys from previous customer namespaces
        pattern = f"customer:*:call:{uuid}"
        for key in redis_client.scan_iter(match=pattern, count=100):
            pipe.delete(key)
        pipe.srem("active_calls", uuid)
        pipe.execute()
        if getattr(config, "LOG_DEBUG_EVENTS", False):
            logger.debug(f"Redis cleanup uuid={uuid[:8]}... requested_customer={customer_id or 'none'}")
        customer_cache.remove(uuid)
    except Exception as e:
        logger.error(f"Redis remove error: {e}")


# =============================================================================
# CDR BATCHER FOR PHASE 1 SCALING (300-400 CC)
# =============================================================================


class CDRSpool:
    """Durable local spool for zero-loss CDR buffering."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self.db_path, timeout=30)

    def _init_db(self):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cdr_spool (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL,
                    created_ts INTEGER NOT NULL
                )
            """)
            conn.commit()
        finally:
            conn.close()

    def enqueue(self, cdr: Dict[str, Any]) -> int:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO cdr_spool (payload, created_ts) VALUES (?, ?)",
                (json.dumps(cdr, separators=(",", ":")), int(time.time()))
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            conn.close()

    def fetch_batch(self, limit: int) -> list:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, payload FROM cdr_spool ORDER BY id ASC LIMIT ?",
                (limit,)
            )
            rows = cur.fetchall()
            parsed = []
            for row_id, payload in rows:
                try:
                    parsed.append((int(row_id), json.loads(payload)))
                except Exception:
                    # Skip malformed payloads but keep record for manual recovery.
                    logger.error(f"Malformed CDR spool payload id={row_id}")
            return parsed
        finally:
            conn.close()

    def delete_batch(self, row_ids: list):
        if not row_ids:
            return
        conn = self._connect()
        try:
            cur = conn.cursor()
            placeholders = ",".join(["?"] * len(row_ids))
            cur.execute(
                f"DELETE FROM cdr_spool WHERE id IN ({placeholders})",
                tuple(row_ids)
            )
            conn.commit()
        finally:
            conn.close()

    def size(self) -> int:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM cdr_spool")
            row = cur.fetchone()
            return int(row[0] if row else 0)
        finally:
            conn.close()

class CDRBatcher:
    """Batch CDRs for bulk insert - reduces DB load for high CC."""
    
    def __init__(self, batch_size: int = 50, timeout: float = 2.0):
        self.batch = []
        self.batch_size = batch_size
        self.timeout = timeout
        self.last_flush = time.time()
        self.lock = threading.Lock()
        self._flush_interval = max(0.1, getattr(config, 'CDR_FLUSH_INTERVAL', 0.5))
        self._max_retries = max(1, getattr(config, 'CDR_INSERT_MAX_RETRIES', 5))
        self._retry_backoff = max(0.1, getattr(config, 'CDR_INSERT_RETRY_BACKOFF', 0.5))
        self._stop_event = threading.Event()
        self._flush_event = threading.Event()
        self._flusher_started = False
        self._use_durable_spool = getattr(config, 'CDR_DURABLE_SPOOL_ENABLED', True)
        self._spool = CDRSpool(getattr(config, 'CDR_SPOOL_DB_PATH', 'cdr_spool.db')) if self._use_durable_spool else None

    def start(self):
        """Start periodic flusher loop once."""
        if self._flusher_started:
            return
        t = threading.Thread(target=self._flusher_loop, daemon=True, name="cdr-flusher")
        t.start()
        self._flusher_started = True
        logger.info(
            f"CDR flusher started (batch={self.batch_size}, timeout={self.timeout}s, durable_spool={self._use_durable_spool})"
        )

    def _flusher_loop(self):
        while not self._stop_event.is_set():
            self._flush_event.wait(timeout=self._flush_interval)
            self._flush_event.clear()
            try:
                self.flush()
            except Exception as e:
                logger.error(f"CDR periodic flush error: {e}")
    
    def add(self, cdr: Dict[str, Any]):
        """Add CDR to batch, flush if needed."""
        if self._use_durable_spool and self._spool:
            row_id = self._spool.enqueue(cdr)
            if row_id % self.batch_size == 0:
                self._flush_event.set()
            return

        with self.lock:
            self.batch.append(cdr)

            if len(self.batch) >= self.batch_size:
                batch_to_save = self.batch.copy()
                self.batch = []
                self.last_flush = time.time()
            elif time.time() - self.last_flush >= self.timeout:
                batch_to_save = self.batch.copy()
                self.batch = []
                self.last_flush = time.time()
            else:
                batch_to_save = []

        if batch_to_save:
            self._bulk_insert_with_retry(batch_to_save)
    
    def flush(self):
        """Force flush remaining CDRs."""
        if self._use_durable_spool and self._spool:
            while True:
                rows = self._spool.fetch_batch(self.batch_size)
                if not rows:
                    return
                row_ids = [row_id for row_id, _ in rows]
                cdrs = [payload for _, payload in rows]

                if self._bulk_insert_with_retry(cdrs):
                    self._spool.delete_batch(row_ids)
                else:
                    # Keep rows in spool for retry in next flush cycle.
                    return

                if len(rows) < self.batch_size:
                    return

        with self.lock:
            if not self.batch:
                return
            batch_to_save = self.batch.copy()
            self.batch = []
            self.last_flush = time.time()

        self._bulk_insert_with_retry(batch_to_save)

    def _bulk_insert_with_retry(self, cdrs: list) -> bool:
        """Retry bulk insert with backoff. Keeps zero-loss durability with spool."""
        for attempt in range(1, self._max_retries + 1):
            if self._bulk_insert_to_db(cdrs):
                return True
            delay = self._retry_backoff * attempt
            logger.warning(
                f"Bulk insert retry {attempt}/{self._max_retries} in {delay:.1f}s for {len(cdrs)} CDRs"
            )
            time.sleep(delay)

        logger.error(f"Bulk insert failed after retries for {len(cdrs)} CDRs")
        return False
    
    def _bulk_insert_to_db(self, cdrs: list):
        """Bulk insert CDRs to database."""
        if not cdrs:
            return True
        
        try:
            with get_pg_connection() as conn:
                if not conn:
                    logger.error("Cannot get DB connection for bulk insert")
                    return False
                
                with conn.cursor() as cur:
                    # Build multi-row VALUES clause
                    placeholders = []
                    values = []
                    
                    for cdr in cdrs:
                        # Each CDR has 19 parameters
                        placeholders.append(f"(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
                        
                        values.extend([
                            cdr.get("uuid"),
                            cdr.get("b_uuid") or None,
                            cdr.get("event_type") or cdr.get("call_status"),
                            cdr.get("event_ts") or cdr.get("start_ts"),
                            cdr.get("caller"),
                            cdr.get("callee"),
                            cdr.get("customer_id") or None,
                            cdr.get("dest_type"),
                            cdr.get("dest_value"),
                            cdr.get("status_code", "UNKNOWN"),
                            cdr.get("call_type"),
                            cdr.get("outbound_caller_id") or None,
                            cdr.get("originating_extension") or None,
                            cdr.get("originating_leg_uuid") or None,
                            cdr.get("ingress_trunk") or None,
                            cdr.get("egress_trunk") or None,
                            int(cdr.get("duration", 0)),
                            int(cdr.get("billsec", 0)),
                            bool(cdr.get("recording", False))
                        ])
                    
                    # Single bulk insert
                    query = f"""
                        INSERT INTO live_call_events 
                        (uuid, b_uuid, event_type, event_ts, caller, callee, 
                         customer_id, dest_type, dest_value, status_code, 
                         call_type, outbound_caller_id, originating_extension, 
                         originating_leg_uuid, ingress_trunk, egress_trunk, 
                         duration, billsec, recording)
                        VALUES {','.join(placeholders)}
                        ON CONFLICT (uuid) DO NOTHING
                    """
                    
                    cur.execute(query, values)
                    conn.commit()
                    logger.info(f"Bulk inserted {len(cdrs)} CDRs in 1 query")
                    return True
                    
        except Exception as e:
            logger.error(f"Bulk insert error: {e}")
            return False

    def pending_count(self) -> int:
        """Approximate pending records waiting to be inserted."""
        if self._use_durable_spool and self._spool:
            return self._spool.size()
        with self.lock:
            return len(self.batch)


# Global batcher instance
cdr_batcher = CDRBatcher(
    batch_size=config.CDR_BATCH_SIZE,
    timeout=config.CDR_BATCH_TIMEOUT
)


# =============================================================================
# CDR STORAGE WITH ASYNC QUEUE
# =============================================================================

# CDR Queue for background processing
cdr_queue: queue.Queue = queue.Queue()
_cdr_workers_started = False


def _save_cdr_to_postgres_sync(call_data: Dict[str, Any]) -> bool:
    """Internal: Save CDR to PostgreSQL (blocking)."""
    try:
        with get_pg_connection() as conn:
            if not conn:
                return False
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO live_call_events 
                    (uuid, b_uuid, event_type, event_ts, caller, callee, 
                     customer_id, dest_type, dest_value, status_code, 
                     call_type, outbound_caller_id, originating_extension, 
                     originating_leg_uuid, ingress_trunk, egress_trunk, gateway_id, 
                     duration, billsec, currency, transaction_id, is_rated, recording)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (uuid) DO NOTHING
                """, (
                    call_data.get("uuid"),
                    call_data.get("b_uuid") or None,
                    call_data.get("call_status"),
                    call_data.get("event_ts") or call_data.get("start_ts"),
                    call_data.get("caller"),
                    call_data.get("callee"),
                    call_data.get("customer_id") or None,
                    call_data.get("dest_type"),
                    call_data.get("dest_value"),
                    call_data.get("status_code", "UNKNOWN"),
                    call_data.get("call_type"),
                    call_data.get("outbound_caller_id") or None,
                    call_data.get("originating_extension") or None,
                    call_data.get("originating_leg_uuid") or None,
                    call_data.get("ingress_trunk") or None,
                    call_data.get("egress_trunk") or None,
                    call_data.get("gateway_id") or None,
                    int(call_data.get("duration", 0)),
                    int(call_data.get("billsec", 0)),
                    call_data.get("currency", "USD"),  # Default USD
                    int(call_data.get("transaction_id", 0)),  # Default 0
                    call_data.get("is_rated", False),  # Default False
                    bool(call_data.get("recording", False))
                ))
                conn.commit()
                return True
    except Exception as e:
        logger.error(f"CDR save error: {e}")
        return False


def _cdr_worker_thread(worker_id: int):
    """Background worker thread to process CDR queue."""
    logger.info(f"CDR worker {worker_id} started")
    while True:
        try:
            call_data = cdr_queue.get(timeout=5)
            if call_data is None:  # Shutdown signal
                break
            _save_cdr_to_postgres_sync(call_data)
            cdr_queue.task_done()
        except queue.Empty:
            continue  # No items, keep waiting
        except Exception as e:
            logger.error(f"CDR worker {worker_id} error: {e}")


def start_cdr_workers():
    """Start background CDR batching/flusher worker."""
    global _cdr_workers_started
    if _cdr_workers_started:
        return

    cdr_batcher.start()
    _cdr_workers_started = True
    logger.info("Started CDR batch flusher")


def save_cdr_to_postgres(call_data: Dict[str, Any]) -> bool:
    """Save CDR to durable spool for guaranteed zero-loss."""
    try:
        # Always use durable spool for zero-loss guarantee
        if cdr_batcher._use_durable_spool and cdr_batcher._spool:
            cdr_batcher._spool.enqueue(call_data.copy())
            return True
        
        # Fallback if spool disabled: use in-memory batcher
        cdr_batcher.add(call_data.copy())
        return True
    except Exception as e:
        logger.error(f"CDR save error: {e}")
        return False


def get_cdr_queue_size() -> int:
    """Get current CDR queue size (for monitoring)."""
    return cdr_batcher.pending_count()


def recover_orphan_call(uuid: str, reason: str = "stale") -> bool:
    """Force-finalize a potentially orphaned call into DB, then cleanup Redis/cache.

    This is used by periodic/startup reconciliation when a call remains in active_calls
    but normal hangup finalization did not complete.
    """
    try:
        now_ts = int(time.time())
        redis_data = get_call_from_redis(uuid)

        if redis_data:
            call_type, dest_type, dest_value, egress_trunk = classify_call_type(redis_data)
            start_ts_raw = redis_data.get("start_ts") or "0"
            try:
                start_ts = int(start_ts_raw)
            except Exception:
                start_ts = 0

            duration = max(0, now_ts - start_ts) if start_ts > 0 else 0
            cdr = {
                "uuid": uuid,
                "b_uuid": redis_data.get("b_uuid"),
                "event_type": "orphan_cleanup",
                "event_ts": now_ts,
                "caller": redis_data.get("caller") or "unknown",
                "callee": _preferred_cdr_callee(redis_data) or "unknown",
                "customer_id": redis_data.get("customer_id") or "unknown",
                "dest_type": dest_type or "unknown",
                "dest_value": dest_value or "",
                "status_code": "ORPHAN_RECOVERED",
                "call_type": call_type or "unknown",
                "duration": duration,
                "billsec": int(redis_data.get("billsec") or 0),
                "ingress_trunk": redis_data.get("ingress_trunk") or "",
                "egress_trunk": egress_trunk or redis_data.get("egress_trunk") or "",
                "call_status": "hangup",
                "recording": redis_data.get("recording") == "true",
            }

            if call_type == "OUTBOUND":
                cdr["outbound_caller_id"] = redis_data.get("outbound_caller_id", redis_data.get("caller"))
                cdr["originating_extension"] = redis_data.get("originating_extension", redis_data.get("caller"))
                cdr["originating_leg_uuid"] = uuid
            elif call_type == "INBOUND":
                cdr["outbound_caller_id"] = ""
                cdr["originating_extension"] = ""
        else:
            # Worst-case fallback: we only know UUID from active_calls; still persist a marker CDR.
            cdr = {
                "uuid": uuid,
                "b_uuid": "",
                "event_type": "orphan_cleanup",
                "event_ts": now_ts,
                "caller": "unknown",
                "callee": "unknown",
                "customer_id": "unknown",
                "dest_type": "unknown",
                "dest_value": "",
                "status_code": "ORPHAN_RECOVERED",
                "call_type": "unknown",
                "duration": 0,
                "billsec": 0,
                "ingress_trunk": "",
                "egress_trunk": "",
                "call_status": "hangup",
                "recording": False,
            }

        if not save_cdr_to_postgres(cdr):
            logger.error(f"Orphan recovery DB enqueue failed uuid={uuid[:8]}... reason={reason}")
            return False

        redis_client = get_redis()
        if redis_client:
            redis_client.publish("calls_stream", json.dumps(cdr))

        remove_call_from_redis(uuid, (cdr.get("customer_id") or "unknown"))
        logger.warning(f"Recovered orphan call uuid={uuid[:8]}... reason={reason}")
        return True

    except Exception as e:
        logger.error(f"Orphan recovery error uuid={uuid[:8] if uuid else 'unknown'}...: {e}")
        return False


# =============================================================================
# EVENT HANDLERS
# =============================================================================

class EventStats:
    """Track event statistics."""
    def __init__(self):
        self.count = 0
        self.last_time = time.time()
        self.errors = 0
    
    def increment(self):
        self.count += 1
        self.last_time = time.time()
    
    def error(self):
        self.errors += 1

stats = EventStats()


def handle_create(event_dict: Dict[str, str]):
    """Handle CHANNEL_CREATE - capture A-leg and B-leg."""
    try:
        uuid = event_dict.get("Unique-ID")
        direction = determine_call_direction(event_dict)
        other_leg = event_dict.get("Other-Leg-Unique-ID")  # Present if this is B-leg
        start_ts = int(event_dict.get("Event-Date-Timestamp") or 0) // 1000000
        
        if direction == "inbound" and not other_leg:
            # === A-LEG (Incoming call) ===
            caller = event_dict.get("Caller-Caller-ID-Number", "")
            callee = event_dict.get("Caller-Destination-Number", "")  # The DID
            
            # Determine ingress
            ingress = (event_dict.get("variable_sip_received_ip") or 
                      event_dict.get("Caller-Network-Addr", ""))
            
            # Lookup customer_id: use extension table for outbound (internal caller),
            # tfns table for inbound/DID_FORWARD (external caller with DID)
            if is_internal(caller):
                customer_id = get_customer_id_from_extension(caller) or ""
            else:
                customer_id = get_customer_id_from_number(callee) or ""
            
            call_data = {
                "uuid": uuid,
                "b_uuid": "",
                "start_ts": str(start_ts),
                "caller": caller,
                "callee": callee,  # Original DID dialed
                "customer_id": customer_id,
                "call_status": "new",
                "ingress_trunk": ingress,
                "call_type": "pending",
                "originating_extension": "",
                "outbound_caller_id": "",
                "forwarded_to": "",      # Will fill when B-leg arrives
                "has_rdnis": "false",    # Will set to true if B-leg has RDNIS
                "original_did": callee,  # Preserve original DID
                "answer_ts": "",
            }

            entry_route_key, entry_route_type = _resolve_entry_route(event_dict, callee)
            call_data["route_key"] = entry_route_key
            call_data["inbound_did"] = event_dict.get("variable_inbound_did", "")
            call_data["entry_route_key"] = entry_route_key
            call_data["entry_route_type"] = entry_route_type
            
            if not store_call_in_redis(call_data):
                stats.error()
                logger.error(f"CREATE A-leg state store failed: {uuid[:8]}... {caller} -> {callee}")
                return
            logger.info(f"CREATE A-leg: {uuid[:8]}... {caller} -> {callee}")
            
        elif other_leg:
            # === B-LEG (Outbound leg from bridge) ===
            # Link to A-leg
            a_leg = get_call_from_redis(other_leg)
            if not a_leg:
                return
                
            # Capture forward indicators
            rdnis = event_dict.get("Caller-RDNIS", "")  # KEY FIELD for DID_FORWARD
            b_dest = (
                event_dict.get("Caller-Callee-ID-Number") or
                event_dict.get("Caller-Destination-Number", "")
            )
            b_caller_id = event_dict.get("Caller-Caller-ID-Number", "")
            
            # Update A-leg record with B-leg info
            a_leg["b_uuid"] = uuid
            a_leg["forwarded_to"] = b_dest
            a_leg["has_rdnis"] = "true" if rdnis else "false"

            # Preserve first-seen route family and keep latest route key for diagnostics.
            b_route_key, b_route_type = _resolve_entry_route(event_dict, b_dest)
            if b_route_key:
                a_leg["route_key"] = b_route_key
            if not a_leg.get("entry_route_key") and b_route_key:
                a_leg["entry_route_key"] = b_route_key
            if not a_leg.get("entry_route_type") and b_route_type:
                a_leg["entry_route_type"] = b_route_type
                
            # For outbound calls, store the originating extension
            if is_internal(a_leg.get("caller", "")):
                a_leg["originating_extension"] = a_leg["caller"]
                a_leg["outbound_caller_id"] = b_caller_id
                
            if not store_call_in_redis(a_leg):
                stats.error()
                logger.error(f"CREATE B-leg state store failed: {uuid[:8]}... -> {b_dest}")
                return
            logger.info(f"CREATE B-leg: {uuid[:8]}... -> {b_dest} (RDNIS: {rdnis or 'none'})")
            
    except Exception as e:
        stats.error()
        logger.error(f"CREATE error: {e}")


def handle_progress(event_dict: Dict[str, str]):
    """Handle CHANNEL_PROGRESS - ringing."""
    try:
        uuid = event_dict.get("Unique-ID")
        call_data = get_call_from_redis(uuid)
        if not call_data:
            return
        
        # Don't downgrade status
        current_status = call_data.get("call_status", "")
        if current_status in ["answered", "connected"]:
            return
        
        call_data["call_status"] = "ringing"
        if not store_call_in_redis(call_data):
            stats.error()
            logger.error(f"PROGRESS state store failed: {uuid[:8]}...")
            return
        stats.increment()
        logger.debug(f"PROGRESS: {uuid[:8]}... ringing")
        
    except Exception as e:
        stats.error()
        logger.error(f"PROGRESS error: {e}")


def handle_answer(event_dict: Dict[str, str]):
    """Handle CHANNEL_ANSWER - call answered."""
    try:
        uuid = event_dict.get("Unique-ID")
        answer_ts = int(event_dict.get("Event-Date-Timestamp") or 0) // 1000000
        
        call_data = get_call_from_redis(uuid)
        if not call_data:
            return
        
        call_data["call_status"] = "answered"
        call_data["answer_ts"] = str(answer_ts)
        
        dest_type, dest_value = determine_destination(event_dict)
        call_data["dest_type"] = dest_type
        call_data["dest_value"] = dest_value
        
        if not store_call_in_redis(call_data):
            stats.error()
            logger.error(f"ANSWER state store failed: {uuid[:8]}...")
            return
        stats.increment()
        logger.info(f"ANSWER: {uuid[:8]}... -> {dest_type}:{dest_value}")
        
    except Exception as e:
        stats.error()
        logger.error(f"ANSWER error: {e}")


def handle_bridge(event_dict: Dict[str, str]):
    """Handle CHANNEL_BRIDGE - call connected to agent/extension."""
    try:
        uuid = event_dict.get("Unique-ID")
        b_uuid = event_dict.get("Other-Leg-Unique-ID") or ""
        bridge_ts = int(event_dict.get("Event-Date-Timestamp") or 0) // 1000000
        
        call_data = get_call_from_redis(uuid)
        if not call_data:
            return
        
        if b_uuid:
            call_data["b_uuid"] = b_uuid
            call_data["call_status"] = "connected"
            
            dest_type, dest_value = determine_destination(event_dict)
            call_data["dest_type"] = dest_type
            call_data["dest_value"] = dest_value
            
            # Handle race condition - set answer_ts if missing
            if not call_data.get("answer_ts") or call_data.get("answer_ts") == "":
                call_data["answer_ts"] = str(bridge_ts)
            
            if not store_call_in_redis(call_data):
                stats.error()
                logger.error(f"BRIDGE state store failed: {uuid[:8]}... -> {b_uuid[:8]}...")
                return
            stats.increment()
            logger.info(f"BRIDGE: {uuid[:8]}... -> {b_uuid[:8]}... ({dest_type}:{dest_value})")
        
    except Exception as e:
        stats.error()
        logger.error(f"BRIDGE error: {e}")


def handle_hangup_complete(event_dict: Dict[str, str]):
    """Handle CHANNEL_HANGUP_COMPLETE - aggregate and finalize CDR."""
    try:
        uuid = event_dict.get("Unique-ID")
        other_leg = event_dict.get("Other-Leg-Unique-ID")
        
        logger.debug(f"HANGUP_COMPLETE received: uuid={uuid[:8]}... other_leg={other_leg[:8] if other_leg else 'none'}")
        
        # Check if THIS uuid is in Redis (means it's the A-leg we're tracking)
        redis_data = get_call_from_redis(uuid)
        
        # If this UUID is NOT in Redis but has other_leg, it's a B-leg hangup - ignore
        if not redis_data and other_leg:
            customer_cache.remove(uuid)
            logger.debug(f"B-leg hangup ignored (uuid not in Redis)")
            return
        
        # If we have no Redis data at all, nothing to process
        if not redis_data:
            customer_cache.remove(uuid)
            logger.debug(f"HANGUP_COMPLETE: {uuid[:8]}... - no Redis data")
            return
        
        # === A-LEG HANGUP - Finalize CDR ===
        logger.info(f"HANGUP_COMPLETE A-leg: {uuid[:8]}... processing CDR")
        
        # Extract metrics from HANGUP_COMPLETE event (authoritative)
        duration = int(event_dict.get("variable_duration", 0))
        billsec = int(event_dict.get("variable_billsec", 0))
        hangup_cause = event_dict.get("Hangup-Cause", "UNKNOWN")
        hangup_ts = int(event_dict.get("Caller-Channel-Hangup-Time", 0)) // 1000000
        
        # Determine event_type (final disposition)
        if hangup_cause == "USER_BUSY":
            event_type = "busy"
        elif hangup_cause == "CALL_REJECTED":
            event_type = "declined"
        elif hangup_cause in ["NO_ANSWER", "NO_USER_RESPONSE", "ORIGINATOR_CANCEL", "RECOVERY_ON_TIMER_EXPIRE"]:
            event_type = "no_answer"
        elif hangup_cause == "INCOMPATIBLE_DESTINATION":
            event_type = "failed"
        elif hangup_cause == "NORMAL_CLEARING" and billsec > 0:
            event_type = "answered"
        elif hangup_cause == "NORMAL_CLEARING":
            event_type = "no_answer"
        else:
            event_type = "answered" if billsec > 0 else "failed"
        
        # Classify call type using RDNIS detection
        call_type, dest_type, dest_value, egress_trunk = classify_call_type(redis_data)

        # If extension routing failed and IVR answered afterwards, do not mark as answered.
        dialstatus = (event_dict.get("variable_DIALSTATUS") or "").upper()
        originate_disposition = (event_dict.get("variable_originate_disposition") or "").upper()
        if call_type == "INBOUND" and event_type == "answered":
            if (dialstatus and dialstatus not in ["SUCCESS", "ANSWER"]) or (
                originate_disposition and originate_disposition != "SUCCESS"
            ):
                event_type = "no_answer"
        
        # Build CDR
        recording = redis_data.get("recording") == "true"
        cdr = {
            "uuid": uuid,
            "b_uuid": redis_data.get("b_uuid"),
            "event_type": event_type,
            "event_ts": hangup_ts,
            "caller": redis_data.get("caller"),
            "callee": _preferred_cdr_callee(redis_data),
            "customer_id": redis_data.get("customer_id"),
            "dest_type": dest_type,
            "dest_value": dest_value,  # Where it actually went
            "status_code": hangup_cause,
            "call_type": call_type,
            "duration": duration,
            "billsec": billsec,
            "ingress_trunk": redis_data.get("ingress_trunk"),
            "egress_trunk": egress_trunk or event_dict.get("variable_default_gateway", ""),
            "call_status": "hangup",
            "recording": recording,
        }
        
        # Type-specific fields
        if call_type == "OUTBOUND":
            cdr["outbound_caller_id"] = redis_data.get("outbound_caller_id", redis_data.get("caller"))
            cdr["originating_extension"] = redis_data.get("originating_extension", redis_data.get("caller"))
            cdr["originating_leg_uuid"] = uuid
            
        elif call_type == "INBOUND":
            cdr["outbound_caller_id"] = ""
            cdr["originating_extension"] = ""
        
        # Save to DB
        save_cdr_to_postgres(cdr)
        
        # Publish to stream
        redis_client = get_redis()
        if redis_client:
            redis_client.publish("calls_stream", json.dumps(cdr))
        
        # Cleanup Redis
        customer_id = redis_data.get("customer_id", "unknown")
        remove_call_from_redis(uuid, customer_id)
        
        logger.info(f"CDR: {uuid[:8]}... {call_type} {event_type} "
                   f"to {dest_value} ({duration}s/{billsec}s)")
        
    except Exception as e:
        stats.error()
        logger.error(f"HANGUP error: {e}", exc_info=True)


def handle_record_start(event_dict: Dict[str, str]):
    """Handle RECORD_START - set recording flag to true."""
    try:
        uuid = event_dict.get("Unique-ID")
        call_data = get_call_from_redis(uuid)
        if not call_data:
            return
        
        call_data["recording"] = "true"
        if not store_call_in_redis(call_data):
            stats.error()
            logger.error(f"RECORD_START state store failed: {uuid[:8]}...")
            return
        stats.increment()
        logger.info(f"RECORD_START: {uuid[:8]}... recording enabled")
    except Exception as e:
        stats.error()
        logger.error(f"RECORD_START error: {e}")


def handle_record_stop(event_dict: Dict[str, str]):
    """Handle RECORD_STOP - keep recording flag to true if we already started."""
    try:
        uuid = event_dict.get("Unique-ID")
        logger.info(f"RECORD_STOP: {uuid[:8]}...")
        stats.increment()
    except Exception as e:
        stats.error()
        logger.error(f"RECORD_STOP error: {e}")


# CHANNEL_DESTROY removed - cleanup done in CHANNEL_HANGUP_COMPLETE
# CHANNEL_HANGUP removed - using CHANNEL_HANGUP_COMPLETE for accuracy


# =============================================================================
# EVENT ROUTER
# =============================================================================

EVENT_HANDLERS = {
    "CHANNEL_CREATE": handle_create,
    "CHANNEL_PROGRESS": handle_progress,
    "CHANNEL_ANSWER": handle_answer,
    "CHANNEL_BRIDGE": handle_bridge,
    "CHANNEL_HANGUP_COMPLETE": handle_hangup_complete,
    "RECORD_START": handle_record_start,
    "RECORD_STOP": handle_record_stop,
}


# Per-call ordering guard: process same call-group events serially while
# preserving parallelism across different calls.
EVENT_LOCK_SHARDS = 256
_event_shard_locks = [threading.Lock() for _ in range(EVENT_LOCK_SHARDS)]


def _event_anchor_uuid(event_dict: Dict[str, str]) -> str:
    """Return stable call-group anchor UUID for event ordering.

    For B-leg events, Other-Leg-Unique-ID usually points to A-leg UUID,
    which groups A/B events for the same call under one lock shard.
    """
    return (event_dict.get("Other-Leg-Unique-ID") or event_dict.get("Unique-ID") or "")


def _event_lock_for(anchor_uuid: str):
    """Get sharded lock for a call-group anchor UUID."""
    if not anchor_uuid:
        return None
    return _event_shard_locks[hash(anchor_uuid) % EVENT_LOCK_SHARDS]


def process_event(event):
    """Route event to appropriate handler."""
    try:
        event_dict = event.headers if hasattr(event, 'headers') else event
        event_name = event_dict.get("Event-Name")
        
        # Debug: Log ALL events to see what we're receiving
        logger.debug(f"Event received: {event_name}")
        
        handler = EVENT_HANDLERS.get(event_name)
        if handler:
            anchor_uuid = _event_anchor_uuid(event_dict)
            lock = _event_lock_for(anchor_uuid)
            if lock:
                with lock:
                    handler(event_dict)
            else:
                handler(event_dict)
        else:
            # Log unhandled events for debugging
            logger.debug(f"Unhandled event: {event_name}")
            
    except Exception as e:
        stats.error()
        logger.error(f"Event processing error: {e}")
