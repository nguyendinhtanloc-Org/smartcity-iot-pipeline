"""
storage.py
----------
Chặng Storage - Ghi dữ liệu vào PostgreSQL
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional, List
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch

logger = logging.getLogger("storage")


class Storage:
    """Chặng Storage - Ghi dữ liệu vào PostgreSQL"""
    
    def __init__(
        self,
        in_queue: "queue.Queue",
        db_config: Optional[Dict] = None,
        batch_size: int = 100,
        flush_interval: float = 5.0,
    ):
        self.in_queue = in_queue
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        
        # DB config
        self.db_config = db_config or {
            "host": "localhost",
            "port": 5432,
            "database": "smartcity",
            "user": "postgres",
            "password": "postgres",
        }
        
        self.conn: Optional[psycopg2.extensions.connection] = None
        self.buffer: List[Dict] = []
        self.last_flush = time.time()
        
        self.total_stored = 0
        self.total_errors = 0
        self.window_stored = 0
        self.window_started = time.monotonic()
        self.started = time.monotonic()
        self.running = False
    
    def run(self):
        """Chạy storage loop"""
        self.running = True
        self._connect()
        self._init_tables()
        logger.info("Storage started")
        
        while self.running:
            try:
                item = self.in_queue.get(timeout=1)
            except queue.Empty:
                # Check flush interval
                if time.time() - self.last_flush >= self.flush_interval and self.buffer:
                    self._flush()
                continue
            
            if item is None:
                self.running = False
                self._flush()  # Flush remaining
                break
            
            if isinstance(item, tuple):
                msg_type, payload = item
                if msg_type == "alert":
                    self._buffer_alert(payload)
                else:
                    self._buffer_event(payload)
            else:
                # Direct payload
                self._buffer_event(item)
        
        self._finalize()
        logger.info("Storage stopped")
    
    def _connect(self):
        """Kết nối database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = False
            logger.info("Database connected")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    @contextmanager
    def _cursor(self):
        """Context manager cho cursor"""
        if self.conn is None or self.conn.closed:
            self._connect()
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cur
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise
        finally:
            cur.close()
    
    def _init_tables(self):
        """Khởi tạo bảng"""
        with self._cursor() as cur:
            # Raw events table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw_events (
                    id BIGSERIAL PRIMARY KEY,
                    dev_id VARCHAR(100) NOT NULL,
                    khu_cn VARCHAR(10) NOT NULL,
                    source_name VARCHAR(50),
                    ts VARCHAR(50),
                    tsunix BIGINT,
                    received_at BIGINT,
                    payload JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_raw_events_dev_id ON raw_events(dev_id);
                CREATE INDEX IF NOT EXISTS idx_raw_events_khu_cn ON raw_events(khu_cn);
                CREATE INDEX IF NOT EXISTS idx_raw_events_tsunix ON raw_events(tsunix);
            """)
            
            # Violations table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS violations (
                    id BIGSERIAL PRIMARY KEY,
                    dev_id VARCHAR(100) NOT NULL,
                    khu_cn VARCHAR(10) NOT NULL,
                    group_name VARCHAR(50),
                    violation_type VARCHAR(50),
                    field_name VARCHAR(100),
                    value NUMERIC,
                    min_threshold NUMERIC,
                    max_threshold NUMERIC,
                    unit VARCHAR(20),
                    streak INT,
                    payload JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_violations_dev_id ON violations(dev_id);
                CREATE INDEX IF NOT EXISTS idx_violations_khu_cn ON violations(khu_cn);
                CREATE INDEX IF NOT EXISTS idx_violations_created_at ON violations(created_at);
            """)
            
            # Alerts table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id BIGSERIAL PRIMARY KEY,
                    alert_type VARCHAR(50),
                    dev_id VARCHAR(100),
                    khu_cn VARCHAR(10),
                    group_name VARCHAR(50),
                    streak INT,
                    payload JSONB,
                    sent_channels JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_alerts_dev_id ON alerts(dev_id);
                CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts(created_at);
            """)
            
            logger.info("Database tables initialized")
    
    def _buffer_event(self, payload: Dict):
        """Buffer raw event"""
        self.buffer.append(("event", payload))
        if len(self.buffer) >= self.batch_size:
            self._flush()
    
    def _buffer_alert(self, alert: Dict):
        """Buffer alert"""
        self.buffer.append(("alert", alert))
        if len(self.buffer) >= self.batch_size:
            self._flush()
    
    def _flush(self):
        """Flush buffer to database"""
        if not self.buffer:
            return
        
        events = [item for item in self.buffer if item[0] == "event"]
        alerts = [item for item in self.buffer if item[0] == "alert"]
        
        try:
            if events:
                self._insert_events(events)
            if alerts:
                self._insert_alerts(alerts)
            
            self.conn.commit()
            self.total_stored += len(self.buffer)
            self.window_stored += len(self.buffer)
            self.buffer.clear()
            self.last_flush = time.time()
        except Exception as e:
            logger.error(f"Flush failed: {e}")
            self.total_errors += len(self.buffer)
            self.buffer.clear()
            try:
                self.conn.rollback()
            except:
                pass
    
    def _insert_events(self, events: List):
        """Insert raw events batch"""
        if not events:
            return
        
        with self._cursor() as cur:
            data = []
            for _, payload in events:
                data.append((
                    payload.get("dev_id"),
                    payload.get("khu_cn"),
                    payload.get("source_name"),
                    payload.get("ts"),
                    payload.get("tsunix"),
                    payload.get("received_at"),
                    json.dumps(payload),
                ))
            
            execute_batch(cur, """
                INSERT INTO raw_events (dev_id, khu_cn, source_name, ts, tsunix, received_at, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, data)
    
    def _insert_alerts(self, alerts: List):
        """Insert alerts batch"""
        if not alerts:
            return
        
        with self._cursor() as cur:
            data = []
            for _, alert in alerts:
                data.append((
                    alert.get("type"),
                    alert.get("device_id"),
                    alert.get("khu_cn"),
                    alert.get("group"),
                    alert.get("streak"),
                    json.dumps(alert),
                    json.dumps(["telegram", "email"]),  # channels sent
                ))
            
            execute_batch(cur, """
                INSERT INTO alerts (alert_type, dev_id, khu_cn, group_name, streak, payload, sent_channels)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, data)
    
    def _maybe_log_window(self):
        now = time.monotonic()
        elapsed = now - self.window_started
        if elapsed >= 10:
            logger.info(
                f"[STORAGE] window={elapsed:.1f}s stored={self.window_stored} "
                f"buffer={len(self.buffer)} rate={self.window_stored/elapsed:.1f} msg/s"
            )
            self.window_stored = 0
            self.window_started = now
    
    def _finalize(self):
        self._flush()
        elapsed = time.monotonic() - self.started
        logger.info(
            f"[STORAGE] DONE stored={self.total_stored} errors={self.total_errors} "
            f"avg_rate={self.total_stored/elapsed:.1f} msg/s elapsed={elapsed:.1f}s"
        )
        if self.conn:
            self.conn.close()
    
    def summary_dict(self) -> dict:
        return {
            "total_stored": self.total_stored,
            "total_errors": self.total_errors,
        }


if __name__ == "__main__":
    import queue
    import sys
    import os
    
    # Test with in-memory SQLite if PostgreSQL not available
    logging.basicConfig(level=logging.INFO)
    
    q_in = queue.Queue()
    q_out = queue.Queue()
    
    # Test without actual DB
    logger.info("Storage module test - requires PostgreSQL to run")