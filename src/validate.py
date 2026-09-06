"""
validate.py
-----------
Chặng Validation - Schema check + Range check + Poison pill handling
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Optional

from schemas import validate_event, UnifiedTelemetry, ValidationResult

logger = logging.getLogger("validation")


class Validator:
    """Chặng Validation - tiêu thụ từ ingestion queue, validate, đẩy đến detect"""
    
    def __init__(
        self,
        in_queue: "queue.Queue",
        out_queue: "queue.Queue",
        invalid_path: Path,
        dead_letter_path: Path,
    ):
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.invalid_path = invalid_path
        self.dead_letter_path = dead_letter_path
        
        self.valid_count = 0
        self.invalid_count = 0
        self.error_type_counter: Counter = Counter()
        self.device_error_streak: Counter = Counter()
        self.RETRY_LIMIT = 3
        
        self._invalid_file = open(self.invalid_path, "a", encoding="utf-8")
        self._dead_letter_file = open(self.dead_letter_path, "a", encoding="utf-8")
        
        self.window_valid = 0
        self.window_invalid = 0
        self.window_started = time.monotonic()
        self.started = time.monotonic()
        self.running = False
    
    def run(self):
        """Chạy validation loop"""
        self.running = True
        logger.info("Validator started")
        
        while self.running:
            try:
                item = self.in_queue.get(timeout=1)
            except queue.Empty:
                continue
            
            if item is None:  # Poison pill - shutdown signal
                self.running = False
                self.out_queue.put(None)  # Pass to next stage
                break
            
            topic, payload = item
            self._process(topic, payload)
        
        self._finalize()
        logger.info("Validator stopped")
    
    def _process(self, topic: str, payload: dict):
        # Validate với UnifiedTelemetry (có khu_cn, source_name)
        result = validate_event(payload, UnifiedTelemetry)
        
        if result.is_valid:
            self.valid_count += 1
            self.window_valid += 1
            # Reset error streak
            if result.device_id:
                self.device_error_streak[result.device_id] = 0
            
            # Push to detect queue
            self.out_queue.put((topic, payload))
        else:
            self.invalid_count += 1
            self.window_invalid += 1
            self.error_type_counter[result.error_type] += 1
            
            # Write invalid event
            self._invalid_file.write(json.dumps({
                "topic": topic,
                "error_type": result.error_type,
                "error_detail": result.error_detail,
                "device_id": result.device_id,
                "khu_cn": result.khu_cn,
                "raw": payload,
            }, ensure_ascii=False) + "\n")
            
            # Poison pill handling
            if result.device_id:
                self.device_error_streak[result.device_id] += 1
                if self.device_error_streak[result.device_id] >= 3:
                    self._dead_letter_file.write(json.dumps(payload, ensure_ascii=False) + "\n")
                    self.device_error_streak[result.device_id] = 0
        
        self._maybe_log_window()
    
    def _maybe_log_window(self):
        now = time.monotonic()
        elapsed = now - self.window_started
        if elapsed >= 10:
            total = self.window_valid + self.window_invalid
            rate = total / elapsed if elapsed > 0 else 0
            logger.info(
                f"[VALIDATE] window={elapsed:.1f}s valid={self.window_valid} invalid={self.window_invalid} "
                f"rate={rate:.1f} msg/s total_valid={self.valid_count} total_invalid={self.invalid_count}"
            )
            self.window_valid = 0
            self.window_invalid = 0
            self.window_started = now
    
    def _finalize(self):
        elapsed = time.monotonic() - self.started
        total = self.valid_count + self.invalid_count
        rate = total / elapsed if elapsed > 0 else 0
        error_rate = (self.invalid_count / total * 100) if total else 0
        
        logger.info(
            f"[VALIDATE] DONE total={total} valid={self.valid_count} invalid={self.invalid_count} "
            f"error_rate={error_rate:.2f}% avg_rate={rate:.1f} msg/s elapsed={elapsed:.1f}s "
            f"top_errors={dict(self.error_type_counter.most_common(3))}"
        )
        
        self._invalid_file.close()
        self._dead_letter_file.close()
    
    def summary_dict(self) -> dict:
        total = self.valid_count + self.invalid_count
        elapsed = time.monotonic() - self.started
        return {
            "total_processed": total,
            "valid": self.valid_count,
            "invalid": self.invalid_count,
            "error_rate_pct": round(self.invalid_count / total * 100, 2) if total else 0,
            "elapsed_seconds": round(elapsed, 2),
            "avg_rate_msg_per_s": round(total / elapsed, 2) if elapsed > 0 else 0,
            "top_error_types": dict(self.error_type_counter.most_common(5)),
        }


if __name__ == "__main__":
    # Test standalone
    import queue
    logging.basicConfig(level=logging.INFO)
    
    q_in = queue.Queue()
    q_out = queue.Queue()
    validator = Validator(q_in, q_out, Path("logs/invalid.jsonl"), Path("logs/dead_letter.jsonl"))
    
    # Test data
    test_data = {"dev_id": "TEST-001", "khu_cn": "A", "source_name": "CN_A", "ts": "2026-09-07T10:00:00", "tsunix": 1788694281, "U": 220, "I": 10, "Power_kW": 2.2, "Energy_kwh": 100, "Alr_Current": 0, "Alr_Volt": 0, "Mode_c": 1, "Line 8": 1, "Line 9": 0, "Line 10": 1}
    
    q_in.put(("test/topic", test_data))
    q_in.put(None)
    
    validator.run()
    print(validator.summary_dict())