"""
validate_stage.py
-------------------
Chặng VALIDATION — tiêu thụ message từ queue nội bộ (do Ingestion
đẩy vào), validate bằng schemas.validate_event(), phân loại
valid/invalid, log thống kê và ghi message lỗi ra file riêng.
"""

from __future__ import annotations

import json
import logging
import queue
import time
from collections import Counter
from pathlib import Path

from schemas import validate_event

logger = logging.getLogger("validation")


class Validator:
    def __init__(self, in_queue: "queue.Queue", invalid_log_path: Path, dead_letter_path: Path):
        self.in_queue = in_queue
        self.invalid_log_path = invalid_log_path
        self.dead_letter_path = dead_letter_path

        self.valid_count = 0
        self.invalid_count = 0
        self.error_type_counter: Counter = Counter()

        self.window_valid = 0
        self.window_invalid = 0
        self.window_started = time.monotonic()
        self.started = time.monotonic()

        self._invalid_file = open(self.invalid_log_path, "a", encoding="utf-8")
        self._dead_letter_file = open(self.dead_letter_path, "a", encoding="utf-8")

        # retry-limit cho message lỗi liên tục cùng 1 device (poison pill)
        self._error_streak_by_device: Counter = Counter()
        self.RETRY_LIMIT = 3

    def process_one(self, topic: str, payload: dict):
        result = validate_event(payload)

        if result.is_valid:
            self.valid_count += 1
            self.window_valid += 1
            # reset streak lỗi của device này nếu có
            if result.device_id:
                self._error_streak_by_device[result.device_id] = 0
        else:
            self.invalid_count += 1
            self.window_invalid += 1
            self.error_type_counter[result.error_type] += 1

            self._invalid_file.write(
                json.dumps({
                    "topic": topic,
                    "error_type": result.error_type,
                    "error_detail": result.error_detail,
                    "device_id": result.device_id,
                    "group": result.group,
                    "raw": payload,
                }, ensure_ascii=False) + "\n"
            )

            # Poison pill handling: nếu 1 device lỗi liên tục quá
            # RETRY_LIMIT lần, đẩy sang dead-letter riêng để không
            # làm nghẽn log lỗi chính, không retry vô hạn.
            if result.device_id:
                self._error_streak_by_device[result.device_id] += 1
                if self._error_streak_by_device[result.device_id] >= self.RETRY_LIMIT:
                    self._dead_letter_file.write(json.dumps(payload, ensure_ascii=False) + "\n")
                    self._error_streak_by_device[result.device_id] = 0

        self._maybe_log_window()

    def _maybe_log_window(self):
        now = time.monotonic()
        elapsed = now - self.window_started
        if elapsed >= 10:
            total = self.window_valid + self.window_invalid
            rate = total / elapsed if elapsed > 0 else 0
            logger.info(
                "[VALIDATE] window=%.1fs valid=%d invalid=%d rate=%.1f msg/s total_valid=%d total_invalid=%d",
                elapsed, self.window_valid, self.window_invalid, rate,
                self.valid_count, self.invalid_count,
            )
            self.window_valid = 0
            self.window_invalid = 0
            self.window_started = now

    def run_from_queue(self):
        """Chạy ở chế độ live: đọc tuần tự từ queue tới khi nhận sentinel None."""
        while True:
            item = self.in_queue.get()
            if item is None:
                break
            topic, payload = item
            self.process_one(topic, payload)

        self._finalize()

    def _finalize(self):
        elapsed = time.monotonic() - self.started
        total = self.valid_count + self.invalid_count
        rate = total / elapsed if elapsed > 0 else 0
        error_rate_pct = (self.invalid_count / total * 100) if total else 0

        logger.info(
            "[VALIDATE] DONE total=%d valid=%d invalid=%d error_rate=%.2f%% "
            "avg_rate=%.1f msg/s elapsed=%.1fs top_errors=%s",
            total, self.valid_count, self.invalid_count, error_rate_pct,
            rate, elapsed, dict(self.error_type_counter.most_common(3)),
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
            "error_rate_pct": round((self.invalid_count / total * 100), 2) if total else 0,
            "elapsed_seconds": round(elapsed, 2),
            "avg_rate_msg_per_s": round(total / elapsed, 2) if elapsed > 0 else 0,
            "top_error_types": dict(self.error_type_counter.most_common(5)),
        }
