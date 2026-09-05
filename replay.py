"""
replay.py
----------
Test tăng tải bằng cách nhân bản (replay) data thật từ main.py.
Mục tiêu: giả lập ĐÚNG tốc độ mục tiêu (msg/s) + đo throughput thực tế.

Cách chạy:
    python replay.py --input data/raw/raw_events_XXXX.jsonl --rate 20000 --duration 1200

Cơ chế rate-limiting:
    - Dùng Token Bucket per worker thay vì time.sleep()
    - Busy-wait (spin-loop) để tránh Windows timer resolution 15ms
    - Mỗi worker tự throttle ở mức (rate / num_workers) msg/s
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from collections import Counter
from itertools import cycle, islice
from multiprocessing import Process, Queue
from pathlib import Path

from schemas import validate_event


BASE_DIR = Path(__file__).parent
REPLAY_LOG_DIR = BASE_DIR / "logs" / "replay"
REPLAY_LOG_DIR.mkdir(parents=True, exist_ok=True)

# Khoảng thời gian report progress (giây)
REPORT_INTERVAL_S = 5


# ---------------------------------------------------------------
# Token Bucket — rate limiter không dùng sleep
# ---------------------------------------------------------------

class TokenBucket:
    """
    Token Bucket đơn giản dùng busy-wait.
    Tránh time.sleep() vì trên Windows độ phân giải ~15ms gây sai lệch
    nghiêm trọng khi delay_per_msg < 15ms (tức rate > ~67 msg/s/worker).
    """

    def __init__(self, rate_per_s: float):
        self.rate = rate_per_s          # token/giây
        self.tokens = rate_per_s        # bắt đầu đầy
        self.last_refill = time.monotonic()
        self.max_tokens = rate_per_s    # bucket size = 1 giây

    def consume(self) -> None:
        """Block (busy-wait) cho đến khi có token."""
        while True:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.tokens = min(self.max_tokens, self.tokens + elapsed * self.rate)
            self.last_refill = now

            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            # Nhường CPU một chút để tránh 100% spin
            # time.sleep(0) = yield to OS scheduler (không sleep thật)
            time.sleep(0)


# ---------------------------------------------------------------
# Worker process
# ---------------------------------------------------------------

def worker_task(
    worker_id: int,
    base_records: list[dict],
    total_per_worker: int,
    worker_rate: float,
    queue: Queue,
    report_every: int,
) -> None:
    """
    Worker process: validate message và throttle theo worker_rate msg/s.
    Gửi progress qua queue về main process.
    """
    valid_count = 0
    invalid_count = 0
    error_types: Counter = Counter()

    bucket = TokenBucket(worker_rate) if worker_rate > 0 else None

    stream = islice(cycle(base_records), total_per_worker)
    started = time.monotonic()
    last_report_time = started

    for i, record in enumerate(stream, start=1):
        # Throttle: chờ token trước khi xử lý
        if bucket is not None:
            bucket.consume()

        result = validate_event(record)
        if result.is_valid:
            valid_count += 1
        else:
            invalid_count += 1
            error_types[result.error_type] += 1

        # Report định kỳ theo số message hoặc theo thời gian
        if i % report_every == 0:
            now = time.monotonic()
            elapsed = now - started
            rate_actual = i / elapsed if elapsed > 0 else 0
            queue.put({
                "type": "progress",
                "worker_id": worker_id,
                "processed": i,
                "valid": valid_count,
                "invalid": invalid_count,
                "rate": round(rate_actual, 1),
                "elapsed": round(elapsed, 1),
            })
            last_report_time = now

    elapsed = time.monotonic() - started
    rate_final = total_per_worker / elapsed if elapsed > 0 else 0

    # Gửi kết quả cuối cùng
    queue.put({
        "type": "done",
        "worker_id": worker_id,
        "processed": total_per_worker,
        "valid": valid_count,
        "invalid": invalid_count,
        "errors": dict(error_types),
        "elapsed": round(elapsed, 3),
        "rate_final": round(rate_final, 1),
    })


# ---------------------------------------------------------------
# Main replay logic
# ---------------------------------------------------------------

def load_records(input_path: Path) -> list[dict]:
    records = []
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def replay(input_path: Path, rate: int, duration: int, num_workers: int = 12):
    total_messages = rate * duration
    log_filename = f"replay_{rate}msps_{duration}s"

    # --- Logger setup ---
    logger = logging.getLogger("replay")
    log_path = REPLAY_LOG_DIR / f"{log_filename}.log"
    if not logger.handlers:
        handler = logging.FileHandler(log_path, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(message)s"))
        logger.addHandler(handler)
        logger.addHandler(logging.StreamHandler())
        logger.setLevel(logging.INFO)

    # --- Load data ---
    base_records = load_records(input_path)
    if not base_records:
        logger.error("Cannot load records from %s", input_path)
        return

    logger.info("Loaded %d base records from %s", len(base_records), input_path)
    logger.info(
        "Target: %d messages | Rate: %d msg/s | Duration: %ds | Workers: %d",
        total_messages, rate, duration, num_workers,
    )

    # --- Phân chia tải ---
    total_per_worker = total_messages // num_workers
    # Phần dư (nếu có) phân vào worker 0
    remainder = total_messages - total_per_worker * num_workers

    worker_rate = rate / num_workers  # float, chính xác hơn integer division

    # report mỗi ~10 giây hoặc tối thiểu 10k msg
    report_every = max(10_000, int(worker_rate * 10))

    logger.info(
        "Each worker: %d msgs, rate=%.1f msg/s, report_every=%d msgs",
        total_per_worker, worker_rate, report_every,
    )

    # --- Khởi động workers ---
    q: Queue = Queue()
    processes = []
    started = time.monotonic()

    for i in range(num_workers):
        msgs = total_per_worker + (remainder if i == 0 else 0)
        p = Process(
            target=worker_task,
            args=(i, base_records, msgs, worker_rate, q, report_every),
            daemon=True,
        )
        processes.append(p)
        p.start()

    logger.info("All %d workers started.", num_workers)

    # --- Collect progress từ queue ---
    worker_progress: dict[int, dict] = {}
    worker_done: dict[int, dict] = {}
    last_log_time = started

    while len(worker_done) < num_workers:
        # Drain hết queue hiện tại (non-blocking)
        drained = 0
        while True:
            try:
                msg = q.get_nowait()
                wid = msg["worker_id"]
                if msg["type"] == "progress":
                    worker_progress[wid] = msg
                elif msg["type"] == "done":
                    worker_done[wid] = msg
                    worker_progress[wid] = msg
                    logger.info(
                        "Worker %d DONE: %d msgs, valid=%d, invalid=%d, "
                        "elapsed=%.1fs, rate=%.1f msg/s",
                        wid, msg["processed"], msg["valid"], msg["invalid"],
                        msg["elapsed"], msg["rate_final"],
                    )
                drained += 1
            except Exception:
                break

        # Log tổng hợp mỗi REPORT_INTERVAL_S giây
        now = time.monotonic()
        if now - last_log_time >= REPORT_INTERVAL_S:
            elapsed = now - started
            current_total = sum(p.get("processed", 0) for p in worker_progress.values())
            rate_actual = current_total / elapsed if elapsed > 0 else 0
            pct = current_total / total_messages * 100 if total_messages > 0 else 0
            logger.info(
                "Progress: %d/%d (%.1f%%) | actual=%.1f msg/s | target=%d msg/s | elapsed=%.0fs",
                current_total, total_messages, pct, rate_actual, rate, elapsed,
            )
            last_log_time = now

        # Nếu không drain được gì, sleep nhỏ để tránh busy-loop ở main
        if drained == 0:
            time.sleep(0.1)

    # --- Join workers ---
    for p in processes:
        p.join(timeout=5)

    elapsed = time.monotonic() - started

    # --- Tổng hợp kết quả ---
    total_valid = sum(d.get("valid", 0) for d in worker_done.values())
    total_invalid = sum(d.get("invalid", 0) for d in worker_done.values())
    total_errors: Counter = Counter()
    for d in worker_done.values():
        total_errors.update(d.get("errors", {}))

    total_processed = total_valid + total_invalid
    avg_rate = total_processed / elapsed if elapsed > 0 else 0
    rate_accuracy_pct = avg_rate / rate * 100 if rate > 0 else 0

    summary = {
        "target_messages": total_messages,
        "target_rate_msg_per_s": rate,
        "target_duration_seconds": duration,
        "num_workers": num_workers,
        "base_records_loaded": len(base_records),
        "total_processed": total_processed,
        "valid": total_valid,
        "invalid": total_invalid,
        "elapsed_seconds": round(elapsed, 3),
        "avg_rate_msg_per_s": round(avg_rate, 1),
        "rate_accuracy_pct": round(rate_accuracy_pct, 2),
        "top_error_types": dict(total_errors.most_common(5)),
    }

    logger.info("=" * 60)
    logger.info("DONE | %s", json.dumps(summary, ensure_ascii=False))
    logger.info("Rate accuracy: %.2f%% (actual %.1f / target %d msg/s)",
                rate_accuracy_pct, avg_rate, rate)

    summary_path = REPLAY_LOG_DIR / f"{log_filename}_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"\n[Summary saved] {summary_path}")


# ---------------------------------------------------------------
# CLI
# ---------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Replay data local với multiprocessing + token-bucket rate limiting"
    )
    parser.add_argument("--input", required=True, help="File raw_events_*.jsonl từ main.py")
    parser.add_argument("--rate", type=int, required=True, help="Target rate (msg/s), e.g. 20000")
    parser.add_argument("--duration", type=int, required=True, help="Thời gian (giây), e.g. 1200")
    parser.add_argument("--workers", type=int, default=10,
                        help="Số worker processes (default: 10)")
    args = parser.parse_args()

    replay(Path(args.input), args.rate, args.duration, args.workers)


if __name__ == "__main__":
    main()
