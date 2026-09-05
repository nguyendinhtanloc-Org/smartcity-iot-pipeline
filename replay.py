"""
replay.py
----------
Test tăng tải TUẦN TỰ bằng cách nhân bản (replay) chính bộ data
thật đã thu được từ main.py (data/raw/raw_events_*.jsonl).

Không tạo tải song song thật, không dựng nhiều process/worker —
chỉ lặp qua danh sách message thật, đẩy tuần tự qua đúng logic
validate_event() giống hệt lúc chạy live, để kiểm tra pipeline
giữ đúng logic khi số lượng message tăng lên.

Cách chạy:

    python3 replay.py --input data/raw/raw_events_XXXX.jsonl --target 10000
    python3 replay.py --input data/raw/raw_events_XXXX.jsonl --target 100000

Kết quả:
    logs/replay/replay_<target>.log
    logs/replay/replay_<target>_summary.json
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from collections import Counter
from itertools import cycle, islice
from pathlib import Path

from schemas import validate_event


BASE_DIR = Path(__file__).parent
REPLAY_LOG_DIR = BASE_DIR / "logs" / "replay"
REPLAY_LOG_DIR.mkdir(parents=True, exist_ok=True)


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


def replay(input_path: Path, target: int):
    logger = logging.getLogger(f"replay_{target}")
    log_path = REPLAY_LOG_DIR / f"replay_{target}.log"
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(message)s"))
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    base_records = load_records(input_path)
    if not base_records:
        logger.error("Không đọc được record nào từ %s", input_path)
        return

    logger.info("Load %d record gốc từ %s, nhân bản lên target=%d (tuần tự, không song song)",
                len(base_records), input_path, target)

    valid_count = 0
    invalid_count = 0
    error_type_counter: Counter = Counter()

    window_count = 0
    window_started = time.monotonic()
    started = time.monotonic()

    # Lặp tuần tự qua base_records, quay vòng (cycle) tới khi đủ target
    stream = islice(cycle(base_records), target)

    for i, record in enumerate(stream, start=1):
        result = validate_event(record)
        if result.is_valid:
            valid_count += 1
        else:
            invalid_count += 1
            error_type_counter[result.error_type] += 1

        window_count += 1
        now = time.monotonic()
        elapsed_window = now - window_started
        if elapsed_window >= 5 or i == target:
            rate = window_count / elapsed_window if elapsed_window > 0 else 0
            logger.info(
                "processed=%d/%d valid=%d invalid=%d window_rate=%.1f msg/s",
                i, target, valid_count, invalid_count, rate,
            )
            window_count = 0
            window_started = now

    elapsed = time.monotonic() - started
    total = valid_count + invalid_count
    avg_rate = total / elapsed if elapsed > 0 else 0

    summary = {
        "target": target,
        "base_records_loaded": len(base_records),
        "total_processed": total,
        "valid": valid_count,
        "invalid": invalid_count,
        "elapsed_seconds": round(elapsed, 3),
        "avg_rate_msg_per_s": round(avg_rate, 1),
        "top_error_types": dict(error_type_counter.most_common(5)),
    }

    logger.info("DONE %s", json.dumps(summary, ensure_ascii=False))

    with open(REPLAY_LOG_DIR / f"replay_{target}_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description="Replay data local để test tải tăng dần (tuần tự)")
    parser.add_argument("--input", required=True, help="File raw_events_*.jsonl từ main.py")
    parser.add_argument("--target", type=int, required=True, help="Số message mục tiêu (vd 10000, 100000)")
    args = parser.parse_args()

    replay(Path(args.input), args.target)


if __name__ == "__main__":
    main()
