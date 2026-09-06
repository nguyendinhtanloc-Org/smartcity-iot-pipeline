"""
main.py
--------
Chạy LIVE: kết nối MQTT thật, Ingestion đẩy vào queue, Validation
tiêu thụ tuần tự (chạy trên thread riêng để không block MQTT
network loop). Sau khi hết thời gian chạy, ghi summary.json.

Cách chạy (ví dụ 20 phút = 1200 giây):

    python3 main.py \
        --host dathoc.net --port 443 --ws-path /mq \
        --username test1 --password '123456' \
        --topic 'qa-smartcity/#' \
        --duration 1200

Kết quả:
    logs/ingest.log      - log chặng Ingestion
    logs/validate.log    - log chặng Validation
    logs/summary.json    - tổng hợp số liệu cuối cùng
    data/raw/raw_events_<timestamp>.jsonl - data thật lưu local
                                             (dùng cho replay.py)
    logs/invalid_events.jsonl - message không hợp lệ
    logs/dead_letter.jsonl    - message lỗi liên tục (poison pill)
"""

from __future__ import annotations

import argparse
import json
import logging
import queue
import threading
import time
from pathlib import Path

from ingest import Ingestor
from validate_stage import Validator


BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"
RAW_DIR = BASE_DIR / "data" / "raw"
LOG_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)


def setup_logging():
    fmt = "%(asctime)s [%(name)s] %(levelname)s %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8"),
    ])

    # log riêng theo từng chặng, để nộp bài đúng yêu cầu
    # "log riêng .log file cho từng chặng"
    for name, filename in [("ingestion", "ingest.log"), ("validation", "validate.log")]:
        logger = logging.getLogger(name)
        handler = logging.FileHandler(LOG_DIR / filename, encoding="utf-8")
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)


def main():
    parser = argparse.ArgumentParser(description="Ingestion + Validation live pipeline")
    parser.add_argument("--host", default="dathoc.net")
    parser.add_argument("--port", type=int, default=443)
    parser.add_argument("--ws-path", default="/mq")
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--topic", default="v1/C001/+/up/telemetry")
    parser.add_argument("--qos", type=int, default=0)
    parser.add_argument("--client-id", default=None)
    parser.add_argument("--insecure", action="store_true")
    parser.add_argument("--duration", type=int, default=1200, help="Thời gian chạy (giây), mặc định 1200s = 20 phút")
    parser.add_argument("--queue-maxsize", type=int, default=20000, help="Bounded queue để tránh OOM")
    args = parser.parse_args()

    if not args.client_id:
        args.client_id = f"ingest-validate-{int(time.time())}"

    setup_logging()

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    raw_path = RAW_DIR / f"raw_events_{timestamp}.jsonl"
    invalid_path = LOG_DIR / "invalid_events.jsonl"
    dead_letter_path = LOG_DIR / "dead_letter.jsonl"

    internal_queue: "queue.Queue" = queue.Queue(maxsize=args.queue_maxsize)

    ingestor = Ingestor(args, internal_queue, raw_path)
    validator = Validator(internal_queue, invalid_path, dead_letter_path)

    # Validation chạy trên thread riêng, đọc tuần tự từ queue —
    # tách khỏi MQTT network loop để không làm nghẽn nhận message.
    validate_thread = threading.Thread(target=validator.run_from_queue, daemon=True)
    validate_thread.start()

    ingestor.run(duration_seconds=args.duration)

    # Đợi Validation xử lý nốt phần còn lại trong queue
    validate_thread.join(timeout=60)

    summary = {
        "run_timestamp": timestamp,
        "duration_seconds": args.duration,
        "ingestion": {
            "total_received": ingestor.count,
        },
        "validation": validator.summary_dict(),
        "raw_data_file": str(raw_path),
    }

    with open(LOG_DIR / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
