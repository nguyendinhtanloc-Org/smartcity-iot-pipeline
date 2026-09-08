"""
main.py
-------
Entry point cho Multi-Source SmartCity IoT Pipeline.

Pipeline: Multi-Source Ingestion → Validate → Detect → Alert → Storage → Daily Report

Chạy:
    python main.py --config config/sources.yaml --duration 1200
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import threading
import time
from pathlib import Path

from src.ingest import MultiSourceIngestor, load_sources_config
from src.validate import Validator
from src.detect import Detector
from src.alert import Alerter
from src.storage import Storage
from src.daily_report import DailyReportGenerator
from schemas import UnifiedTelemetry, validate_event

BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"
RAW_DIR = BASE_DIR / "data" / "raw"
LOG_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)


def setup_logging(log_level: str = "INFO"):
    fmt = "%(asctime)s [%(name)s] %(levelname)s %(message)s"
    logging.basicConfig(level=getattr(logging, log_level.upper()), format=fmt, handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8"),
    ])

    # Log riêng từng chặng
    for name, filename in [
        ("ingestion", "ingest.log"),
        ("validation", "validate.log"),
        ("detect", "detect.log"),
        ("alert", "alert.log"),
        ("storage", "storage.log"),
    ]:
        logger = logging.getLogger(name)
        # Tránh thêm handler trùng lặp nếu setup_logging được gọi nhiều lần
        if not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
            handler = logging.FileHandler(LOG_DIR / filename, encoding="utf-8")
            handler.setFormatter(logging.Formatter(fmt))
            logger.addHandler(handler)


def main():
    parser = argparse.ArgumentParser(description="SmartCity IoT Pipeline - Multi-Source")
    parser.add_argument("--config", default="config/sources.yaml", help="Config file YAML")
    parser.add_argument("--duration", type=int, default=1200, help="Thời gian chạy (giây)")
    parser.add_argument("--queue-maxsize", type=int, default=100000, help="Queue max size")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    setup_logging(args.log_level)
    logger = logging.getLogger("pipeline")

    # Load config
    sources, global_config = load_sources_config(args.config)
    global_config["queue_maxsize"] = args.queue_maxsize
    global_config["log_level"] = args.log_level

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    raw_path = BASE_DIR / "data" / "raw" / f"raw_events_{timestamp}.jsonl"
    invalid_path = BASE_DIR / "logs" / "invalid_events.jsonl"
    dead_letter_path = BASE_DIR / "logs" / "dead_letter.jsonl"

    # Queue internal cho pipeline
    ingest_queue: "queue.Queue" = queue.Queue(maxsize=args.queue_maxsize)
    validate_queue: "queue.Queue" = queue.Queue(maxsize=args.queue_maxsize)
    detect_queue: "queue.Queue" = queue.Queue(maxsize=args.queue_maxsize)
    alert_queue: "queue.Queue" = queue.Queue(maxsize=args.queue_maxsize)

    # Khởi tạo các component
    ingestor = MultiSourceIngestor(
        sources_config=sources,
        global_config=global_config,
        out_queue=ingest_queue,
        raw_dir=RAW_DIR,
    )

    # Validator
    validator = Validator(
        in_queue=ingest_queue,
        out_queue=validate_queue,
        invalid_path=BASE_DIR / "logs" / "invalid_events.jsonl",
        dead_letter_path=BASE_DIR / "logs" / "dead_letter.jsonl"
    )

    # Detector
    detector = Detector(
        in_queue=validate_queue,
        out_queue=detect_queue
    )

    # Alerter
    alerter = Alerter(
        in_queue=detect_queue,
        out_queue=alert_queue
    )

    # Storage
    storage = Storage(
        in_queue=alert_queue
    )

    logger.info("=" * 60)
    logger.info("SMARTCITY IOT PIPELINE STARTING")
    logger.info("=" * 60)

    # Start all components
    threads = []

    # 1. Ingestion (multi-source)
    def run_ingestion():
        ingestor.run(duration_seconds=args.duration)
    
    ingest_thread = threading.Thread(target=run_ingestion, daemon=True)
    threads.append(("ingestion", ingest_thread))

    # 2. Validation
    validate_thread = threading.Thread(target=validator.run, daemon=True)
    threads.append(("validation", validate_thread))

    # 3. Detection
    detect_thread = threading.Thread(target=detector.run, daemon=True)
    threads.append(("detection", detect_thread))

    # 4. Alert
    alert_thread = threading.Thread(target=alerter.run, daemon=True)
    threads.append(("alert", alert_thread))

    # 5. Storage
    storage_thread = threading.Thread(target=storage.run, daemon=True)
    threads.append(("storage", storage_thread))

    # Start all threads
    for name, thread in threads:
        thread.start()
        logger.info(f"Started {name} thread")

    # Wait for ingestion to complete
    ingest_thread.join()

    # Ghi summary riêng cho chặng INGESTION
    ingest_summary = ingestor.summary_dict()
    ingest_summary["started_at"] = time.strftime(
        "%Y-%m-%d %H:%M:%S", time.localtime(time.time() - args.duration)
    )
    ingest_summary["ended_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    ingest_summary["config_file"] = args.config
    with open(BASE_DIR / "logs" / "ingest_summary.json", "w", encoding="utf-8") as f:
        json.dump(ingest_summary, f, ensure_ascii=False, indent=2)
    logger.info("Ingestion summary saved: logs/ingest_summary.json")

    # Signal downstream to finish
    ingest_queue.put(None)  # Poison pill for validator
    
    # Wait for validation
    validate_thread.join(timeout=30)
    validate_queue.put(None)
    
    # Wait for detection
    detect_thread.join(timeout=30)
    detect_queue.put(None)
    
    # Wait for alert
    alert_thread.join(timeout=30)
    alert_queue.put(None)
    
    # Wait for storage
    storage_thread.join(timeout=30)

    logger.info("All pipeline stages completed")

    # Summary - tất cả các stage
    summary = {
        "run_timestamp": time.strftime("%Y%m%d_%H%M%S"),
        "duration_seconds": args.duration,
        "pipeline_stages": ["ingestion", "validation", "detection", "alert", "storage"],
        "ingestion": ingest_summary,
        "validation": validator.summary_dict(),
        "detection": detector.summary_dict(),
        "alert": alerter.summary_dict(),
        "storage": storage.summary_dict(),
    }

    with open(BASE_DIR / "logs" / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # Generate daily report (nếu có DB)
    try:
        report_gen = DailyReportGenerator(
            db_config={
                "host": os.environ.get("DB_HOST", "localhost"),
                "port": int(os.environ.get("DB_PORT", 5432)),
                "database": os.environ.get("DB_NAME", "smartcity"),
                "user": os.environ.get("DB_USER", "postgres"),
                "password": os.environ.get("DB_PASSWORD", "postgres"),
            },
            output_dir=str(BASE_DIR / "reports"),
        )
        report = report_gen.generate_daily_report()
        summary["daily_report"] = report
        logger.info("Daily report generated")
    except Exception as e:
        logger.warning(f"Daily report generation skipped: {e}")

    # Rewrite summary with daily report
    with open(BASE_DIR / "logs" / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()