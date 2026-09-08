#!/usr/bin/env python3
"""
baseline.py
-----------
BLEASE GIAI ĐOẠN 0 - Đo tốc độ THỰC TẾ từ broker TRƯỚC khi chạy Ingestion.

Kết nối MQTT, subscribe, đếm message trong duration_second,
không validate, không lưu payload — chỉ đo throughput thuần của broker.
Log kết quả ra file + summary.json để đối chiếu với throughput sau Ingestion.

Usage:
    python3 baseline.py --host dathoc.net --port 443 --ws-path /mq \
        --username test1 --password '123456' \
        --topic 'v1/C001/+/up/telemetry' \
        --duration 1200 --insecure

Output:
    logs/baseline/baseline_<timestamp>.log
    logs/baseline/baseline_<timestamp>_summary.json
"""

from __future__ import annotations

import argparse
import json
import logging
import socket
import ssl
import sys
import time
from collections import Counter
from pathlib import Path

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("Missing paho-mqtt. Install: pip3 install paho-mqtt", file=sys.stderr)
    sys.exit(2)

BASE_DIR = Path(__file__).parent
BASELINE_LOG_DIR = BASE_DIR / "logs" / "baseline"
BASELINE_LOG_DIR.mkdir(parents=True, exist_ok=True)


def parse_topic(topic: str) -> dict:
    """Tách topic v1/{company}/{gateway}/up/telemetry thành company + gateway."""
    parts = topic.split("/")
    company_id = parts[1] if len(parts) > 1 else ""
    gateway = parts[2] if len(parts) > 2 else ""
    return {"company_id": company_id, "gateway": gateway, "topic": topic}


class BaselineCounter:
    def __init__(self):
        self.count = 0
        self.started = time.monotonic()
        self.by_company: Counter = Counter()
        self.by_gateway: Counter = Counter()
        self.last_window_count = 0
        self.last_window_time = self.started
        self.logger = logging.getLogger("baseline")

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc != 0:
            self.logger.error("CONNECT FAILED rc=%s", rc)
            return
        self.logger.info("CONNECTED")
        topics = userdata["topics"]
        if len(topics) == 1:
            result, mid = client.subscribe(topics[0], qos=0)
            self.logger.info("SUBSCRIBED topic=%s mid=%s", topics[0], mid)
        else:
            result, mid = client.subscribe([(t, 0) for t in topics])
            self.logger.info("SUBSCRIBED topics=%s mid=%s", topics, mid)

    def on_message(self, client, userdata, msg):
        self.count += 1
        info = parse_topic(msg.topic)
        self.by_company[info["company_id"]] += 1
        self.by_gateway[info["gateway"]] += 1

        if self.count <= 3:
            payload = msg.payload.decode("utf-8", errors="replace")
            self.logger.info("sample [%d] %s %s", self.count, msg.topic, payload[:150])

        # Report window mỗi 10 giây
        now = time.monotonic()
        elapsed = now - self.last_window_time
        if elapsed >= 10:
            delta = self.count - self.last_window_count
            rate = delta / elapsed if elapsed > 0 else 0
            rate_all = self.count / (now - self.started) if (now - self.started) > 0 else 0
            self.logger.info(
                "window=%.1fs delta=%d rate=%.1f msg/s avg_rate=%.1f msg/s total=%d",
                elapsed, delta, rate, rate_all, self.count,
            )
            self.last_window_count = self.count
            self.last_window_time = now


def main():
    parser = argparse.ArgumentParser(description="Baseline: đo tốc độ MQTT trước khi Ingestion")
    parser.add_argument("--host", default="dathoc.net")
    parser.add_argument("--port", type=int, default=443)
    parser.add_argument("--ws-path", default="/mq")
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--topic", action="append", required=True,
                        help="Topic đo (có thể truyền nhiều lần, mỗi lần 1 CN)")
    parser.add_argument("--duration", type=int, default=1200, help="Thời gian đo (giây)")
    parser.add_argument("--insecure", action="store_true")
    args = parser.parse_args()

    if args.duration <= 0:
        print("--duration phải > 0", file=sys.stderr)
        sys.exit(1)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    log_file = BASELINE_LOG_DIR / f"baseline_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    logger = logging.getLogger("baseline")

    counter = BaselineCounter()

    topics = list(args.topic)

    try:
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"baseline-{int(time.time())}",
            transport="websockets",
        )
    except Exception:
        client = mqtt.Client(
            client_id=f"baseline-{int(time.time())}",
            transport="websockets",
        )

    client.username_pw_set(args.username, args.password)
    client.ws_set_options(path=args.ws_path)

    if args.insecure:
        client.tls_set(cert_reqs=ssl.CERT_NONE)
    else:
        client.tls_set()

    client.on_connect = counter.on_connect
    client.on_message = counter.on_message
    client.user_data_set({"topics": topics})

    original_getaddrinfo = socket.getaddrinfo

    def ipv4_only(host, port, family=0, type=0, proto=0, flags=0):
        if host == args.host:
            return original_getaddrinfo(host, port, socket.AF_INET, type or socket.SOCK_STREAM, proto, flags)
        return original_getaddrinfo(host, port, family, type, proto, flags)

    socket.getaddrinfo = ipv4_only

    logger.info("=== BASELINE TEST ===")
    logger.info("Broker: %s:%s%s", args.host, args.port, args.ws_path)
    logger.info("Topics: %s", topics)
    logger.info("Duration: %ds", args.duration)
    logger.info("Start: %s", time.strftime("%Y-%m-%d %H:%M:%S"))

    started = time.monotonic()
    try:
        client.connect(args.host, args.port, keepalive=60)
        client.loop_start()
        time.sleep(args.duration)
        client.loop_stop()
    except KeyboardInterrupt:
        logger.info("STOPPED by user")
    except Exception as e:
        logger.error("Connection error: %s", e)
    finally:
        socket.getaddrinfo = original_getaddrinfo
        client.disconnect()

        elapsed = time.monotonic() - started
        rate = counter.count / elapsed if elapsed > 0 else 0

        summary = {
            "test": "baseline_pre_ingestion",
            "timestamp": timestamp,
            "broker": {"host": args.host, "port": args.port, "ws_path": args.ws_path},
            "topics": topics,
            "duration_expected_seconds": args.duration,
            "elapsed_seconds": round(elapsed, 3),
            "total_messages": counter.count,
            "throughput_msg_per_s": round(rate, 2),
            "per_company": dict(counter.by_company),
            "per_gateway": dict(counter.by_gateway),
        }

        logger.info("=== BASELINE RESULT ===")
        logger.info("Total messages: %s", f"{counter.count:,}")
        logger.info("Elapsed: %.1fs", elapsed)
        logger.info("Throughput: %.1f msg/s", rate)
        logger.info("Per company: %s", dict(counter.by_company))
        logger.info("Per gateway: %s", dict(counter.by_gateway))

        if counter.count == 0:
            logger.warning("Không nhận được message nào!")
            logger.warning("  - Kiểm tra broker có simulator đang chạy không (online 9h-19h VN)")
        elif rate < 100:
            logger.info("Tốc độ thấp (%.0f msg/s), broker có thể đang idle", rate)
        else:
            logger.info("Tốc độ ~%.0f msg/s", rate)

        summary_path = BASELINE_LOG_DIR / f"baseline_{timestamp}_summary.json"
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        logger.info("Summary saved: %s", summary_path)


if __name__ == "__main__":
    main()