#!/usr/bin/env python3
"""
baseline.py
-----------
Đo tốc độ THỰC TẾ từ broker trước khi chạy Ingestion.

Kết nối MQTT, subscribe, đếm message trong duration_second,
không validate, không lưu — chỉ đo throughput thuần.

Usage:
    python3 baseline.py --host dathoc.net --port 443 --ws-path /mq \
        --username test1 --password '123456' --topic 'v1/C001/+/up/telemetry' \
        --duration 60 --insecure
"""

from __future__ import annotations

import argparse
import socket
import ssl
import sys
import time

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("Missing paho-mqtt. Install: pip3 install paho-mqtt", file=sys.stderr)
    sys.exit(2)


class BaselineCounter:
    def __init__(self):
        self.count = 0
        self.started = time.monotonic()

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc != 0:
            print(f"[FAIL] CONNECT rc={rc}", file=sys.stderr, flush=True)
            return
        print("[OK] CONNECTED", flush=True)
        result, mid = client.subscribe(userdata["topic"], qos=0)
        print(f"[OK] SUBSCRIBED topic={userdata['topic']} mid={mid}", flush=True)

    def on_message(self, client, userdata, msg):
        self.count += 1
        if self.count <= 3:
            payload = msg.payload.decode("utf-8", errors="replace")
            print(f"  [{self.count}] {msg.topic} {payload[:150]}...", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Baseline: đo tốc độ MQTT trước khi Ingestion")
    parser.add_argument("--host", default="dathoc.net")
    parser.add_argument("--port", type=int, default=443)
    parser.add_argument("--ws-path", default="/mq")
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--topic", default="v1/C001/+/up/telemetry")
    parser.add_argument("--duration", type=int, default=60, help="Thời gian đo (giây)")
    parser.add_argument("--insecure", action="store_true")
    args = parser.parse_args()

    counter = BaselineCounter()

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
    client.user_data_set({"topic": args.topic})

    original_getaddrinfo = socket.getaddrinfo

    def ipv4_only(host, port, family=0, type=0, proto=0, flags=0):
        if host == args.host:
            return original_getaddrinfo(host, port, socket.AF_INET, type or socket.SOCK_STREAM, proto, flags)
        return original_getaddrinfo(host, port, family, type, proto, flags)

    socket.getaddrinfo = ipv4_only

    print(f"=== BASELINE TEST ===", flush=True)
    print(f"Broker: {args.host}:{args.port}{args.ws_path}", flush=True)
    print(f"Topic: {args.topic}", flush=True)
    print(f"Duration: {args.duration}s", flush=True)
    print(f"Start: {time.strftime('%H:%M:%S')}", flush=True)
    print(flush=True)

    try:
        client.connect(args.host, args.port, keepalive=60)
        client.loop_start()
        time.sleep(args.duration)
        client.loop_stop()
    except KeyboardInterrupt:
        print("\n[STOPPED]", flush=True)
    finally:
        socket.getaddrinfo = original_getaddrinfo
        client.disconnect()

        elapsed = time.monotonic() - counter.started
        rate = counter.count / elapsed if elapsed > 0 else 0

        print(flush=True)
        print(f"=== BASELINE RESULT ===", flush=True)
        print(f"Total messages: {counter.count:,}", flush=True)
        print(f"Elapsed: {elapsed:.1f}s", flush=True)
        print(f"Throughput: {rate:.1f} msg/s", flush=True)
        print(flush=True)

        if counter.count == 0:
            print("[WARNING] Không nhận được message nào!", flush=True)
            print("  - Kiểm tra broker có simulator đang chạy không", flush=True)
            print("  - Data online từ 9h-19h (giờ VN)", flush=True)
        elif rate < 100:
            print(f"[INFO] Tốc độ thấp ({rate:.0f} msg/s), broker có thể đang idle", flush=True)
        elif rate < 2000:
            print(f"[INFO] Tốc độ ~{rate:.0f} msg/s — đạt target 2k msg/s", flush=True)
        else:
            print(f"[INFO] Tốc độ cao {rate:.0f} msg/s — trên target 2k msg/s", flush=True)


if __name__ == "__main__":
    main()
