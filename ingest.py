"""
ingest.py
----------
Chặng INGESTION.

Kế thừa logic kết nối MQTT WSS từ mor_payload.py (mentor cung
cấp), nhưng thay vì chỉ print ra màn hình:
  1. Đẩy mỗi message vào 1 queue nội bộ (queue.Queue) để chặng
     Validate tiêu thụ.
  2. Ghi raw message xuống file .jsonl local (để dùng lại cho
     bước replay 10k/100k msg sau này).
  3. Log throughput (msg/s) định kỳ.

Không tự dựng worker/consumer song song ở đây — 1 thread MQTT
network loop, 1 thread xử lý (Validate) đọc từ queue tuần tự,
đúng tinh thần "làm tuần tự, không tự code logic vận hành song
song" mà mentor yêu cầu.
"""

from __future__ import annotations

import json
import logging
import queue
import socket
import ssl
import sys
import time
from pathlib import Path

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("Missing paho-mqtt. Install: pip3 install paho-mqtt", file=sys.stderr)
    sys.exit(2)


logger = logging.getLogger("ingestion")


class Ingestor:
    def __init__(self, args, out_queue: "queue.Queue", raw_log_path: Path):
        self.args = args
        self.out_queue = out_queue
        self.raw_log_path = raw_log_path
        self.count = 0
        self.window_count = 0
        self.window_started = time.monotonic()
        self.started = time.monotonic()
        self._raw_file = open(self.raw_log_path, "a", encoding="utf-8")

    # -------------------------------------------------------
    # MQTT callbacks
    # -------------------------------------------------------

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc != 0:
            logger.error("[MQTT] CONNECT FAILED rc=%s", rc)
            return
        logger.info("[MQTT] CONNECTED")
        result, mid = client.subscribe(self.args.topic, qos=self.args.qos)
        if result != mqtt.MQTT_ERR_SUCCESS:
            logger.error("[MQTT] SUBSCRIBE FAILED rc=%s", result)
            return
        logger.info("[MQTT] SUBSCRIBED topic=%s mid=%s", self.args.topic, mid)

    def on_disconnect(self, client, userdata, disconnect_flags=None, rc=None, properties=None):
        logger.warning("[MQTT] DISCONNECTED rc=%s", rc)

    def on_message(self, client, userdata, msg):
        self.count += 1
        self.window_count += 1

        payload_str = msg.payload.decode("utf-8", errors="replace")

        # Ghi raw xuống local để dùng lại cho replay sau này
        self._raw_file.write(payload_str + "\n")

        try:
            payload = json.loads(payload_str)
        except json.JSONDecodeError:
            payload = {"_raw_unparsed": payload_str, "_topic": msg.topic}

        # Đẩy vào queue nội bộ cho chặng Validate tiêu thụ.
        # Nếu queue đầy (bounded), chặn lại (backpressure đơn giản)
        # thay vì phình vô hạn gây OOM.
        self.out_queue.put((msg.topic, payload))

        # Log throughput mỗi 10 giây
        now = time.monotonic()
        elapsed_window = now - self.window_started
        if elapsed_window >= 10:
            rate = self.window_count / elapsed_window
            logger.info(
                "[INGEST] window=%.1fs recv=%d rate=%.1f msg/s total=%d",
                elapsed_window, self.window_count, rate, self.count,
            )
            self.window_count = 0
            self.window_started = now

    # -------------------------------------------------------
    # Run
    # -------------------------------------------------------

    def run(self, duration_seconds: int):
        try:
            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=self.args.client_id,
                transport="websockets",
            )
        except Exception:
            client = mqtt.Client(client_id=self.args.client_id, transport="websockets")

        client.username_pw_set(self.args.username, self.args.password)
        client.ws_set_options(path=self.args.ws_path)

        if self.args.insecure:
            import ssl
            client.tls_set(cert_reqs=ssl.CERT_NONE)
        else:
            client.tls_set()

        client.on_connect = self.on_connect
        client.on_disconnect = self.on_disconnect
        client.on_message = self.on_message

        # Force IPv4 (giữ nguyên logic từ mor_payload.py — môi trường
        # test đôi khi IPv6 route không thông)
        original_getaddrinfo = socket.getaddrinfo

        def ipv4_only(host, port, family=0, type=0, proto=0, flags=0):
            if host == self.args.host:
                return original_getaddrinfo(host, port, socket.AF_INET, type or socket.SOCK_STREAM, proto, flags)
            return original_getaddrinfo(host, port, family, type, proto, flags)

        socket.getaddrinfo = ipv4_only

        logger.info("Connecting WSS to %s:%s%s", self.args.host, self.args.port, self.args.ws_path)

        try:
            client.connect(self.args.host, self.args.port, keepalive=60)
            client.loop_start()
            time.sleep(duration_seconds)
            client.loop_stop()
        except KeyboardInterrupt:
            logger.info("Stopping (Ctrl+C)...")
        finally:
            socket.getaddrinfo = original_getaddrinfo
            try:
                client.disconnect()
            except Exception:
                pass
            self._raw_file.close()

            elapsed = time.monotonic() - self.started
            avg_rate = self.count / elapsed if elapsed > 0 else 0
            logger.info(
                "[INGEST] DONE total=%d elapsed=%.1fs avg_rate=%.1f msg/s",
                self.count, elapsed, avg_rate,
            )

        # Báo hiệu kết thúc cho consumer (Validate)
        self.out_queue.put(None)
