"""
publisher.py
------------
Đọc lại data thật đã crawl trước đó (raw_events_*.jsonl, do main.py ghi ra
lúc broker dathoc.net còn chạy) và publish lại vào Mosquitto local qua MQTT
thật — dùng thư viện paho-mqtt (tool có sẵn) để publish, KHÔNG tự viết
thread pool / worker pool tay (đó là lỗi replay.py cũ mắc phải).

3 "khu CN" là 3 topic khác nhau (local/A/.., local/B/.., local/C/..),
mỗi khu Telegraf sẽ subscribe riêng -> vẫn là multi-source MQTT thật,
chỉ khác nguồn dữ liệu gốc do broker dathoc.net đã tắt giả lập.
"""

import argparse
import json
import time

import paho.mqtt.client as mqtt

# data_type/group (trong payload thật) -> gateway_id mà detect_type_from_topic() nhận diện
GROUP_TO_GATEWAY = {
    "water": "GW_WATER_001",
    "lighting": "GW_LIGHT_001",
    "electricity": "GW_ELECTRIC_001",
    "wastewater": "GW_WWTP_001",
}
ZONES = ["A", "B", "C"]


def _gateway_from_record(record: dict, payload: dict) -> str:
    """Ưu tiên lấy gateway_id từ topic gốc (parts[2]) vì topic crawl thật
    là v1/C001/GW_WATER_001/up/telemetry -> detect_type_from_topic() giữ nguyên
    nhận diện đúng loại data khi publish sang topic local.
    Fallback: payload group/data_type (dành cho file chưa có topic gốc)."""
    topic = record.get("topic", "")
    parts = topic.split("/")
    if len(parts) >= 3 and parts[2].startswith("GW_"):
        return parts[2]
    name = payload.get("group") or payload.get("data_type") or ""
    return GROUP_TO_GATEWAY.get(name, "GW_UNKNOWN_001")


def load_lines(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield line


class TokenBucket:
    """Rate limiter busy-wait (không dùng time.sleep).
    Trên Windows, time.sleep() có độ phân giải ~15ms -> không throttle chính xác
    khi delay < 15ms (tức rate > ~67 msg/s). Busy-wait cho phép lên hàng nghìn msg/s.
    rate <= 0 => không giới hạn (publish tối đa)."""

    def __init__(self, rate_per_s: float):
        self.rate = rate_per_s if rate_per_s > 0 else 0
        self.tokens = self.rate
        self.last_refill = time.monotonic()
        self.max_tokens = max(self.rate, 1.0)

    def consume(self):
        if self.rate <= 0:
            return
        while True:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.tokens = min(self.max_tokens, self.tokens + elapsed * self.rate)
            self.last_refill = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            time.sleep(0)  # yield CPU, không ngủ thật


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="raw_events_*.jsonl đã crawl trước đó")
    ap.add_argument("--broker", default="localhost")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--rate", type=float, default=1000, help="msg/s cho MỖI khu CN (rate<=0 = tối đa tốc độ)")
    ap.add_argument("--max-lines", type=int, default=0,
                    help="Dừng sau N dòng nguồn (mặc định 0 = đọc hết file 1 lần rồi dừng; "
                         "không loop vô hạn để demo có thời gian chạy kiểm soát được)")
    args = ap.parse_args()

    client = mqtt.Client()
    client.connect(args.broker, args.port, keepalive=60)
    client.loop_start()
    bucket = TokenBucket(args.rate)

    if args.rate > 0:
        print(f"[PUBLISHER] {args.input} -> {args.broker}:{args.port}  (~{args.rate} msg/s/zone "
              f"= ~{args.rate * len(ZONES)} msg/s tổng)")
    else:
        print(f"[PUBLISHER] {args.input} -> {args.broker}:{args.port}  (rate=0 => publish tối đa tốc độ)")

    sent = 0
    lines_used = 0
    for line in load_lines(args.input):
        bucket.consume()  # giữ đúng nhịp, kể cả khi rate cao

        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        payload = record.get("payload", record)  # hỗ trợ cả 2 dạng file lưu
        gateway_id = _gateway_from_record(record, payload)

        for zone in ZONES:
            topic = f"local/{zone}/{gateway_id}/telemetry"
            client.publish(topic, json.dumps(payload), qos=0)
            sent += 1

        lines_used += 1
        if sent % 300 == 0:
            print(f"[PUBLISHER] sent={sent}")

        if args.max_lines and lines_used >= args.max_lines:
            break

    client.loop_stop()
    client.disconnect()
    print(f"[PUBLISHER] DONE sent={sent} (từ {lines_used} dòng nguồn)")


if __name__ == "__main__":
    main()
