"""
publisher_load.py
-------------------
Publisher giả lập tốc độ cao vào broker MQTT local (mosquitto), để test câu hỏi
mentor: "100k / 500k events/sec -> CPU/RAM/latency thế nào?".

Publisher thật của dathoc.net chỉ ra ~930 msg/s/khu -- không ép tốc độ lên được,
nên phải dựng nguồn giả riêng, trỏ Telegraf sang broker local này.

Dùng multiprocessing (không phải threading) để né GIL, mỗi process gánh
1 phần target rate. Payload giữ đúng field dev_id/ts/tsunix để tái dùng được
schemas.py validate_event() và các script phân tích latency/ordering đã có.

Cách chạy:
    pip install paho-mqtt
    python3 publisher_load.py --broker localhost --rate 100000 --duration 60 --workers 8
"""
import argparse
import json
import multiprocessing as mp
import random
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

GATEWAYS = ["GW_WATER_001", "GW_LIGHT_001", "GW_ELECTRIC_001"]


def make_payload(dev_id: str) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "dev_id": dev_id,
        "ts": now.strftime("%Y-%m-%d %H:%M:%S"),
        "tsunix": now.timestamp(),  # dùng float để đo latency mịn hơn (script gốc dùng epoch giây)
        "Qt": round(random.uniform(0, 100), 2),
        "V": round(random.uniform(200, 240), 1),
    }


def worker(worker_id: int, broker: str, port: int, rate_per_worker: float, duration: int):
    client = mqtt.Client(client_id=f"loadtest-worker-{worker_id}")
    client.connect(broker, port, keepalive=30)
    client.loop_start()

    interval = 1.0 / rate_per_worker if rate_per_worker > 0 else 0
    end_time = time.time() + duration
    sent = 0
    next_send = time.time()

    while time.time() < end_time:
        gw = GATEWAYS[sent % len(GATEWAYS)]
        dev_id = f"SIM-{worker_id:02d}-{sent % 1000:04d}"
        topic = f"v1/C001/{gw}/up/telemetry"
        payload = make_payload(dev_id)
        client.publish(topic, json.dumps(payload), qos=0)
        sent += 1

        next_send += interval
        sleep_for = next_send - time.time()
        if sleep_for > 0:
            time.sleep(sleep_for)

    client.loop_stop()
    client.disconnect()
    print(f"[worker {worker_id}] đã gửi {sent} message trong {duration}s "
          f"(~{sent/duration:.0f} msg/s thực tế)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", default="localhost")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--rate", type=float, required=True, help="tổng target msg/s")
    ap.add_argument("--duration", type=int, default=60, help="giây")
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()

    rate_per_worker = args.rate / args.workers
    print(f"Target tổng: {args.rate} msg/s | {args.workers} worker | "
          f"~{rate_per_worker:.0f} msg/s/worker | {args.duration}s")
    print("LƯU Ý: 1 process Python đơn lẻ khó ép được tốc độ cực cao vì overhead publish + GIL. "
          "Nếu số 'msg/s thực tế' in ra thấp hơn target nhiều, cần tăng --workers hoặc chạy publisher "
          "trên nhiều máy/container khác nhau, không chỉ tăng số trên 1 máy.")

    procs = []
    for i in range(args.workers):
        p = mp.Process(target=worker, args=(i, args.broker, args.port, rate_per_worker, args.duration))
        p.start()
        procs.append(p)
    for p in procs:
        p.join()


if __name__ == "__main__":
    main()
