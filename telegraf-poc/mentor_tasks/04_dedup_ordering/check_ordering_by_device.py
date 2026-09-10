"""
check_ordering_by_device.py
------------------------------
Kiểm tra: sau khi loại bỏ duplicate (do 3 khu trùng topic), event của CÙNG
1 dev_id có tới theo đúng thứ tự thời gian gửi (payload["ts"]) hay không,
so với thứ tự thật sự ARRIVAL trong file (tức thứ tự Telegraf ghi ra).

Đây là câu hỏi mentor: "Event order theo device_id -> đảm bảo thế nào?"
Hiện tại pipeline KHÔNG có logic reorder/watermark nào -- script này đo xem
trong thực tế có bị đảo thứ tự hay không, và đảo bao nhiêu.

Cách chạy:
    python3 check_ordering_by_device.py data/unified_events.jsonl
"""
import json
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime

TS_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S")


def parse_ts(ts_str):
    for fmt in TS_FORMATS:
        try:
            return datetime.strptime(ts_str, fmt)
        except (ValueError, TypeError):
            continue
    return None


def dedup_key(payload: dict) -> str:
    if payload.get("event_id"):
        return f"event_id:{payload['event_id']}"
    return f"devts:{payload.get('dev_id', '?')}:{payload.get('ts', payload.get('tsunix', '?'))}"


def main():
    path = Path(sys.argv[1] if len(sys.argv) > 1 else "data/unified_events.jsonl")

    seen_keys = set()
    last_ts_per_device = {}
    out_of_order_count = 0
    out_of_order_by_device = defaultdict(int)
    total_events_checked = 0
    unparseable_ts = 0

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            fields = record.get("fields", {})
            try:
                payload = json.loads(fields.get("value", "{}"))
            except json.JSONDecodeError:
                continue

            key = dedup_key(payload)
            if key in seen_keys:
                continue  # bỏ qua duplicate (cross-zone hoặc cùng zone), chỉ xét unique event
            seen_keys.add(key)

            dev_id = payload.get("dev_id", "unknown")
            ts_raw = payload.get("ts")
            ts = parse_ts(ts_raw)
            if ts is None:
                unparseable_ts += 1
                continue

            total_events_checked += 1
            last_ts = last_ts_per_device.get(dev_id)
            if last_ts is not None and ts < last_ts:
                out_of_order_count += 1
                out_of_order_by_device[dev_id] += 1
            else:
                last_ts_per_device[dev_id] = ts

    print("===== KẾT QUẢ KIỂM TRA ORDERING THEO DEVICE_ID =====")
    print(f"Tổng số unique event kiểm tra được : {total_events_checked}")
    print(f"Số event có ts không parse được    : {unparseable_ts}")
    print(f"Số lần bị ra-order (ts đến sau lại nhỏ hơn ts trước đó, cùng dev_id): {out_of_order_count}")
    if total_events_checked:
        print(f"Tỉ lệ ra-order: {out_of_order_count/total_events_checked*100:.3f}%")

    if out_of_order_by_device:
        print("\nTop 10 device bị ra-order nhiều nhất:")
        for dev_id, cnt in sorted(out_of_order_by_device.items(), key=lambda x: -x[1])[:10]:
            print(f"  {dev_id}: {cnt} lần")
    else:
        print("\nKhông phát hiện ra-order nào trong dữ liệu đã kiểm tra.")

    print("\nGhi chú gửi mentor: pipeline hiện tại KHÔNG có cơ chế reorder/watermark chủ động -- "
          "kết quả trên chỉ phản ánh thực tế đo được trong phiên chạy này (mạng ổn định, 1 broker), "
          "chưa chứng minh được hành vi khi có multi-broker/multi-partition thật.")


if __name__ == "__main__":
    main()
