"""
batch_validate.py
-------------------
Bản batch (đọc hết file 1 lần rồi in kết quả cuối, KHÔNG tail -f) của
consume_and_validate.py -- dùng để lấy số liệu cuối cùng trên dữ liệu đã
thu thập xong (data/unified_events.jsonl), thay vì phải canh giờ chạy
song song với Telegraf.

Khác biệt quan trọng so với bản gốc: tách riêng 2 loại duplicate, vì 3 khu
A/B/C hiện đang subscribe CÙNG 1 topic (broker chỉ có company C001) nên mỗi
message thật bị nhân bản 3 lần -- đây LÀ ARTIFACT của cách dựng test, không
phải "duplicate" theo nghĩa mentor hỏi (device/broker gửi lại message thật):

  - duplicate_cross_zone: cùng key nhưng khác khu_cn với lần thấy đầu tiên
    -> do 3 client trùng topic, KHÔNG phải lỗi hệ thống.
  - duplicate_same_zone:  cùng key VÀ cùng khu_cn với lần thấy đầu tiên
    -> đây mới là duplicate/re-delivery thật (vd QoS 1 gửi lại do chưa nhận
    được ACK, hoặc device tự gửi lại).

Cách chạy:
    python3 batch_validate.py data/unified_events.jsonl
"""
import json
import sys
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))  # trỏ tới thư mục chứa schemas.py
try:
    from schemas import detect_type_from_topic, validate_event  # noqa: E402
except ImportError:
    print("Không import được schemas.py -- chạy script này từ trong thư mục telegraf-poc/, "
          "hoặc sửa sys.path.insert phía trên cho đúng đường dẫn.")
    raise


def dedup_key(payload: dict) -> str:
    if payload.get("event_id"):
        return f"event_id:{payload['event_id']}"
    return f"devts:{payload.get('dev_id', '?')}:{payload.get('ts', payload.get('tsunix', '?'))}"


def main():
    path = Path(sys.argv[1] if len(sys.argv) > 1 else "data/unified_events.jsonl")

    seen_zone_for_key: dict[str, str] = {}
    valid_count = 0
    invalid_count = 0
    dup_cross_zone = 0
    dup_same_zone = 0
    error_types = Counter()
    khu_cn_count = Counter()
    json_errors = 0
    total_lines = 0

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total_lines += 1
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                json_errors += 1
                continue

            tags = record.get("tags", {})
            fields = record.get("fields", {})
            topic = tags.get("topic", "")
            khu_cn = tags.get("khu_cn", "unknown")
            raw_payload_str = fields.get("value", "{}")

            try:
                payload = json.loads(raw_payload_str)
            except json.JSONDecodeError:
                json_errors += 1
                continue

            key = dedup_key(payload)
            if key in seen_zone_for_key:
                if seen_zone_for_key[key] == khu_cn:
                    dup_same_zone += 1
                else:
                    dup_cross_zone += 1
                continue
            seen_zone_for_key[key] = khu_cn

            data_type = detect_type_from_topic(topic)
            payload["data_type"] = data_type
            payload["khu_cn"] = khu_cn
            result = validate_event(payload)

            khu_cn_count[khu_cn] += 1
            if result.is_valid:
                valid_count += 1
            else:
                invalid_count += 1
                error_types[result.error_type] += 1

            if total_lines % 500_000 == 0:
                print(f"...đã xử lý {total_lines} dòng", file=sys.stderr)

    total_unique = valid_count + invalid_count
    total_dup = dup_cross_zone + dup_same_zone

    print("\n===== KẾT QUẢ BATCH VALIDATE =====")
    print(f"Tổng số dòng đọc được       : {total_lines}")
    print(f"Lỗi parse JSON (record/payload): {json_errors}")
    print(f"Unique event (đã dedup)     : {total_unique}  (valid={valid_count}, invalid={invalid_count})")
    print(f"Duplicate cross-zone (artifact 3 khu trùng topic): {dup_cross_zone}")
    print(f"Duplicate cùng zone (re-delivery/dup THẬT)       : {dup_same_zone}")
    print(f"Tổng duplicate                                  : {total_dup}")
    if total_lines:
        print(f"Tỉ lệ duplicate cross-zone / tổng : {dup_cross_zone/total_lines*100:.1f}%")
        print(f"Tỉ lệ duplicate cùng zone / tổng   : {dup_same_zone/total_lines*100:.2f}%")
    print(f"\nSố unique event theo khu_cn : {dict(khu_cn_count)}")
    print(f"Top lỗi validate            : {dict(error_types.most_common(5))}")

    report = {
        "total_lines": total_lines,
        "json_errors": json_errors,
        "unique_valid": valid_count,
        "unique_invalid": invalid_count,
        "duplicate_cross_zone": dup_cross_zone,
        "duplicate_same_zone": dup_same_zone,
        "per_khu_cn_unique": dict(khu_cn_count),
        "top_validate_errors": dict(error_types.most_common(10)),
    }
    out_path = path.parent / "batch_validate_report.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\nĐã ghi báo cáo vào {out_path}")


if __name__ == "__main__":
    main()
