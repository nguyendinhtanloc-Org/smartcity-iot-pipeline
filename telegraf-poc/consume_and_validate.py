"""
Input: file JSONL do Telegraf ghi ra (đã gộp cả 3 khu CN A/B/C)
Output: log valid/invalid + duplicate, giống hệt behavior cũ của src/validate.py
        + thêm dedup logic (mentor yêu cầu: "Duplicate event → xử lý thế nào?")
"""

import json
import sys
import time
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).resolve().parent))
from schemas import detect_type_from_topic, validate_event  # noqa: E402

UNIFIED_FILE = Path("data/unified_events.jsonl")

# Dedup: lưu key đã xử lý trong bộ nhớ (set). Với pipeline dài hạn/nhiều
# ngày, set này cần thay bằng TTL cache (vd Redis) để không phình vô hạn —
# ở scope hiện tại (single-process, chạy demo vài giờ) set() là đủ, và cần
# nói rõ giới hạn này với mentor khi trả lời.
_seen_keys: set[str] = set()


def _dedup_key(payload: dict) -> str:
    """Ưu tiên event_id nếu payload có sẵn (theo đúng schema payload thật,
    xem ví dụ mor_payload.py: mỗi message có 'event_id' UUID riêng).
    Fallback: dev_id + ts nếu event_id không có (một số payload rút gọn
    trong data đã crawl không có event_id, chỉ có dev_id/ts)."""
    if payload.get("event_id"):
        return f"event_id:{payload['event_id']}"
    return f"devts:{payload.get('dev_id', '?')}:{payload.get('ts', payload.get('tsunix', '?'))}"


def _process_line(line):
    """Validate 1 dòng JSONL từ Telegraf. Trả về
    (topic, khu_cn, device_id, is_valid, error_type, error_detail, is_duplicate)."""
    line = line.strip()
    if not line:
        return None
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        return ("unknown", "unknown", None, False, "json_parse_error", "telegraf line is not valid json", False)

    tags = record.get("tags", {})
    fields = record.get("fields", {})

    topic = tags.get("topic", "")
    khu_cn = tags.get("khu_cn", "unknown")
    raw_payload_str = fields.get("value", "{}")

    # ---- Đây là phần CODE THẬT: xác định data_type từ topic ----
    # (dùng lại detect_type_from_topic đã có trong schemas.py,
    #  không viết logic mới, không phải "lý thuyết mới")
    data_type = detect_type_from_topic(topic)

    try:
        payload = json.loads(raw_payload_str)
    except json.JSONDecodeError:
        return (topic, khu_cn, None, False, "json_parse_error", "payload is not valid json", False)

    # ---- Dedup logic: check TRƯỚC khi validate, đúng như luồng thật ----
    # (dupe event không nên tốn công validate lại, tương tự idempotency
    # check ở đầu pipeline thay vì cuối)
    key = _dedup_key(payload)
    if key in _seen_keys:
        return (topic, khu_cn, payload.get("dev_id"), False, "duplicate", key, True)
    _seen_keys.add(key)

    payload["data_type"] = data_type
    payload["khu_cn"] = khu_cn

    # ---- Validate logic thật, tái sử dụng validate_event() ----
    result = validate_event(payload)

    return (
        topic,
        khu_cn,
        result.device_id,
        result.is_valid,
        result.error_type,
        result.error_detail,
        False,
    )


def tail_and_validate(poll_interval: float = 1.0):
    """Đọc file Telegraf ghi ra (đã gộp 3 khu CN) theo kiểu `tail -f`:
    chỉ đọc phần mới thêm (giữ vị trí con trỏ), không đọc lại cả file mỗi vòng.
    Đây là kỹ thuật tiêu chuẩn để theo dõi file ghi nối tiếp (JSONL)."""
    valid_count = 0
    invalid_count = 0
    duplicate_count = 0
    error_types = Counter()
    khu_cn_count = Counter()

    print(f"[VALIDATE] Watching {UNIFIED_FILE} ...")

    # Chờ file được tạo (Telegraf bắt đầu ghi)
    while not UNIFIED_FILE.exists():
        time.sleep(poll_interval)

    # Mở file 1 lần & giữ con trỏ (kỹ thuật `tail -f`):
    # chỉ đọc phần mới thêm, không đọc lại cả file mỗi vòng.
    with open(UNIFIED_FILE, "r", encoding="utf-8") as f:
        while True:
            line = f.readline()
            if not line:
                time.sleep(poll_interval)
                continue
            r = _process_line(line)
            if r is None:
                continue
            _, khu_cn, _dev, is_valid, etype, _detail, is_dup = r

            if is_dup:
                duplicate_count += 1
            else:
                khu_cn_count[khu_cn] += 1
                if is_valid:
                    valid_count += 1
                else:
                    invalid_count += 1
                    error_types[etype] += 1

            total = valid_count + invalid_count + duplicate_count
            if total and total % 1000 == 0:
                print(
                    f"[VALIDATE] total={total} valid={valid_count} invalid={invalid_count} "
                    f"duplicate={duplicate_count} per_khu_cn={dict(khu_cn_count)} "
                    f"top_errors={dict(error_types.most_common(3))}"
                )


if __name__ == "__main__":
    try:
        tail_and_validate()
    except KeyboardInterrupt:
        print("\n[VALIDATE] Stopped.")

