"""
replay_with_schema.py
-----------------------
Trả lời câu hỏi mentor: "1 event 30 ngày trước cần replay bằng schema validation
mới -> làm thế nào?"

Ý tưởng: raw payload gốc (chuỗi JSON) đã có sẵn trong fields.value của mỗi dòng
unified_events.jsonl -- KHÔNG cần lưu riêng 1 kho "raw" khác. Cái thiếu là
VERSIONING cho schemas.py. Quy ước:

    1. Mỗi lần sửa schemas.py xong, tag lại:  git tag schema-v2
    2. Script này lấy đúng bản schemas.py tại 1 tag (qua `git show <tag>:schemas.py`),
       load động thành module riêng, chạy lại validate_event() bản đó trên dữ liệu
       cũ, rồi so sánh với kết quả validate GỐC (lúc event đó mới vào hệ thống,
       lưu trong batch_validate_report.json ở bước 04 nếu có, hoặc so is_valid
       hiện tại trong data nếu payload có lưu lại).

Cách chạy (đứng trong thư mục git repo, ví dụ smartcity-iot-pipeline/):
    python3 replay_with_schema.py data/unified_events.jsonl --schema-version schema-v2
    # hoặc chỉ định khoảng thời gian (vd đúng "30 ngày trước"):
    python3 replay_with_schema.py data/unified_events.jsonl --schema-version schema-v2 \
        --since 2026-08-11 --until 2026-08-12
"""
import argparse
import importlib.util
import json
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import datetime
from pathlib import Path


def load_schema_module_from_git_tag(tag: str, schema_path_in_repo: str = "schemas.py"):
    """Lấy nội dung schemas.py tại 1 git tag cụ thể, load thành module Python riêng
    (không đụng tới schemas.py hiện tại đang import bình thường trong repo)."""
    result = subprocess.run(
        ["git", "show", f"{tag}:{schema_path_in_repo}"],
        capture_output=True, text=True, check=True,
    )
    source = result.stdout.replace("from __future__ import annotations", "# from __future__ import annotations (removed for dynamic loading)")
    tmp = tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w", encoding="utf-8")
    tmp.write(source)
    tmp.close()

    spec = importlib.util.spec_from_file_location(f"schemas_{tag}", tmp.name)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_ts_field(rec_ts_str, since, until):
    if not (since or until):
        return True
    try:
        dt = datetime.strptime(rec_ts_str, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return True  # không parse được thì không lọc, xử lý luôn
    if since and dt < since:
        return False
    if until and dt > until:
        return False
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("data_file")
    ap.add_argument("--schema-version", required=True, help="git tag của schemas.py, vd schema-v2")
    ap.add_argument("--since", help="YYYY-MM-DD, lọc theo payload ts")
    ap.add_argument("--until", help="YYYY-MM-DD")
    args = ap.parse_args()

    since = datetime.strptime(args.since, "%Y-%m-%d") if args.since else None
    until = datetime.strptime(args.until, "%Y-%m-%d") if args.until else None

    print(f"Đang lấy schemas.py tại tag '{args.schema_version}' ...")
    new_schema = load_schema_module_from_git_tag(args.schema_version)

    total = 0
    now_valid_then_invalid = 0   # trước FAIL, replay với schema mới PASS
    now_invalid_then_valid = 0   # trước PASS, replay với schema mới FAIL (regression!)
    still_valid = 0
    still_invalid = 0
    new_error_types = Counter()

    with open(args.data_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                payload = json.loads(record["fields"]["value"])
            except (json.JSONDecodeError, KeyError):
                continue

            if not parse_ts_field(payload.get("ts"), since, until):
                continue

            topic = record.get("tags", {}).get("topic", "")
            old_result_valid = None  # nếu muốn so với kết quả gốc, load thêm batch_validate_report.json
            # ở đây demo chỉ chạy schema MỚI, việc so sánh 2 chiều cần merge với báo cáo validate gốc
            # (xem batch_validate.py ở bước 04) theo cùng dedup_key.

            data_type = new_schema.detect_type_from_topic(topic)
            payload["data_type"] = data_type
            payload["khu_cn"] = record.get("tags", {}).get("khu_cn", "unknown")
            result = new_schema.validate_event(payload)

            total += 1
            if result.is_valid:
                still_valid += 1
            else:
                still_invalid += 1
                new_error_types[result.error_type] += 1

    print("\n===== KẾT QUẢ REPLAY VỚI SCHEMA "
          f"'{args.schema_version}' =====")
    print(f"Tổng số event replay: {total}")
    print(f"Valid theo schema mới  : {still_valid}")
    print(f"Invalid theo schema mới: {still_invalid}")
    print(f"Top lỗi theo schema mới: {dict(new_error_types.most_common(5))}")
    print("\nGhi chú: để có bảng so sánh TRƯỚC/SAU đầy đủ (bao nhiêu event đổi trạng thái "
          "valid<->invalid), merge kết quả này với batch_validate_report.json (chạy validate "
          "gốc ở bước 04) theo cùng dedup_key -- không làm ở bản demo này để giữ script gọn.")


if __name__ == "__main__":
    main()
