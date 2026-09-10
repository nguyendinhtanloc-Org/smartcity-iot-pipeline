# Replay event cũ bằng schema mới

## 1. Thiết lập versioning (làm 1 lần)

Trong `schemas.py`, thêm ở đầu file:
```python
SCHEMA_VERSION = "v1"
```
Mỗi lần sửa logic validate xong, commit rồi tag:
```bash
git add schemas.py
git commit -m "schemas: siết lại range V cho water sensor"
git tag schema-v2
```

## 2. Vì sao không cần lưu riêng "raw" data

Nhìn vào `data/unified_events.jsonl`: mỗi dòng Telegraf ghi ra đã có sẵn
`fields.value` = chuỗi JSON payload GỐC (chưa qua xử lý), tách biệt hoàn toàn với
kết quả validate. Nghĩa là dữ liệu thô để replay **đã có sẵn**, không cần thêm
bước "lưu raw" riêng như comment cũ trong README (`data/raw` để trống).

## 3. Replay

```bash
python3 06_replay/replay_with_schema.py data/unified_events.jsonl \
    --schema-version schema-v2 \
    --since 2026-08-11 --until 2026-08-12
```

## 4. Gửi mentor

- Bảng valid/invalid theo schema mới trên đúng đoạn dữ liệu "30 ngày trước".
- Nếu ghép thêm `batch_validate_report.json` (bước 04) theo `dedup_key`, ra được
  bảng chuyển trạng thái: bao nhiêu event trước INVALID nay VALID (schema nới lỏng),
  bao nhiêu event trước VALID nay INVALID (schema siết chặt hơn / regression).
