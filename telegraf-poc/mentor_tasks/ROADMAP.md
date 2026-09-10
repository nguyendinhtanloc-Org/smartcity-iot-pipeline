# Roadmap trả lời đủ 5 câu hỏi của mentor

Copy cả folder `mentor_tasks/` vào trong `telegraf-poc/` (cùng cấp với `data/`, `telegraf.conf`...).
Thứ tự làm theo đúng số 01 → 06 bên dưới. Mỗi bước ghi rõ: chạy gì, lấy log/số liệu gì để gửi mentor.

---

## Bước 01 — Restart / disconnect thật xảy ra bao nhiêu → mất bao nhiêu message (câu hỏi #2, #3)

**Vấn đề của lần chạy trước:** `analyze_gaps.py` gộp cả 3 khu A/B/C rồi sort mới tìm gap, nên
13 lần "connection lost" trong log chỉ hiện ra 1 gap — vì 2/3 client còn sống sẽ lấp đầy timeline gộp.

**File:** `01_restart_disconnect/analyze_disconnect_events.py`
Script này tự động:
1. Đọc `telegraf_full.log`, tìm mọi dòng `connection lost` → lấy epoch.
2. Với MỖI epoch đó, tách riêng từng khu_cn trong cửa sổ ±60s quanh thời điểm đó, tìm gap thật của khu bị rớt.
3. Ước tính số message mất bằng tốc độ trung bình CỦA RIÊNG khu đó (không dùng tốc độ gộp 3 khu — vì gộp bị nhân 3, xem bước 04).
4. Xuất bảng tổng hợp + ghi ra `disconnect_report.json`.

```bash
python3 01_restart_disconnect/analyze_disconnect_events.py \
    --log telegraf_full.log \
    --data data/unified_events.jsonl
```

**Test restart chủ động (khác với auto-reconnect):**
```bash
docker compose restart telegraf   # ép container restart thật, không phải chỉ mất ping
docker compose logs -f telegraf   # xem thời gian downtime thật
```
Ghi lại epoch lúc restart, chạy lại script trên để đo đúng lần restart này (không lẫn với 13 lần auto-reconnect do pingresp timeout).

**Gửi mentor:** output console của script + file `disconnect_report.json` — có breakdown theo TỪNG lần rớt, TỪNG khu, không phải 1 con số gộp mù mờ.

---

## Bước 02 — Buffer đầy thì mất gì (câu hỏi #2)

Chưa làm ở lần trước (README có ghi bước này nhưng bỏ qua).

```bash
# Sửa tạm telegraf.conf: metric_buffer_limit = 50, flush_interval = "60s"
docker compose restart telegraf
docker compose logs -f telegraf   # đợi dòng "metric buffer overflow; N metrics dropped"
# Ctrl+C sau khi thấy vài dòng overflow, rồi:
docker compose logs telegraf > telegraf_overflow.log
python3 02_buffer_overflow/parse_buffer_overflow.py telegraf_overflow.log
# Trả lại metric_buffer_limit = 100000, flush_interval = "5s", restart lại
docker compose restart telegraf
```

**Gửi mentor:** tổng số metric bị drop, khoảng thời gian buffer đầy kéo dài bao lâu, so sánh với throughput bình thường.

---

## Bước 03 — Kafka/Redpanda unavailable 5 phút → dữ liệu đi đâu (câu hỏi #3)

**Lưu ý quan trọng phải nói với mentor:** stack hiện tại (`docker-compose.yml` gốc) KHÔNG có Kafka/Redpanda,
chỉ ghi thẳng ra file. Câu hỏi này chưa thể trả lời bằng PoC cũ — cần thêm output thật rồi mới đo được.

**File:** `03_kafka_outage/docker-compose.redpanda.yml` (thêm Redpanda 1-node) +
`03_kafka_outage/telegraf.kafka-test.conf` (thêm `[[outputs.kafka]]` song song với `outputs.file`,
giữ nguyên file output để không mất dữ liệu gốc trong lúc test).

```bash
docker compose -f docker-compose.yml -f 03_kafka_outage/docker-compose.redpanda.yml up -d
# đổi telegraf.conf sang bản có outputs.kafka (03_kafka_outage/telegraf.kafka-test.conf), restart telegraf
docker compose stop redpanda        # giả lập Redpanda unavailable
sleep 300                            # 5 phút
docker compose logs telegraf | grep -i "kafka\|buffer\|error" > kafka_outage_during.log
docker compose start redpanda
sleep 30
docker compose logs telegraf | tail -100 > kafka_outage_recovery.log
```

**Gửi mentor:**
- Trong 5 phút mất Redpanda: log lỗi ghi output + xem `metric_buffer_limit` của output đó có bị đầy/drop không (đọc README trong `03_kafka_outage/`).
- Sau khi Redpanda sống lại: có flush lại hết buffer không, hay đã mất phần vượt buffer.
- Dữ liệu vẫn còn nguyên trong `outputs.file` (vì test giữ song song 2 output) → đây chính là câu trả lời thực tế cho "dữ liệu đi đâu": nằm trong buffer RAM của Telegraf tới khi đầy thì rớt, KHÔNG có disk-backed queue thật ở output Kafka (khác với `outputs.file` là ghi thẳng ra đĩa).

---

## Bước 04 — Duplicate xử lý thế nào, Ordering theo device_id đảm bảo thế nào (câu hỏi #4)

**Phát hiện quan trọng phải nói rõ với mentor trước khi đưa số liệu:**
3 khu A/B/C subscribe CÙNG 1 topic (broker chỉ có company C001) → mỗi message thật bị nhân bản 3 lần
(đã verify: cùng dev_id + cùng ts xuất hiện ở cả 3 khu_cn). Vậy "duplicate" đo được sẽ có 2 loại,
cần tách riêng khi báo cáo:
- **duplicate cross-zone** (do 3 client trùng topic — là artifact của cách dựng test, không phải lỗi hệ thống)
- **duplicate cùng zone** (device gửi lại / broker redeliver thật — đây mới là "duplicate thật" mentor hỏi)

**File:** `04_dedup_ordering/batch_validate.py` — bản batch (đọc hết file 1 lần, không tail -f) của
`consume_and_validate.py`, có tách 2 loại duplicate trên + ghi summary JSON cuối cùng.

```bash
python3 04_dedup_ordering/batch_validate.py data/unified_events.jsonl
```

**File:** `04_dedup_ordering/check_ordering_by_device.py` — kiểm tra, với từng `dev_id`, sau khi dedup,
event có tới theo đúng thứ tự thời gian gửi (`ts`) hay không (so với thứ tự ARRIVAL trong file).

```bash
python3 04_dedup_ordering/check_ordering_by_device.py data/unified_events.jsonl
```

**Gửi mentor:** số duplicate cross-zone vs cùng-zone, số lần ra-order theo device, danh sách device bị ra-order nhiều nhất.

---

## Bước 05 — 100k/500k events/sec: CPU/RAM/latency (câu hỏi #5)

Simulator thật của `dathoc.net` chỉ ~930 msg/s thật (2791 msg/s đo được là do bị nhân 3 — xem bước 04),
không thể ép nó chạy nhanh hơn. Phải tự dựng nguồn giả tốc độ cao, trỏ Telegraf vào broker local.

**File:** `05_load_test/mosquitto.conf`, `05_load_test/publisher_load.py` (publisher đa luồng, tốc độ
cấu hình được qua `--rate`), `05_load_test/monitor_resources.sh` (poll `docker stats` mỗi giây, ghi CSV).

```bash
docker compose -f docker-compose.yml -f 05_load_test/docker-compose.mosquitto.yml up -d mosquitto
# đổi telegraf.conf trỏ servers = ["tcp://mosquitto:1883"], topics đúng pattern cũ
docker compose restart telegraf

./05_load_test/monitor_resources.sh telegraf-multi-mqtt > cpu_ram_100k.csv &
python3 05_load_test/publisher_load.py --broker localhost --rate 100000 --duration 60

# lặp lại với --rate 500000
```

Latency: so sánh timestamp lúc publisher gửi (ghi trong payload `ts`) với `timestamp` Telegraf ghi ra file
(dùng lại logic trong `analyze_gaps.py`, cộng thêm hiệu số 2 mốc này).

**Gửi mentor:** CSV CPU/RAM theo thời gian ở 2 mức tải, latency trung bình/p95, và tại tải nào bắt đầu thấy
`metric buffer overflow` hoặc CPU container chạm 100%.

---

## Bước 06 — Replay message 30 ngày trước bằng schema mới (câu hỏi #5)

Raw payload gốc (chuỗi JSON string) vẫn còn nguyên trong `fields.value` của mỗi dòng `unified_events.jsonl`
→ replay được mà không cần lưu riêng "raw" (đỡ tốn thêm storage). Cái còn thiếu là **versioning cho schema**.

1. Thêm `SCHEMA_VERSION = "v1"` vào `schemas.py`, gắn field này vào mọi record sau khi validate.
2. Mỗi lần sửa `schemas.py`, tag git: `git tag schema-v2`.
3. Dùng `06_replay/replay_with_schema.py` để lấy 1 version cụ thể của `schemas.py` (qua `git show <tag>:schemas.py`),
   nạp động (dynamic import), chạy lại `validate_event()` version đó trên dữ liệu raw cũ, so sánh với kết quả
   validate lúc đầu (bao nhiêu event trước đó FAIL nay PASS, hoặc ngược lại).

```bash
python3 06_replay/replay_with_schema.py data/unified_events.jsonl --schema-version schema-v2
```

**Gửi mentor:** bảng so sánh valid/invalid trước-sau khi đổi schema, chứng minh replay hoạt động đúng
mà không cần lưu raw riêng.
