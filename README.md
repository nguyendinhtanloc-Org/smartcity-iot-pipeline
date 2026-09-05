# THIẾT KẾ PIPELINE — BÓC TÁCH DỮ LIỆU ĐIỆN/NƯỚC/ÁNH SÁNG (BÀI TOÁN 1)

**Người thực hiện:** Nguyễn Đình Tấn Lộc  
**Ngày:** 04/09/2026  
**Deadline:** 05/09/2026  
**Trạng thái:** Giai đoạn 2 — Ingestion + Validation

---

## 1. Phạm vi

Xử lý stream MQTT, 3 nhóm thiết bị điện/nước/ánh sáng:
- **Target throughput:** 2k msg/s (giai đoạn này), scale lên 500k–1M msg/s (giai đoạn sau)
- **Data online:** 9h–19h (giờ VN)
- **Trọng tâm:** Logic kiểm tra data đúng/sai (Validation) — không tự dựng logic vận hành song song

---

## 2. Kiến trúc tổng thể

```
                    ┌─────────────────┐
                    │  MQTT Broker    │
                    │ dathoc.net:443  │
                    └────────┬────────┘
                             │ WSS (WebSocket Secure)
                             ▼
                    ┌─────────────────┐
                    │    BASELINE     │ ← Đo tốc độ thực tế từ broker
                    │  (baseline.py)  │
                    └────────┬────────┘
                             │
                             ▼
┌────────────────────────────────────────────┐
│              main.py (Entry point)         │
│  ┌──────────────┐      ┌──────────────┐   │
│  │  Ingestion   │ queue│  Validation  │   │
│  │ (ingest.py)  │─────▶│(validate.py) │   │
│  └──────┬───────┘      └──────┬───────┘   │
│         │                     │            │
│         ▼                     ▼            │
│  data/raw/*.jsonl    logs/invalid_*.jsonl  │
└────────────────────────────────────────────┘
```

---

## 3. Cấu trúc dự án

```
smartcity-iot-pipeline/
├── main.py              # Entry point — chạy LIVE (kết nối MQTT thật)
├── baseline.py          # Đo baseline — tốc độ thực tế từ broker trước khi Ingestion
├── ingest.py            # Chặng Ingestion: nhận MQTT, đẩy queue, lưu raw, log throughput
├── validate_stage.py    # Chặng Validation: validate schema + range, phân loại valid/invalid
├── schemas.py           # Pydantic schema theo device_type + range vật lý
├── replay.py            # Replay data local để test tải tăng dần (10k, 100k)
├── mor_payload.py       # Script monitor từ mentor (subscribe & in message)
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

**Thư mục output:**
```
data/raw/
├── raw_events_<ts>.jsonl         # Data thô từ MQTT (dùng cho replay)
logs/
├── ingest.log                    # Log chặng Ingestion
├── validate.log                  # Log chặng Validation
├── summary.json                  # Tổng hợp số liệu
├── invalid_events.jsonl          # Message không hợp lệ (kèm lý do)
├── dead_letter.jsonl             # Poison pill (device lỗi ≥3 lần)
└── replay/
    ├── replay_10000.log
    ├── replay_10000_summary.json
    ├── replay_100000.log
    └── replay_100000_summary.json
```

---

## 4. Chi tiết từng chặng

### 4.1. Chặng BASELINE — Đo tốc độ thực tế

| Thông tin | Chi tiết |
|-----------|----------|
| **Script** | `baseline.py` |
| **Công nghệ** | paho-mqtt,subscribe topic, đếm message |
| **Data vào** | MQTT message từ broker |
| **Data ra** | Tổng msg, throughput (msg/s), sample message |
| **Mục đích** | Xác nhận broker có data, đo tốc độ thực tế trước khi Ingestion |

**Cách chạy:**
```bash
python3 baseline.py \
    --host dathoc.net --port 443 --ws-path /mq \
    --username test1 --password '123456' \
    --topic 'qa-smartcity/#' --duration 60 --insecure
```

**Kết quả mong đợi:**
```
=== BASELINE RESULT ===
Total messages: 125,000
Elapsed: 60.0s
Throughput: 2,083.3 msg/s
```

**Nếu total=0:** Broker không có data → Liên hệ mentor bật simulator

---

### 4.2. Chặng INGESTION

| Thông tin | Chi tiết |
|-----------|----------|
| **Script** | `ingest.py` |
| **Công nghệ** | paho-mqtt (WebSocket Secure), queue.Queue (bounded) |
| **Data vào** | MQTT message từ broker, topic `qa-smartcity/#` |
| **Data ra** | JSON object đẩy vào queue + raw file `.jsonl` |
| **Log** | `logs/ingest.log` — throughput mỗi 10 giây |

**Logic xử lý:**
1. Kết nối WSS (TLS, force IPv4)
2. Subscribe topic `qa-smartcity/#`
3. Nhận message → parse JSON → push vào queue (bounded, maxsize=20000)
4. Ghi raw message xuống `data/raw/raw_events_<ts>.jsonl`
5. Log throughput mỗi 10 giây
6. Xử lý reconnect tự động khi MQTT rớt

**Log output mẫu:**
```
[ingestion] INFO [MQTT] CONNECTED
[ingestion] INFO [MQTT] SUBSCRIBED topic=qa-smartcity/# mid=1
[ingestion] INFO [INGEST] window=10.0s recv=10234 rate=1023.4 msg/s total=10234
[ingestion] INFO [INGEST] window=10.0s recv=10189 rate=1018.9 msg/s total=20423
...
[ingestion] INFO [INGEST] DONE total=125000 elapsed=120.1s avg_rate=1040.8 msg/s
```

---

### 4.3. Chặng VALIDATION

| Thông tin | Chi tiết |
|-----------|----------|
| **Script** | `validate_stage.py` |
| **Công nghệ** | Pydantic (schema + range validation) |
| **Data vào** | JSON object từ queue (do Ingestion đẩy vào) |
| **Data ra** | Valid / Invalid (kèm lý do lỗi) |
| **Log** | `logs/validate.log`, `logs/invalid_events.jsonl` |

**Logic validation:**
1. **Bước 1 — Schema check:** Kiểm tra field bắt buộc, kiểu dữ liệu
   - `event_id`: string
   - `device_id`: string
   - `ts`: int (epoch ms)
   - `metrics`: dict
   - `quality`: dict {rssi_dbm, snr_db, latency_ms}
   - ... (xem `schemas.py`)

2. **Bước 2 — Range check:** Kiểm tra giá trị vật lý theo từng nhóm
   - **Water:** pH 0-14, pressure 0-50 bar, flow ≥ 0, temperature -10~60°C
   - **Electricity/Light:** Đang dùng `GenericMetrics` (TODO khi có sample)

3. **Phân loại kết quả:**
   - `valid`: Message hợp lệ → đẩy sang Detect (giai đoạn 3)
   - `schema_error`: Thiếu field hoặc sai kiểu dữ liệu
   - `range_error`: Giá trị ngoài range vật lý hợp lệ

4. **Poison pill handling:** Device nào lỗi liên tục ≥3 lần → đẩy sang `dead_letter.jsonl`

**Log output mẫu:**
```
[validation] INFO [VALIDATE] window=10.0s valid=10089 invalid=145 rate=1023.3 msg/s total_valid=10089 total_invalid=145
...
[validation] INFO [VALIDATE] DONE total=125000 valid=123100 invalid=1900 error_rate=1.52% avg_rate=1040.7 msg/s elapsed=120.1s top_errors={'range_error': 1900}
```

**File output:**
- `logs/invalid_events.jsonl` — Message lỗi + lý do chi tiết
- `logs/dead_letter.jsonl` — Poison pill (device lỗi ≥3 lần liên tiếp)

---

## 5. Hướng dẫn chạy — Thứ tự thực hiện

### Bước 1: Verify kết nối broker

```bash
# Test baseline 60 giây — kiểm tra broker có data không
python3 baseline.py \
    --host dathoc.net --port 443 --ws-path /mq \
    --username test1 --password '123456' \
    --topic 'qa-smartcity/#' --duration 60 --insecure
```

**Kết quả:**
- `total=0` → Broker không có data → Liên hệ mentor bật simulator
- `total>0` → Broker có data → Tiếp tục Bước 2

---

### Bước 2: Chạy LIVE 20 phút — Thu data local

```bash
python3 main.py \
    --host dathoc.net --port 443 --ws-path /mq \
    --username test1 --password '123456' \
    --topic 'qa-smartcity/#' --duration 1200 --insecure
```

**Kết quả:**
- `logs/ingest.log` — Log Ingestion
- `logs/validate.log` — Log Validation
- `logs/summary.json` — Tổng hợp số liệu
- `data/raw/raw_events_<ts>.jsonl` — Data thô (dùng cho replay)
- `logs/invalid_events.jsonl` — Message lỗi
- `logs/dead_letter.jsonl` — Poison pill

---

### Bước 3: Kiểm tra kết quả

```bash
# Xem tổng hợp
cat logs/summary.json

# Xem chi tiết Ingestion
cat logs/ingest.log | tail -5

# Xem chi tiết Validation
cat logs/validate.log | tail -5

# Xem message lỗi (10 dòng đầu)
head -10 logs/invalid_events.jsonl
```

**Mentor yêu cầu gửi:**
- Code (toàn bộ file)
- Log (`ingest.log`, `validate.log`)
- Kết quả (`summary.json`)

---

### Bước 4: Test replay tuần tự

```bash
# Hoàn thành 10k trước, rồi mới chạy 100k
python3 replay.py --input data/raw/raw_events_<ts>.jsonl --target 10000
python3 replay.py --input data/raw/raw_events_<ts>.jsonl --target 100000
```

---

## 6. Kết quả mong đợi (sau 20 phút)

### Ingestion

| Metric | Giá trị |
|--------|---------|
| Total messages | ~125,000 (tùy tốc độ broker) |
| Avg throughput | ~1,040 msg/s |
| Elapsed | 120s |
| Raw file | `data/raw/raw_events_<ts>.jsonl` |

### Validation

| Metric | Giá trị |
|--------|---------|
| Total processed | ~125,000 |
| Valid | ~123,100 (98.5%) |
| Invalid | ~1,900 (1.5%) |
| Top error | range_error |
| Invalid file | `logs/invalid_events.jsonl` |

---

## 7. Test throughput theo yêu cầu mentor

Mentor yêu cầu test ở 3 mức: **2k, 500k, 1M msg/s**

### 2k msg/s (target hiện tại)

```bash
# Chạy LIVE 20 phút — broker cần publish ~2k msg/s
python3 main.py \
    --host dathoc.net --port 443 --ws-path /mq \
    --username test1 --password '123456' \
    --topic 'qa-smartcity/#' --duration 1200 --insecure
```

### 500k–1M msg/s (scale lý thuyết)

**Lưu ý:** Không thể test 500k/1M msg/s trên máy local — cần:
- Kafka/Redpanda (partition theo device_id)
- Cluster nhiều node
- ClickHouse/TimescaleDB cho storage

Chi tiết ở Addendum (giai đoạn sau).

---

## 8. Validation kiểm tra những gì?

| Loại lỗi | Mô tả | Ví dụ |
|-----------|-------|-------|
| `schema_error` | Thiếu field bắt buộc hoặc sai kiểu dữ liệu | Thiếu `device_id`, `ts` không phải int |
| `range_error` | Giá trị ngoài range vật lý hợp lệ | pH = 15, pressure = -1, temperature = 100°C |

**Lưu ý:** Đây là kiểm tra **schema + range vật lý**, KHÔNG phải ngưỡng vi phạm nghiệp vụ.

---

## 9. Cài đặt & Chạy Docker

```bash
# Cài đặt
pip install -r requirements.txt

# Chạy trực tiếp
python3 main.py --host dathoc.net --port 443 --ws-path /mq \
    --username test1 --password '123456' \
    --topic 'qa-smartcity/#' --duration 1200 --insecure

# Hoặc Docker
docker compose up --build
```

---

## 10. Debug — Kiểm tra kết nối

Nếu không nhận được data:

```bash
# Chạy baseline test
python3 baseline.py \
    --host dathoc.net --port 443 --ws-path /mq \
    --username test1 --password '123456' \
    --topic 'qa-smartcity/#' --duration 30 --insecure
```

**Kết quả:**
- `CONNECTED` + `SUBSCRIBED` + `total=0` → Broker không có data
- `CONNECT FAILED` → Lỗi kết nối网络
- `SSL error` → Thêm flag `--insecure`

---

## 11. Bài toán 2 (Video)

Triển khai sau khi bài 1 được duyệt.

---

## 12. Đã nhận từ mentor

- Giữ tự chủ động code và tự tìm vấn đề trước khi hỏi lại
- Bắt đầu ở ~1k msg/s, tăng dần bằng cách replay data local
- Không tự dựng logic song song/worker tay
