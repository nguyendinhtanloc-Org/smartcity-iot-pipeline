# Hướng Dẫn Sử Dụng — SmartCity IoT Pipeline

## Yêu Cầu Hệ Thống

- Python 3.11+
- pip (package manager)
- Docker (tùy chọn)

## Cài Đặt

### Cách 1: Cài đặt trực tiếp

```bash
# Clone repository
git clone <repository-url>
cd smartcity-iot-pipeline

# Tạo virtual environment
python -m venv venv

# Kích hoạt virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Cài đặt dependencies
pip install -r requirements.txt
```

### Cách 2: Docker

```bash
# Build image
docker compose build

# Hoặc build thủ công
docker build -t smartcity-pipeline .
```

---

## Chạy Pipeline

### Bước 1: Verify Kết Nối Broker

Trước khi chạy pipeline, cần kiểm tra xem broker có data không:

```bash
python baseline.py \
    --host dathoc.net --port 443 --ws-path /mq \
    --username test1 --password '123456' \
    --topic 'v1/C001/+/up/telemetry' \
    --duration 60 \
    --insecure
```

**Kết quả:**
- `total=0` → Broker không có data → Liên hệ mentor bật simulator
- `total>0` → Broker có data → Tiếp tục Bước 2

**Log mẫu:**
```
=== BASELINE TEST ===
Broker: dathoc.net:443/mq
Topic: v1/C001/+/up/telemetry
Duration: 60s
Start: 14:30:00

[OK] CONNECTED
[OK] SUBSCRIBED topic=v1/C001/+/up/telemetry mid=1

=== BASELINE RESULT ===
Total messages: 125,000
Elapsed: 60.0s
Throughput: 2,083.3 msg/s
```

---

### Bước 2: Chạy LIVE 20 phút

```bash
python main.py \
    --host dathoc.net --port 443 --ws-path /mq \
    --username test1 --password '123456' \
    --topic 'v1/C001/+/up/telemetry' \
    --duration 1200 \
    --insecure
```

**Parameters:**

| Parameter | Mặc định | Mô tả |
|-----------|----------|-------|
| `--host` | dathoc.net | MQTT broker host |
| `--port` | 443 | MQTT broker port |
| `--ws-path` | /mq | WebSocket path |
| `--username` | (required) | Username MQTT |
| `--password` | (required) | Password MQTT |
| `--topic` | v1/C001/+/up/telemetry | Topic subscribe |
| `--qos` | 0 | QoS level |
| `--client-id` | auto-generated | Client ID |
| `--insecure` | false | Bypass TLS verification |
| `--duration` | 1200 | Thời gian chạy (giây) |
| `--queue-maxsize` | 20000 | Max queue size |

**Kết quả:**
- `logs/ingest.log` — Log Ingestion
- `logs/validate.log` — Log Validation
- `logs/summary.json` — Tổng hợp số liệu
- `data/raw/raw_events_<timestamp>.jsonl` — Data thô
- `logs/invalid_events.jsonl` — Message lỗi
- `logs/dead_letter.jsonl` — Poison pill

**Log mẫu:**
```
[ingestion] INFO [MQTT] CONNECTED
[ingestion] INFO [MQTT] SUBSCRIBED topic=v1/C001/+/up/telemetry mid=1
[ingestion] INFO [INGEST] window=10.0s recv=10234 rate=1023.4 msg/s total=10234
[ingestion] INFO [INGEST] window=10.0s recv=10189 rate=1018.9 msg/s total=20423
...
[ingestion] INFO [INGEST] DONE total=125000 elapsed=120.1s avg_rate=1040.8 msg/s

[validation] INFO [VALIDATE] window=10.0s valid=10089 invalid=145 rate=1023.3 msg/s total_valid=10089 total_invalid=145
...
[validation] INFO [VALIDATE] DONE total=125000 valid=123100 invalid=1900 error_rate=1.52% avg_rate=1040.7 msg/s elapsed=120.1s top_errors={'range_error': 1900}
```

---

### Bước 3: Kiểm Tra Kết Quả

```bash
# Xem tổng hợp
cat logs/summary.json

# Xem chi tiết Ingestion
cat logs/ingest.log | tail -5

# Xem chi tiết Validation
cat logs/validate.log | tail -5

# Xem message lỗi (10 dòng đầu)
head -10 logs/invalid_events.jsonl

# Xem poison pill
cat logs/dead_letter.jsonl
```

---

### Bước 4: Test Replay Tuần Tự

Sau khi chạy LIVE xong, test replay với data thật:

```bash
# Hoàn thành 10k trước, rồi mới chạy 100k
python replay.py \
    --input data/raw/raw_events_<timestamp>.jsonl \
    --target 10000

python replay.py \
    --input data/raw/raw_events_<timestamp>.jsonl \
    --target 100000
```

**Kết quả:**
- `logs/replay/replay_10000.log`
- `logs/replay/replay_10000_summary.json`
- `logs/replay/replay_100000.log`
- `logs/replay/replay_100000_summary.json`

---

## Chạy Docker

### Docker Compose (Khuyến nghị)

```bash
# Chạy LIVE 20 phút
docker compose up --build

# Hoặc chạy nền
docker compose up --build -d

# Xem logs
docker compose logs -f

# Dừng
docker compose down
```

### Docker Container Thủ Công

```bash
# Build image
docker build -t smartcity-pipeline .

# Chạy LIVE 20 phút
docker run --rm \
    -v $(pwd)/data:/app/data \
    -v $(pwd)/logs:/app/logs \
    smartcity-pipeline \
    --host dathoc.net --port 443 --ws-path /mq \
    --username test1 --password '123456' \
    --topic 'v1/C001/+/up/telemetry' \
    --duration 1200 \
    --insecure

# Chạy replay 10k
docker run --rm \
    -v $(pwd)/data:/app/data \
    -v $(pwd)/logs:/app/logs \
    smartcity-pipeline \
    python3 replay.py \
    --input data/raw/raw_events_<timestamp>.jsonl \
    --target 10000
```

---

## Debug — Kiểm Tra Kết Nối

### Không Nhận Được Data

```bash
# Chạy baseline test
python baseline.py \
    --host dathoc.net --port 443 --ws-path /mq \
    --username test1 --password '123456' \
    --topic 'v1/C001/+/up/telemetry' \
    --duration 30 \
    --insecure
```

**Kết quả:**
- `CONNECTED` + `SUBSCRIBED` + `total=0` → Broker không có data
- `CONNECT FAILED` → Lỗi kết nối
- `SSL error` → Thêm flag `--insecure`

### Kiểm Tra Log

```bash
# Xem log pipeline
tail -f logs/pipeline.log

# Xem log ingestion
tail -f logs/ingest.log

# Xem log validation
tail -f logs/validate.log
```

### Kiểm Tra Queue

Nếu queue đầy (backpressure), sẽ có log:
```
[ingestion] WARNING Queue full, blocking...
```

Giải pháp:
- Tăng `--queue-maxsize` (mặc định 20000)
- Hoặc giảm tốc độ broker

---

## Monitor Throughput

### Log Output Mẫu

**Ingestion:**
```
[ingestion] INFO [INGEST] window=10.0s recv=10234 rate=1023.4 msg/s total=10234
```

**Validation:**
```
[validation] INFO [VALIDATE] window=10.0s valid=10089 invalid=145 rate=1023.3 msg/s total_valid=10089 total_invalid=145
```

### Summary JSON

```json
{
  "run_timestamp": "20260904_143000",
  "duration_seconds": 1200,
  "ingestion": {
    "total_received": 125000
  },
  "validation": {
    "total_processed": 125000,
    "valid": 123100,
    "invalid": 1900,
    "error_rate_pct": 1.52,
    "elapsed_seconds": 1200.0,
    "avg_rate_msg_per_s": 104.17,
    "top_error_types": {
      "range_error": 1900
    }
  },
  "raw_data_file": "data/raw/raw_events_20260904_143000.jsonl"
}
```

---

## Kết Quả Mong Đợi

### Sau 20 phút chạy LIVE

| Metric | Giá trị |
|--------|---------|
| Total messages | ~125,000 (tùy tốc độ broker) |
| Avg throughput | ~1,040 msg/s |
| Elapsed | 1200s |
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

## Mentor Yêu Cầu Gửi

- Code (toàn bộ file)
- Log (`ingest.log`, `validate.log`)
- Kết quả (`summary.json`)

---

## Lưu Ý Quan Trọng

1. **Data online:** 9h-19h (giờ VN)
2. **Chạy tuần tự:** Không tự dựng logic song song/worker
3. **Replay:** Hoàn thành 10k trước, rồi mới chạy 100k
4. **Debug:** Luôn chạy baseline trước khi chạy LIVE
5. **TLS:** Dùng `--insecure` nếu có lỗi certificate
