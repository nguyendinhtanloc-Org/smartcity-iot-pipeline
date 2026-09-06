# Hướng Dẫn Sử Dụng — SmartCity IoT Pipeline (Multi-Source)

## Yêu Cầu Hệ Thống

- Python 3.11+
- pip (package manager)
- Docker & Docker Compose (tùy chọn)
- PostgreSQL 15+ (cho production)

---

## Cài Đặt

### Cách 1: Cài đặt trực tiếp (Local)

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

### Cách 2: Docker (Khuyến nghị cho Production)

```bash
# Build image
docker compose build

# Hoặc build thủ công
docker build -t smartcity-pipeline .
```

---

## Cấu Hình Nguồn Dữ Liệu (Multi-Source)

Chỉnh sửa file `config/sources.yaml`:

```yaml
mqtt_sources:
  - name: "CN_A"
    host: "dathoc.net"
    port: 443
    ws_path: "/mq"
    username: "test1"
    password: "123456"
    company_id: "C001"
    gateways:
      - "electricity"
      - "water"
      - "lighting"
    khu_cn: "A"
    topic: "v1/C001/+/up/telemetry"
    qos: 0

  - name: "CN_B"
    host: "dathoc.net"
    port: 443
    ws_path: "/mq"
    username: "test1"
    password: "123456"
    company_id: "C002"
    gateways:
      - "electricity"
      - "water"
      - "lighting"
    khu_cn: "B"
    topic: "v1/C002/+/up/telemetry"
    qos: 0

  - name: "CN_C"
    host: "dathoc.net"
    port: 443
    ws_path: "/mq"
    username: "test1"
    password: "123456"
    company_id: "C003"
    gateways:
      - "electricity"
      - "water"
      - "lighting"
    khu_cn: "C"
    topic: "v1/C003/+/up/telemetry"
    qos: 0

global:
  queue_maxsize: 20000
  reconnect_delay: 5
  checkpoint_interval: 1000
  log_level: "INFO"
```

---

## Chạy Pipeline

### 1. Verify Kết Nối Broker (Baseline)

```bash
python src/baseline.py \
    --host dathoc.net --port 443 --ws-path /mq \
    --username test1 --password '123456' \
    --topic 'v1/C001/+/up/telemetry' \
    --duration 60 --insecure
```

**Kết quả:**
- `total=0` → Broker không có data → Liên hệ mentor bật simulator
- `total>0` → Broker có data → Tiếp tục chạy pipeline

**Log mẫu:**
```
=== BASELINE TEST ===
Broker: dathoc.net:443/mq
Topic: v1/C001/+/up/telemetry
Duration: 60s

[OK] CONNECTED
[OK] SUBSCRIBED topic=v1/C001/+/up/telemetry mid=1

=== BASELINE RESULT ===
Total messages: 125,000
Elapsed: 60.0s
Throughput: 2,083.3 msg/s
```

---

### 2. Chạy Pipeline Multi-Source (20 phút)

```bash
# Cấu hình sources trong config/sources.yaml trước
python main.py --config config/sources.yaml --duration 1200
```

**Parameters:**

| Parameter | Mặc định | Mô tả |
|-----------|----------|-------|
| `--config` | config/sources.yaml | File config multi-source |
| `--duration` | 1200 | Thời gian chạy (giây) |
| `--queue-maxsize` | 20000 | Queue max size |
| `--log-level` | INFO | DEBUG/INFO/WARNING/ERROR |

**Kết quả:**
- `logs/ingest.log` — Log Ingestion (per source)
- `logs/validate.log` — Log Validation
- `logs/detect.log` — Log Detection
- `logs/alert.log` — Log Alert
- `logs/storage.log` — Log Storage
- `logs/summary.json` — Tổng hợp số liệu
- `logs/invalid_events.jsonl` — Message lỗi schema/range
- `logs/dead_letter.jsonl` — Poison pill (device lỗi ≥3 lần)

**Log mẫu Ingestion:**
```
[ingestion] INFO [CN_A] CONNECTED
[ingestion] INFO [CN_A] SUBSCRIBED topic=v1/C001/+/up/telemetry mid=1
[ingestion] INFO [CN_A] window=10.0s recv=10234 rate=1023.4 msg/s total=10234
[ingestion] INFO [CN_B] window=10.0s recv=9876 rate=987.6 msg/s total=9876
[ingestion] INFO [CN_C] window=10.0s recv=10123 rate=1012.3 msg/s total=10123
```

**Log mẫu Validation:**
```
[validation] INFO window=10.0s valid=30123 invalid=45 rate=3016.8 msg/s total_valid=30123 total_invalid=45
[validation] INFO DONE total=360000 valid=359820 invalid=180 error_rate=0.05% avg_rate=3000.0 msg/s
```

**Log mẫu Detection:**
```
[detect] INFO window=10.0s processed=30000 violations=15 alerts=2 rate=3000.0 msg/s
[detect] WARNING [ALERT] 3-strike violation: A:DEV-001 (streak=3)
```

---

### 3. Kiểm Tra Kết Quả

```bash
# Xem tổng hợp
cat logs/summary.json

# Xem chi tiết Ingestion
cat logs/ingest.log | tail -10

# Xem chi tiết Validation
cat logs/validate.log | tail -10

# Xem chi tiết Detection
cat logs/detect.log | tail -10

# Xem message lỗi (10 dòng đầu)
head -10 logs/invalid_events.jsonl

# Xem poison pill
cat logs/dead_letter.jsonl
```

---

### 4. Test Replay (Tăng Tải Tuần Tự)

```bash
# Chạy baseline trước để lấy data
python main.py --config config/sources.yaml --duration 1200

# Replay 10k messages (nhanh, không delay)
python src/replay.py --input data/raw/raw_events_<timestamp>.jsonl --target 10000

# Replay 100k messages
python src/replay.py --input data/raw/raw_events_<timestamp>.jsonl --target 100000

# Replay với tốc độ 20k msg/s trong 20 phút
python src/replay.py --input data/raw/raw_events_<timestamp>.jsonl --rate 20000 --duration 1200
```

---

### 5. Chạy Daily Report

```bash
# Tạo báo cáo hôm qua
python -m src.daily_report

# Tạo báo cáo cho ngày cụ thể
python -c "
from src.daily_report import run_daily_report
from datetime import datetime
run_daily_report(target_date=datetime(2026, 9, 6))
"
```

---

## Chạy với Docker

### Docker Compose (Khuyến nghị)

```bash
# Build & chạy
docker compose up --build

# Chạy nền
docker compose up --build -d

# Xem logs
docker compose logs -f app

# Dừng
docker compose down
```

### Docker Container Thủ Công

```bash
# Build image
docker build -t smartcity-pipeline .

# Chạy pipeline 20 phút
docker run --rm \
    -v $(pwd)/data:/app/data \
    -v $(pwd)/logs:/app/logs \
    -v $(pwd)/config:/app/config \
    -v $(pwd)/src:/app/src \
    smartcity-pipeline \
    python main.py --config config/sources.yaml --duration 1200

# Chạy replay 10k
docker run --rm \
    -v $(pwd)/data:/app/data \
    -v $(pwd)/logs:/app/logs \
    smartcity-pipeline \
    python src/replay.py --input data/raw/raw_events_<timestamp>.jsonl --target 10000
```

---

## Debug & Troubleshooting

### Không Nhận Được Data

```bash
# Chạy baseline test
python src/baseline.py \
    --host dathoc.net --port 443 --ws-path /mq \
    --username test1 --password '123456' \
    --topic 'v1/C001/+/up/telemetry' \
    --duration 30 --insecure
```

**Kết quả:**
- `CONNECTED` + `SUBSCRIBED` + `total=0` → Broker không có data
- `CONNECT FAILED` → Lỗi kết nối mạng
- `SSL error` → Thêm flag `--insecure`

### Kiểm Tra Log Real-time

```bash
# Log tổng
tail -f logs/pipeline.log

# Log Ingestion
tail -f logs/ingest.log

# Log Validation
tail -f logs/validate.log

# Log Detection
tail -f logs/detect.log

# Log Storage
tail -f logs/storage.log
```

### Kiểm Tra Queue Backpressure

```
[ingestion] WARNING Queue full, blocking...
```

**Giải pháp:**
- Tăng `--queue-maxsize` (mặc định 20000)
- Kiểm tra downstream stages có bị chậm không

---

## Monitor Throughput

### Log Output Mẫu

**Ingestion (per source):**
```
[ingestion] INFO [CN_A] window=10.0s recv=10234 rate=1023.4 msg/s total=10234
[ingestion] INFO [CN_B] window=10.0s recv=9876 rate=987.6 msg/s total=9876
```

**Validation:**
```
[validation] INFO window=10.0s valid=30123 invalid=45 rate=3016.8 msg/s total_valid=30123 total_invalid=45
```

**Detection:**
```
[detect] INFO window=10.0s processed=30000 violations=15 alerts=2 rate=3000.0 msg/s
```

**Storage:**
```
[storage] INFO window=10.0s stored=30000 buffer=0 rate=3000.0 msg/s
```

### Summary JSON

```json
{
  "run_timestamp": "20260907_143000",
  "duration_seconds": 1200,
  "pipeline_stages": ["ingestion", "validation", "detection", "alert", "storage"],
  "ingestion": {
    "CN_A": {"total": 120000, "avg_rate": 1000},
    "CN_B": {"total": 115000, "avg_rate": 958},
    "CN_C": {"total": 110000, "avg_rate": 917}
  },
  "validation": {
    "total_processed": 345000,
    "valid": 344800,
    "invalid": 200,
    "error_rate_pct": 0.06
  },
  "detection": {
    "total_processed": 344800,
    "violations": 150,
    "alerts_triggered": 12
  },
  "storage": {
    "total_stored": 345000,
    "errors": 0
  }
}
```

---

## Lưu Ý Quan Trọng

1. **Data online:** 9h-19h (giờ VN)
2. **Chạy tuần tự:** Baseline → Pipeline → Replay (10k → 100k)
3. **Config thật:** Cập nhật `config/sources.yaml` với info thật từ mentor
4. **TLS:** Dùng `--insecure` nếu có lỗi certificate
5. **PostgreSQL:** Cần chạy PostgreSQL 15+ cho storage
6. **Sequential processing:** Không tự dựng logic parallel thủ công (dùng Kafka/Redis khi scale thật)

---

## Files Quan Trọng

| File | Mô tả |
|------|-------|
| `config/sources.yaml` | Config 3 nguồn MQTT |
| `src/schemas.py` | Schema `UnifiedTelemetry` + `khu_cn` |
| `src/ingest.py` | Multi-threaded ingestion |
| `src/validate.py` | Validation + poison pill |
| `src/detect.py` | Threshold + 3-strike |
| `src/alert.py` | Telegram/Email alert |
| `src/storage.py` | Postgres batch insert |
| `src/daily_report.py` | Daily report generator |
| `main.py` | Orchestration entry point |
| `docker-compose.yml` | App + Postgres containers |