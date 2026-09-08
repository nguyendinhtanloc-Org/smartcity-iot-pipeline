# Hướng Dẫn Sử Dụng

## Yêu cầu

- Python 3.11+
- PostgreSQL 15+ (hoặc chạy không cần DB với resilient mode)

## Cài đặt

```bash
git clone <repo-url>
cd smartcity-iot-pipeline
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**Lưu ý Windows:** Phải dùng WSL hoặc Git Bash, không dùng PowerShell (không có `python3` trong PATH).

## Chạy Pipeline

### 1. Baseline (Đo Throughput Broker)

Trước khi chạy pipeline, nên đo throughput MQTT thuần làm baseline:

```bash
python3 baseline.py --host dathoc.net --port 443 --ws-path /mq \
    --username test1 --password '123456' \
    --topic 'v1/C001/+/up/telemetry' \
    --topic 'v1/C002/+/up/telemetry' \
    --topic 'v1/C003/+/up/telemetry' \
    --duration 1200 --insecure
```

**Kết quả:** `logs/baseline/baseline_<ts>_summary.json`

### 2. Chạy Ingestion (Standalone)

```bash
python3 src/ingest.py \
    --config config/sources.yaml \
    --duration 1200
```

**Kết quả:** `logs/ingest_summary.json` + raw files per source

### 3. Chạy Pipeline Đầy Đủ

```bash
python3 main.py --config config/sources.yaml --duration 1200
```

**Kết quả:** `logs/summary.json` + all stage logs

## Chạy với Docker

```bash
# Build và chạy
docker compose up --build

# Xem logs
docker compose logs -f app

# Dừng
docker compose down
```

## Config

**File:** `config/sources.yaml`

### Thêm nguồn mới

```yaml
sources:
  CN_D_D:
    name: "CN_D_D"
    host: dathoc.net
    port: 443
    ws_path: /mq
    username: test2
    password: "654321"
    topic: "v1/C004/+/up/telemetry"
    use_websocket: true
    use_ssl: true
    insecure: true
    tls_version: "tlsv1_2"
    protocol_version: "5"
    clean_start: true
    keepalive: 30
    subscribe_ack_timeout: 30
    max_inflight: 10000
    queue_maxsize: 100000
    send_speed_bytes_per_sec: 1000000
    max_retry: 3
    retry_delay: 2
    report_interval: 10
    raw_flush_lines: 1000
```

### Thay đổi config

| Key | Mô tả | Mặc định |
|-----|-------|----------|
| `queue_maxsize` | Kích thước queue tối đa | 100,000 |
| `max_retry` | Số lần retry khi mất kết nối | 3 |
| `retry_delay` | Delay giữa mỗi retry (giây) | 2 |
| `report_interval` | Khoảng cách giữa mỗi lần log report (giây) | 10 |
| `raw_flush_lines` | Số dòng flush raw file | 1000 |

## Log Output

### Pipeline

```bash
# Xem log real-time
tail -f logs/pipeline.log
tail -f logs/ingest.log
tail -f logs/validate.log
tail -f logs/detect.log
```

### Xem kết quả

```bash
# Kết quả ingestion
cat logs/ingest_summary.json | python3 -m json.tool

# Kết quả baseline
cat logs/baseline/baseline_*_summary.json | python3 -m json.tool

# Pipeline summary
cat logs/summary.json | python3 -m json.tool

# Invalid events
wc -l logs/invalid_events.jsonl

# Dead letter (poison pill)
wc -l logs/dead_letter.jsonl
```

## Validate File

```bash
python3 -m py_compile main.py
python3 -m py_compile schemas.py
python3 -m py_compile baseline.py
python3 -m py_compile src/ingest.py
python3 -m py_compile src/validate.py
python3 -m py_compile src/detect.py
python3 -m py_compile src/alert.py
python3 -m py_compile src/storage.py
python3 -m py_compile src/daily_report.py
```

## Troubleshooting

### Không nhận data từ broker

1. Kiểm tra kết nối network
2. Kiểm tra credentials trong `config/sources.yaml`
3. Chạy `baseline.py` trước để verify broker có data
4. Kiểm tra topic format: `v1/{company}/{gateway}/up/telemetry`

### Broker chỉ có CN_A data

Hiện tại broker `dathoc.net` chỉ có CN_A (`C001`) có data stream. CN_B (`C002`) và CN_C (`C003`) kết nối được nhưng không có data.

### Queue full (drops > 0)

Tăng `queue_maxsize` trong config hoặc tăng `report_interval` để downstream xử lý nhanh hơn.

### Import error

Đảm bảo chạy từ root directory:
```bash
cd smartcity-iot-pipeline
python3 main.py ...
```
