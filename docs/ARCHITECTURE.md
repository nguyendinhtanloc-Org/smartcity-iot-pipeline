# Kiến Trúc Dự Án — SmartCity IoT Pipeline

## Tổng Quan

Pipeline xử lý dữ liệu IoT thời gian thực từ các cảm biến điện/nước/ánh sáng trong hệ thống SmartCity. Dữ liệu được thu nhận qua MQTT, validate theo schema và range vật lý, sau đó lưu trữ để phân tích tiếp.

---

## Kiến Trúc Hệ Thống

```
┌─────────────────────────────────────────────────────────────┐
│                    MQTT Broker (dathoc.net:443)             │
│                    Topic: v1/C001/+/up/telemetry                    │
└──────────────────────────┬──────────────────────────────────┘
                           │ WebSocket Secure (WSS)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                     BASELINE (baseline.py)                  │
│              Đo tốc độ thực tế trước khi chạy              │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  main.py (Entry Point)                      │
│                                                             │
│  ┌──────────────────────┐    ┌──────────────────────┐      │
│  │   INGESTION          │    │   VALIDATION          │      │
│  │   (ingest.py)        │    │   (validate_stage.py) │      │
│  │                      │    │                       │      │
│  │  • MQTT subscribe    │───▶│  • Schema check       │      │
│  │  • Parse JSON        │    │  • Range validation   │      │
│  │  • Push to queue     │    │  • Poison pill detect │      │
│  │  • Write raw .jsonl  │    │  • Write invalid logs │      │
│  │  • Log throughput    │    │  • Log statistics     │      │
│  └──────────┬───────────┘    └──────────┬────────────┘      │
│             │                           │                   │
│             ▼                           ▼                   │
│  data/raw/raw_events_*.jsonl   logs/invalid_events.jsonl   │
│                                logs/dead_letter.jsonl       │
└─────────────────────────────────────────────────────────────┘
```

---

## Luồng Dữ Liệu (Data Flow)

### 1. Thu Nhận (Ingestion)

```
MQTT Message
    │
    ▼
Parse JSON ──(Lỗi)──▶ Ghi raw với _raw_unparsed
    │
    ▼
Đẩy vào Queue (bounded, maxsize=20000)
    │
    ▼
Ghi raw file .jsonl (dùng cho replay)
```

**Chi tiết:**
- Kết nối WSS (TLS, force IPv4)
- Subscribe topic `v1/C001/+/up/telemetry`
- Nhận message → parse JSON → push vào `queue.Queue`
- Ghi raw message xuống `data/raw/raw_events_<timestamp>.jsonl`
- Log throughput mỗi 10 giây
- Xử lý reconnect tự động khi MQTT rớt

### 2. Xác Thực (Validation)

```
Queue Item (topic, payload)
    │
    ▼
Schema Check (Pydantic)
    │
    ├──(Lỗi schema)──▶ error_type="schema_error"
    │                   Ghi vào invalid_events.jsonl
    │
    ▼
Range Check (theo device_type)
    │
    ├──(Lỗi range)───▶ error_type="range_error"
    │                   Ghi vào invalid_events.jsonl
    │
    ▼
Valid Message ──▶ Chuyển sang Detect (giai đoạn sau)
```

**Logic xử lý:**

1. **Bước 1 — Schema Check:**
   - Kiểm tra field bắt buộc: `event_id`, `device_id`, `ts`, `metrics`, `quality`
   - Kiểm tra kiểu dữ liệu (string, int, dict)
   - Sử dụng Pydantic `ValidationError` để bắt lỗi

2. **Bước 2 — Range Check:**
   - Kiểm tra giá trị vật lý theo từng nhóm thiết bị
   - **Water:** pH 0-14, pressure 0-50 bar, flow ≥ 0, temperature -10~60°C
   - **Electricity/Light:** Sử dụng `GenericMetrics` (TODO khi có sample)

3. **Poison Pill Handling:**
   - Device nào lỗi liên tục ≥3 lần → đẩy sang `dead_letter.jsonl`
   - Reset counter khi device gửi message hợp lệ

---

## Cấu Trúc Files

```
smartcity-iot-pipeline/
├── main.py              # Entry point — chạy LIVE
├── baseline.py          # Đo baseline — tốc độ từ broker
├── ingest.py            # Chặng Ingestion
├── validate_stage.py    # Chặng Validation
├── schemas.py           # Pydantic schema + range validation
├── replay.py            # Test replay data local
├── mor_payload.py       # Script monitor từ mentor
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── docs/
│   └── ARCHITECTURE.md  # Tài liệu này
├── data/
│   └── raw/
│       └── raw_events_<timestamp>.jsonl
└── logs/
    ├── pipeline.log
    ├── ingest.log
    ├── validate.log
    ├── summary.json
    ├── invalid_events.jsonl
    ├── dead_letter.jsonl
    └── replay/
        ├── replay_10000.log
        ├── replay_10000_summary.json
        ├── replay_100000.log
        └── replay_100000_summary.json
```

---

## Mô Tả Từng File

### `main.py`
- **Vai trò:** Entry point của pipeline
- **Chức năng:**
  - Khởi tạo queue nội bộ (bounded, maxsize=20000)
  - Tạo `Ingestor` và `Validator`
  - Chạy Validation trên thread riêng
  - Chạy Ingestion trên main thread
  - Ghi `summary.json` khi hoàn thành

### `ingest.py`
- **Vai trò:** Chặng Ingestion
- **Lớp:** `Ingestor`
- **Công nghệ:** paho-mqtt (WebSocket Secure), queue.Queue
- **Chức năng:**
  - Kết nối MQTT WSS
- Subscribe topic `v1/C001/+/up/telemetry`
- Parse JSON, push vào queue
  - Ghi raw file .jsonl
  - Log throughput mỗi 10 giây

### `validate_stage.py`
- **Vai trò:** Chặng Validation
- **Lớp:** `Validator`
- **Công nghệ:** Pydantic (schema + range validation)
- **Chức năng:**
  - Đọc tuần tự từ queue
  - Validate schema + range vật lý
  - Phân loại valid/invalid
  - Xử lý poison pill (device lỗi liên tục)
  - Ghi invalid events và dead letter

### `schemas.py`
- **Vai trò:** Định nghĩa schema và logic validation
- **Công nghệ:** Pydantic BaseModel
- **Chứa:**
  - `EventEnvelope`: Schema chung cho mọi message
  - `Quality`: Schema cho chất lượng tín hiệu
  - `WaterMetrics`: Schema cho thiết bị nước (có range)
  - `GenericMetrics`: Schema tạm cho electricity/light
  - `validate_event()`: Hàm validate chính

### `baseline.py`
- **Vai trò:** Đo tốc độ thực tế từ broker
- **Công nghệ:** paho-mqtt
- **Chức năng:**
  - Kết nối MQTT, subscribe, đếm message
  - Không validate, không lưu
  - Chỉ đo throughput thuần

### `replay.py`
- **Vai trò:** Test tăng tải tuần tự
- **Công nghệ:** itertools (cycle, islice)
- **Chức năng:**
  - Đọc data thật từ `raw_events_*.jsonl`
  - Nhân bản lên target (10k, 100k)
  - Validate tuần tự (không song song)
  - Ghi kết quả vào `logs/replay/`

### `mor_payload.py`
- **Vai trò:** Script monitor từ mentor
- **Chức năng:** Subscribe & in message ra màn hình

---

## Schema Validation Chi Tiết

### EventEnvelope (Mọi thiết bị)

| Field | Kiểu | Bắt buộc | Ghi chú |
|-------|------|-----------|---------|
| `event_id` | string | ✅ | |
| `schema_version` | string | ✅ | |
| `source` | string | ✅ | |
| `device_id` | string | ✅ | |
| `device_type` | string | ✅ | |
| `group` | string | ✅ | water/electricity/light |
| `location_id` | string | ✅ | |
| `ts` | int | ✅ | Epoch milliseconds |
| `ts_iso` | string | ✅ | |
| `local_hour` | float | ✅ | |
| `seq` | int | ✅ | |
| `use_case` | string | ✅ | |
| `alerts` | list | ❌ | Default: [] |
| `metrics` | dict | ✅ | |
| `quality` | Quality | ✅ | |

### Quality

| Field | Kiểu | Bắt buộc | Range |
|-------|------|-----------|-------|
| `rssi_dbm` | float | ✅ | |
| `snr_db` | float | ✅ | |
| `latency_ms` | float | ✅ | ≥ 0 |

### WaterMetrics

| Field | Kiểu | Range | Ghi chú |
|-------|------|-------|---------|
| `flow_m3_h` | float | 0-1000 | Lưu lượng |
| `pressure_bar` | float | 0-50 | Áp suất |
| `cumulative_m3` | float | ≥ 0 | Tổng lưu lượng |
| `turbidity_ntu` | float | ≥ 0 | Độ đục |
| `residual_chlorine_mg_l` | float | ≥ 0 | Clo dư |
| `ph` | float | 0-14 | Độ pH |
| `conductivity_us_cm` | float | ≥ 0 | Độ dẫn điện |
| `temperature_c` | float | -10~60 | Nhiệt độ |
| `status` | string | - | Trạng thái |

### GenericMetrics (Electricity/Light)

- Hiện tại chỉ kiểm tra có field `status` không
- Các field còn lại là số hợp lệ (không NaN/None sai kiểu)
- **TODO:** Bổ sung khi có sample thật

---

## Docker

### Dockerfile

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENTRYPOINT ["python3", "-u", "main.py"]
CMD ["--duration", "1200"]
```

### docker-compose.yml

```yaml
services:
  app:
    build: .
    container_name: smartcity-ingest-validate
    environment:
      - MQTT_HOST=dathoc.net
      - MQTT_PORT=443
      - MQTT_USERNAME=test1
      - MQTT_PASSWORD=123456
    command:
      - "--host=dathoc.net"
      - "--port=443"
      - "--ws-path=/mq"
      - "--username=test1"
      - "--password=123456"
      - "--topic=v1/C001/+/up/telemetry"
      - "--duration=1200"
      - "--insecure"
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
```

---

## Output Files

### `logs/summary.json`

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

### `logs/invalid_events.jsonl`

```json
{
  "topic": "v1/C001/+/up/telemetry",
  "error_type": "range_error",
  "error_detail": "...",
  "device_id": "device-001",
  "group": "water",
  "raw": { ... }
}
```

### `logs/dead_letter.jsonl`

```json
{
  "event_id": "...",
  "device_id": "device-001",
  "...": "..."
}
```

---

## Các Loại Lỗi Validation

| Loại lỗi | Mô tả | Ví dụ |
|-----------|-------|-------|
| `schema_error` | Thiếu field bắt buộc hoặc sai kiểu dữ liệu | Thiếu `device_id`, `ts` không phải int |
| `range_error` | Giá trị ngoài range vật lý hợp lệ | pH = 15, pressure = -1, temperature = 100°C |

**Lưu ý:** Đây là kiểm tra **schema + range vật lý**, KHÔNG phải ngưỡng vi phạm nghiệp vụ.

---

## Throughput Targets

| Mục tiêu | Phương pháp | Ghi chú |
|----------|-------------|---------|
| 2k msg/s | Chạy LIVE với broker | Target hiện tại |
| 10k msg | Replay data local | Test tuần tự |
| 100k msg | Replay data local | Test tuần tự |
| 500k-1M msg/s | Kafka/Redpanda + Cluster | Giai đoạn sau |

---

## Phụ Thuộc (Dependencies)

```txt
paho-mqtt>=2.0.0
pydantic>=2.0.0
```

---

## Môi Trường

- **Python:** 3.11+
- **MQTT Broker:** dathoc.net:443 (WSS)
- **Topic:** v1/C001/+/up/telemetry
- **Data online:** 9h-19h (giờ VN)
- **TLS:** Có thể dùng `--insecure` để bypass certificate verification
