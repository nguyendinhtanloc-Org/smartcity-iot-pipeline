# Kiến Trúc Dự Án — SmartCity IoT Pipeline (Multi-Source)

## Tổng Quan

Pipeline xử lý dữ liệu IoT thời gian thực từ **3 khu công nghiệp** (A, B, C) thông qua **multi-threaded ingestion** từ MQTT WSS, validate dữ liệu theo schema thống nhất, phát hiện vi phạm ngưỡng nghiệp vụ, gửi cảnh báo và lưu trữ vào PostgreSQL.

---

## Kiến Trúc Hệ Thống

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  CN A       │  │  CN B       │  │  CN C       │
│  (mqtt1)    │  │  (mqtt2)    │  │  (mqtt3)    │
│  v1/C001/+/ │  │  v1/C002/+/ │  │  v1/C003/+/ │
│  up/telemetry   up/telemetry   up/telemetry   │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────┐
│              MULTI-SOURCE INGESTION (Multi-thread)          │
│  3 MQTT Workers (Thread) → Unified Queue + khu_cn, source  │
└────────────────────────────────┬────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│                     VALIDATION                              │
│  Schema (Pydantic UnifiedTelemetry) + Range Check          │
│  Poison Pill: device lỗi ≥3 lần → dead_letter               │
└────────────────────────────────┬────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│                     DETECT                                  │
│  Rule-based Threshold + 3-Strike Violation Counter         │
└────────────────────────────────┬────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│                     ALERT                                   │
│  3-Strike Violation → Telegram Bot / SMTP Email            │
└────────────────────────────────┬────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│                     STORAGE (PostgreSQL)                    │
│  raw_events | violations | alerts  (Batch Insert)          │
└────────────────────────────────┬────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│                     DAILY REPORT                            │
│  Group by khu_cn/device/group → Telegram/Email/JSON        │
└─────────────────────────────────────────────────────────────┘
```

---

## Luồng Dữ Liệu (Data Flow)

### 1. Multi-Source Ingestion (ingest.py)

```
MQTT Message (CN A/B/C)
    │
    ▼
3 Thread Workers (paho-mqtt WSS)
    │
    ▼
Parse JSON → Enrich (+khu_cn, source_name, received_at)
    │
    ▼
Unified Queue (thread-safe, bounded)
    │
    ▼
Log throughput per source (10s window)
```

**Chi tiết:**
- 3 Thread Workers, mỗi thread connect 1 MQTT source (CN A, B, C)
- Topic: `v1/C001/+/up/telemetry`, `v1/C002/+/up/telemetry`, `v1/C003/+/up/telemetry`
- Parse JSON → Enrich payload: `+khu_cn, +source_name, +received_at`
- Push vào Unified Queue (bounded, maxsize=20000)
- Log throughput per source mỗi 10 giây
- Auto-reconnect khi MQTT rớt (configurable delay)

---

### 2. Validation (validate.py)

```
Queue Item (topic, payload)
    │
    ▼
Schema Check (Pydantic UnifiedTelemetry)
    │
    ├──(Schema Error)──▶ error_type="schema_error"
    │                    Ghi invalid_events.jsonl
    │
    ▼
Valid Message ──▶ Detect Queue
```

**Logic Validation:**
1. **Schema Check:** Validate với `UnifiedTelemetry` (Pydantic v2)
   - Required fields: `dev_id`, `ts`, `tsunix`, `U`, `I`, `Power_kW`, `Energy_kwh`, `Alr_Current`, `Alr_Volt`, `Mode_c`, `Line 8/9/10`, `khu_cn`, `source_name`
   - Optional: `Lux`, `Contactor`
2. **Range Check:** Kiểm tra ngưỡng vật lý (voltage 180-250V, current 0-100A, power 0-50kW, etc.)
3. **Poison Pill:** Device lỗi liên tục ≥3 lần → ghi `dead_letter.jsonl`

---

### 3. Detection (detect.py)

```
Valid Message
    │
    ▼
Threshold Check (theo group: electricity/water/lighting)
    │
    ├──(Violation)──▶ Violation streak++
    │                    │
    │                    ├── streak ≥ 3 → Alert Queue + Alert triggered
    │                    │
    │                    ▼
    │               Reset streak = 0 (khi normal)
    │
    ▼
Pass to Alert Queue
```

**Logic Detect:**
- **Threshold Rules:** Ngưỡng nghiệp vụ theo group (electricity: U 180-250V, I 0-100A, Power 0-50kW; Water: flow 0-100, pressure 0-10, pH 6.5-8.5; Lighting: Power 0-10kW, Lux 0-10000)
- **3-Strike Rule:** Device vi phạm ≥3 lần liên tiếp → trigger Alert
- **State Management:** Track `violation_streak`, `total_violations` per device per khu_cn

---

### 4. Alert (alert.py)

```
Alert Item (3-strike violation)
    │
    ▼
Format Message (Markdown)
    │
    ├──▶ Telegram Bot API (multi chat_id)
    │
    ├──▶ SMTP Email (multi recipient)
    │
    ▼
Pass to Storage Queue
```

---

### 5. Storage (storage.py)

```
Queue Items (events + alerts)
    │
    ▼
Buffer (batch_size=100, flush_interval=5s)
    │
    ▼
PostgreSQL Batch Insert
    ├── raw_events (dev_id, khu_cn, payload JSONB)
    ├── violations (dev_id, khu_cn, field, value, min/max, streak)
    └── alerts (alert_type, dev_id, khu_cn, streak, payload)
```

---

### 6. Daily Report (daily_report.py)

```
Schedule: 00:00 hàng ngày
    │
    ▼
Query PostgreSQL (raw_events, violations, alerts)
    │
    ▼
Aggregate: total_events, violations_by_group/khu_cn, top_devices, 3-strike_alerts
    │
    ▼
Generate: JSON file + Telegram Markdown + Email HTML
    │
    ▼
Send notifications
```

---

## Cấu Trúc Files

```
smartcity-iot-pipeline/
├── config/
│   └── sources.yaml          # Config 3 nguồn MQTT (CN A, B, C)
├── src/
│   ├── __init__.py
│   ├── ingest.py             # MultiSourceIngestor (multi-thread)
│   ├── validate.py           # Validate (Pydantic + range + poison pill)
│   ├── detect.py             # Detect violations (threshold + 3-strike)
│   ├── alert.py              # Alert (Telegram/Email)
│   ├── storage.py            # Postgres batch insert
│   ├── daily_report.py       # Daily report generator
│   ├── schemas.py            # UnifiedTelemetry + khu_cn
│   ├── baseline.py           # Baseline test
│   ├── replay.py             # Replay tool
│   └── mor_payload.py        # Mentor's monitor script
├── main.py                   # Entry point - orchestrate all stages
├── docker-compose.yml        # 2 containers: app + postgres
├── Dockerfile
├── requirements.txt
├── config.yaml
└── README.md
```

---

## Docker Deployment

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:15-alpine
    container_name: smartcity-postgres
    environment:
      - POSTGRES_DB=smartcity
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5

  app:
    build: .
    container_name: smartcity-pipeline
    environment:
      - DB_HOST=postgres
      - DB_PORT=5432
      - DB_NAME=smartcity
      - DB_USER=postgres
      - DB_PASSWORD=postgres
      - PYTHONUNBUFFERED=1
    command:
      - "python"
      - "main.py"
      - "--config"
      - "config/sources.yaml"
      - "--duration"
      - "1200"
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
      - ./config:/app/config
      - ./src:/app/src
    depends_on:
      postgres:
        condition: service_healthy

volumes:
  postgres_data:
```

---

## Scale lên 500k–1M msg/s (Giai đoạn sau)

| Component | Scale Strategy |
|-----------|----------------|
| **Ingestion** | Kafka/Redpanda (partition by device_id), multi-replica ingestion pods |
| **Validation/Detect** | Stateless workers, horizontal scaling via Kubernetes HPA |
| **State** | Redis Cluster (violation counter, checkpoint, session) |
| **Storage** | ClickHouse/TimescaleDB (time-series), partitioned by time + khu_cn |
| **Orchestration** | Kubernetes (Deployment, Service, ConfigMap, Secret) |
| **Monitoring** | Prometheus + Grafana (throughput, lag, error rate, latency) |

---

## Môi Trường

- **Python:** 3.11+
- **MQTT Broker:** dathoc.net:443 (WSS)
- **Topics:** `v1/C001/+/up/telemetry`, `v1/C002/+/up/telemetry`, `v1/C003/+/up/telemetry`
- **Data online:** 9h-19h (giờ VN)
- **TLS:** `--insecure` để bypass certificate verification
- **PostgreSQL:** 15+ (JSONB support)

---

## Phụ Thuộc (Dependencies)

```txt
paho-mqtt>=2.0.0
pydantic>=2.0.0
psycopg2-binary>=2.9.0
requests>=2.31.0
pyyaml>=6.0
```