# Kiến Trúc Pipeline IoT SmartCity

## Tổng Quan

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        PIPELINE IoT SMARTCITY                          │
│                                                                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                             │
│  │  MQTT    │  │  MQTT    │  │  MQTT    │   ← 3 luồng MQTT Worker    │
│  │  CN A    │  │  CN B    │  │  CN C    │     song song (Thread)      │
│  │ (C001)   │  │ (C002)   │  │ (C003)   │                             │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘                             │
│       └──────────────┼──────────────┘                                  │
│                      ▼                                                  │
│            ┌──────────────────┐                                        │
│            │  Unified Queue   │  ← queue.Queue(100,000)                │
│            │  (messages in)   │     += {area_id, data_type, source}    │
│            └────────┬─────────┘                                        │
│                     │                                                   │
│    ┌────────────────┼──────────────────┐                               │
│    │ [INGEST-AGG]   │                  │                               │
│    │ monitor 10s    │                  │                               │
│    ▼                ▼                  ▼                               │
│  ┌─────────┐  ┌──────────┐  ┌────────────┐                           │
│  │Validate │  │  Detect  │  │   Alert    │                            │
│  └────┬────┘  └────┬─────┘  └────┬───────┘                            │
│       └─────────────┼────────────┘                                     │
│                     ▼                                                  │
│            ┌──────────────────┐                                        │
│            │  PostgreSQL      │ ← postgres:15-alpine                  │
│            │  (containers)    │   data persisted vào volume            │
│            └──────────────────┘                                        │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Kiến Trúc Chi Tiết

### Stage 0: Baseline (Pre-Ingestion)

**Script:** `baseline.py` (root)  
**Mục đích:** Đo throughput MQTT thuần (không processing), serve như baseline cho ingestion.

**Đặc điểm:**
- Multi-topic subscribe (3 topics cho CN_A, CN_B, CN_C)
- Per-company + per-device-group breakdown (ELECTRIC, WATER, LIGHT)
- Ghi log: `logs/baseline/baseline_<ts>.log` + `_summary.json`
- Không enrich, không queue — chỉ đếm msg đến

**Cách chạy:**
```bash
python3 baseline.py --host dathoc.net --port 443 --ws-path /mq \
    --username test1 --password '123456' \
    --topic 'v1/C001/+/up/telemetry' \
    --topic 'v1/C002/+/up/telemetry' \
    --topic 'v1/C003/+/up/telemetry' \
    --duration 1200 --insecure
```

**Output:**
```
logs/baseline/baseline_20260907_153045.log
logs/baseline/baseline_20260907_153045_summary.json
```

**Summary JSON:**
```json
{
  "started_at": "2026-09-07T15:30:45+07:00",
  "ended_at": "2026-09-07T15:32:45+07:00",
  "duration_s": 120,
  "total_received": 1117126,
  "per_company": {
    "CN_A": 560000,
    "CN_B": 300000,
    "CN_C": 257126
  },
  "per_gateway": {
    "GW_ELECTRIC_001": 380000,
    "GW_WATER_001": 368563,
    "GW_LIGHT_001": 368563
  }
}
```

---

### Stage 1: Ingestion (Multi-Source)

**Script:** `src/ingest.py`  
**Mô tả:** 3 MQTT Worker threads chạy song song, mỗi thread nhận message từ 1 broker, enrich + metadata, gom vào unified queue chung.

**Cấu trúc:**

```
┌─────────────────────────────────────────────────────────────┐
│                   MULTI-SOURCE INGESTION                     │
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │ CN_A     │  │ CN_B     │  │ CN_C     │                  │
│  │ Worker   │  │ Worker   │  │ Worker   │ ← Thread         │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘                  │
│       └──────────────┼──────────────┘                        │
│                      ▼                                       │
│            ┌──────────────────┐                              │
│            │  Unified Queue   │  queue.Queue(100,000)        │
│            │  + Metadata      │  += area_id, data_type,      │
│            │                  │    source, source_gateway     │
│            └────────┬─────────┘                              │
│                     │                                        │
│         ┌───────────┼───────────────┐                       │
│         │ [INGEST-AGG]              │                       │
│         │ monitor thread 10s        │                       │
│         │ aggregate per source      │                       │
│         │ drops counter             │                       │
│         └───────────────────────────┘                       │
└─────────────────────────────────────────────────────────────┘
```

**Đặc điểm chính:**

1. **Multi-Source:** Mỗi CN (A/B/C) có config riêng trong `config/sources.yaml`
2. **Multi-Thread:** Mỗi CN chạy 1 MQTT Worker thread riêng
3. **Unified Queue:** Tất cả worker cùng gom vào 1 queue chung cho downstream processing
4. **Aggregate Monitor:** Thread `[INGEST-AGG]` log mỗi 10s:
   - Tổng rate (msg/s)
   - Per-source breakdown (CN_A: xx, CN_B: xx, CN_C: xx)
   - Drops counter (queue full)
5. **Raw File:** Mỗi source ghi raw messages ra file riêng (`raw_CN_A_A.jsonl`, `raw_CN_B_B.jsonl`, `raw_CN_C_C.jsonl`)
6. **Buffer Flush:** Flush buffer mỗi 1000 dòng hoặc khi timeout

**Cách chạy (standalone):**
```bash
python3 src/ingest.py \
    --config config/sources.yaml \
    --duration 1200
```

**Cách chạy (qua main.py):**
```bash
python3 main.py --config config/sources.yaml --duration 1200
```

**Output:**
```
logs/ingest_summary.json   # Tổng hợp ingestion
data/raw/raw_CN_A_A.jsonl  # Raw file CN A
data/raw/raw_CN_B_B.jsonl  # Raw file CN B (0 messages nếu broker không có data)
data/raw/raw_CN_C_C.jsonl  # Raw file CN C (0 messages nếu broker không có data)
```

**ingest_summary.json:**
```json
{
  "started_at": "2026-09-07T15:30:45+07:00",
  "ended_at": "2026-09-07T15:32:45+07:00",
  "duration_s": 120,
  "total": 274870,
  "rate_msg_s": 2748.7,
  "drops": 0,
  "per_source": {
    "CN_A_A": {
      "received": 238140,
      "rate_msg_s": 2381.4
    },
    "CN_B_B": {
      "received": 0,
      "rate_msg_s": 0
    },
    "CN_C_C": {
      "received": 0,
      "rate_msg_s": 0
    }
  },
  "raw_dir": "data/raw"
}
```

**Smoke test kết quả (live broker, 10s):**
```
[INGEST-AGG] total=13999 msgs | rate=1399.9 msg/s | drops=0 | CN_A:13999 (1399.9 msg/s) | CN_B:0 | CN_C:0
```

---

### Stage 2: Validation

**Script:** `src/validate.py`  
**Mô tả:** 3 Schema types + poison pill detection + 3-strike dead letter.

**3 Schema Types:**
- `WaterTelemetry`: timestamp, tsunix, dev_id, Qt, V
- `LightingTelemetry`: timestamp, tsunix, dev_id, U, I, Power_kW, Energy_kwh, Lux, Contactor
- `ElectricityTelemetry`: timestamp, tsunix, dev_id, Uab, Ubc, Uca, Ia, Ib, Ic, P_Total, F, PFavg, THD_V, THD_I

**Logic:**
1. `validate_event(payload)` - tự detect schema từ `dev_id` hoặc topic
2. Schema validation bằng Pydantic
3. Poison Pill: device gửi data sai liên tiếp 3 lần → dead_letter, skip device 5 phút

---

### Stage 3: Detect

**Script:** `src/detect.py`  
**Mô tả:** Rule-based threshold + 3-strike violation counter.

**Thresholds:**
- **Điện 3 pha:** Uab/Ubc/Uca ≥400V, |Ia-Ib|>2A hoặc |Ib-Ic|>2A, P<0.3 P_avg, F<48Hz, THD>8%
- **Nước:** Q<10 L/h khiوزن phát hiện, P🟡>600W (Pump ON)
- **Đèn:** I>150mA, Power_kW>0.03 (ON), Lux<200 khi ON

---

### Stage 4: Alert

**Script:** `src/alert.py`  
**Mô tả:** Gửi alert khi device vi phạm ≥3 streak.

**Channels:** Telegram Bot, SMTP Email (chưa config thật)

---

### Stage 5: Storage

**Script:** `src/storage.py`  
**Mô tả:** PostgreSQL batch insert. Resilient mode: chạy không cần DB.

**Tables:** `raw_events`, `violations`, `alerts`

---

### Stage 6: Daily Report

**Script:** `src/daily_report.py`  
**Mô tả:** Group by `khu_cn`/`device_group`/`dev_id`, xuất JSON/Telegram/Email.

---

## Schema Validation

**File:** `schemas.py` (root)

```
schemas.py
├── WaterTelemetry          (qt, v)
├── LightingTelemetry       (u, i, power_kw, energy_kwh, lux, contactor)
├── ElectricityTelemetry    (uab, ubc, uca, ia, ib, ic, p_total, f, pfavg, thd_v, thd_i)
├── UnifiedTelemetry        (schema union từ 3 schema trên)
└── validate_event(payload) → (is_valid, UnifiedTelemetry | None, error_msg)
```

**Cách detect type:** Từ `dev_id` hoặc topic: `ELECTRIC` → Electricity, `WATER` → Water, `LIGHT` → Lighting

---

## Config

**File:** `config/sources.yaml`

```yaml
sources:
  CN_A_A:
    name: "CN_A_A"
    host: dathoc.net
    port: 443
    ws_path: /mq
    username: test1
    password: "123456"
    topic: "v1/C001/+/up/telemetry"
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
    # Global
    report_interval: 10      # Giây giữa mỗi lần log report
    raw_flush_lines: 1000    # Số dòng flush raw file
```

---

## Docker Deployment

**File:** `docker-compose.yml`

```yaml
services:
  postgres:
    image: postgres:15-alpine
    container_name: smartcity-postgres
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: smartcity
    volumes:
      - pgdata:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  app:
    build: .
    container_name: smartcity-app
    depends_on:
      - postgres
    environment:
      - DB_HOST=postgres
      - DB_PORT=5432
      - DB_NAME=smartcity
      - DB_USER=postgres
      - DB_PASSWORD=postgres
    command: ["--config", "config/sources.yaml", "--duration", "1200"]

volumes:
  pgdata:
```

---

## File Output

```
logs/
├── pipeline.log
├── ingest.log
├── validate.log
├── detect.log
├── alert.log
├── storage.log
├── ingest_summary.json          # ← Ingestion results
├── summary.json                 # ← Pipeline summary
├── invalid_events.jsonl
├── dead_letter.jsonl
└── baseline/
    └── baseline_<ts>_summary.json   # ← Baseline results

data/raw/
├── raw_CN_A_A.jsonl
├── raw_CN_B_B.jsonl
└── raw_CN_C_C.jsonl
```
