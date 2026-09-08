# PIPELINE IoT SMARTCITY — BÓC TÁCH DỮ LIỆU ĐIỆN/NƯỚC/ÁNH SÁNG

**Người thực hiện:** Nguyễn Đình Tấn Lộc  
**Cập nhật:** 07/09/2026  
**Trạng thái:** Giai đoạn 2 — Multi-Source Ingestion hoàn thiện (baseline + multi-thread + aggregate monitor + summary)

---

## Tài Liệu Bổ Sung

| File | Mô tả |
|------|-------|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Kiến trúc chi tiết, data flow, mô tả từng file |
| [docs/USAGE.md](docs/USAGE.md) | Hướng dẫn cài đặt và chạy pipeline |
| [docs/VALIDATION.md](docs/VALIDATION.md) | Quy tắc validation, 3 schema types, poison pill |

---

## 1. Phạm vi

Xử lý stream MQTT từ **3 khu công nghiệp** (A, B, C), 3 nhóm thiết bị điện/nước/ánh sáng:
- **Target throughput:** 2k msg/s (giai đoạn này), scale lên 500k–1M msg/s (giai đoạn sau)
- **Data online:** 9h–19h (giờ VN), broker `dathoc.net:443`
- **Trọng tâm:** Multi-source ingestion đa luồng, validation data đúng/sai, detect violations, alert, storage

---

## 2. Kiến trúc tổng thể

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
│  Aggregate monitor [INGEST-AGG] mỗi 10s                    │
│  Raw file per source + drops counter                        │
└────────────────────────────────┬────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│                     VALIDATION                              │
│  3 Schema: Water/Lighting/Electricity + Validate Event      │
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

## 3. Cấu trúc dự án

```
smartcity-iot-pipeline/
├── config/
│   └── sources.yaml          # Config 3 nguồn MQTT (CN A, B, C)
├── src/
│   ├── __init__.py
│   ├── ingest.py             # MultiSourceIngestor (multi-thread + aggregate monitor)
│   ├── validate.py           # Validate (3 schema types + poison pill)
│   ├── detect.py             # Detect violations (threshold + 3-strike)
│   ├── alert.py              # Alert (Telegram/Email)
│   ├── storage.py            # Postgres batch insert (resilient no-DB mode)
│   └── daily_report.py       # Daily report generator
├── schemas.py                # WaterTelemetry/LightingTelemetry/ElectricityTelemetry/UnifiedTelemetry
├── baseline.py               # Pre-ingestion baseline measurement (multi-topic, per_company/gateway)
├── replay.py                 # Replay tool (multiprocessing + token bucket rate limiting)
├── mor_payload.py            # Mentor's MQTT capture script (--output JSONL)
├── main.py                   # Entry point - orchestrate all stages
├── docker-compose.yml        # 2 containers: app + postgres
├── Dockerfile                # python:3.11-slim, ENTRYPOINT python3 -u main.py
├── requirements.txt
└── README.md
```

---

## 4. Demo Multi-Thread Ingestion

### Tại sao 3 nguồn cùng subscribe C001?

Broker hiện tại (`dathoc.net`) chỉ có **simulator C001** đang chạy data. CN_B (C002) và CN_C (C003) kết nối được nhưng không có data stream.

Để demo **multi-thread architecture**, em subscribe cả 3 nguồn vào cùng C001:
- Mỗi nguồn là **1 MQTT connection riêng biệt** (1 `threading.Thread`)
- Mỗi nguồn ghi **raw file riêng** (`raw_CN_A_A.jsonl`, `raw_CN_B_B.jsonl`, `raw_CN_C_C.jsonl`)
- Mỗi message được **enrich với `khu_cn: A/B/C`** để downstream phân biệt
- **Aggregate monitor** `[INGEST-AGG]` log rõ per-source breakdown

Trong production, mỗi khu CN sẽ có broker/data riêng. Demo này chứng minh kiến trúc đa luồng hoạt động đúng.

### Kết quả thực tế (20 phút Docker)

```
[INGEST-AGG] per-source: CN_A=36133 CN_B=49486 CN_C=37035
Tổng: ~2,400 msg/s | Drops: 0 | Invalid: 3.5%
```

### Baseline (Chặng 0)
```
python3 baseline.py \
    --host dathoc.net --port 443 --ws-path /mq \
    --username test1 --password '123456' \
    --topic 'v1/C001/+/up/telemetry' \
    --topic 'v1/C002/+/up/telemetry' \
    --topic 'v1/C003/+/up/telemetry' \
    --duration 1200 --insecure
```
- Đo throughput thuần broker, tách per_company + per_gateway
- Output: `logs/baseline/baseline_<ts>.log` + `_summary.json`

### Ingestion (Chặng 1)
```
python3 src/ingest.py \
    --config config/sources.yaml \
    --duration 1200
```
- 3 MQTT Worker threads → 1 unified queue chung
- Aggregate monitor `[INGEST-AGG]` log mỗi 10s: total rate + per-source + drops
- Raw file per source: `data/raw/raw_CN_A_A.jsonl`, `raw_CN_B_B.jsonl`, `raw_CN_C_C.jsonl`
- Output: `logs/ingest_summary.json`

### Kết quả thực tế (Docker 20 phút, 3 workers)

| Metric | Kết quả |
|--------|---------|
| **Tổng messages** | 2,669,880 |
| **Tốc độ TB** | 2,224 msg/s |
| **CN_A** | ~700 msg/s |
| **CN_B** | ~990 msg/s |
| **CN_C** | ~690 msg/s |
| **Drops** | 0 |
| **Valid** | 96.5% |
| **Invalid** | 3.5% (schema_error — Lighting field names) |
| **Violations** | 0 (data trong ngưỡng) |
| **Storage** | 1,748,760 records, 0 errors |

---

## 5. Chi tiết từng chặng

| Chặng | Data vào | Data ra | Script | Log kết quả |
|-------|----------|---------|--------|-------------|
| **Baseline** | MQTT broker (thuần) | throughput + per_company/gateway | `baseline.py` | `logs/baseline/baseline_<ts>.json` |
| **Ingestion** | 3 luồng MQTT (WSS) | enriched payload + khu_cn + data_type vào queue | `src/ingest.py` | `logs/ingest_summary.json` + raw CN_X files |
| **Validation** | JSON từ queue | Valid/Invalid + lý do | `src/validate.py` | `logs/validate.log` + `invalid_events.jsonl` |
| **Detect** | Valid JSON | Violation + streak | `src/detect.py` | `logs/detect.log` |
| **Alert** | Violation ≥3 streak | Telegram/Email | `src/alert.py` | `logs/alert.log` |
| **Storage** | Events + Alerts | Postgres tables | `src/storage.py` | `logs/storage.log` |

---

## 6. Cài đặt & Chạy

### Cài đặt

```bash
git clone <repo-url>
cd smartcity-iot-pipeline
python -m venv venv
source venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
```

### Chạy Pipeline (Local)

```bash
# 1. Baseline (đo throughput broker trước)
python3 baseline.py --host dathoc.net --port 443 --ws-path /mq \
    --username test1 --password '123456' \
    --topic 'v1/C001/+/up/telemetry' --duration 120 --insecure

# 2. Chạy pipeline 20 phút
python3 main.py --config config/sources.yaml --duration 1200
```

### Chạy với Docker

```bash
# Build và chạy (default 20 phút)
docker compose up --build

# Chạy nền
docker compose up --build -d

# Chạy với thời gian tùy chỉnh (vd: 5 phút)
docker compose run app --config config/sources.yaml --duration 300
```

### Kiểm tra Docker đang chạy

```bash
# Xem containers đang chạy
docker compose ps

# Xem logs real-time (tất cả services)
docker compose logs -f

# Xem logs app only
docker compose logs -f app

# Xem logs postgres only
docker compose logs -f postgres

# Kiểm tra throughput từ logs
docker compose logs app 2>&1 | grep "INGEST-AGG"

# Kiểm tra violations
docker compose logs app 2>&1 | grep "ALERT"

# Kiểm tra validation
docker compose logs app 2>&1 | grep "VALIDATE"

# Xem summary khi chạy xong
cat logs/summary.json | python3 -m json.tool

# Xem ingest summary
cat logs/ingest_summary.json | python3 -m json.tool

# Kiểm tra raw files đã ghi
ls -la data/raw/

# Kiểm tra invalid events
wc -l logs/invalid_events.jsonl

# Kiểm tra dead letter (poison pill)
wc -l logs/dead_letter.jsonl

# Dừng containers
docker compose down

# Dừng + xóa volumes
docker compose down -v
```

### Query PostgreSQL (qua Docker)

```bash
# Vào psql
docker compose exec postgres psql -U postgres -d smartcity

# Số lượng records
SELECT 'raw_events' as tbl, COUNT(*) FROM raw_events
UNION ALL
SELECT 'violations', COUNT(*) FROM violations
UNION ALL
SELECT 'alerts', COUNT(*) FROM alerts;

# Vi phạm theo khu CN
SELECT khu_cn, COUNT(*) FROM violations GROUP BY khu_cn;

# Top devices vi phạm
SELECT dev_id, khu_cn, COUNT(*) as cnt FROM violations GROUP BY dev_id, khu_cn ORDER BY cnt DESC LIMIT 10;

# Alerts 3-strike
SELECT COUNT(*) FROM alerts WHERE alert_type = 'VIOLATION_3_STRIKE';
```

---

## 7. Log Output

```
logs/
├── pipeline.log              # Tổng log
├── ingest.log                # Ingestion (per source + aggregate)
├── validate.log              # Validation stats
├── detect.log                # Detection stats
├── alert.log                 # Alert stats
├── storage.log               # Storage stats
├── ingest_summary.json       # Tổng hợp INGESTION (total, rate, drops, per_source, raw_dir)
├── summary.json              # Tổng hợp pipeline (all stages)
├── invalid_events.jsonl      # Invalid messages
├── dead_letter.jsonl         # Poison pill
├── baseline/
│   └── baseline_<ts>_summary.json   # Kết quả baseline
└── reports/
    └── daily_report_<date>.json     # Báo cáo vi phạm cuối ngày

data/raw/
├── raw_CN_A_A.jsonl          # Raw file CN A (khu_cn=A)
├── raw_CN_B_B.jsonl          # Raw file CN B (khu_cn=B)
├── raw_CN_C_C.jsonl          # Raw file CN C (khu_cn=C)
└── raw_events_<ts>.jsonl     # Raw events (nếu chạy qua main.py)
```

---

## 8. Scale lên 500k–1M msg/s (Giai đoạn sau)

Khi cần scale thật, dùng tool có sẵn:
- **Kafka/Redpanda** (partition theo device_id)
- **Redis** cho state phân tán
- **ClickHouse/TimescaleDB** cho storage
- **Kubernetes** cho orchestration

---

## 9. Bài toán 2 (Video)

Triển khai sau khi bài 1 được duyệt.

---

## 10. Liên hệ

**Nguyễn Đình Tấn Lộc**  
Email: ndtl05062005@gmail.com  
GitHub: https://github.com/nguyendinhtanloc-Org/smartcity-iot-pipeline