# THIẾT KẾ PIPELINE — BÓC TÁCH DỮ LIỆU ĐIỆN/NƯỚC/ÁNH SÁNG (BÀI TOÁN 1)

**Người thực hiện:** Nguyễn Đình Tấn Lộc  
**Ngày:** 04/09/2026  
**Deadline:** 05/09/2026  
**Trạng thái:** Giai đoạn 2 — Multi-Source Ingestion + Validation + Detect + Alert + Storage

---

## Tài Liệu Bổ Sung

| File | Mô tả |
|------|-------|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Kiến trúc chi tiết, data flow, mô tả từng file |
| [docs/USAGE.md](docs/USAGE.md) | Hướng dẫn cài đặt và chạy pipeline |
| [docs/VALIDATION.md](docs/VALIDATION.md) | Quy tắc validation, ví dụ, poison pill |

---

## 1. Phạm vi

Xử lý stream MQTT từ **3 khu công nghiệp** (A, B, C), 3 nhóm thiết bị điện/nước/ánh sáng:
- **Target throughput:** 2k msg/s (giai đoạn này), scale lên 500k–1M msg/s (giai đoạn sau)
- **Data online:** 9h–19h (giờ VN)
- **Trọng tâm:** Multi-source ingestion, logic kiểm tra data đúng/sai (Validation), detect violations, alert, storage

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

## 3. Cấu trúc dự án

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
│   ├── schemas.py            # Unified schema + khu_cn
│   ├── baseline.py           # Baseline test
│   ├── replay.py             # Replay tool
│   └── mor_payload.py        # Mentor's monitor script
├── main.py                   # Entry point - orchestrate all stages
├── config.yaml               # Global config
├── docker-compose.yml        # 2 containers: app + postgres
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## 4. Chi tiết từng chặng

| Chặng | Data vào | Data ra | Công nghệ | Script | Log kết quả |
|-------|----------|---------|-----------|--------|-------------|
| **Ingestion** | 3 luồng MQTT (WSS) | JSON + khu_cn vào queue | paho-mqtt multi-thread | `src/ingest.py` | Tổng msg, msg/s, reconnect |
| **Validation** | JSON từ queue | Valid/Invalid + lý do | Pydantic (UnifiedTelemetry) | `src/validate.py` | Valid/Invalid, top errors |
| **Detect** | Valid JSON | Violation + streak | Rule-based threshold + 3-strike | `src/detect.py` | Violations, 3-strike alerts |
| **Alert** | Violation ≥3 streak | Telegram/Email | Telegram Bot API / SMTP | `src/alert.py` | Alerts sent/failed |
| **Storage** | Events + Alerts | Postgres tables | psycopg2 batch insert | `src/storage.py` | Rows stored, errors |
| **Daily Report** | Violations DB | JSON/Telegram/Email | SQL aggregation | `src/daily_report.py` | Report file |

---

## 5. Cài đặt & Chạy

### Cài đặt

```bash
# Clone
git clone <repo-url>
cd smartcity-iot-pipeline

# Tạo venv
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate  # Windows

# Cài đặt dependencies
pip install -r requirements.txt
```

### Chạy Pipeline (Local)

```bash
# 1. Cấu hình sources (chỉnh sửa config/sources.yaml)
# 2. Chạy pipeline 20 phút
python main.py --config config/sources.yaml --duration 1200
```

### Chạy với Docker

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

---

## 6. Kết quả mong đợi (sau 20 phút)

| Chặng | Metric | Target |
|-------|--------|--------|
| **Ingestion** | Total messages | ~240,000 (3 sources × 2k msg/s × 1200s) |
| | Avg throughput | ~2,000 msg/s per source |
| **Validation** | Valid rate | >99% |
| | Invalid rate | <1% |
| **Detect** | Violations detected | Theo ngưỡng nghiệp vụ |
| **Alert** | 3-strike alerts | Số device vi phạm ≥3 lần liên tiếp |
| **Storage** | Rows stored | Events + Violations + Alerts |

---

## 7. Log Output

```
logs/
├── pipeline.log              # Tổng log
├── ingest.log                # Ingestion (per source)
├── validate.log              # Validation stats
├── detect.log                # Detection stats
├── alert.log                 # Alert stats
├── storage.log               # Storage stats
├── invalid_events.jsonl      # Invalid messages
├── dead_letter.jsonl         # Poison pill
└── summary.json              # Tổng hợp
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