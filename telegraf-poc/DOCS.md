# telegraf-poc — Tài Liệu Dự Án

**Mục đích:** Proof-of-Concept sử dụng **Telegraf 1.30** thay thế hệ thống multi-threading tự viết (`src/ingest.py`) để ingest dữ liệu IoT MQTT từ 3 khu công nghiệp.

---

## Mục lục

1. [Tổng quan kiến trúc](#1-tổng-quan-kiến-trúc)
2. [Cấu trúc thư mục](#2-cấu-trúc-thư-mục)
3. [Luồng dữ liệu (Data Flow)](#3-luồng-dữ-liệu-data-flow)
4. [Chi tiết từng file](#4-chi-tiết-từng-file)
5. [Cách chạy](#5-cách-chạy)
6. [Các bài test đã thực hiện](#6-các-bài-test-đã-thực-hiện)
7. [Kết quả đo lường](#7-kết-quả-đo-lường)
8. [Mentor Tasks — 6 bài tập](#8-mentor-tasks--6-bài-tập)
9. [So sánh Telegraf vs Threading tự viết](#9-so-sánh-telegraf-vs-threading-tự-viết)
10. [Hạn chế & Hướng phát triển](#10-hạn-chế--hướng-phát-triển)

---

## 1. Tổng quan kiến trúc

```
┌─────────────────────────────────────────────────────────┐
│                  MQTT Broker (dathoc.net)               │
│            wss://dathoc.net:443/mq                      │
│         Simulator C001 — Water/Lighting/Electricity      │
└───────┬──────────────┬──────────────┬───────────────────┘
        │              │              │
        ▼              ▼              ▼
┌──────────────┐┌──────────────┐┌──────────────┐
│ Telegraf     ││ Telegraf     ││ Telegraf     │
│ Client A     ││ Client B     ││ Client C     │
│ QoS=1        ││ QoS=1        ││ QoS=1        │
│ persistent   ││ persistent   ││ persistent   │
│ _session     ││ _session     ││ _session     │
└──────┬───────┘└──────┬───────┘└──────┬───────┘
       │               │               │
       └───────────────┼───────────────┘
                       ▼
            ┌─────────────────────┐
            │  outputs.file       │
            │  /data/unified_     │
            │  events.jsonl       │
            └──────────┬──────────┘
                       ▼
            ┌─────────────────────┐
            │ consume_and_        │
            │ validate.py         │
            │ (tail -f + dedup)   │
            └─────────────────────┘
```

### Nguyên lý hoạt động

- **3 input MQTT** trong cùng 1 container Telegraf, mỗi input là 1 client kết nối riêng biệt đến broker `dathoc.net`
- Tất cả cùng subscribe topic `v1/C001/+/up/telemetry` (vì broker hiện chỉ có simulator C001)
- Phân biệt khu công nghiệp bằng **tag** `khu_cn` (A/B/C) trong config
- **Output**: gộp cả 3 luồng về 1 file JSONL duy nhất (`unified_events.jsonl`)
- Telegraf tự xử lý reconnect, buffer, retry — không cần viết code quản lý thread

---

## 2. Cấu trúc thư mục

```
telegraf-poc/
├── docker-compose.yml              # Container Telegraf kết nối broker thật
├── telegraf.conf                   # Config chính (3 input + 1 output)
├── telegraf.conf.orig              # Backup config
├── schemas.py                      # Copy từ repo chính — Pydantic schemas
├── consume_and_validate.py         # Đọc JSONL real-time + validate + dedup
├── analyze_gaps.py                 # Tìm gap thời gian bất thường
├── check_disconnect_gap.py         # Kiểm tra gap tại 1 thời điểm (gộp 3 khu)
├── check_disconnect_gap_by_zone.py # Kiểm tra gap tách theo từng khu
├── count_disconnects_per_zone.py   # So sánh throughput toàn phiên theo khu
├── disconnect_report.json          # Báo cáo chi tiết các sự kiện disconnect
├── telegraf_full.log               # Log Telegraf phiên chạy đầy đủ
├── telegraf_overflow.log           # Log khi test buffer overflow
├── README.md                       # Hướng dẫn chạy lại từ đầu
├── data/
│   ├── unified_events.jsonl        # Dữ liệu JSONL đầu ra
│   ├── unified_events.jsonl.bak_*  # Backup dữ liệu
│   └── raw/                        # Thư mục trống (dự phòng)
└── mentor_tasks/
    ├── ROADMAP.md                  # Lộ trình 6 bài tập cho mentor
    ├── 01_restart_disconnect/      # Phân tích restart/disconnect
    ├── 02_buffer_overflow/         # Parse buffer overflow
    ├── 03_kafka_outage/            # Test Kafka/Redpanda outage
    ├── 04_dedup_ordering/          # Dedup + ordering theo device
    ├── 05_load_test/               # Load test 100k-500k msg/s
    └── 06_replay/                  # Replay data với schema mới
```

---

## 3. Luồng dữ liệu (Data Flow)

### Bước 1: Telegraf nhận MQTT

```
MQTT Broker → Telegraf input.mqtt_consumer
  → JSON envelope: { "name": "iot_raw", "tags": {...}, "fields": { "value": "<json_string>" }, "timestamp": <epoch> }
  → Ghi append vào unified_events.jsonl
```

**Format mỗi dòng JSONL:**
```json
{
  "name": "iot_raw",
  "tags": {
    "host": "658c8b63f1b7",
    "khu_cn": "A",
    "source_name": "CN_A",
    "topic": "v1/C001/GW_WATER_001/up/telemetry"
  },
  "fields": {
    "value": "{\"dev_id\":\"W-HH-001\",\"ts\":\"2026-09-10 11:32:00\",\"tsunix\":1789011120,\"Qt\":1.5,\"V\":100.2}"
  },
  "timestamp": 1789011120
}
```

### Bước 2: Validate + Dedup

`consume_and_validate.py` đọc file JSONL theo kiểu `tail -f`:

1. **Parse** JSON envelope → extract `tags.topic`, `tags.khu_cn`, `fields.value`
2. **JSON parse** payload string → dict
3. **Dedup**: kiểm tra `event_id` hoặc `dev_id + ts` trong set `_seen_keys`
4. **Validate**: gọi `validate_event()` từ `schemas.py` → phân loại water/lighting/electricity
5. **Log** mỗi 1000 messages: total, valid, invalid, duplicate, per_khu_cn, top_errors

### Bước 3: Phân tích

Các script phân tích đọc lại file JSONL và log Telegraf để trả lời câu hỏi:
- Có bao nhiêu lần disconnect? Mỗi lần mất bao nhiêu message?
- Buffer overflow gây drop bao nhiêu metric?
- Khu nào bị mất dữ liệu nhiều hơn?
- Data có bị duplicated khi 3 client cùng subscribe 1 topic?

---

## 4. Chi tiết từng file

### 4.1 `docker-compose.yml`

```yaml
services:
  telegraf:
    image: telegraf:1.30
    container_name: telegraf-multi-mqtt
    volumes:
      - ./telegraf.conf:/etc/telegraf/telegraf.conf:ro
      - ./data:/data
    restart: unless-stopped
```

- Dùng image chính thức `telegraf:1.30`
- Mount config (read-only) và thư mục data
- `restart: unless-stopped` — tự restart khi crash

### 4.2 `telegraf.conf`

**Cấu hình chung:**
```toml
[agent]
  interval = "1s"              # Collect mỗi 1 giây
  flush_interval = "5s"        # Ghi file mỗi 5 giây
  metric_buffer_limit = 100000 # Buffer tối đa 100k metrics
```

**Mỗi input MQTT (3 input A/B/C):**
```toml
[[inputs.mqtt_consumer]]
  name_override = "iot_raw"
  servers = ["wss://dathoc.net:443/mq"]
  topics = ["v1/C001/+/up/telemetry"]
  username = "test1"
  password = "123456"
  qos = 1                    # QoS 1: broker giữ msg chưa ACK
  persistent_session = true  # Giữ session khi disconnect → nhận lại msg
  client_id = "telegraf-khu-cn-a"  # Mỗi khu 1 client_id riêng
  topic_tag = "topic"        # Giữ topic gốc làm tag
  data_format = "value"
  data_type = "string"       # Nhận raw string, parse sau
  [inputs.mqtt_consumer.tags]
    khu_cn = "A"             # Tag phân biệt khu
    source_name = "CN_A"
```

**Key differences giữa 3 input:** Chỉ khác `client_id` và tags (`khu_cn`, `source_name`).

**Output:**
```toml
[[outputs.file]]
  files = ["/data/unified_events.jsonl"]
  data_format = "json"
```

### 4.3 `schemas.py`

Copy nguyên bản từ `smartcity-iot-pipeline/schemas.py`. Chứa:

- **`GATEWAY_TYPE_MAP`**: ánh xạ gateway_id → data type
  - `GW_WATER_001` → `"water"`
  - `GW_LIGHT_001` → `"lighting"`
  - `GW_ELECTRIC_001` → `"electricity"`
  - `GW_WWTP_001` → `"wastewater"`

- **`detect_type_from_topic(topic)`**: extract gateway từ topic, trả về data type

- **4 Pydantic schemas:**
  - `WaterTelemetry`: dev_id, ts, tsunix, Qt, V
  - `LightingTelemetry`: U, I, Power_kW, Energy_kWh, Alr_*, Line_1..10, Lux, Contactor
  - `ElectricityTelemetry`: 3-phase U/I/P/Q/S/PF/F/EP/THD/T
  - `WastewaterTelemetry`: pH, turbidity, conductivity, temperature

- **`validate_event(raw)`**: detect type → validate bằng schema tương ứng → trả `ValidationResult`

### 4.4 `consume_and_validate.py`

**Thuật toán:**
```
while True:
    line = f.readline()           # tail -f: đọc dòng mới
    if not line: sleep(1); continue
    record = json.loads(line)     # Parse JSON envelope
    topic = tags["topic"]
    khu_cn = tags["khu_cn"]
    payload = json.loads(fields["value"])  # Parse payload string

    key = dedup_key(payload)      # event_id hoặc dev_id+ts
    if key in _seen_keys:
        duplicate_count += 1
        continue
    _seen_keys.add(key)

    result = validate_event(payload)
    # Log mỗi 1000 messages
```

**Dedup strategy:**
- Ưu tiên `event_id` (UUID) nếu có
- Fallback: `dev_id + ts` (hoặc `tsunix`)
- Giữ trong `set()` trong bộ nhớ — giới hạn cho session ngắn hạn

### 4.5 `analyze_gaps.py`

- Đọc toàn bộ JSONL, sort theo `timestamp` (epoch ngoài cùng của Telegraf)
- Tìm các khoảng cách > 3 giây giữa 2 message liên tiếp
- Ước tính số message mất = `gap_duration × avg_rate`
- **Lưu ý:** Phân tích gộp 3 khu → nếu 1 khu mất连接, 2 khu còn lại sẽ "lấp đầy" timeline

### 4.6 `check_disconnect_gap.py`

- Kiểm tra trong cửa sổ ±60s quanh 1 epoch disconnect cụ thể
- Tìm gap lớn nhất trong cửa sổ đó
- **Giới hạn:** Gộp cả 3 khu → dễmiss nếu chỉ 1/3 client bị ảnh hưởng

### 4.7 `check_disconnect_gap_by_zone.py`

- Phiên bản nâng cấp của `check_disconnect_gap.py`
- **Tách riêng theo từng khu_cn** (A/B/C)
- Phát hiện chính xác khu nào bị disconnect có gây mất dữ liệu thật

### 4.8 `count_disconnects_per_zone.py`

- Đếm tổng số record theo từng khu
- Tính tốc độ msg/s trung bình cho mỗi khu
- So sánh: nếu 1 khu có tốc độ thấp hơn >20-30% so với 2 khu còn lại → khu đó có vấn đề

### 4.9 `disconnect_report.json`

- Output từ `analyze_disconnect_events.py` (mentor_tasks/01)
- Chứa `connection_lost_events`: 32 sự kiện disconnect, mỗi sự kiện có:
  - `timestamp`, `epoch`
  - Per-zone gap analysis (gap大小, estimated lost messages)
- Chứa `restart_events`: 1 sự kiện restart

---

## 5. Cách chạy

### Chuẩn bị
```bash
cd telegraf-poc

# Xoá data cũ (bắt buộc)
rm -f data/unified_events.jsonl

# Đảm bảo schemas.py là bản mới nhất
cp ../schemas.py .
```

### Chạy chính

**Terminal 1 — Telegraf:**
```bash
docker compose up
# Để chạy tối thiểu 10-15 phút
```

**Terminal 2 — Validate (song song):**
```bash
python3 consume_and_validate.py
# Chạy cùng thời lượng với Telegraf
```

### Sau khi dừng

**Lấy log:**
```bash
docker compose logs telegraf > telegraf_full.log 2>&1
grep -c "connection lost" telegraf_full.log
```

**Phân tích:**
```bash
python3 analyze_gaps.py data/unified_events.jsonl
python3 count_disconnects_per_zone.py data/unified_events.jsonl

# Nếu analyze_gaps.py tìm thấy gap:
python3 check_disconnect_gap_by_zone.py data/unified_events.jsonl <epoch_gap>
```

### Test buffer overflow (tùy chọn)
```bash
# Sửa telegraf.conf: metric_buffer_limit = 50, flush_interval = "60s"
docker compose restart telegraf
docker compose logs -f telegraf  # Chờ dòng "metric buffer overflow"
# Ctrl+C, lấy log, trả lại config gốc
```

---

## 6. Các bài test đã thực hiện

### Test 1: Disconnect/Reconnect tự nhiên

- **Mục đích:** Đo xem khi MQTT connection mất, Telegraf mất bao nhiêu message
- **Thực hiện:** Để Telegraf chạy 10-15 phút, quan sát log `connection lost`
- **Kết quả:** 32 lần disconnect trong phiên chạy, trung bình ~1 lần/131 giây
- **Tool phân tích:** `analyze_disconnect_events.py` → `disconnect_report.json`

### Test 2: Buffer Overflow

- **Mục đích:** Xem Telegraf xử lý khi buffer đầy
- **Thực hiện:** Set `metric_buffer_limit = 50`, `flush_interval = "60s"`
- **Kết quả:** ~950 metrics bị drop mỗi đợt overflow
- **Log:** `telegraf_overflow.log`

### Test 3: Persistent Session

- **Mục đích:** Xem QoS 1 + persistent_session có giữ được message khi disconnect không
- **Thực hiện:** Kiểm tra gap bằng `check_disconnect_gap_by_zone.py`
- **Kết quả:** Kết hợp persistent_session giảm mất dữ liệu, nhưng không hoàn toàn (broker chỉ giữ message chưa ACK)

### Test 4: Dedup & Ordering

- **Mục đích:** Khi 3 client cùng subscribe 1 topic, có bị duplicate không? Ordering có đảm bảo không?
- **Thực hiện:** `batch_validate.py` + `check_ordering_by_device.py`
- **Kết quả:** Duplicate cross-zone xảy ra khi cùng 1 message được cả 3 client nhận

---

## 7. Kết quả đo lường

### Kết quả chính (phiên chạy 10-15 phút)

| Metric | Giá trị |
|--------|---------|
| Tổng record trong JSONL | ~100,000+ |
| Số lần disconnect | 32 |
| Trung bình interval disconnect | ~131 giây |
| Buffer overflow drops (test) | ~950 metrics/đợt |
| Throughput trung bình | ~2,400 msg/s (gộp 3 khu) |

### Phân tích disconnect

- Hầu hết disconnect là `pingresp not received` (broker không phản hồi ping)
- Telegraf tự reconnect sau vài giây
- Persistent session giúp nhận lại một số message, nhưng không đảm bảo 100%
- Khi gộp 3 khu, gap thực tế khó thấy vì 2 khu còn lại tiếp tục ghi data

---

## 8. Mentor Tasks — 6 bài tập

| # | Bài tập | File | Câu hỏi mentor |
|---|---------|------|-----------------|
| 01 | Restart/Disconnect | `analyze_disconnect_events.py` | Restart xảy ra bao nhiêu → mất bao nhiêu msg? |
| 02 | Buffer Overflow | `parse_buffer_overflow.py` | Buffer đầy thì mất gì? |
| 03 | Kafka Outage | `docker-compose.redpanda.yml` | Kafka down 5 phút → data đi đâu? |
| 04 | Dedup & Ordering | `batch_validate.py`, `check_ordering_by_device.py` | Duplicate xử lý thế nào? Ordering đảm bảo ra sao? |
| 05 | Load Test 100k-500k | `publisher_load.py`, `monitor_resources.sh` | CPU/RAM/latency ở 100k-500k msg/s? |
| 06 | Replay 30 ngày | `replay_with_schema.py` | Replay data cũ với schema mới hoạt động ra sao? |

### Chi tiết từng task

#### Task 01: Restart/Disconnect Analysis
- Script tự động: đọc log → tìm "connection lost" → lấy epoch → tách per-zone → ước tính message mất
- Output: `disconnect_report.json` breakdown theo từng sự kiện, từng khu

#### Task 02: Buffer Overflow
- Giả lập: set buffer limit = 50, flush_interval = 60s
- Đo: tổng metric drop, thời gian overflow kéo dài bao lâu

#### Task 03: Kafka/Redpanda Outage
- Thêm output Kafka (Redpanda) song song với file output
- Test: dừng Redpanda 5 phút → kiểm tra Telegraf có mất data không
- **Lưu ý:** Chưa thực hiện được vì cần thêm infrastructure

#### Task 04: Dedup & Ordering
- `batch_validate.py`: phân biệt duplicate cross-zone vs same-zone
- `check_ordering_by_device.py`: kiểm tra thứ tự event theo device_id

#### Task 05: Load Test
- `publisher_load.py`: multiprocessing-based MQTT publisher, cấu hình rate/workers/duration
- `monitor_resources.sh`: poll `docker stats` mỗi giây → CSV CPU%/RAM
- Mosquitto broker riêng cho load test (không dùng dathoc.net thật)

#### Task 06: Replay
- `replay_with_schema.py`: load schemas.py từ git tag, replay validation trên data cũ
- So sánh kết quả giữa schema versions

---

## 9. So sánh Telegraf vs Threading tự viết

| Tiêu chí | Telegraf (PoC này) | Threading (`src/ingest.py`) |
|----------|---------------------|----------------------------|
| **Số dòng code** | ~50 dòng config | ~300 dòng Python |
| **Quản lý reconnect** | Built-in (tự động) | Tự viết (try/except + sleep) |
| **Buffer & retry** | Built-in (metric_buffer_limit) | Tự quản lý queue + drops |
| **QoS support** | Native MQTT QoS 0/1/2 | paho-mqtt callback |
| **Persistent session** | Config 1 dòng | Tự implement |
| **Monitoring** | Log format chuẩn, có overflow warning | Tự viết aggregate monitor |
| **Output flexibility** | 20+ outputs (file, Kafka, DB, HTTP...) | Chỉ file + custom |
| **Plugin ecosystem** | 300+ input plugins | Chỉ MQTT |
| **Resource usage** | Container (~50MB RAM) | Python process |
| **Khả năng custom** | Limited (config-based) | Full control |
| **Phù hợp khi** | Production, cần reliability | Prototype, cần custom logic |

### Khi nào dùng Telegraf?

- Pipeline đã ổn định, cần reliability và auto-reconnect
- Cần kết nối nhiều nguồn khác nhau (MQTT, HTTP, SNMP, ...)
- Muốn giảm maintenance cost (không cần tự viết error handling)

### Khi nào giữ threading tự viết?

- Cần custom logic phức tạp (aggregation theo window, state machine, ...)
- Pipeline còn prototype, thay đổi thường xuyên
- Cần kiểm soát chi tiết resource usage

---

## 10. Hạn chế & Hướng phát triển

### Hạn chế hiện tại

1. **PoC chỉ ghi file** — chưa kết nối Kafka/DB downstream
2. **Dedup bằng set()** — chỉ hoạt động trong 1 session, mất khi restart
3. **Gap analysis gộp 3 khu** — dễ bỏ qua gap của 1 khu (đã cải thiện ở `by_zone` version)
4. **Chưa có monitoring dashboard** — chỉ có log text
5. **Broker chỉ có C001** — chưa test được multi-company thật

### Hướng phát triển

1. **Thêm output Kafka/Redpanda** cho production pipeline
2. **Thay set() dedup bằng Redis TTL cache** để persistent cross-session
3. **Tích hợp Telegraf với pipeline chính** (`main.py`) thay vì Python threading
4. **Thêm Telegraf output plugin** cho PostgreSQL/TimescaleDB
5. **Load test thật** với 100k-500k msg/s để benchmark Telegraf vs threading
6. **Schema evolution** — test replay data cũ với schema mới (Task 06)
