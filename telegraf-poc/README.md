# PoC: Multi-source MQTT Ingestion bằng Telegraf (thay cho threading tự viết)

## Vì sao có Mosquitto local + publisher.py

Broker gốc `dathoc.net` đã tắt simulator, không còn phát data trực tiếp
được nữa. Để vẫn test được full pipeline MQTT thật (không phải giả lập
bằng cách gọi hàm Python trực tiếp), mình:

1. Dựng 1 Mosquitto broker local trong Docker.
2. Dùng `publisher.py` đọc lại **data thật đã crawl trước đó**
   (`raw_events_*.jsonl`, lúc dathoc.net còn chạy) và publish lại vào
   Mosquitto local qua **3 topic khác nhau** (`local/A/..`, `local/B/..`,
   `local/C/..`) để giả lập đúng 3 khu CN.
3. Telegraf vẫn subscribe y hệt kiến trúc cũ, chỉ đổi `servers` trỏ về
   `mosquitto:1883` (Docker network nội bộ) thay vì `dathoc.net`.

→ Vẫn là MQTT thật, đa nguồn thật (3 kết nối/subscription độc lập), chỉ
khác broker do mình tự dựng lại thay vì bên cấp đề, vì bên đó đã tắt.

## Chạy thử

**Bước 1 — Bật Mosquitto + Telegraf:**
```bash
cd telegraf-poc
docker compose up
```

**Bước 2 — Cài paho-mqtt cho publisher (chỉ 1 lần):**
```bash
pip install paho-mqtt
```

**Bước 3 — Dọn file output cũ của lần chạy lỗi (nếu có):**
```bash
# unified_events.jsonl hiện có thể chứa topic GW_UNKNOWN_001 (lần chạy bug trước)
# -> xóa để log chạy mới sạch, không lẫn lộn số liệu
Clear-Content data/unified_events.jsonl   # Windows PowerShell
: > data/unified_events.jsonl             # Linux/Mac
```

**Bước 4 — Chạy publisher (terminal khác), trỏ đúng file data cũ đã crawl:**
```bash
python3 publisher.py --input data/raw/raw_data.jsonl --rate 700 --max-lines 20000
```
(`raw_data.jsonl` = copy nguyên file crawl `../data/raw/raw_CN_A_A.jsonl`; mỗi dòng được
publish sang cả 3 topic `local/A|B|C/...` để giả lập 3 khu CN.
- `--rate <N>` = N msg/s cho **MỖI khu CN** (tổng = 3×N). Publisher dùng Token Bucket
  busy-wait nên chạy được rate cao chính xác kể cả trên Windows (không bị giới hạn
  độ phân giải time.sleep ~15ms).
- `--rate 0` = không giới hạn tốc độ, publish tối đa (đo throughput trần của publisher).
- `--max-lines <N>` = chạy đủ N dòng nguồn rồi dừng (5000 dòng × 3 zone ≈ 15k msg).
  Với `--rate 700`: 20.000 dòng ≈ 60k msg, chạy ~30 giây — đủ cho log demo.

Kiểm tra file gộp (Telegraf tự động gộp cả 3 khu CN vào đây):
```bash
tail -f data/unified_events.jsonl
```

Mỗi dòng có dạng:
```json
{"fields":{"value":"{...raw payload gốc từ broker...}"},"name":"iot_raw","tags":{"khu_cn":"A","source_name":"CN_A","topic":"local/A/GW_WATER_001/telemetry"},"timestamp":1788700000}
```
Lưu ý: topic là `local/<khu>/<gateway>/telemetry` — `consume_and_validate.py` vẫn dùng
segment `GW_*` để detect đúng `data_type` (water/lighting/electricity).

## Chạy validate (tái sử dụng code cũ, không viết logic mới)

```bash
# copy schemas.py (đã có sẵn trong repo) vào cùng thư mục này
cp ../smartcity-iot-pipeline/schemas.py .
python3 consume_and_validate.py
```

## Trả lời trực tiếp 3 câu hỏi của mentor

1. **"Mỗi khu có worker nối tới" — dùng tool gì?**
   → `telegraf.conf`, 3 block `[[inputs.mqtt_consumer]]` (Khu A/B/C). Mỗi block là
   1 connection MQTT độc lập, do **Telegraf agent tự quản lý concurrency nội bộ**
   (Telegraf's Go runtime, không phải `threading.Thread` tự viết).

2. **"Tất cả worker đẩy dữ liệu về" — dùng gì để gộp, kiểm soát tốc độ, order?**
   → `[[outputs.file]]` trong `telegraf.conf`: cả 3 input tự động gộp vào
   `data/unified_events.jsonl`.
   - **Tốc độ**: `[agent] metric_buffer_limit = 100000` — buffer nội bộ của
     Telegraf, khi đầy sẽ log cảnh báo và drop, đây là cơ chế backpressure
     **built-in của tool**, cấu hình bằng 1 dòng, không phải code tay.
   - **Order**: Telegraf flush theo `flush_interval`, FIFO trong từng input
     instance; giữa các input instance thì interleaved theo thời điểm nhận —
     đây là hành vi mặc định của tool, không phải logic tự thiết kế.

3. **"Downstream biết đang dùng data gì để validate?"**
   → `topic_tag = "topic"` trong mỗi input block: Telegraf tự gắn full MQTT
   topic gốc vào tag `topic` của mỗi record khi ghi ra file. Downstream
   (`consume_and_validate.py`) đọc tag này, gọi lại **đúng hàm
   `detect_type_from_topic()` đã có sẵn trong `schemas.py`** để suy ra
   `data_type`, rồi gọi `validate_event()` — đây là phần code thật duy nhất
   mình tự viết, đúng theo yêu cầu: chỉ code logic kiểm tra đúng/sai, không
   code lại phần ingestion/gộp luồng.
