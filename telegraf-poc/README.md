# PoC: Multi-source MQTT Ingestion bằng Telegraf (thay cho threading tự viết)

## LƯU Ý về nguồn data demo

Broker `dathoc.net` hiện **chỉ có simulator C001** đang chạy data (C002/C003 chưa có data).
Để demo 3 luồng hoạt động, `telegraf.conf` cho cả 3 `[[inputs.mqtt_consumer]]` (CN_A, CN_B, CN_C)
đều subscribe cùng topic C001 nhưng **mỗi luồng vẫn là 1 connection MQTT độc lập**, được phân biệt
bằng tag `khu_cn` (A/B/C) + `source_name` (CN_A/CN_B/CN_C).

Trong production, mỗi khu CN sẽ có broker/topic riêng (`v1/C002/...`, `v1/C003/...`) — chỉ cần đổi
dòng `topics` trong block tương ứng.

## Chạy thử

```bash
cd telegraf-poc
docker compose up
```

Kiểm tra file gộp (Telegraf tự động gộp cả 3 khu CN vào đây):
```bash
tail -f data/unified_events.jsonl
```

Mỗi dòng có dạng:
```json
{"fields":{"value":"{...raw payload gốc từ broker...}"},"name":"iot_raw","tags":{"khu_cn":"A","source_name":"CN_A","topic":"v1/C001/GW_WATER_001/up/telemetry"},"timestamp":1788700000}
```

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
