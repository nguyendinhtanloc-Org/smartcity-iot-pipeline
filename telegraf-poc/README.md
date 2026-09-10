# telegraf-poc — Multi-source MQTT Ingestion (thay thế threading tự viết)

## Danh sách file trong folder này

| File | Vai trò |
|---|---|
| `docker-compose.yml` | Chạy Telegraf, kết nối thẳng broker thật `dathoc.net` |
| `telegraf.conf` | Config 3 input MQTT (khu A/B/C) + output gộp về 1 file. Đã bật `qos=1` + `persistent_session` + `client_id` riêng mỗi khu |
| `schemas.py` | **Copy nguyên bản** từ repo chính (`smartcity-iot-pipeline/schemas.py`) — chứa `detect_type_from_topic()` và `validate_event()` |
| `consume_and_validate.py` | Đọc file Telegraf ghi ra, validate + dedup (dùng lại `schemas.py`, không viết logic mới) |
| `analyze_gaps.py` | Tìm mọi gap bất thường trong toàn bộ file (dùng cho Test 1 - restart) |
| `check_disconnect_gap.py` | Kiểm tra gap tại 1 thời điểm disconnect cụ thể (gộp cả 3 khu) |
| `check_disconnect_gap_by_zone.py` | Giống trên nhưng tách riêng từng khu_cn (chính xác hơn) |
| `count_disconnects_per_zone.py` | So sánh throughput từng khu trên toàn phiên chạy — phát hiện lệch tải giữa các khu |
| `mosquitto.conf`, `publisher.py` | **Dự phòng** — chỉ dùng nếu `dathoc.net` tắt simulator lần nữa (xem mục cuối) |
| `EMAIL_REPLY.txt` | Bản nháp email trả lời mentor, tổng hợp từ lần chạy trước |

## Chạy lại từ đầu (số liệu sạch)

**Bước 0 — Xoá data cũ (bắt buộc, để không lẫn số liệu phiên trước):**
```bash
cd telegraf-poc
rm -f data/unified_events.jsonl
```

**Bước 1 — Đảm bảo `schemas.py` là bản mới nhất:**
```bash
cp ../schemas.py .
```

**Bước 2 — Chạy Telegraf (terminal 1):**
```bash
docker compose up
```
Để chạy tối thiểu **10-15 phút** để có đủ dữ liệu và khả năng bắt được vài lần disconnect tự nhiên (trung bình ~1 lần/131 giây theo lần đo trước).

**Bước 3 — Chạy validate + dedup (terminal 2, chạy song song, không cần đợi terminal 1):**
```bash
python3 consume_and_validate.py
```
Để chạy cùng thời lượng với Telegraf, quan sát log định kỳ dạng:
```
[VALIDATE] total=... valid=... invalid=... duplicate=... per_khu_cn={...} top_errors={...}
```

**Bước 4 — (Optional, nếu còn thời gian) Test buffer overflow:**
Sửa tạm trong `telegraf.conf`: `metric_buffer_limit = 50`, `flush_interval = "60s"`, restart:
```bash
docker compose restart telegraf
docker compose logs -f telegraf   # chờ dòng "metric buffer overflow; N metrics dropped"
```
Xong thì trả lại `metric_buffer_limit = 100000`, `flush_interval = "5s"`.

**Bước 5 — Sau khi dừng (Ctrl+C cả 2 terminal), lấy log đầy đủ:**
```bash
docker compose logs telegraf > telegraf_full.log 2>&1
grep -c "connection lost" telegraf_full.log
```

**Bước 6 — Chạy các script phân tích:**
```bash
python3 analyze_gaps.py data/unified_events.jsonl
python3 count_disconnects_per_zone.py data/unified_events.jsonl
```
(Nếu `analyze_gaps.py` báo có gap, lấy epoch của gap đó chạy tiếp:)
```bash
python3 check_disconnect_gap_by_zone.py data/unified_events.jsonl <epoch_gap>
```

## Gửi lại cho mình

Sau khi chạy xong, gửi output của:
1. `consume_and_validate.py` (vài dòng log cuối cùng, có `duplicate` count)
2. `analyze_gaps.py`
3. `count_disconnects_per_zone.py`
4. (nếu làm) log buffer overflow ở bước 4
5. `wc -l data/unified_events.jsonl` (tổng số record)

Mình sẽ tổng hợp thành nội dung email hoàn chỉnh cuối cùng.

---

## Phương án dự phòng — nếu dathoc.net tắt simulator lần nữa

Dùng `mosquitto.conf` + `docker-compose.yml` (thêm lại service `mosquitto`) +
`publisher.py` để replay lại data cũ qua broker local. Hỏi lại nếu cần,
mình sẽ khôi phục hướng dẫn chi tiết.
