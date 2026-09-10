# Test: Redpanda/Kafka unavailable 5 phút

```bash
# 1. Bật thêm Redpanda cạnh Telegraf
docker compose -f docker-compose.yml -f 03_kafka_outage/docker-compose.redpanda.yml up -d

# 2. Đổi sang config có outputs.kafka (nhớ backup telegraf.conf gốc trước)
cp telegraf.conf telegraf.conf.bak
cp 03_kafka_outage/telegraf.kafka-test.conf telegraf.conf
docker compose restart telegraf

# 3. Chạy ổn định vài phút để chắc kafka output đang hoạt động bình thường
docker compose logs -f telegraf | grep -i kafka

# 4. Giả lập Redpanda unavailable 5 phút
docker compose stop redpanda
sleep 300

# 5. Lấy log trong lúc outage
docker compose logs telegraf --since 6m > 03_kafka_outage/kafka_outage_during.log
grep -i "error\|buffer\|kafka" 03_kafka_outage/kafka_outage_during.log

# 6. Bật lại Redpanda, xem có tự phục hồi/flush lại không
docker compose start redpanda
sleep 30
docker compose logs telegraf --since 1m > 03_kafka_outage/kafka_outage_recovery.log

# 7. Dọn dẹp, trả lại config gốc
cp telegraf.conf.bak telegraf.conf
docker compose restart telegraf
```

## Đọc kết quả thế nào

- Nếu thấy log kiểu `E! [outputs.kafka] ... dial tcp ... connect: connection refused` lặp lại suốt
  5 phút → đúng như dự đoán, Telegraf không tự tạo queue trên đĩa cho output Kafka, chỉ giữ trong
  buffer RAM (`metric_buffer_limit`).
- Nếu trong 5 phút số message phát sinh (theo throughput ~930 msg/s thật/khu, xem Bước 04) VƯỢT QUÁ
  `metric_buffer_limit` đang cấu hình cho output đó → sẽ thấy dòng `metric buffer overflow; N metrics
  dropped` giống hệt test buffer ở Bước 02. Tính thử: 930 msg/s × 300s = ~279,000 message trong 5 phút,
  trong khi `metric_buffer_limit = 100000` → **chắc chắn sẽ mất dữ liệu** nếu không tăng buffer hoặc
  không có giải pháp khác (disk queue riêng, hoặc giảm outage xuống dưới thời gian buffer chịu được).
- Sau khi Redpanda sống lại: đếm số message trong topic `iot.telemetry.raw` (dùng `rpk topic consume
  iot.telemetry.raw --num 10` hoặc console UI của Redpanda) so với số dòng ghi trong
  `data/unified_events.jsonl` cùng khung giờ đó (là "ground truth" vì `outputs.file` không outage) →
  ra đúng số lượng message đã mất qua đường Kafka.

## Kết luận cần nói với mentor

Với cấu hình mặc định, Telegraf **không đảm bảo zero-loss** khi output đích down quá lâu — chỉ có
buffer RAM giới hạn kích thước, không phải durable queue. Muốn chịu được outage 5 phút ở tải thật
(~930 msg/s/khu) cần: tăng `metric_buffer_limit` đủ lớn (tính theo msg/s × thời gian outage tối đa
muốn chịu được), hoặc thêm 1 lớp buffer bền hơn trước Kafka (vd ghi tạm ra disk/outputs.file rồi có
job replay riêng — không phải tính năng có sẵn của Telegraf).
