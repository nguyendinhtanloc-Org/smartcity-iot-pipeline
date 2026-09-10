# Load test 100k / 500k events/sec

```bash
chmod +x 05_load_test/monitor_resources.sh

# 1. Bật mosquitto local
docker compose -f docker-compose.yml -f 05_load_test/docker-compose.mosquitto.yml up -d mosquitto

# 2. Đổi telegraf.conf: servers = ["tcp://mosquitto:1883"] (bỏ wss/dathoc.net tạm thời),
#    giữ nguyên topics = ["v1/C001/+/up/telemetry"], bỏ username/password/insecure_skip_verify
#    (mosquitto local không cần các field này)
docker compose restart telegraf

# 3. Chạy test 100k msg/s trong 60s, đồng thời ghi CPU/RAM
./05_load_test/monitor_resources.sh telegraf-multi-mqtt > cpu_ram_100k.csv &
MONITOR_PID=$!
pip install paho-mqtt
python3 05_load_test/publisher_load.py --broker localhost --rate 100000 --duration 60 --workers 8
kill $MONITOR_PID

# 4. Lặp lại với --rate 500000 --workers 16 (hoặc hơn, tuỳ CPU máy test)
./05_load_test/monitor_resources.sh telegraf-multi-mqtt > cpu_ram_500k.csv &
MONITOR_PID=$!
python3 05_load_test/publisher_load.py --broker localhost --rate 500000 --duration 60 --workers 16
kill $MONITOR_PID
```

## Đo latency

Payload publisher ghi field `tsunix` (epoch float lúc gửi). Sau khi Telegraf ghi ra
`data/unified_events.jsonl`, mỗi record có field `timestamp` (epoch lúc Telegraf nhận/ghi).
Latency = `timestamp - tsunix`. Có thể tính nhanh bằng 1 lệnh:

```bash
python3 -c "
import json
lat = []
with open('data/unified_events.jsonl') as f:
    for line in f:
        try:
            r = json.loads(line)
            p = json.loads(r['fields']['value'])
            if 'SIM-' in str(p.get('dev_id','')):
                lat.append(r['timestamp'] - p['tsunix'])
        except Exception:
            pass
lat.sort()
if lat:
    print('n=', len(lat), 'avg=', sum(lat)/len(lat), 'p95=', lat[int(len(lat)*0.95)])
"
```

## Việc cần theo dõi khi tăng tải

- CSV `cpu_ram_*.csv`: CPU container Telegraf có chạm 100% (1 core) không, RAM có tăng liên tục
  (dấu hiệu buffer phình do output không kịp flush) không.
- Log Telegraf lúc load test: có xuất hiện `metric buffer overflow` không -- nếu có, đó là ngưỡng
  tải thật mà cấu hình hiện tại (`metric_buffer_limit`, số input plugin) chịu được.
- So `msg/s thực tế` publisher in ra cuối test với target -- nếu thấp hơn nhiều, ghi rõ với mentor
  đây là giới hạn của publisher test (Python/GIL/máy test), không phải giới hạn của Telegraf, và nêu
  hướng khắc phục (nhiều máy publisher, hoặc dùng tool load test C/Go chuyên dụng như `mqtt-bench`).

## Gửi mentor

- 2 file CSV (100k, 500k) + nhận xét CPU/RAM theo tải.
- Latency avg/p95 ở mỗi mức tải.
- Ngưỡng tải mà bắt đầu thấy buffer overflow/drop (nếu có).
