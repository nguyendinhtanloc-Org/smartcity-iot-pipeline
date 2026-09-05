# Quick Start — SmartCity IoT Pipeline

## Cài Đặt Nhanh

```bash
# Clone và cài đặt
git clone <repository-url>
cd smartcity-iot-pipeline
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

---

## Chạy Nhanh

### 1. Verify Broker

```bash
python baseline.py --host dathoc.net --port 443 --ws-path /mq --username test1 --password '123456' --topic 'qa-smartcity/#' --duration 60 --insecure
```

### 2. Chạy LIVE 20 phút

```bash
python main.py --host dathoc.net --port 443 --ws-path /mq --username test1 --password '123456' --topic 'qa-smartcity/#' —duration 1200 --insecure
```

### 3. Test Replay

```bash
python replay.py --input data/raw/raw_events_<timestamp>.jsonl --target 10000
python replay.py --input data/raw/raw_events_<timestamp>.jsonl --target 100000
```

---

## Docker

```bash
docker compose up --build
```

---

## Output Files

```
logs/
├── ingest.log              # Log Ingestion
├── validate.log            # Log Validation
├── summary.json            # Tổng hợp số liệu
├── invalid_events.jsonl    # Message lỗi
└── dead_letter.jsonl       # Poison pill

data/raw/
└── raw_events_<ts>.jsonl   # Data thô
```

---

## Kiểm Tra Kết Quả

```bash
cat logs/summary.json
head -10 logs/invalid_events.jsonl
```

---

## Debug

```bash
# Không nhận data?
python baseline.py --host dathoc.net --port 443 --ws-path /mq --username test1 --password '123456' --topic 'qa-smartcity/#' --duration 30 --insecure

# Xem log real-time
tail -f logs/pipeline.log
```

---

## Lưu Ý

- Data online: 9h-19h (giờ VN)
- Dùng `--insecure` nếu có lỗi TLS
- Hoàn thành 10k replay trước khi chạy 100k
