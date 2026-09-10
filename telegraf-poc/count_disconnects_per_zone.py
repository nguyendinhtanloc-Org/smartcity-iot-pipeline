"""
count_disconnects_per_zone.py
-------------------------------
Đếm số dòng log "connection lost" xảy ra, và ước tính lượng message mỗi
khu_cn nhận được trong TOÀN BỘ thời gian chạy (không chỉ 1 cửa sổ), để
kiểm tra xem Khu B có bị mất kết nối/rớt dữ liệu nhiều hơn hẳn A/C hay
không - đây là câu hỏi quan trọng hơn 1 gap đơn lẻ.

Cách dùng:
    python3 count_disconnects_per_zone.py data/unified_events.jsonl
"""
import json
import sys
from collections import Counter

INPUT = sys.argv[1] if len(sys.argv) > 1 else "data/unified_events.jsonl"

zone_counts = Counter()
zone_first_ts = {}
zone_last_ts = {}

with open(INPUT, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        khu_cn = rec.get("tags", {}).get("khu_cn", "unknown")
        ts = rec.get("timestamp")
        zone_counts[khu_cn] += 1
        if ts is not None:
            if khu_cn not in zone_first_ts or ts < zone_first_ts[khu_cn]:
                zone_first_ts[khu_cn] = ts
            if khu_cn not in zone_last_ts or ts > zone_last_ts[khu_cn]:
                zone_last_ts[khu_cn] = ts

print("Tổng số record + tốc độ trung bình theo từng khu_cn (toàn bộ file):\n")
for zone in sorted(zone_counts):
    count = zone_counts[zone]
    span = zone_last_ts[zone] - zone_first_ts[zone]
    rate = count / span if span > 0 else 0
    print(f"  Khu {zone}: {count:>10,} record | span={span}s | ~{rate:.1f} msg/s")

print("\n=> Nếu 1 khu có tốc độ msg/s thấp hơn HẲN 2 khu còn lại (>20-30% chênh lệch),"
      "\n   trong khi cả 3 subscribe cùng 1 topic giống hệt nhau, thì khu đó đang có vấn đề"
      "\n   thật (disconnect lặp lại nhiều hơn, hoặc nghẽn ở tầng output/buffer riêng)."
      "\n   Chạy tiếp: docker compose logs telegraf | grep -c 'connection lost' để so global,"
      "\n   rồi grep theo timestamp gần các đợt để đối chiếu cụ thể khu nào rớt nhiều nhất.")
