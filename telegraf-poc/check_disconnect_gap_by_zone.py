"""
check_disconnect_gap_by_zone.py
---------------------------------
Giống check_disconnect_gap.py nhưng TÁCH RIÊNG theo từng khu_cn (A/B/C),
để xác định chính xác khu nào bị disconnect có bị mất dữ liệu thật hay
không - thay vì nhìn gộp cả 3 (dễ bị che lấp vì chỉ 1/3 kết nối rớt tại
1 thời điểm).

Cách dùng:
    python3 check_disconnect_gap_by_zone.py data/unified_events.jsonl <epoch_disconnect>
"""
import json
import sys
from collections import defaultdict

INPUT = sys.argv[1]
CENTER = int(sys.argv[2])
WINDOW = 60

by_zone = defaultdict(list)

with open(INPUT, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        ts = rec.get("timestamp")
        if ts is None or not (CENTER - WINDOW <= ts <= CENTER + WINDOW):
            continue
        khu_cn = rec.get("tags", {}).get("khu_cn", "unknown")
        by_zone[khu_cn].append(ts)

for zone in sorted(by_zone):
    ts_list = sorted(by_zone[zone])
    print(f"\n=== Khu CN {zone} ===")
    print(f"  Số record trong cửa sổ ±{WINDOW}s: {len(ts_list)}")
    if len(ts_list) < 2:
        print("  (Không đủ dữ liệu để tìm gap)")
        continue
    max_gap = 0
    gap_at = None
    for i in range(1, len(ts_list)):
        d = ts_list[i] - ts_list[i - 1]
        if d > max_gap:
            max_gap = d
            gap_at = (ts_list[i - 1], ts_list[i])
    print(f"  Record đầu/cuối: {ts_list[0]} -> {ts_list[-1]}")
    print(f"  Gap lớn nhất: {max_gap}s" + (f" (từ {gap_at[0]} đến {gap_at[1]})" if gap_at else ""))
    if max_gap > 2:
        print(f"  => Khu {zone} CÓ gap thật {max_gap}s quanh lúc disconnect.")
    else:
        print(f"  => Khu {zone} không bị gián đoạn đáng kể.")
