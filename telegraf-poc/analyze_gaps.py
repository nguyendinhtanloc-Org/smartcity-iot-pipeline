"""
analyze_gaps.py
----------------
Phân tích data/unified_events.jsonl để tìm các khoảng gap thời gian bất
thường (do Telegraf restart hoặc mất kết nối MQTT), và ước tính số message
có thể đã mất trong khoảng gap đó, dựa trên tốc độ trung bình đo được.

Dùng để trả lời Test 1 (restart) và Test 3 (disconnect/reconnect) bằng
chính log thật đã có sẵn, không cần tạo lại test thủ công.

Cách chạy:
    python3 analyze_gaps.py data/unified_events.jsonl
"""

import json
import sys
from pathlib import Path

INPUT = Path(sys.argv[1] if len(sys.argv) > 1 else "data/unified_events.jsonl")
GAP_THRESHOLD_SEC = 3.0  # gap giữa 2 record liên tiếp lớn hơn ngưỡng này -> bất thường


def main():
    timestamps = []
    skipped = 0
    with open(INPUT, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue
            ts = rec.get("timestamp")
            if ts is not None:
                timestamps.append(ts)

    timestamps.sort()
    print(f"Tổng số record đọc được: {len(timestamps)} (bỏ qua {skipped} dòng lỗi JSON)")
    if len(timestamps) < 2:
        print("Không đủ dữ liệu để phân tích.")
        return

    total_span = timestamps[-1] - timestamps[0]
    avg_rate = len(timestamps) / total_span if total_span > 0 else 0
    print(f"Thời lượng dữ liệu: {total_span}s  |  Tốc độ trung bình: {avg_rate:.1f} msg/s\n")

    gaps = []
    for i in range(1, len(timestamps)):
        delta = timestamps[i] - timestamps[i - 1]
        if delta > GAP_THRESHOLD_SEC:
            gaps.append((timestamps[i - 1], timestamps[i], delta))

    print(f"Số gap bất thường (> {GAP_THRESHOLD_SEC}s): {len(gaps)}")
    for start, end, delta in gaps:
        estimated_lost = max(int(delta * avg_rate) - 1, 0)
        print(
            f"  Gap {delta:>6.1f}s | từ ts={start} đến ts={end} "
            f"| ~ước tính mất {estimated_lost} message (theo tốc độ TB {avg_rate:.1f} msg/s)"
        )

    if not gaps:
        print("  (Không phát hiện gap nào lớn hơn ngưỡng — có thể Telegraf buffer đã che lấp,"
              " hoặc chưa xảy ra sự cố disconnect/restart trong khoảng dữ liệu này.)")


if __name__ == "__main__":
    main()
