"""
check_disconnect_gap.py
------------------------
Kiểm tra CHÍNH XÁC 1 sự kiện disconnect cụ thể: có gap trong dữ liệu ghi ra
(theo field "timestamp" NGOÀI CÙNG của Telegraf - giờ Telegraf ghi nhận
thật, KHÔNG dùng "tsunix"/"ts" bên trong payload vì đó là giờ mô phỏng
riêng của simulator, không đồng bộ với giờ thật).

Cách dùng:
    python3 check_disconnect_gap.py data/unified_events.jsonl <epoch_disconnect>

Ví dụ với sự kiện disconnect 2026-09-10T03:48:28Z (epoch 1789012108):
    python3 check_disconnect_gap.py data/unified_events.jsonl 1789012108
"""
import json
import sys

INPUT = sys.argv[1]
CENTER = int(sys.argv[2])
WINDOW = 60  # xem trước/sau 60 giây quanh mốc disconnect

timestamps = []
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
        if ts is not None and CENTER - WINDOW <= ts <= CENTER + WINDOW:
            timestamps.append(ts)

timestamps.sort()
print(f"Số record trong khoảng [{CENTER-WINDOW}, {CENTER+WINDOW}] (±{WINDOW}s quanh disconnect): {len(timestamps)}")

if not timestamps:
    print("=> KHÔNG có record nào trong cả cửa sổ 120s này -> cần mở rộng WINDOW hoặc kiểm tra lại epoch.")
    sys.exit(0)

# Tìm gap lớn nhất trong cửa sổ này
max_gap = 0
gap_at = None
for i in range(1, len(timestamps)):
    d = timestamps[i] - timestamps[i-1]
    if d > max_gap:
        max_gap = d
        gap_at = (timestamps[i-1], timestamps[i])

print(f"Record đầu: {timestamps[0]} (cách disconnect {timestamps[0]-CENTER:+d}s)")
print(f"Record cuối: {timestamps[-1]} (cách disconnect {timestamps[-1]-CENTER:+d}s)")
print(f"Gap lớn nhất trong cửa sổ: {max_gap}s, từ {gap_at[0]} đến {gap_at[1]}" if gap_at else "Không có gap đáng kể")

if max_gap <= 2:
    print("\n=> KẾT LUẬN: Không phát hiện gián đoạn ghi dữ liệu quanh lúc disconnect."
          "\n   Có thể do: output flush_interval=5s đã che lấp gap ngắn, HOẶC "
          "persistent_session hoạt động tốt, HOẶC broker chỉ 1 trong 3 kết nối bị disconnect"
          "\n   (2 kết nối còn lại vẫn ghi liên tục nên file tổng không thấy gap).")
else:
    print(f"\n=> KẾT LUẬN: Có gap thật {max_gap}s quanh lúc disconnect -> dữ liệu bị gián đoạn thật,"
          "\n   persistent_session KHÔNG ngăn được mất dữ liệu trong khoảng này.")
