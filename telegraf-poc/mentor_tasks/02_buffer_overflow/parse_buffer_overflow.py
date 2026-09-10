"""
parse_buffer_overflow.py
-------------------------
Đọc log Telegraf sau khi đã ép metric_buffer_limit thấp (test Bước 4 trong README
gốc), đếm tổng số metric bị drop do buffer đầy, và khoảng thời gian xảy ra.

Cách chạy:
    python3 parse_buffer_overflow.py telegraf_overflow.log
"""
import re
import sys
from datetime import datetime, timezone

OVERFLOW_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z.*metric buffer overflow;\s*(?P<n>\d+)\s*metrics dropped",
    re.IGNORECASE,
)


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "telegraf_overflow.log"
    total_dropped = 0
    events = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = OVERFLOW_RE.search(line)
            if not m:
                continue
            n = int(m.group("n"))
            total_dropped += n
            events.append((m.group("ts"), n))

    print(f"Số lần log 'metric buffer overflow': {len(events)}")
    print(f"Tổng số metric bị drop: {total_dropped}")
    if events:
        first_ts = datetime.strptime(events[0][0], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        last_ts = datetime.strptime(events[-1][0], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        span = (last_ts - first_ts).total_seconds()
        print(f"Khoảng thời gian xảy ra overflow: {first_ts.isoformat()} -> {last_ts.isoformat()} ({span:.0f}s)")
        print("\nChi tiết từng lần:")
        for ts, n in events:
            print(f"  {ts}Z  dropped={n}")
    else:
        print("Không tìm thấy dòng overflow nào — kiểm tra lại đã hạ metric_buffer_limit "
              "và restart telegraf trước khi chạy test chưa.")


if __name__ == "__main__":
    main()
