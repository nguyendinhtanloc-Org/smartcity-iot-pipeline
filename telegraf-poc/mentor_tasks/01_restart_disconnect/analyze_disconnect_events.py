"""
analyze_disconnect_events.py
-----------------------------
Sửa lỗi phương pháp của analyze_gaps.py cũ: KHÔNG gộp 3 khu_cn rồi sort,
mà lấy đúng epoch của từng lần "connection lost" trong telegraf_full.log,
rồi soi riêng từng khu quanh thời điểm đó -> mới thấy đúng khu nào rớt,
mất bao nhiêu message.

Cách chạy:
    python3 analyze_disconnect_events.py --log telegraf_full.log --data data/unified_events.jsonl
"""
import argparse
import json
import re
from collections import defaultdict
from datetime import datetime, timezone

WINDOW = 60  # giây, soi mỗi bên trước/sau epoch disconnect

# Bỏ '^' vì docker compose logs thêm prefix "tên_container  | " trước timestamp
# -> nếu để '^' thì KHÔNG BAO GIỜ khớp (vẫn "search" được cả dòng nhưng ^ ép
# phải khớp từ vị trí 0 của toàn chuỗi, trong khi vị trí 0 là chữ cái tên container).
TS_RE = r"(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z"
CONN_LOST_RE = re.compile(TS_RE + r".*connection lost", re.IGNORECASE)
STOPPING_RE = re.compile(TS_RE + r".*Stopping running outputs", re.IGNORECASE)
STARTING_RE = re.compile(TS_RE + r".*Starting Telegraf", re.IGNORECASE)


def _to_epoch(ts_str: str) -> int:
    return int(datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp())


def parse_disconnect_epochs(log_path: str) -> list[int]:
    """Các lần auto-reconnect do pingresp timeout (mất vài giây, có thể được
    QoS1 + persistent_session giảm thiểu mất mát)."""
    epochs = []
    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = CONN_LOST_RE.search(line)
            if m:
                epochs.append(_to_epoch(m.group("ts")))
    return sorted(set(epochs))


def parse_restart_windows(log_path: str) -> list[tuple[int, int]]:
    """Các lần container/process bị dừng hẳn rồi khởi động lại (docker compose
    restart, hoặc crash) -- downtime đo TRỰC TIẾP từ log (chính xác hơn ước
    tính qua gap dữ liệu), vì trong khoảng này chắc chắn KHÔNG có message nào
    được ghi (khác hẳn loại pingresp timeout, tự hồi phục trong vài giây)."""
    stop_ts = None
    windows = []
    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m_stop = STOPPING_RE.search(line)
            if m_stop:
                stop_ts = _to_epoch(m_stop.group("ts"))
                continue
            m_start = STARTING_RE.search(line)
            if m_start and stop_ts is not None:
                start_ts = _to_epoch(m_start.group("ts"))
                windows.append((stop_ts, start_ts))
                stop_ts = None
    return windows


def load_by_zone(data_path: str) -> dict[str, list[int]]:
    by_zone = defaultdict(list)
    with open(data_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = rec.get("timestamp")
            if ts is None:
                continue
            zone = rec.get("tags", {}).get("khu_cn", "unknown")
            by_zone[zone].append(ts)
    for zone in by_zone:
        by_zone[zone].sort()
    return by_zone


def local_rate(ts_list: list[int], center: int, lookback: int = 120) -> float:
    """Tốc độ trung bình CỦA RIÊNG khu này, đo trong khoảng lookback giây
    NGAY TRƯỚC thời điểm disconnect (tránh lẫn tốc độ đã bị nhân 3 do gộp)."""
    window = [t for t in ts_list if center - lookback <= t < center]
    if len(window) < 2:
        return 0.0
    span = window[-1] - window[0]
    return (len(window) - 1) / span if span > 0 else 0.0


def analyze_one_zone(ts_list: list[int], center: int) -> dict:
    window = [t for t in ts_list if center - WINDOW <= t <= center + WINDOW]
    if len(window) < 2:
        return {"n_records": len(window), "max_gap": None}
    max_gap = 0
    gap_at = None
    for i in range(1, len(window)):
        d = window[i] - window[i - 1]
        if d > max_gap:
            max_gap = d
            gap_at = (window[i - 1], window[i])
    rate = local_rate(ts_list, center)
    estimated_lost = max(int(max_gap * rate) - 1, 0) if rate > 0 else None
    return {
        "n_records_in_window": len(window),
        "max_gap_sec": max_gap,
        "gap_between": gap_at,
        "local_rate_msg_per_sec": round(rate, 1),
        "estimated_lost": estimated_lost,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--out", default="disconnect_report.json")
    args = ap.parse_args()

    epochs = parse_disconnect_epochs(args.log)
    restart_windows = parse_restart_windows(args.log)
    print(f"Tìm thấy {len(epochs)} lần 'connection lost' (auto-reconnect, vài giây) trong {args.log}")
    print(f"Tìm thấy {len(restart_windows)} lần restart hẳn (container/process dừng rồi start lại)\n")

    if restart_windows:
        print("===== CÁC LẦN RESTART HẲN (downtime đo trực tiếp từ log) =====")
        for stop_ts, start_ts in restart_windows:
            downtime = start_ts - stop_ts
            t0 = datetime.fromtimestamp(stop_ts, tz=timezone.utc).isoformat()
            t1 = datetime.fromtimestamp(start_ts, tz=timezone.utc).isoformat()
            print(f"  Dừng lúc {t0}  ->  Start lại lúc {t1}  => downtime = {downtime}s")
        print()

    by_zone = load_by_zone(args.data)
    print(f"Đã load dữ liệu {sum(len(v) for v in by_zone.values())} record, "
          f"{len(by_zone)} khu: {list(by_zone.keys())}\n")

    # Với restart hẳn: trong khoảng downtime chắc chắn KHÔNG có message nào,
    # nên ước tính mất mát = downtime x tốc độ riêng khu đó NGAY TRƯỚC lúc dừng
    # (rate đo trong window ±60s quanh mốc stop, dùng lại analyze_one_zone
    # với center = stop_ts để lấy local_rate, rồi tự nhân downtime thật).
    restart_report = []
    for stop_ts, start_ts in restart_windows:
        downtime = start_ts - stop_ts
        entry = {"stop_epoch": stop_ts, "start_epoch": start_ts, "downtime_sec": downtime, "zones": {}}
        for zone in sorted(by_zone):
            rate = local_rate(by_zone[zone], stop_ts)
            estimated_lost = int(downtime * rate)
            entry["zones"][zone] = {"local_rate_msg_per_sec": round(rate, 1), "estimated_lost": estimated_lost}
        restart_report.append(entry)

    report = {"connection_lost_events": [], "restart_events": restart_report}
    for epoch in epochs:
        entry = {"epoch": epoch, "time_utc": datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(),
                  "zones": {}}
        print(f"=== Disconnect tại {entry['time_utc']} (epoch={epoch}) ===")
        for zone in sorted(by_zone):
            r = analyze_one_zone(by_zone[zone], epoch)
            entry["zones"][zone] = r
            if "max_gap_sec" not in r:
                print(f"  Khu {zone}: không đủ dữ liệu quanh mốc này")
            else:
                flag = "CÓ gap thật" if r["max_gap_sec"] and r["max_gap_sec"] > 2 else "không gián đoạn đáng kể"
                print(f"  Khu {zone}: gap lớn nhất={r['max_gap_sec']}s | rate riêng khu={r['local_rate_msg_per_sec']}msg/s "
                      f"| ước tính mất={r['estimated_lost']} | => {flag}")
        report["connection_lost_events"].append(entry)
        print()

    if restart_windows:
        print("===== CHI TIẾT ƯỚC TÍNH MẤT DATA DO RESTART HẲN =====")
        for entry in restart_report:
            print(f"  Downtime {entry['downtime_sec']}s (epoch {entry['stop_epoch']} -> {entry['start_epoch']}):")
            for zone, z in entry["zones"].items():
                print(f"    Khu {zone}: rate trước lúc dừng={z['local_rate_msg_per_sec']}msg/s "
                      f"| ước tính mất={z['estimated_lost']} message")

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\nĐã ghi báo cáo chi tiết vào {args.out}")


if __name__ == "__main__":
    main()
