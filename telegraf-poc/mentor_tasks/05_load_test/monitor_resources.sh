#!/usr/bin/env bash
# monitor_resources.sh
# Poll `docker stats` mỗi giây, ghi CPU%/RAM ra CSV để so sánh giữa các mức tải.
#
# Cách chạy (chạy nền song song lúc load test):
#   ./monitor_resources.sh telegraf-multi-mqtt > cpu_ram_100k.csv &
#   python3 publisher_load.py --broker localhost --rate 100000 --duration 60
#   kill %1   # dừng monitor sau khi load test xong

CONTAINER="${1:-telegraf-multi-mqtt}"

echo "timestamp,cpu_percent,mem_usage,mem_percent"
while true; do
  TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  STATS=$(docker stats "$CONTAINER" --no-stream --format "{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}")
  echo "${TS},${STATS}"
  sleep 1
done
