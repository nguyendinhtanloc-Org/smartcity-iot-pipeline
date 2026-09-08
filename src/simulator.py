"""
simulator.py
------------
IoT Multi-Tenant Data Simulator for SmartCity Pipeline.
Simulates 3 independent streams (KCN A, KCN B, KCN C) at configurable message rate (~2,000 msgs/s).
Uses real sample templates (Water, Lighting, Electricity, Wastewater) with a controlled ~2.5% invalid data mix
to test Schema Validation, Ingestion, and Multi-tenant partitioning.
"""

import json
import random
import time
import argparse
import logging
from pathlib import Path
from typing import Dict, Any, List

logger = logging.getLogger("simulator")

GATEWAYS = ["GW_WATER_001", "GW_LIGHT_001", "GW_ELECTRIC_001", "GW_WWTP_001"]
TENANTS = [
    {"name": "CN_A", "khu_cn": "A", "company_id": "C001", "prefix": "kcn_a"},
    {"name": "CN_B", "khu_cn": "B", "company_id": "C002", "prefix": "kcn_b"},
    {"name": "CN_C", "khu_cn": "C", "company_id": "C003", "prefix": "kcn_c"},
]

def generate_water_payload(device_idx: int) -> Dict[str, Any]:
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    now_unix = int(time.time())
    return {
        "dev_id": f"SL-WATER-{device_idx:03d}",
        "ts": now,
        "tsunix": now_unix,
        "Qt": round(random.uniform(5.0, 150.0), 2),
        "V": round(random.uniform(1000.0, 50000.0), 2),
    }

def generate_lighting_payload(device_idx: int) -> Dict[str, Any]:
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    now_unix = int(time.time())
    return {
        "dev_id": f"SL-LIGHT-{device_idx:03d}",
        "ts": now,
        "tsunix": now_unix,
        "U": round(random.uniform(215.0, 235.0), 1),
        "I": round(random.uniform(2.0, 15.0), 2),
        "Power_kW": round(random.uniform(0.5, 4.0), 2),
        "Energy_kWh": round(random.uniform(100.0, 5000.0), 1),
        "Alr_Current": 0,
        "Alr_Volt": 0,
        "Mode_c": random.choice([0, 1, 2, 3]),
        "EMG Stop monitor": random.choice([0, "ON", "OFF"]),
        "Line 1": random.choice([0, 1]),
        "Line 2": random.choice([0, 1]),
        "Lux": {"ch1": round(random.uniform(10.0, 100.0), 1), "ch2": round(random.uniform(10.0, 100.0), 1)},
    }

def generate_electricity_payload(device_idx: int) -> Dict[str, Any]:
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    now_unix = int(time.time())
    return {
        "dev_id": f"SL-ELEC-{device_idx:03d}",
        "ts": now,
        "tsunix": now_unix,
        "Uab": round(random.uniform(370.0, 395.0), 1),
        "Ubc": round(random.uniform(370.0, 395.0), 1),
        "Uca": round(random.uniform(370.0, 395.0), 1),
        "Ia": round(random.uniform(10.0, 100.0), 2),
        "Ib": round(random.uniform(10.0, 100.0), 2),
        "Ic": round(random.uniform(10.0, 100.0), 2),
        "Pa": round(random.uniform(5.0, 30.0), 2),
        "Pb": round(random.uniform(5.0, 30.0), 2),
        "Pc": round(random.uniform(5.0, 30.0), 2),
        "P_Total": round(random.uniform(15.0, 90.0), 2),
        "PFavg": round(random.uniform(0.85, 0.99), 2),
        "F": round(random.uniform(49.8, 50.2), 2),
        "EP": round(random.uniform(5000.0, 20000.0), 1),
    }

def load_real_templates() -> Dict[str, List[dict]]:
    """Load real raw messages from data/raw/ (including mor_payload.py output)"""
    all_real_payloads = []
    raw_dir = Path("data") / "raw"
    if raw_dir.exists():
        for p in raw_dir.glob("*.jsonl"):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            rec = json.loads(line)
                            payload = rec.get("payload", rec)
                            if isinstance(payload, dict) and "_raw_unparsed" not in payload:
                                all_real_payloads.append(payload)
            except Exception:
                pass

    templates = {"CN_A": all_real_payloads, "CN_B": all_real_payloads, "CN_C": all_real_payloads}
    return templates

REAL_TEMPLATES = load_real_templates()

def generate_sample_message() -> tuple:
    """Generate (topic, payload_dict, tenant_dict) using real downloaded templates or synthetic fallback"""
    tenant = random.choice(TENANTS)
    gateway = random.choice(GATEWAYS)
    dev_idx = random.randint(1, 100)

    topic = f"v1/{tenant['company_id']}/{gateway}/up/telemetry"

    # Use REAL downloaded payload template if available
    real_list = REAL_TEMPLATES.get(tenant["name"], [])
    if real_list and random.random() < 0.85: # 85% real broker data template
        payload = dict(random.choice(real_list))
        # Update timestamp to current
        payload["ts"] = time.strftime("%Y-%m-%d %H:%M:%S")
        payload["tsunix"] = int(time.time())
        # Override tenant info to match selected tenant
        payload["khu_cn"] = tenant["khu_cn"]
        payload["source_name"] = tenant["name"]
    else:
        if gateway == "GW_WATER_001":
            payload = generate_water_payload(dev_idx)
        elif gateway == "GW_LIGHT_001":
            payload = generate_lighting_payload(dev_idx)
        else:
            payload = generate_electricity_payload(dev_idx)

    # Inject bad data at ~1% rate matching real broker invalid rate
    r = random.random()
    if r < 0.005:
        payload.pop("ts", None)
    elif r < 0.01:
        payload = {"_raw_unparsed": "{invalid_json_str"}

    return topic, payload, tenant

def run_simulation(target_rate: int = 2000, duration: int = 60, output_file: str = None):
    logger.info(f"Starting Multi-tenant IoT Simulator: rate={target_rate} msg/s, duration={duration}s")

    start_time = time.monotonic()
    total_sent = 0
    tenant_counts = {"CN_A": 0, "CN_B": 0, "CN_C": 0}

    out_fp = open(output_file, "w", encoding="utf-8") if output_file else None

    try:
        while (time.monotonic() - start_time) < duration:
            batch_size = max(1, target_rate // 10) # 100ms batches
            batch_start = time.monotonic()

            for _ in range(batch_size):
                topic, payload, tenant = generate_sample_message()
                tenant_counts[tenant["name"]] += 1
                total_sent += 1

                record = {
                    "topic": topic,
                    "khu_cn": tenant["khu_cn"],
                    "source_name": tenant["name"],
                    "company_id": tenant["company_id"],
                    "received_at": int(time.time()),
                    "payload": payload
                }

                if out_fp:
                    out_fp.write(json.dumps(record, ensure_ascii=False) + "\n")

            batch_elapsed = time.monotonic() - batch_start
            sleep_time = (0.1) - batch_elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    finally:
        if out_fp:
            out_fp.close()

    elapsed = time.monotonic() - start_time
    actual_rate = total_sent / elapsed if elapsed > 0 else 0
    logger.info(f"Simulation completed: total={total_sent} msgs, rate={actual_rate:.1f} msg/s")
    logger.info(f"Tenant distribution: {tenant_counts}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Multi-tenant IoT Data Simulator")
    parser.add_argument("--rate", type=int, default=2000, help="Target messages per second")
    parser.add_argument("--duration", type=int, default=30, help="Duration in seconds")
    parser.add_argument("--output", default="data/raw/simulated_stream.jsonl", help="Output stream jsonl file")
    args = parser.parse_args()

    run_simulation(target_rate=args.rate, duration=args.duration, output_file=args.output)
