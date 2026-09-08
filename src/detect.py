"""
detect.py
---------
Chặng Detect - Rule-based threshold checking, 3-strike violation counter
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Any, Optional

from schemas import UnifiedTelemetry, detect_type_from_topic

logger = logging.getLogger("detect")


# ================================================================
# Threshold Rules - Ngưỡng vi phạm nghiệp vụ
# ================================================================

THRESHOLDS = {
    "electricity": {
        "Uab": {"min": 180, "max": 260, "unit": "V", "name": "Điện áp pha AB"},
        "Ubc": {"min": 180, "max": 260, "unit": "V", "name": "Điện áp pha BC"},
        "Uca": {"min": 180, "max": 260, "unit": "V", "name": "Điện áp pha CA"},
        "Ia": {"min": 0, "max": 100, "unit": "A", "name": "Dòng pha A"},
        "Ib": {"min": 0, "max": 100, "unit": "A", "name": "Dòng pha B"},
        "Ic": {"min": 0, "max": 100, "unit": "A", "name": "Dòng pha C"},
        "P_Total": {"min": 0, "max": 50, "unit": "kW", "name": "Công suất tổng"},
        "F": {"min": 45, "max": 55, "unit": "Hz", "name": "Tần số"},
        "PFavg": {"min": -1, "max": 1, "unit": "", "name": "Hệ số công suất"},
        "THD_Vab": {"min": 0, "max": 8, "unit": "%", "name": "THD điện áp"},
        "THD_Ia": {"min": 0, "max": 8, "unit": "%", "name": "THD dòng A"},
    },
    "water": {
        "Qt": {"min": 0, "max": 100, "unit": "m3/h", "name": "Lưu lượng"},
        "V": {"min": 0, "max": 1000000, "unit": "m3", "name": "Tổng lưu lượng"},
    },
    "lighting": {
        "U": {"min": 180, "max": 260, "unit": "V", "name": "Điện áp"},
        "I": {"min": 0, "max": 100, "unit": "A", "name": "Dòng điện"},
        "Power_kW": {"min": 0, "max": 10, "unit": "kW", "name": "Công suất đèn"},
        "Energy_kwh": {"min": 0, "max": 100000, "unit": "kWh", "name": "Năng lượng"},
    },
    "wastewater": {
        "Qt": {"min": 0, "max": 500, "unit": "m3/h", "name": "Lưu lượng nước thải"},
        "pH": {"min": 4, "max": 10, "unit": "", "name": "pH"},
        "turbidity": {"min": 0, "max": 100, "unit": "NTU", "name": "Độ đục"},
    },
}


class DeviceState:
    """State tracking cho 1 device"""
    def __init__(self, device_id: str, khu_cn: str):
        self.device_id = device_id
        self.khu_cn = khu_cn
        self.violation_streak = 0
        self.total_violations = 0
        self.last_violation_time = 0
        self.violation_details: list = []


class Detector:
    """Chặng Detect - Kiểm tra ngưỡng vi phạm, 3-strike rule"""
    
    def __init__(
        self,
        in_queue: "queue.Queue",
        out_queue: "queue.Queue",
        thresholds: Optional[Dict] = None,
    ):
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.thresholds = thresholds or THRESHOLDS
        
        self.devices: Dict[str, DeviceState] = {}
        self.total_processed = 0
        self.total_violations = 0
        self.alerts_triggered = 0
        
        self.window_processed = 0
        self.window_violations = 0
        self.window_alerts = 0
        self.window_started = time.monotonic()
        self.started = time.monotonic()
        self.running = False
    
    def run(self):
        """Chạy detection loop"""
        self.running = True
        logger.info("Detector started")
        
        while self.running:
            try:
                item = self.in_queue.get(timeout=1)
            except queue.Empty:
                continue
            
            if item is None:
                self.running = False
                self.out_queue.put(None)
                break
            
            topic, payload = item
            self._process(topic, payload)
        
        self._finalize()
        logger.info("Detector stopped")
    
    def _get_group(self, payload: dict) -> str:
        """Xác định group từ data_type hoặc field"""
        # Use data_type if available
        data_type = payload.get("data_type", "")
        if data_type in ("water", "lighting", "electricity"):
            return data_type
        
        # Fallback: detect from fields
        if "Qt" in payload or "V" in payload:
            return "water"
        elif "Lux" in payload or "Contactor" in payload:
            return "lighting"
        elif "Uab" in payload or "P_Total" in payload:
            return "electricity"
        elif "U" in payload and "I" in payload:
            return "lighting"  # Lighting has U, I, Power_kW
        return "unknown"
    
    def _check_thresholds(self, payload: dict, group: str) -> list:
        """Kiểm tra ngưỡng, trả về list vi phạm"""
        violations = []
        group_thresholds = self.thresholds.get(group, {})
        
        for field, limits in group_thresholds.items():
            value = payload.get(field)
            if value is None:
                continue
            
            # Skip non-numeric values (e.g. "NOT_A_NUMBER")
            if isinstance(value, str):
                continue
            
            # Handle nested Lux object
            if field == "Lux" and isinstance(value, dict):
                for k, v in value.items():
                    if v is not None and isinstance(v, (int, float)) and (v < limits["min"] or v > limits["max"]):
                        violations.append({
                            "field": f"{field}.{k}",
                            "value": v,
                            "min": limits["min"],
                            "max": limits["max"],
                            "unit": limits["unit"],
                            "name": limits["name"],
                        })
                continue
            
            if isinstance(value, (int, float)):
                if value < limits["min"] or value > limits["max"]:
                    violations.append({
                        "field": field,
                        "value": value,
                        "min": limits["min"],
                        "max": limits["max"],
                        "unit": limits["unit"],
                        "name": limits["name"],
                    })
        
        # Phase imbalance check (electricity only)
        if group == "electricity":
            violations.extend(self._check_phase_imbalance(payload))
        
        return violations
    
    def _check_phase_imbalance(self, payload: dict) -> list:
        """Kiểm tra mất cân pha (current imbalance)"""
        violations = []
        ia = payload.get("Ia")
        ib = payload.get("Ib")
        ic = payload.get("Ic")
        
        if not all(isinstance(v, (int, float)) for v in [ia, ib, ic]):
            return violations
        
        avg = (ia + ib + ic) / 3
        if avg == 0:
            return violations
        
        for name, val in [("Ia", ia), ("Ib", ib), ("Ic", ic)]:
            imbalance_pct = abs(val - avg) / avg * 100
            if imbalance_pct > 20:
                violations.append({
                    "field": f"{name}_imbalance",
                    "value": round(imbalance_pct, 1),
                    "min": 0,
                    "max": 20,
                    "unit": "%",
                    "name": f"Mất cân pha {name}",
                })
        
        return violations
    
    def _process(self, topic: str, payload: dict):
        self.total_processed += 1
        self.window_processed += 1
        
        device_id = payload.get("dev_id", "unknown")
        khu_cn = payload.get("khu_cn", "unknown")
        group = self._get_group(payload)
        
        # Get or create device state
        device_key = f"{khu_cn}:{device_id}"
        if device_key not in self.devices:
            self.devices[device_key] = DeviceState(device_id, khu_cn)
        
        device = self.devices[device_key]
        
        # Check thresholds
        violations = self._check_thresholds(payload, group)
        
        if violations:
            self.total_violations += 1
            self.window_violations += 1
            device.violation_streak += 1
            device.total_violations += 1
            device.last_violation_time = time.time()
            
            for v in violations:
                v["device_id"] = device_id
                v["khu_cn"] = khu_cn
                v["group"] = group
                v["timestamp"] = time.time()
            device.violation_details.extend(violations)
            
            # 3-strike rule
            if device.violation_streak >= 3:
                alert = {
                    "type": "VIOLATION_3_STRIKE",
                    "device_id": device_id,
                    "khu_cn": khu_cn,
                    "group": group,
                    "streak": device.violation_streak,
                    "total_violations": device.total_violations,
                    "violations": violations,
                    "timestamp": time.time(),
                }
                self.out_queue.put(alert)
                self.alerts_triggered += 1
                self.window_alerts += 1
                logger.warning(f"[ALERT] 3-strike violation: {device_key} (streak={device.violation_streak})")
        else:
            # Reset streak nếu không vi phạm
            device.violation_streak = 0
        
        # Pass payload to next stage (alert)
        self.out_queue.put(("data", payload))
        
        self._maybe_log_window()
    
    def _maybe_log_window(self):
        now = time.monotonic()
        elapsed = now - self.window_started
        if elapsed >= 10:
            logger.info(
                f"[DETECT] window={elapsed:.1f}s processed={self.window_processed} "
                f"violations={self.window_violations} alerts={self.window_alerts} "
                f"rate={self.window_processed/elapsed:.1f} msg/s"
            )
            self.window_processed = 0
            self.window_violations = 0
            self.window_alerts = 0
            self.window_started = now
    
    def _finalize(self):
        elapsed = time.monotonic() - self.started
        logger.info(
            f"[DETECT] DONE processed={self.total_processed} violations={self.total_violations} "
            f"alerts={self.alerts_triggered} devices={len(self.devices)} "
            f"avg_rate={self.total_processed/elapsed:.1f} msg/s elapsed={elapsed:.1f}s"
        )
    
    def summary_dict(self) -> dict:
        return {
            "total_processed": self.total_processed,
            "total_violations": self.total_violations,
            "alerts_triggered": self.alerts_triggered,
            "unique_devices": len(self.devices),
        }


if __name__ == "__main__":
    import queue
    logging.basicConfig(level=logging.INFO)
    
    q_in = queue.Queue()
    q_out = queue.Queue()
    detector = Detector(q_in, q_out)
    
    test_data = {"dev_id": "TEST-001", "khu_cn": "A", "U": 280, "I": 10, "Power_kW": 2.2, "Energy_kwh": 100, "Alr_Current": 0, "Alr_Volt": 0, "Mode_c": 1, "Line 8": 1, "Line 9": 0, "Line 10": 1}
    
    q_in.put(("test/topic", test_data))
    q_in.put(None)
    
    detector.run()
    print(detector.summary_dict())