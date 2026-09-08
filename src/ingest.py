"""
ingest.py
----------
Multi-source Ingestion - Multi-threaded MQTT ingestion from multiple sources.
Gộp nhiều luồng MQTT từ nhiều khu CN vào 1 pipeline thống nhất.
"""

from __future__ import annotations

import json
import logging
import queue
import socket
import ssl
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# Đảm bảo import được module schemas ở thư mục gốc dù chạy trực tiếp python3 src/ingest.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("Missing paho-mqtt. Install: pip3 install paho-mqtt", file=__import__('sys').stderr)
    __import__('sys').exit(2)

from schemas import validate_event, UnifiedTelemetry, detect_type_from_topic

logger = logging.getLogger("ingestion")


class MQTTSourceConfig:
    """Config cho 1 nguồn MQTT"""
    def __init__(self, config: Dict[str, Any]):
        self.name: str = config["name"]
        self.host: str = config["host"]
        self.port: int = config["port"]
        self.ws_path: str = config["ws_path"]
        self.username: str = config["username"]
        self.password: str = config["password"]
        self.company_id: str = config["company_id"]
        self.gateways: List[str] = config["gateways"]
        self.khu_cn: str = config["khu_cn"]
        self.topic: str = config["topic"]
        self.qos: int = config.get("qos", 0)
        self.client_id: Optional[str] = config.get("client_id")
    
    @property
    def broker_key(self) -> str:
        return f"{self.host}:{self.port}"


class SourceWorker:
    """Worker chạy 1 MQTT connection trong 1 thread.
    Mỗi worker = 1 luồng MQTT (1 khu CN). Ghi raw message theo từng nguồn,
    đẩy enriched payload vào queue chung để gộp 3 luồng tại 1 nơi.
    """

    def __init__(self, source: MQTTSourceConfig, out_queue: "queue.Queue", global_config: Dict, raw_dir: Path):
        self.source = source
        self.out_queue = out_queue
        self.global_config = global_config
        self.raw_dir = raw_dir
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.client: Optional[mqtt.Client] = None
        self.count = 0
        self.window_count = 0
        self.window_started = time.monotonic()
        self.started = time.monotonic()
        self.reconnect_count = 0
        self.last_checkpoint = 0
        self.checkpoint_interval = global_config.get("checkpoint_interval", 1000)
        self.drops = 0
        self.raw_file: Optional[Path] = None
        self._raw_buffer: List[str] = []
        self._raw_flush_lines = global_config.get("raw_flush_lines", 1000)

    def _open_raw_file(self):
        self.raw_file = self.raw_dir / f"raw_{self.source.name}_{self.source.khu_cn}.jsonl"
        self.raw_file.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"[{self.source.name}] Raw file: {self.raw_file}")

    def _flush_raw(self):
        if self._raw_buffer:
            with open(self.raw_file, "a", encoding="utf-8") as f:
                f.write("\n".join(self._raw_buffer) + "\n")
            self._raw_buffer.clear()

    def _write_raw(self, payload: dict):
        self._raw_buffer.append(json.dumps(payload, ensure_ascii=False, default=str))
        if len(self._raw_buffer) >= self._raw_flush_lines:
            self._flush_raw()

    def start(self):
        self.running = True
        self._open_raw_file()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        logger.info(f"[{self.source.name}] Worker started")

    def stop(self):
        self.running = False
        if self.client:
            try:
                self.client.disconnect()
            except Exception:
                pass
        if self.thread:
            self.thread.join(timeout=10)
        self._flush_raw()
        logger.info(f"[{self.source.name}] Worker stopped total={self.count} drops={self.drops}")
    
    def _run(self):
        reconnect_delay = self.global_config.get("reconnect_delay", 5)
        max_delay = 30
        
        while self.running:
            try:
                self._connect_and_run()
            except Exception as e:
                logger.error(f"[{self.source.name}] Connection error: {e}")
                self.reconnect_count += 1
                if self.running:
                    logger.info(f"[{self.source.name}] Reconnecting in {reconnect_delay}s...")
                    time.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_delay)
                else:
                    break
            else:
                reconnect_delay = self.global_config.get("reconnect_delay", 5)
    
    def _connect_and_run(self):
        try:
            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=self.source.client_id or f"{self.global_config.get('client_id_prefix', 'ingest')}-{self.source.name}-{int(time.time())}",
                transport="websockets",
            )
        except Exception:
            client = mqtt.Client(
                client_id=self.source.client_id or f"{self.global_config.get('client_id_prefix', 'ingest')}-{self.source.name}-{int(time.time())}",
                transport="websockets",
            )
        
        self.client = client
        client.username_pw_set(self.source.username, self.source.password)
        client.ws_set_options(path=self.source.ws_path)
        client.tls_set(cert_reqs=ssl.CERT_NONE)
        client.tls_insecure_set(True)
        
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        client.reconnect_delay_set(min_delay=1, max_delay=15)
        
        logger.info(f"[{self.source.name}] Connecting WSS to {self.source.host}:{self.source.port}{self.source.ws_path}")
        logger.info(f"[{self.source.name}] Subscribing to {self.source.topic}")
        
        client.connect(self.source.host, self.source.port, keepalive=120)
        client.loop_forever()
    
    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc != 0:
            logger.error(f"[{self.source.name}] CONNECT FAILED rc={rc}")
            return
        logger.info(f"[{self.source.name}] CONNECTED")
        result, mid = client.subscribe(self.source.topic, qos=self.source.qos)
        if result != mqtt.MQTT_ERR_SUCCESS:
            logger.error(f"[{self.source.name}] SUBSCRIBE FAILED rc={result}")
            return
        logger.info(f"[{self.source.name}] SUBSCRIBED topic={self.source.topic} mid={mid}")
    
    def _on_disconnect(self, client, userdata, disconnect_flags=None, rc=None, properties=None):
        # Handle positional args flexibility between paho-mqtt v1 and v2
        rc_code = rc if rc is not None else disconnect_flags
        now = time.monotonic()
        if hasattr(self, "_last_disc_time") and (now - self._last_disc_time < 0.5):
            return  # Prevent double logging
        self._last_disc_time = now

        if rc_code == 0 or (hasattr(rc_code, "value") and rc_code.value == 0):
            logger.info(f"[{self.source.name}] DISCONNECTED (clean)")
        else:
            logger.warning(f"[{self.source.name}] DISCONNECTED rc={rc_code} — loop_forever will auto-reconnect")
    
    def _on_message(self, client, userdata, msg):
        self.count += 1
        self.window_count += 1
        
        payload_str = msg.payload.decode("utf-8", errors="replace")
        
        try:
            payload = json.loads(payload_str)
        except json.JSONDecodeError:
            payload = {"_raw_unparsed": payload_str, "_topic": msg.topic}
        
        # Thêm metadata multi-source
        data_type = detect_type_from_topic(msg.topic)
        enriched_payload = {
            **payload,
            "data_type": data_type,
            "khu_cn": self.source.khu_cn,
            "source_name": self.source.name,
            "received_at": int(time.time()),
        }
        
        # Validate ngay tại ingestion (optional - có thể để validate stage làm)
        # validation_result = validate_event(enriched_payload)
        # if not validation_result.is_valid:
        #     logger.warning(f"[{self.source.name}] Invalid message: {validation_result.error_detail}")
        
        # Đẩy vào queue chung (điểm gộp 3 luồng)
        record = {"topic": msg.topic, "payload": enriched_payload}
        self._write_raw(record)
        try:
            self.out_queue.put((msg.topic, enriched_payload), timeout=1)
        except queue.Full:
            self.drops += 1
            logger.warning(f"[{self.source.name}] Queue full, dropping message")
        
        # Log throughput mỗi 10 giây
        now = time.monotonic()
        elapsed = now - self.window_started
        if elapsed >= 10:
            rate = self.window_count / elapsed if elapsed > 0 else 0
            logger.info(
                f"[{self.source.name}] window={elapsed:.1f}s recv={self.window_count} rate={rate:.1f} msg/s total={self.count}"
            )
            self.window_count = 0
            self.window_started = now


class MultiSourceIngestor:
    """Quản lý multi-source ingestion từ nhiều MQTT sources.
    
    Gộp N luồng MQTT (N khu CN) vào 1 queue chung bằng cách
    mỗi source chạy 1 SourceWorker thread riêng (1 MQTT connection/thread).
    Monitor thread định kỳ log TỔNG throughput của cả N luồng.
    """

    def __init__(
        self,
        sources_config: List[Dict],
        global_config: Dict,
        out_queue: "queue.Queue",
        raw_dir: Path = None,
    ):
        self.sources_config = sources_config
        self.global_config = global_config
        self.out_queue = out_queue
        self.raw_dir = Path(raw_dir) if raw_dir else Path("data") / "raw"
        self.workers: List[SourceWorker] = []
        self.running = False
        self.started = time.monotonic()
        self._monitor_thread: Optional[threading.Thread] = None
        self._report_interval = global_config.get("report_interval", 10)

    def start(self):
        self.running = True
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        for src_config in self.sources_config:
            source = MQTTSourceConfig(src_config)
            worker = SourceWorker(source, self.out_queue, self.global_config, self.raw_dir)
            self.workers.append(worker)
            worker.start()

        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

        logger.info(f"MultiSourceIngestor started with {len(self.workers)} workers")

    def stop(self):
        self.running = False
        for worker in self.workers:
            worker.stop()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        logger.info("MultiSourceIngestor stopped")

    def _monitor_loop(self):
        """Định kỳ log TỔNG throughput của tất cả nguồn (bằng chứng gộp 3 luồng)."""
        last_total = self.get_stats()["total_messages"]
        last_time = time.monotonic()
        while self.running:
            time.sleep(self._report_interval)
            if not self.running:
                break
            now = time.monotonic()
            current_total = self.get_stats()["total_messages"]
            elapsed = now - last_time
            delta = current_total - last_total
            rate = delta / elapsed if elapsed > 0 else 0
            per_source = " ".join(
                f"{w.source.name}={w.count}" for w in self.workers
            )
            drops = sum(w.drops for w in self.workers)
            logger.info(
                f"[INGEST-AGG] window={elapsed:.1f}s delta={delta} rate={rate:.1f} msg/s "
                f"total={current_total} drops={drops} | per-source: {per_source}"
            )
            last_total = current_total
            last_time = now

    def run(self, duration_seconds: int = 0):
        """Run ingestion for specified duration or until stopped"""
        self.start()
        try:
            if duration_seconds > 0:
                time.sleep(duration_seconds)
            else:
                while self.running:
                    time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping (Ctrl+C)...")
        finally:
            self.stop()

    def get_stats(self) -> Dict:
        return {
            "workers": len(self.workers),
            "total_messages": sum(w.count for w in self.workers),
            "total_drops": sum(w.drops for w in self.workers),
            "reconnect_count": sum(w.reconnect_count for w in self.workers),
            "workers_detail": [
                {
                    "name": w.source.name,
                    "khu_cn": w.source.khu_cn,
                    "count": w.count,
                    "drops": w.drops,
                    "reconnect_count": w.reconnect_count,
                    "raw_file": str(w.raw_file) if w.raw_file else None,
                }
                for w in self.workers
            ]
        }

    def summary_dict(self) -> Dict:
        """Tóm tắt chặng INGESTION: total, rate trung bình, per-source, các file raw."""
        elapsed = time.monotonic() - self.started
        stats = self.get_stats()
        total = stats["total_messages"]
        avg_rate = total / elapsed if elapsed > 0 else 0
        return {
            "stage": "ingestion",
            "elapsed_seconds": round(elapsed, 2),
            "total_messages": total,
            "avg_rate_msg_per_s": round(avg_rate, 2),
            "total_drops": stats["total_drops"],
            "reconnect_count": stats["reconnect_count"],
            "per_source": stats["workers_detail"],
            "raw_dir": str(self.raw_dir),
        }


def load_sources_config(config_path: str) -> tuple:
    """Load config từ YAML file"""
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    sources = config.get("mqtt_sources", [])
    global_config = config.get("global", {})
    return sources, global_config


def run_ingestion(config_path: str, duration: int = 1200, out_log: str = None, summary_path: str = None):
    """Chạy ingestion theo config file, ghi log + summary.json cho 1 chặng."""
    from pathlib import Path as P
    sources, global_config = load_sources_config(config_path)
    base_dir = P(__file__).resolve().parent.parent

    if out_log:
        out_log_p = base_dir / out_log
        out_log_p.parent.mkdir(parents=True, exist_ok=True)

    internal_queue: "queue.Queue" = queue.Queue(maxsize=global_config.get("queue_maxsize", 20000))

    ingestor = MultiSourceIngestor(
        sources, global_config, internal_queue, raw_dir=base_dir / "data" / "raw"
    )
    ingestor.start()

    try:
        time.sleep(duration)
    except KeyboardInterrupt:
        logger.info("Stopping (Ctrl+C)...")
    finally:
        ingestor.stop()

        # Ghi summary cho chặng INGESTION
        summary = ingestor.summary_dict()
        summary["started_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - duration))
        summary["ended_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        if summary_path:
            summary_p = base_dir / summary_path
            summary_p.parent.mkdir(parents=True, exist_ok=True)
            with open(summary_p, "w", encoding="utf-8") as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            logger.info(f"Ingestion summary saved: {summary_p}")

        logger.info(f"=== INGESTION SUMMARY ===")
        logger.info(json.dumps(summary, ensure_ascii=False, indent=2))

        stats = ingestor.get_stats()
        logger.info(f"Total workers: {stats['workers']}")
        logger.info(f"Total messages: {stats['total_messages']}")
        logger.info(f"Total drops: {stats['total_drops']}")
        logger.info(f"Total reconnects: {stats['reconnect_count']}")
        for w in stats["workers_detail"]:
            logger.info(f"  {w['name']} (khu_cn={w['khu_cn']}): {w['count']} msgs, {w['drops']} drops, {w['reconnect_count']} reconnects")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        handlers=[logging.StreamHandler()]
    )
    
    import argparse
    parser = argparse.ArgumentParser(description="Multi-source MQTT Ingestion")
    parser.add_argument("--config", default="config/sources.yaml")
    parser.add_argument("--duration", type=int, default=1200)
    parser.add_argument("--summary", default="logs/ingest_summary.json", help="Đường dẫn summary output")
    args = parser.parse_args()
    
    run_ingestion(args.config, args.duration, summary_path=args.summary)