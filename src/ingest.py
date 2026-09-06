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
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("Missing paho-mqtt. Install: pip3 install paho-mqtt", file=__import__('sys').stderr)
    __import__('sys').exit(2)

from schemas import validate_event, UnifiedTelemetry

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
    """Worker chạy 1 MQTT connection trong 1 thread"""
    
    def __init__(self, source: MQTTSourceConfig, out_queue: "queue.Queue", global_config: Dict):
        self.source = source
        self.out_queue = out_queue
        self.global_config = global_config
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
        
    def start(self):
        self.running = True
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
        logger.info(f"[{self.source.name}] Worker stopped")
    
    def _run(self):
        original_getaddrinfo = socket.getaddrinfo
        
        def ipv4_only(host, port, family=0, type=0, proto=0, flags=0):
            if host == self.source.host:
                return original_getaddrinfo(host, port, socket.AF_INET, type or socket.SOCK_STREAM, proto, flags)
            return original_getaddrinfo(host, port, family, type, proto, flags)
        
        socket.getaddrinfo = ipv4_only
        
        while self.running:
            try:
                self._connect_and_run()
            except Exception as e:
                logger.error(f"[{self.source.name}] Connection error: {e}")
                self.reconnect_count += 1
                if self.running:
                    logger.info(f"[{self.source.name}] Reconnecting in {self.global_config.get('reconnect_delay', 5)}s...")
                    time.sleep(self.global_config.get("reconnect_delay", 5))
                else:
                    break
            finally:
                socket.getaddrinfo = original_getaddrinfo
    
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
        
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        
        logger.info(f"[{self.source.name}] Connecting WSS to {self.source.host}:{self.source.port}{self.source.ws_path}")
        logger.info(f"[{self.source.name}] Subscribing to {self.source.topic}")
        
        client.connect(self.source.host, self.source.port, keepalive=60)
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
        logger.warning(f"[{self.source.name}] DISCONNECTED rc={rc}")
    
    def _on_message(self, client, userdata, msg):
        self.count += 1
        self.window_count += 1
        
        payload_str = msg.payload.decode("utf-8", errors="replace")
        
        try:
            payload = json.loads(payload_str)
        except json.JSONDecodeError:
            payload = {"_raw_unparsed": payload_str, "_topic": msg.topic}
        
        # Thêm metadata multi-source
        enriched_payload = {
            **payload,
            "khu_cn": self.source.khu_cn,
            "source_name": self.source.name,
            "received_at": int(time.time()),
        }
        
        # Validate ngay tại ingestion (optional - có thể để validate stage làm)
        # validation_result = validate_event(enriched_payload)
        # if not validation_result.is_valid:
        #     logger.warning(f"[{self.source.name}] Invalid message: {validation_result.error_detail}")
        
        # Đẩy vào queue chung
        try:
            self.out_queue.put((msg.topic, enriched_payload), timeout=1)
        except queue.Full:
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
    """Quản lý multi-source ingestion từ nhiều MQTT sources"""
    
    def __init__(self, sources_config: List[Dict], global_config: Dict, out_queue: "queue.Queue"):
        self.sources_config = sources_config
        self.global_config = global_config
        self.out_queue = out_queue
        self.workers: List[SourceWorker] = []
        self.running = False
        
    def start(self):
        self.running = True
        
        for src_config in self.sources_config:
            source = MQTTSourceConfig(src_config)
            worker = SourceWorker(source, self.out_queue, self.global_config)
            self.workers.append(worker)
            worker.start()
        
        logger.info(f"MultiSourceIngestor started with {len(self.workers)} workers")
    
    def stop(self):
        self.running = False
        for worker in self.workers:
            worker.stop()
        logger.info("MultiSourceIngestor stopped")
    
    def get_stats(self) -> Dict:
        return {
            "workers": len(self.workers),
            "total_messages": sum(w.count for w in self.workers),
            "reconnect_count": sum(w.reconnect_count for w in self.workers),
            "workers_detail": [
                {
                    "name": w.source.name,
                    "khu_cn": w.source.khu_cn,
                    "count": w.count,
                    "reconnect_count": w.reconnect_count,
                }
                for w in self.workers
            ]
        }


def load_sources_config(config_path: str) -> tuple:
    """Load config từ YAML file"""
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    sources = config.get("mqtt_sources", [])
    global_config = config.get("global", {})
    return sources, global_config


def run_ingestion(config_path: str, duration: int = 1200):
    """Chạy ingestion theo config file"""
    sources, global_config = load_sources_config(config_path)
    
    internal_queue: "queue.Queue" = queue.Queue(maxsize=global_config.get("queue_maxsize", 20000))
    
    ingestor = MultiSourceIngestor(sources, global_config, internal_queue)
    ingestor.start()
    
    try:
        time.sleep(duration)
    except KeyboardInterrupt:
        logger.info("Stopping (Ctrl+C)...")
    finally:
        ingestor.stop()
        
        # Print final stats
        stats = ingestor.get_stats()
        logger.info(f"=== INGESTION SUMMARY ===")
        logger.info(f"Total workers: {stats['workers']}")
        logger.info(f"Total messages: {stats['total_messages']}")
        logger.info(f"Total reconnects: {stats['reconnect_count']}")
        for w in stats["workers_detail"]:
            logger.info(f"  {w['name']} (khu_cn={w['khu_cn']}): {w['count']} msgs, {w['reconnect_count']} reconnects")


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
    args = parser.parse_args()
    
    run_ingestion(args.config, args.duration)