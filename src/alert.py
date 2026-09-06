"""
alert.py
--------
Chặng Alert - Gửi cảnh báo qua Telegram/Email
"""

from __future__ import annotations

import json
import logging
import queue
import smtplib
import threading
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Dict, Any, Optional, List
import requests

logger = logging.getLogger("alert")


class Alerter:
    """Chặng Alert - Gửi cảnh báo về Telegram/Email"""
    
    def __init__(
        self,
        in_queue: "queue.Queue",
        out_queue: "queue.Queue",
        telegram_bot_token: Optional[str] = None,
        telegram_chat_ids: Optional[List[str]] = None,
        smtp_host: Optional[str] = None,
        smtp_port: int = 587,
        smtp_user: Optional[str] = None,
        smtp_password: Optional[str] = None,
        email_from: Optional[str] = None,
        email_to: Optional[List[str]] = None,
    ):
        self.in_queue = in_queue
        self.out_queue = out_queue
        
        # Telegram config
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_ids = telegram_chat_ids or []
        
        # Email config
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.email_from = email_from
        self.email_to = email_to or []
        
        self.alerts_sent = 0
        self.alerts_failed = 0
        self.window_alerts = 0
        self.window_started = time.monotonic()
        self.started = time.monotonic()
        self.running = False
    
    def run(self):
        """Chạy alert loop"""
        self.running = True
        logger.info("Alerter started")
        
        while self.running:
            try:
                item = self.in_queue.get(timeout=1)
            except queue.Empty:
                continue
            
            if item is None:
                self.running = False
                self.out_queue.put(None)
                break
            
            if isinstance(item, tuple) and item[0] == "data":
                # Normal data - pass through
                self.out_queue.put(item)
                continue
            
            # Alert item
            alert = item
            self._process_alert(alert)
        
        self._finalize()
        logger.info("Alerter stopped")
    
    def _process_alert(self, alert: Dict[str, Any]):
        """Xử lý và gửi alert"""
        alert_type = alert.get("type", "UNKNOWN")
        
        # Format message
        message = self._format_alert(alert)
        
        # Send to Telegram
        if self.telegram_bot_token and self.telegram_chat_ids:
            for chat_id in self.telegram_chat_ids:
                success = self._send_telegram(chat_id, message)
                if success:
                    self.alerts_sent += 1
                    self.window_alerts += 1
                else:
                    self.alerts_failed += 1
        
        # Send Email
        if self.smtp_host and self.smtp_user and self.email_to:
            success = self._send_email(message, alert)
            if success:
                self.alerts_sent += 1
                self.window_alerts += 1
            else:
                self.alerts_failed += 1
        
        # Pass to storage
        self.out_queue.put(("alert", alert))
        
        self._maybe_log_window()
    
    def _format_alert(self, alert: Dict[str, Any]) -> str:
        """Format alert message"""
        alert_type = alert.get("type", "UNKNOWN")
        device_id = alert.get("device_id", "N/A")
        khu_cn = alert.get("khu_cn", "N/A")
        group = alert.get("group", "N/A")
        
        if alert_type == "VIOLATION_3_STRIKE":
            streak = alert.get("streak", 0)
            violations = alert.get("violations", [])
            v_details = "; ".join([
                f"{v['name']}={v['value']}{v['unit']} (ngưỡng: {v['min']}-{v['max']}{v['unit']})"
                for v in violations
            ])
            return (
                f"🚨 *CẢNH BÁO VI PHẠM 3 LẦN LIÊN TIẾP*\n"
                f"📍 Khu CN: {khu_cn}\n"
                f"🔧 Thiết bị: {device_id}\n"
                f"📊 Nhóm: {group}\n"
                f"🔄 Số lần liên tiếp: {streak}\n"
                f"⚠️ Chi tiết: {v_details}"
            )
        
        return f"🔔 Alert: {alert_type} - Device: {device_id} - Khu CN: {khu_cn}"
    
    def _send_telegram(self, chat_id: str, message: str) -> bool:
        """Gửi tin nhắn Telegram"""
        if not self.telegram_bot_token:
            return False
        
        try:
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            data = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown",
            }
            response = requests.post(url, json=data, timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
            return False
    
    def _send_email(self, message: str, alert: Dict[str, Any]) -> bool:
        """Gửi email cảnh báo"""
        if not all([self.smtp_host, self.smtp_user, self.smtp_password, self.email_from, self.email_to]):
            return False
        
        try:
            subject = f"🚨 SmartCity Alert - {alert.get('type', 'UNKNOWN')}"
            
            msg = MIMEMultipart()
            msg["From"] = self.email_from
            msg["To"] = ", ".join(self.email_to)
            msg["Subject"] = subject
            
            body = f"""
            SmartCity IoT Pipeline Alert
            
            {message}
            
            ---
            Thời gian: {time.strftime('%Y-%m-%d %H:%M:%S')}
            Chi tiết: {json.dumps(alert, ensure_ascii=False, indent=2)}
            """
            
            msg.attach(MIMEText(body, "plain", "utf-8"))
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            
            return True
        except Exception as e:
            logger.error(f"Email send failed: {e}")
            return False
    
    def _maybe_log_window(self):
        now = time.monotonic()
        elapsed = now - self.window_started
        if elapsed >= 30:
            logger.info(
                f"[ALERT] window={elapsed:.1f}s alerts_sent={self.window_alerts} "
                f"failed={self.alerts_failed}"
            )
            self.window_alerts = 0
            self.window_started = now
    
    def _finalize(self):
        elapsed = time.monotonic() - self.started
        logger.info(
            f"[ALERT] DONE sent={self.alerts_sent} failed={self.alerts_failed} "
            f"elapsed={elapsed:.1f}s"
        )
    
    def summary_dict(self) -> dict:
        return {
            "alerts_sent": self.alerts_sent,
            "alerts_failed": self.alerts_failed,
        }


if __name__ == "__main__":
    import queue
    logging.basicConfig(level=logging.INFO)
    
    q_in = queue.Queue()
    q_out = queue.Queue()
    alerter = Alerter(q_in, q_out)
    
    test_alert = {
        "type": "VIOLATION_3_STRIKE",
        "device_id": "TEST-001",
        "khu_cn": "A",
        "group": "electricity",
        "streak": 3,
        "violations": [
            {"field": "U", "value": 280, "min": 180, "max": 250, "unit": "V", "name": "Điện áp"}
        ]
    }
    
    q_in.put(alert)
    q_in.put(None)
    
    alerter.run()
    print(alerter.summary_dict())