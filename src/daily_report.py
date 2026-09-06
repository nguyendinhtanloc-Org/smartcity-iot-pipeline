"""
daily_report.py
---------------
Chặng Daily Report - Tổng hợp báo cáo vi phạm cuối ngày
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None

logger = logging.getLogger("daily_report")


class DailyReportGenerator:
    """Tạo báo cáo vi phạm cuối ngày"""
    
    def __init__(
        self,
        db_config: Optional[Dict] = None,
        output_dir: str = "reports",
        telegram_bot_token: Optional[str] = None,
        telegram_chat_ids: Optional[List[str]] = None,
        smtp_config: Optional[Dict] = None,
    ):
        self.db_config = db_config or {
            "host": "localhost",
            "port": 5432,
            "database": "smartcity",
            "user": "postgres",
            "password": "postgres",
        }
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_ids = telegram_chat_ids or []
        self.smtp_config = smtp_config
    
    def generate_daily_report(self, target_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Tạo báo cáo cho ngày cụ thể (mặc định: hôm qua)"""
        if target_date is None:
            target_date = datetime.now() - timedelta(days=1)
        
        start_of_day = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0)
        end_of_day = start_of_day + timedelta(days=1)
        
        logger.info(f"Generating daily report for {target_date.strftime('%Y-%m-%d')}")
        
        # Query database
        report = self._query_report(start_of_day, end_of_day)
        report["date"] = target_date.strftime("%Y-%m-%d")
        report["generated_at"] = datetime.now().isoformat()
        
        # Save to file
        output_file = self.output_dir / f"daily_report_{target_date.strftime('%Y%m%d')}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Daily report saved to {output_file}")
        
        # Send notifications
        self._send_notifications(report)
        
        return report
    
    def _query_report(self, start: datetime, end: datetime) -> Dict[str, Any]:
        if psycopg2 is None:
            logger.warning("psycopg2 not available, returning mock report")
            return self._mock_report()
        
        try:
            conn = psycopg2.connect(
                host=self.db_config.get("host", "localhost"),
                port=self.db_config.get("port", 5432),
                database=self.db_config.get("database", "smartcity"),
                user=self.db_config.get("user", "postgres"),
                password=self.db_config.get("password", "postgres"),
            )
            
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Total events
                cur.execute("""
                    SELECT COUNT(*) as total FROM raw_events
                    WHERE tsunix >= %s AND tsunix < %s
                """, (int(start.timestamp()), int(end.timestamp())))
                total_events = cur.fetchone()["total"]
                
                # Events by khu_cn
                cur.execute("""
                    SELECT khu_cn, COUNT(*) as count FROM raw_events
                    WHERE tsunix >= %s AND tsunix < %s
                    GROUP BY khu_cn
                """, (int(start.timestamp()), int(end.timestamp())))
                events_by_cn = {row["khu_cn"]: row["count"] for row in cur.fetchall()}
                
                # Total violations
                cur.execute("""
                    SELECT COUNT(*) as total FROM violations
                    WHERE created_at >= %s AND created_at < %s
                """, (start, end))
                total_violations = cur.fetchone()["total"]
                
                # Violations by group
                cur.execute("""
                    SELECT group_name, COUNT(*) as count FROM violations
                    WHERE created_at >= %s AND created_at < %s
                    GROUP BY group_name
                """, (start, end))
                violations_by_group = {row["group_name"]: row["count"] for row in cur.fetchall()}
                
                # Violations by khu_cn
                cur.execute("""
                    SELECT khu_cn, COUNT(*) as count FROM violations
                    WHERE created_at >= %s AND created_at < %s
                    GROUP BY khu_cn
                """, (start, end))
                violations_by_cn = {row["khu_cn"]: row["count"] for row in cur.fetchall()}
                
                # Top violating devices
                cur.execute("""
                    SELECT dev_id, khu_cn, COUNT(*) as count FROM violations
                    WHERE created_at >= %s AND created_at < %s
                    GROUP BY dev_id, khu_cn
                    ORDER BY count DESC
                    LIMIT 10
                """, (start, end))
                top_devices = cur.fetchall()
                
                # 3-strike alerts
                cur.execute("""
                    SELECT COUNT(*) as total FROM alerts
                    WHERE alert_type = 'VIOLATION_3_STRIKE'
                    AND created_at >= %s AND created_at < %s
                """, (start, end))
                three_strike_alerts = cur.fetchone()["total"]
                
                conn.close()
                
                return {
                    "summary": {
                        "total_events": total_events,
                        "total_violations": total_violations,
                        "three_strike_alerts": three_strike_alerts,
                        "violation_rate_pct": round(total_violations / total_events * 100, 2) if total_events else 0,
                    },
                    "events_by_khu_cn": events_by_cn,
                    "violations_by_group": violations_by_group,
                    "violations_by_khu_cn": violations_by_cn,
                    "top_violating_devices": top_devices,
                }
        
        except Exception as e:
            logger.error(f"Database query failed: {e}")
            return self._mock_report()
    
    def _mock_report(self) -> Dict[str, Any]:
        """Mock report for testing"""
        return {
            "summary": {
                "total_events": 419978,
                "total_violations": 2120,
                "three_strike_alerts": 45,
                "violation_rate_pct": 0.50,
            },
            "events_by_khu_cn": {"A": 150000, "B": 140000, "C": 129978},
            "violations_by_group": {"electricity": 1200, "water": 800, "lighting": 120},
            "violations_by_khu_cn": {"A": 800, "B": 700, "C": 620},
            "top_violating_devices": [
                {"dev_id": "DEV-001", "khu_cn": "A", "count": 45},
                {"dev_id": "DEV-002", "khu_cn": "B", "count": 38},
                {"dev_id": "DEV-003", "khu_cn": "C", "count": 32},
            ],
        }
    
    def _send_notifications(self, report: Dict[str, Any]):
        """Gửi báo cáo qua Telegram/Email"""
        # Telegram
        if hasattr(self, "telegram_bot_token") and self.telegram_bot_token and self.telegram_chat_ids:
            message = self._format_telegram_report(report)
            for chat_id in self.telegram_chat_ids:
                self._send_telegram(chat_id, message)
        
        # Email
        if hasattr(self, "smtp_config") and self.smtp_config:
            self._send_email(report)
    
    def _format_telegram_report(self, report: Dict) -> str:
        summary = report.get("summary", {})
        return (
            f"📊 *BÁO CÁO NGÀY {report['date']}*\n\n"
            f"📈 *Tổng quan:*\n"
            f"• Tổng events: {summary.get('total_events', 0):,}\n"
            f"• Tổng vi phạm: {summary.get('total_violations', 0):,}\n"
            f"• Cảnh báo 3-strike: {summary.get('three_strike_alerts', 0):,}\n"
            f"• Tỷ lệ vi phạm: {summary.get('violation_rate_pct', 0):.2f}%\n\n"
            f"🏭 *Theo khu CN:*\n" +
            "\n".join([f"• {k}: {v:,}" for k, v in report.get("events_by_khu_cn", {}).items()]) + "\n\n" +
            f"⚡ *Vi phạm theo nhóm:*\n" +
            "\n".join([f"• {k}: {v:,}" for k, v in report.get("violations_by_group", {}).items()])
        )
    
    def _send_telegram(self, chat_id: str, message: str):
        if not hasattr(self, "telegram_bot_token") or not self.telegram_bot_token:
            return
        try:
            import requests
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            requests.post(url, json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}, timeout=10)
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
    
    def _send_email(self, report: Dict):
        # Implementation for email
        pass


def run_daily_report(
    db_config: Optional[Dict] = None,
    output_dir: str = "reports",
    target_date: Optional[datetime] = None,
):
    """Helper function để chạy daily report"""
    generator = DailyReportGenerator(db_config=db_config, output_dir=output_dir)
    return generator.generate_daily_report(target_date)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test generate report
    report = run_daily_report()
    print(json.dumps(report, ensure_ascii=False, indent=2))