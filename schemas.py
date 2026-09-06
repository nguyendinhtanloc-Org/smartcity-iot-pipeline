"""
schemas.py
----------
Định nghĩa schema (Pydantic) cho message điện thực tế từ broker.
Data thực tế format:
{
  "dev_id": "SL-AREA-556",
  "ts": "2026-09-06T11:31:21",
  "tsunix": 1788694281,
  "U": 220.0,
  "I": 12.4,
  "Power_kW": 2.71,
  "Energy_kwh": 2969.0,
  "Alr_Current": 0,
  "Alr_Volt": 0,
  "Mode_c": 1,
  "Line 8": 1,
  "Line 9": 0,
  "Line 10": 1,
  "Lux": {"1": 36.0, "2": 38.0},
  "Contactor": {"1": 1, "2": 0}
}
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ValidationError


# ---------------------------------------------------------------
# Lux & Contactor (nested objects)
# ---------------------------------------------------------------

class Lux(BaseModel):
    """Độ sáng các kênh"""
    _1: Optional[float] = Field(None, alias="1")
    _2: Optional[float] = Field(None, alias="2")

    class Config:
        populate_by_name = True
        extra = "allow"


class Contactor(BaseModel):
    """Trạng thái contactor"""
    _1: Optional[int] = Field(None, alias="1")
    _2: Optional[int] = Field(None, alias="2")

    class Config:
        populate_by_name = True
        extra = "allow"


# ---------------------------------------------------------------
# Telemetry Electricity - Data thực tế từ broker
# ---------------------------------------------------------------

class ElectricityTelemetry(BaseModel):
    """Schema cho data điện thực tế từ broker v1/C001/+/up/telemetry"""

    # Identity
    dev_id: str = Field(..., description="Device ID")

    # Timestamp
    ts: str = Field(..., description="Timestamp ISO format")
    tsunix: int = Field(..., description="Timestamp Unix epoch seconds")

    # Electrical measurements
    U: float = Field(..., ge=0, le=500, description="Voltage (V)")
    I: float = Field(..., ge=0, le=1000, description="Current (A)")
    Power_kW: float = Field(..., ge=0, le=1000, description="Power (kW)")
    Energy_kwh: float = Field(..., ge=0, description="Energy (kWh)")

    # Alarms
    Alr_Current: int = Field(..., ge=0, le=1, description="Alarm Current")
    Alr_Volt: int = Field(..., ge=0, le=1, description="Alarm Voltage")

    # Mode & Lines
    Mode_c: int = Field(..., ge=0, le=2, description="Mode")
    Line_8: int = Field(..., ge=0, le=1, alias="Line 8")
    Line_9: int = Field(..., ge=0, le=1, alias="Line 9")
    Line_10: int = Field(..., ge=0, le=1, alias="Line 10")

    # Optional nested objects
    Lux: Optional[Lux] = None
    Contactor: Optional[Contactor] = None

    class Config:
        populate_by_name = True
        extra = "allow"


# ---------------------------------------------------------------
# Unified Telemetry - Schema chung cho multi-source ingestion
# ---------------------------------------------------------------

class UnifiedTelemetry(ElectricityTelemetry):
    """Schema chung cho multi-source ingestion - thêm khu_cn, source_name"""
    
    # Multi-source fields
    khu_cn: str = Field(..., description="Khu công nghiệp: A, B, C")
    source_name: str = Field(..., description="Tên nguồn: CN_A, CN_B, CN_C")
    received_at: int = Field(default_factory=lambda: int(time.time()), description="Thời gian nhận (epoch seconds)")

    class Config:
        populate_by_name = True
        extra = "allow"


# ---------------------------------------------------------------
# Kết quả validate
# ---------------------------------------------------------------

class ValidationResult(BaseModel):
    is_valid: bool
    error_type: Optional[str] = None
    error_detail: Optional[str] = None
    device_id: Optional[str] = None
    khu_cn: Optional[str] = None


def validate_event(raw: dict, schema_class: type = UnifiedTelemetry) -> ValidationResult:
    """
    Validate 1 message raw (dict đã parse từ JSON).
    Trả về ValidationResult — không raise exception ra ngoài.
    """
    try:
        schema_class(**raw)
    except ValidationError as e:
        return ValidationResult(
            is_valid=False,
            error_type="schema_error",
            error_detail=str(e.errors()[:2]),
            device_id=raw.get("dev_id"),
            khu_cn=raw.get("khu_cn"),
        )

    return ValidationResult(
        is_valid=True,
        device_id=raw.get("dev_id"),
        khu_cn=raw.get("khu_cn"),
    )