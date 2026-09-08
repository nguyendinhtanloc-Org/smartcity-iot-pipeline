"""
schemas.py
----------
Schema (Pydantic) cho 3 loại dữ liệu SmartCity từ broker.

1. Water (GW_WATER_001):   dev_id, ts, tsunix, Qt, V
2. Lighting (GW_LIGHT_001): dev_id, ts, tsunix, U, I, Power_kW, Energy_kwh, ...
3. Electricity (GW_ELECTRIC_001): 3-phase Uab, Ia, Pa, P_Total, ...

Format lưu file (mor_payload.py):
{
  "topic": "v1/C001/GW_WATER_001/up/telemetry",
  "ts": "2026-09-07 19:16:22",
  "received_at": 1788783382,
  "payload": { ... actual sensor data ... }
}
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, ValidationError, field_validator


# ---------------------------------------------------------------
# Gateway → Type mapping
# ---------------------------------------------------------------

GATEWAY_TYPE_MAP = {
    "GW_WATER_001": "water",
    "GW_LIGHT_001": "lighting",
    "GW_ELECTRIC_001": "electricity",
    "GW_WWTP_001": "wastewater",
}


def detect_type_from_topic(topic: str) -> str:
    """Extract gateway from topic and return data type."""
    parts = topic.split("/")
    if len(parts) >= 3:
        gateway_id = parts[2]
        return GATEWAY_TYPE_MAP.get(gateway_id, "unknown")
    return "unknown"


# Helper coercers
def _coerce_float(v: Any) -> Optional[float]:
    if v is None or v == "" or (isinstance(v, str) and (v.upper() == "NOT_A_NUMBER" or not v.replace('.', '', 1).replace('-', '', 1).isdigit())):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------
# Water Telemetry
# ---------------------------------------------------------------

class WaterTelemetry(BaseModel):
    """Schema cho data nước: GW_WATER_001"""
    dev_id: Optional[str] = Field(None, description="Device ID")
    ts: Union[str, int] = Field(..., description="Timestamp (string or unix int)")
    tsunix: Optional[Union[int, str]] = Field(None, description="Timestamp Unix")
    Qt: Optional[float] = Field(None, description="Flow rate (m3/h)")
    V: Optional[float] = Field(None, description="Volume (m3)")

    @field_validator("Qt", "V", mode="before")
    @classmethod
    def coerce_numeric(cls, v):
        return _coerce_float(v)

    class Config:
        extra = "allow"


# ---------------------------------------------------------------
# Lighting Telemetry
# ---------------------------------------------------------------

class Lux(BaseModel):
    """Độ sáng các kênh"""
    ch1: Optional[float] = Field(None, alias="1")
    ch2: Optional[float] = Field(None, alias="2")

    class Config:
        populate_by_name = True
        extra = "allow"


class Contactor(BaseModel):
    """Trạng thái contactor"""
    ch1: Optional[int] = Field(None, alias="1")
    ch2: Optional[int] = Field(None, alias="2")

    class Config:
        populate_by_name = True
        extra = "allow"


class LightingTelemetry(BaseModel):
    """Schema cho data đèn: GW_LIGHT_001"""
    dev_id: Optional[str] = Field(None, description="Device ID")
    ts: Union[str, int] = Field(..., description="Timestamp (string or unix int)")
    tsunix: Optional[Union[int, str]] = Field(None, description="Timestamp Unix")

    # Electrical
    U: Optional[float] = Field(None, ge=0, le=500, description="Voltage (V)")
    I: Optional[float] = Field(None, ge=0, le=1000, description="Current (A)")
    Power_kW: Optional[float] = Field(None, ge=0, le=1000, description="Power (kW)")
    Energy_kWh: Optional[float] = Field(None, ge=0, alias="Energy_kWh", description="Energy (kWh)")

    # Alarms
    Alr_Current: Optional[int] = Field(0, ge=0, le=1)
    Alr_Volt: Optional[int] = Field(0, ge=0, le=1)

    # Mode & Lines (broker uses "Mode_control", "Line 1"-"Line 10")
    Mode_control: Optional[int] = Field(None, alias="Mode_c", ge=0, le=3)
    EMG_Stop_monitor: Optional[Union[int, str]] = Field(None, alias="EMG Stop monitor")

    # Lines 1-10
    Line_1: Optional[int] = Field(None, alias="Line 1", ge=0, le=1)
    Line_2: Optional[int] = Field(None, alias="Line 2", ge=0, le=1)
    Line_3: Optional[int] = Field(None, alias="Line 3", ge=0, le=1)
    Line_4: Optional[int] = Field(None, alias="Line 4", ge=0, le=1)
    Line_5: Optional[int] = Field(None, alias="Line 5", ge=0, le=1)
    Line_6: Optional[int] = Field(None, alias="Line 6", ge=0, le=1)
    Line_7: Optional[int] = Field(None, alias="Line 7", ge=0, le=1)
    Line_8: Optional[int] = Field(None, alias="Line 8", ge=0, le=1)
    Line_9: Optional[int] = Field(None, alias="Line 9", ge=0, le=1)
    Line_10: Optional[int] = Field(None, alias="Line 10", ge=0, le=1)

    # Nested (accept dict format from broker)
    Lux: Optional[Any] = None
    Contactor: Optional[Any] = None

    @field_validator("U", "I", "Power_kW", "Energy_kWh", mode="before")
    @classmethod
    def coerce_numeric(cls, v):
        return _coerce_float(v)

    @field_validator("Energy_kWh", mode="before")
    @classmethod
    def accept_energy_aliases(cls, v, info):
        # Fallback if dictionary has 'Energy_kwh'
        return _coerce_float(v)

    @field_validator("EMG_Stop_monitor", mode="before")
    @classmethod
    def coerce_emg_stop(cls, v):
        if isinstance(v, str):
            return 1 if v.upper() in ("ON", "1", "TRUE") else 0
        return v

    @field_validator("Lux", mode="before")
    @classmethod
    def coerce_lux(cls, v):
        if isinstance(v, dict):
            return v
        return None

    class Config:
        populate_by_name = True
        extra = "allow"


# ---------------------------------------------------------------
# Wastewater Telemetry
# ---------------------------------------------------------------

class WastewaterTelemetry(BaseModel):
    """Schema cho data nước thải: GW_WWTP_001"""
    dev_id: Optional[str] = Field(None, description="Device ID")
    ts: Union[str, int] = Field(..., description="Timestamp (string or unix int)")
    tsunix: Optional[Union[int, str]] = Field(None, description="Timestamp Unix")
    Qt: Optional[float] = Field(None, description="Flow rate (m3/h)")
    V: Optional[float] = Field(None, description="Volume (m3)")
    pH: Optional[float] = Field(None, description="pH level")
    turbidity: Optional[float] = Field(None, description="Turbidity (NTU)")
    conductivity: Optional[float] = Field(None, description="Conductivity (uS/cm)")
    temperature: Optional[float] = Field(None, description="Temperature (C)")

    class Config:
        extra = "allow"


# ---------------------------------------------------------------
# Electricity Telemetry (3-phase)
# ---------------------------------------------------------------

class ElectricityTelemetry(BaseModel):
    """Schema cho data điện 3 pha: GW_ELECTRIC_001"""
    dev_id: Optional[str] = Field(None, description="Device ID")
    ts: Union[str, int] = Field(..., description="Timestamp (string or unix int)")
    tsunix: Optional[Union[int, str]] = Field(None, description="Timestamp Unix")

    # 3-phase Voltage
    Uab: Optional[float] = Field(None, ge=0, le=500)
    Ubc: Optional[float] = Field(None, ge=0, le=500)
    Uca: Optional[float] = Field(None, ge=0, le=500)
    Ull: Optional[float] = Field(None, ge=0, le=500)
    Uan: Optional[float] = Field(None, ge=0, le=500)
    Ubn: Optional[float] = Field(None, ge=0, le=500)
    Ucn: Optional[float] = Field(None, ge=0, le=500)
    Uln: Optional[float] = Field(None, ge=0, le=500)

    # 3-phase Current
    Ia: Optional[float] = Field(None, ge=0, le=1000)
    Ib: Optional[float] = Field(None, ge=0, le=1000)
    Ic: Optional[float] = Field(None, ge=0, le=1000)
    In: Optional[float] = Field(None, ge=0, le=1000)
    Ig: Optional[float] = Field(None, ge=0, le=1000)
    Iavg: Optional[float] = Field(None, ge=0, le=1000)

    # Active Power (kW)
    Pa: Optional[float] = Field(None, ge=0)
    Pb: Optional[float] = Field(None, ge=0)
    Pc: Optional[float] = Field(None, ge=0)
    P_Total: Optional[float] = Field(None, ge=0)

    # Reactive Power (kVAR)
    Qa: Optional[float] = Field(None)
    Qb: Optional[float] = Field(None)
    Qc: Optional[float] = Field(None)
    Q_Total: Optional[float] = Field(None)

    # Apparent Power (kVA)
    Sa: Optional[float] = Field(None, ge=0)
    Sb: Optional[float] = Field(None, ge=0)
    Sc: Optional[float] = Field(None, ge=0)
    S_Total: Optional[float] = Field(None, ge=0)

    # Power Factor (may be "NOT_A_NUMBER" from broker)
    PFa: Optional[Union[float, str]] = Field(None)
    PFb: Optional[Union[float, str]] = Field(None)
    PFc: Optional[Union[float, str]] = Field(None)
    PFavg: Optional[Union[float, str]] = Field(None)

    # Frequency
    F: Optional[float] = Field(None, ge=45, le=65)

    # Energy
    EP: Optional[float] = Field(None, ge=0)
    EQ: Optional[float] = Field(None)
    ES: Optional[float] = Field(None, ge=0)

    # THD
    THD_Ia: Optional[float] = Field(None, ge=0)
    THD_Ib: Optional[float] = Field(None, ge=0)
    THD_Ic: Optional[float] = Field(None, ge=0)
    THD_In: Optional[float] = Field(None, ge=0)
    THD_Ig: Optional[float] = Field(None, ge=0)
    THD_Vab: Optional[float] = Field(None, ge=0)
    THD_Vbc: Optional[float] = Field(None, ge=0)
    THD_Vca: Optional[float] = Field(None, ge=0)
    THD_Vll: Optional[float] = Field(None, ge=0)
    THD_Van: Optional[float] = Field(None, ge=0)
    THD_Vbn: Optional[float] = Field(None, ge=0)
    THD_Vcn: Optional[float] = Field(None, ge=0)
    THD_Vln: Optional[float] = Field(None, ge=0)

    # Temperature
    T: Optional[float] = Field(None)

    @field_validator(
        "Uab", "Ubc", "Uca", "Ull", "Uan", "Ubn", "Ucn", "Uln",
        "Ia", "Ib", "Ic", "In", "Ig", "Iavg",
        "Pa", "Pb", "Pc", "P_Total",
        "F", "EP", "EQ", "ES", "T",
        mode="before"
    )
    @classmethod
    def coerce_numeric(cls, v):
        return _coerce_float(v)

    class Config:
        extra = "allow"


# ---------------------------------------------------------------
# Unified Telemetry (any type)
# ---------------------------------------------------------------

class UnifiedTelemetry(BaseModel):
    """Schema chung - chứa 1 trong 3 loại data"""
    dev_id: Optional[str] = None
    ts: Optional[str] = None
    tsunix: Optional[Union[int, str]] = None

    # Multi-source fields
    data_type: str = Field(default="unknown", description="water/lighting/electricity")
    khu_cn: str = Field(default="A", description="Khu CN: A, B, C")
    source_name: str = Field(default="unknown", description="Source name")
    received_at: int = Field(default_factory=lambda: int(time.time()))

    # Water fields
    Qt: Optional[float] = None
    V: Optional[float] = None

    # Lighting fields
    U: Optional[float] = None
    I: Optional[float] = None
    Power_kW: Optional[float] = None
    Energy_kwh: Optional[float] = None
    Alr_Current: Optional[int] = None
    Alr_Volt: Optional[int] = None
    Mode_c: Optional[int] = None
    Lux: Optional[Dict[str, float]] = None
    Contactor: Optional[Dict[str, int]] = None

    # Electricity 3-phase fields
    Uab: Optional[float] = None
    Ubc: Optional[float] = None
    Uca: Optional[float] = None
    Ull: Optional[float] = None
    Uan: Optional[float] = None
    Ubn: Optional[float] = None
    Ucn: Optional[float] = None
    Uln: Optional[float] = None
    Ia: Optional[float] = None
    Ib: Optional[float] = None
    Ic: Optional[float] = None
    In: Optional[float] = None
    Ig: Optional[float] = None
    P_Total: Optional[float] = None
    Q_Total: Optional[float] = None
    S_Total: Optional[float] = None
    PFavg: Optional[Union[float, str]] = None
    F: Optional[float] = None
    EP: Optional[float] = None

    # Wastewater fields
    pH: Optional[float] = None
    turbidity: Optional[float] = None
    conductivity: Optional[float] = None
    temperature: Optional[float] = None

    class Config:
        extra = "allow"


# ---------------------------------------------------------------
# Wrapper message (from mor_payload.py)
# ---------------------------------------------------------------

class RawMessage(BaseModel):
    """Wrapper message format from mor_payload.py"""
    topic: str
    ts: str
    received_at: int
    payload: Dict[str, Any]

    class Config:
        extra = "allow"


# ---------------------------------------------------------------
# Validation result
# ---------------------------------------------------------------

class ValidationResult(BaseModel):
    is_valid: bool
    error_type: Optional[str] = None
    error_detail: Optional[str] = None
    device_id: Optional[str] = None
    data_type: Optional[str] = None
    khu_cn: Optional[str] = None


def validate_event(raw: dict) -> ValidationResult:
    """
    Validate message - detect type and validate accordingly.
    Handles both raw payload and wrapped message formats.
    """
    # If wrapped format (from mor_payload.py), extract payload
    if "payload" in raw and "topic" in raw:
        topic = raw.get("topic", "")
        data_type = detect_type_from_topic(topic)
        payload = raw["payload"]
        khu_cn = "A"  # Default
    else:
        payload = raw
        data_type = raw.get("data_type") or detect_type_from_topic(raw.get("_topic", ""))
        khu_cn = raw.get("khu_cn", "A")

    # If payload could not be parsed as JSON, it's invalid
    if "_raw_unparsed" in payload:
        return ValidationResult(
            is_valid=False,
            error_type="json_parse_error",
            error_detail="Message body is not valid JSON",
            device_id=payload.get("dev_id"),
            data_type=data_type,
            khu_cn=khu_cn,
        )

    # Pre-validation fix: ts fallback → tsunix
    if not payload.get("ts") and payload.get("tsunix"):
        payload["ts"] = payload["tsunix"]

    # Try validate by type
    schema_map = {
        "water": WaterTelemetry,
        "lighting": LightingTelemetry,
        "electricity": ElectricityTelemetry,
        "wastewater": WastewaterTelemetry,
    }

    schema_class = schema_map.get(data_type)

    if schema_class:
        try:
            schema_class(**payload)
            return ValidationResult(
                is_valid=True,
                device_id=payload.get("dev_id"),
                data_type=data_type,
                khu_cn=khu_cn,
            )
        except ValidationError as e:
            return ValidationResult(
                is_valid=False,
                error_type="schema_error",
                error_detail=str(e.errors()[:2]),
                device_id=payload.get("dev_id"),
                data_type=data_type,
                khu_cn=khu_cn,
            )

    # Unknown type - try unified schema
    try:
        UnifiedTelemetry(**payload)
        return ValidationResult(
            is_valid=True,
            device_id=payload.get("dev_id"),
            data_type=data_type,
            khu_cn=khu_cn,
        )
    except ValidationError as e:
        return ValidationResult(
            is_valid=False,
            error_type="schema_error",
            error_detail=str(e.errors()[:2]),
            device_id=payload.get("dev_id"),
            data_type=data_type,
            khu_cn=khu_cn,
        )
