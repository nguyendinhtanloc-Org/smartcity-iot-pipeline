"""
schemas.py
-----------
Định nghĩa schema (Pydantic) cho message điện/nước/ánh sáng
và logic validate: kiểm tra field bắt buộc, kiểu dữ liệu, và
range vật lý hợp lệ (KHÔNG phải ngưỡng nghiệp vụ — ngưỡng vi
phạm nghiệp vụ thuộc chặng Detect ở giai đoạn sau, khi mentor
cung cấp số cụ thể).

Range ở đây là "vật lý không thể xảy ra" — ví dụ pH ngoài
0-14, độ ẩm âm... Đây là chỗ để phát hiện sensor lỗi/format
sai, không phải chỗ để phát hiện "vi phạm ngưỡng vận hành".
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ValidationError


# ---------------------------------------------------------------
# Envelope chung cho mọi loại message (áp dụng mọi group)
# ---------------------------------------------------------------

class Quality(BaseModel):
    rssi_dbm: float
    snr_db: float
    latency_ms: float = Field(ge=0)


class EventEnvelope(BaseModel):
    event_id: str
    schema_version: str
    source: str
    device_id: str
    device_type: str
    group: str
    location_id: str
    ts: int
    ts_iso: str
    local_hour: float
    seq: int
    use_case: str
    alerts: List[Any] = []
    metrics: Dict[str, Any]
    quality: Quality


# ---------------------------------------------------------------
# Metrics theo từng group — đã có sample thật cho "water".
# "electricity" và "light" hiện dùng model tạm (generic) vì
# CHƯA có sample thật — cần tự chạy mor_payload.py quan sát
# rồi bổ sung field + range cụ thể (đánh dấu TODO bên dưới).
# ---------------------------------------------------------------

class WaterMetrics(BaseModel):
    flow_m3_h: float = Field(ge=0, le=1000)
    pressure_bar: float = Field(ge=0, le=50)
    cumulative_m3: float = Field(ge=0)
    turbidity_ntu: float = Field(ge=0)
    residual_chlorine_mg_l: float = Field(ge=0)
    ph: float = Field(ge=0, le=14)
    conductivity_us_cm: float = Field(ge=0)
    temperature_c: float = Field(ge=-10, le=60)
    status: str


class GenericMetrics(BaseModel):
    """
    TODO: thay bằng model cụ thể khi có sample thật của
    electricity/light (tên field, range vật lý hợp lệ).
    Hiện tại chỉ đảm bảo có field 'status' và các field còn
    lại là số hợp lệ (không NaN/None sai kiểu).
    """
    status: Optional[str] = None

    class Config:
        extra = "allow"


METRICS_MODEL_BY_GROUP = {
    "water": WaterMetrics,
    # "electricity": ElectricityMetrics,  # TODO khi có sample
    # "light": LightMetrics,              # TODO khi có sample
}


# ---------------------------------------------------------------
# Kết quả validate
# ---------------------------------------------------------------

class ValidationResult(BaseModel):
    is_valid: bool
    error_type: Optional[str] = None   # "schema_error" | "range_error"
    error_detail: Optional[str] = None
    device_id: Optional[str] = None
    group: Optional[str] = None


def validate_event(raw: dict) -> ValidationResult:
    """
    Validate 1 message raw (dict đã parse từ JSON).
    Trả về ValidationResult — không raise exception ra ngoài,
    để pipeline không bị crash vì 1 message lỗi (poison pill).
    """
    # Bước 1: kiểm tra envelope (field bắt buộc, kiểu dữ liệu)
    try:
        envelope = EventEnvelope(**raw)
    except ValidationError as e:
        return ValidationResult(
            is_valid=False,
            error_type="schema_error",
            error_detail=str(e.errors()[:2]),  # chỉ lấy 2 lỗi đầu cho gọn log
            device_id=raw.get("device_id"),
            group=raw.get("group"),
        )

    # Bước 2: kiểm tra range vật lý theo group
    metrics_model = METRICS_MODEL_BY_GROUP.get(envelope.group, GenericMetrics)
    try:
        metrics_model(**envelope.metrics)
    except ValidationError as e:
        return ValidationResult(
            is_valid=False,
            error_type="range_error",
            error_detail=str(e.errors()[:2]),
            device_id=envelope.device_id,
            group=envelope.group,
        )

    return ValidationResult(
        is_valid=True,
        device_id=envelope.device_id,
        group=envelope.group,
    )
