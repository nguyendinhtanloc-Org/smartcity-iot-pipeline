# Validation Rules — SmartCity IoT Pipeline (Multi-Source)

## Tổng Quan

Pipeline sử dụng Pydantic v2 để validate dữ liệu theo 2 bước:
1. **Schema Check:** Kiểm tra field bắt buộc và kiểu dữ liệu (`UnifiedTelemetry`)
2. **Range Check:** Kiểm tra ngưỡng vật lý + ngưỡng nghiệp vụ

Schema thống nhất cho **tất cả 3 khu CN** (A, B, C) và **tất cả group** (electricity, water, lighting).

---

## Schema: UnifiedTelemetry

```python
class UnifiedTelemetry(BaseModel):
    """Schema thống nhất cho multi-source ingestion"""

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
    Alr_Current: int = Field(..., ge=0, le=1, description="Alarm Current (0/1)")
    Alr_Volt: int = Field(..., ge=0, le=1, description="Alarm Voltage (0/1)")

    # Mode & Lines
    Mode_c: int = Field(..., ge=0, le=2, description="Mode (0/1/2)")
    Line_8: int = Field(..., ge=0, le=1, alias="Line 8")
    Line_9: int = Field(..., ge=0, le=1, alias="Line 9")
    Line_10: int = Field(..., ge=0, le=1, alias="Line 10")

    # Optional nested objects
    Lux: Optional[Lux] = None
    Contactor: Optional[Contactor] = None

    # Multi-source fields
    khu_cn: str = Field(..., description="Khu công nghiệp: A, B, C")
    source_name: str = Field(..., description="Tên nguồn: CN_A, CN_B, CN_C")
    received_at: int = Field(default_factory=lambda: int(time.time()))

    class Config:
        populate_by_name = True
        extra = "allow"


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
```

---

## Bước 1: Schema Check

Validate toàn bộ payload với `UnifiedTelemetry` (Pydantic v2).

### Lỗi Schema

| Lỗi | Mô tả | Ví dụ |
|------|-------|-------|
| `missing` | Thiếu field bắt buộc | Thiếu `dev_id`, `ts`, `U` |
| `type_error` | Sai kiểu dữ liệu | `U` là string thay vì float |
| `value_error` | Giá trị không hợp lệ | `U` = -10 (vi phạm `ge=0`) |

**Log mẫu schema_error:**
```json
{
  "error_type": "schema_error",
  "error_detail": "[{'type': 'missing', 'loc': ('dev_id',), 'msg': 'Field required'}]",
  "device_id": null,
  "khu_cn": "A"
}
```

---

## Bước 2: Range Check (Ngưỡng Nghiệp Vụ)

Kiểm tra ngưỡng nghiệp vụ theo **group** (xác định từ fields có trong payload).

### Electricity Group

| Field | Min | Max | Đơn vị | Mô tả |
|-------|-----|-----|--------|-------|
| `U` | 180 | 250 | V | Điện áp |
| `I` | 0 | 100 | A | Dòng điện |
| `Power_kW` | 0 | 50 | kW | Công suất |
| `Energy_kwh` | 0 | 100000 | kWh | Năng lượng tích lũy |

### Water Group

| Field | Min | Max | Đơn vị | Mô tả |
|-------|-----|-----|--------|-------|
| `flow_m3_h` | 0 | 100 | m³/h | Lưu lượng |
| `pressure_bar` | 0 | 10 | bar | Áp suất |
| `ph` | 6.5 | 8.5 | - | Độ pH |
| `turbidity_ntu` | 0 | 5 | NTU | Độ đục |

### Lighting Group

| Field | Min | Max | Đơn vị | Mô tả |
|-------|-----|-----|--------|-------|
| `Power_kW` | 0 | 10 | kW | Công suất đèn |
| `Lux.1` | 0 | 10000 | lux | Độ sáng kênh 1 |
| `Lux.2` | 0 | 10000 | lux | Độ sáng kênh 2 |

### Alarms & Status (All Groups)

| Field | Min | Max | Mô tả |
|-------|-----|-----|-------|
| `Alr_Current` | 0 | 1 | Cảnh báo dòng điện |
| `Alr_Volt` | 0 | 1 | Cảnh báo điện áp |
| `Mode_c` | 0 | 2 | Chế độ hoạt động |
| `Line_8` | 0 | 1 | Trạng thái line 8 |
| `Line_9` | 0 | 1 | Trạng thái line 9 |
| `Line_10` | 0 | 1 | Trạng thái line 10 |

### Nested Objects

**Lux:**
```json
{
  "Lux": {
    "1": 36.0,
    "2": 38.0
  }
}
```
Mỗi kênh: `0 - 10000 lux`

**Contactor:**
```json
{
  "Contactor": {
    "1": 1,
    "2": 0
  }
}
```
Trạng thái: `0` (mở) / `1` (đóng)

---

## Lỗi Range

| Lỗi | Mô tả | Ví dụ |
|------|-------|-------|
| `value_error` | Giá trị ngoài range | `U` = 300 (vượt 250V) |
| `value_error` | Giá trị âm | `I` = -5A |
| `value_error` | pH ngoài range | `ph` = 5.0 |

**Log mẫu range_error:**
```json
{
  "error_type": "range_error",
  "error_detail": "[{'type': 'value_error', 'loc': ('U',), 'msg': 'Value error, Input should be less than or equal to 250', 'input': 300}]",
  "device_id": "SL-AREA-556",
  "khu_cn": "A"
}
```

---

## Poison Pill Handling

**Logic:** Device gửi message lỗi (schema_error HOẶC range_error) **liên tục ≥3 lần** → coi là "poison pill".

**Xử lý:**
1. Ghi message gốc vào `logs/dead_letter.jsonl`
2. Reset counter sau khi device gửi message **hợp lệ** (valid)
3. Không block pipeline, chỉ log và tiếp tục

**Log mẫu dead_letter:**
```json
{
  "dev_id": "SL-AREA-556",
  "khu_cn": "A",
  "ts": "2026-09-07T10:00:00",
  "tsunix": 1788694281,
  "U": 300,
  "...": "..."
}
```

---

## Kết Quả Validation

```python
class ValidationResult(BaseModel):
    is_valid: bool
    error_type: Optional[str] = None    # "schema_error" | "range_error"
    error_detail: Optional[str] = None
    device_id: Optional[str] = None
    khu_cn: Optional[str] = None
```

**Valid:** `is_valid=true` → Push đến Detect Queue
**Invalid:** `is_valid=false` → Ghi `invalid_events.jsonl` + Poison Pill check

---

## Ví Dụ Payload Hợp Lệ

```json
{
  "dev_id": "SL-AREA-556",
  "ts": "2026-09-07T10:30:00",
  "tsunix": 1788789000,
  "U": 220.5,
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
  "Contactor": {"1": 1, "2": 0},
  "khu_cn": "A",
  "source_name": "CN_A",
  "received_at": 1788789000
}
```

---

## Ví Dụ Payload Sai (Schema Error)

```json
{
  "dev_id": "SL-AREA-556",
  "ts": "2026-09-07T10:30:00",
  "tsunix": 1788789000,
  "U": "invalid",          // Sai kiểu: string thay vì float
  "I": 12.4,
  "khu_cn": "A"            // Thiếu các field bắt buộc khác
}
```

**Kết quả:** `schema_error` - missing fields + type error

---

## Ví Dụ Payload Sai (Range Error)

```json
{
  "dev_id": "SL-AREA-556",
  "ts": "2026-09-07T10:30:00",
  "tsunix": 1788789000,
  "U": 300.0,              // Vượt max 250V
  "I": 12.4,
  "Power_kW": 2.71,
  "Energy_kwh": 2969.0,
  "Alr_Current": 0,
  "Alr_Volt": 0,
  "Mode_c": 1,
  "Line 8": 1,
  "Line 9": 0,
  "Line 10": 1,
  "khu_cn": "A",
  "source_name": "CN_A",
  "received_at": 1788789000
}
```

**Kết quả:** `range_error` - `U` vượt ngưỡng 250V

---

## Implementation Notes

- **File:** `src/schemas.py` - chứa `UnifiedTelemetry`, `Lux`, `Contactor`, `validate_event()`
- **File:** `src/validate.py` - `Validator` class, poison pill logic
- **Validation function:** `validate_event(raw: dict, schema_class=UnifiedTelemetry) -> ValidationResult`
- **Poison pill limit:** 3 liên tiếp (configurable via `RETRY_LIMIT`)
- **Extra fields:** `extra = "allow"` cho phép fields mở rộng trong tương lai