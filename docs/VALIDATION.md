# Validation Rules — SmartCity IoT Pipeline

## Tổng Quan

Pipeline sử dụng Pydantic để validate dữ liệu theo 2 bước:
1. **Schema Check:** Kiểm tra field bắt buộc và kiểu dữ liệu
2. **Range Check:** Kiểm tra giá trị vật lý hợp lệ

---

## Bước 1: Schema Check

### EventEnvelope (Mọi thiết bị)

```python
class EventEnvelope(BaseModel):
    event_id: str                    # Bắt buộc
    schema_version: str              # Bắt buộc
    source: str                      # Bắt buộc
    device_id: str                   # Bắt buộc
    device_type: str                 # Bắt buộc
    group: str                       # Bắt buộc: water/electricity/light
    location_id: str                 # Bắt buộc
    ts: int                          # Bắt buộc: Epoch milliseconds
    ts_iso: str                      # Bắt buộc
    local_hour: float                # Bắt buộc
    seq: int                         # Bắt buộc
    use_case: str                    # Bắt buộc
    alerts: List[Any] = []           # Không bắt buộc, default: []
    metrics: Dict[str, Any]          # Bắt buộc
    quality: Quality                 # Bắt buộc
```

### Quality

```python
class Quality(BaseModel):
    rssi_dbm: float                  # Bắt buộc
    snr_db: float                    # Bắt buộc
    latency_ms: float = Field(ge=0)  # Bắt buộc, ≥ 0
```

### Lỗi Schema

| Lỗi | Mô tả | Ví dụ |
|------|-------|-------|
| `missing` | Thiếu field bắt buộc | Thiếu `device_id` |
| `type_error` | Sai kiểu dữ liệu | `ts` là string thay vì int |
| `value_error` | Giá trị không hợp lệ | `latency_ms` < 0 |

**Log mẫu:**
```json
{
  "error_type": "schema_error",
  "error_detail": "[{'type': 'missing', 'loc': ('device_id',), 'msg': 'Field required'}]",
  "device_id": null,
  "group": null
}
```

---

## Bước 2: Range Check

### WaterMetrics

```python
class WaterMetrics(BaseModel):
    flow_m3_h: float = Field(ge=0, le=1000)           # 0-1000 m³/h
    pressure_bar: float = Field(ge=0, le=50)           # 0-50 bar
    cumulative_m3: float = Field(ge=0)                 # ≥ 0
    turbidity_ntu: float = Field(ge=0)                 # ≥ 0
    residual_chlorine_mg_l: float = Field(ge=0)        # ≥ 0
    ph: float = Field(ge=0, le=14)                     # 0-14
    conductivity_us_cm: float = Field(ge=0)            # ≥ 0
    temperature_c: float = Field(ge=-10, le=60)        # -10~60°C
    status: str                                         # Bắt buộc
```

**Range Chi Tiết:**

| Field | Min | Max | Đơn vị | Ghi chú |
|-------|-----|-----|--------|---------|
| `flow_m3_h` | 0 | 1000 | m³/h | Lưu lượng |
| `pressure_bar` | 0 | 50 | bar | Áp suất |
| `cumulative_m3` | 0 | ∞ | m³ | Tổng lưu lượng |
| `turbidity_ntu` | 0 | ∞ | NTU | Độ đục |
| `residual_chlorine_mg_l` | 0 | ∞ | mg/L | Clo dư |
| `ph` | 0 | 14 | - | Độ pH |
| `conductivity_us_cm` | 0 | ∞ | µS/cm | Độ dẫn điện |
| `temperature_c` | -10 | 60 | °C | Nhiệt độ |

### GenericMetrics (Electricity/Light)

```python
class GenericMetrics(BaseModel):
    status: Optional[str] = None

    class Config:
        extra = "allow"  # Cho phép thêm field tùy ý
```

**Hiện tại chỉ kiểm tra:**
- Có field `status` không
- Các field còn lại là số hợp lệ (không NaN/None sai kiểu)

**TODO:** Bổ sung khi có sample thật từ electricity/light

### Lỗi Range

| Lỗi | Mô tả | Ví dụ |
|------|-------|-------|
| `greater_than` | Giá trị nhỏ hơn min | `ph = -1` |
| `less_than` | Giá trị lớn hơn max | `ph = 15` |
| `greater_than_equal` | Giá trị nhỏ hơn min | `flow_m3_h = -0.1` |

**Log mẫu:**
```json
{
  "error_type": "range_error",
  "error_detail": "[{'type': 'greater_than', 'loc': ('ph',), 'msg': 'Input should be greater than or equal to 0', 'input': -1}]",
  "device_id": "device-001",
  "group": "water"
}
```

---

## Poison Pill Detection

### Logic

- Nếu 1 device lỗi liên tục ≥ **3 lần** → đẩy sang `dead_letter.jsonl`
- Reset counter khi device gửi message hợp lệ

### Code

```python
RETRY_LIMIT = 3

def process_one(self, topic: str, payload: dict):
    result = validate_event(payload)

    if result.is_valid:
        # Reset streak lỗi của device này
        if result.device_id:
            self._error_streak_by_device[result.device_id] = 0
    else:
        # Tăng streak lỗi
        if result.device_id:
            self._error_streak_by_device[result.device_id] += 1
            if self._error_streak_by_device[result.device_id] >= self.RETRY_LIMIT:
                self._dead_letter_file.write(json.dumps(payload) + "\n")
                self._error_streak_by_device[result.device_id] = 0
```

### Output

**`logs/dead_letter.jsonl`:**
```json
{
  "event_id": "...",
  "device_id": "device-001",
  "group": "water",
  "...": "..."
}
```

---

## Hàm Validate Chính

```python
def validate_event(raw: dict) -> ValidationResult:
    # Bước 1: kiểm tra envelope (field bắt buộc, kiểu dữ liệu)
    try:
        envelope = EventEnvelope(**raw)
    except ValidationError as e:
        return ValidationResult(
            is_valid=False,
            error_type="schema_error",
            error_detail=str(e.errors()[:2]),
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
```

---

## Kết Quả Validate

```python
class ValidationResult(BaseModel):
    is_valid: bool
    error_type: Optional[str] = None   # "schema_error" | "range_error"
    error_detail: Optional[str] = None
    device_id: Optional[str] = None
    group: Optional[str] = None
```

---

## Ví Dụ Validate

### Ví Dụ 1: Message Hợp Lệ

**Input:**
```json
{
  "event_id": "evt-001",
  "schema_version": "1.0",
  "source": "sensor",
  "device_id": "device-001",
  "device_type": "water_sensor",
  "group": "water",
  "location_id": "loc-001",
  "ts": 1693843200000,
  "ts_iso": "2026-09-04T14:00:00Z",
  "local_hour": 14.0,
  "seq": 1,
  "use_case": "monitoring",
  "alerts": [],
  "metrics": {
    "flow_m3_h": 10.5,
    "pressure_bar": 2.5,
    "cumulative_m3": 1000.0,
    "turbidity_ntu": 0.5,
    "residual_chlorine_mg_l": 0.2,
    "ph": 7.0,
    "conductivity_us_cm": 500.0,
    "temperature_c": 25.0,
    "status": "normal"
  },
  "quality": {
    "rssi_dbm": -60.0,
    "snr_db": 15.0,
    "latency_ms": 50.0
  }
}
```

**Output:**
```json
{
  "is_valid": true,
  "device_id": "device-001",
  "group": "water"
}
```

### Ví Dụ 2: Thiếu Field (Schema Error)

**Input:**
```json
{
  "event_id": "evt-002",
  "schema_version": "1.0",
  "source": "sensor",
  "device_id": "device-002",
  "device_type": "water_sensor",
  "group": "water",
  "location_id": "loc-001",
  "ts": 1693843200000,
  "ts_iso": "2026-09-04T14:00:00Z",
  "local_hour": 14.0,
  "seq": 2,
  "use_case": "monitoring",
  "alerts": [],
  "metrics": {
    "flow_m3_h": 10.5,
    "pressure_bar": 2.5,
    "cumulative_m3": 1000.0,
    "turbidity_ntu": 0.5,
    "residual_chlorine_mg_l": 0.2,
    "ph": 7.0,
    "conductivity_us_cm": 500.0,
    "temperature_c": 25.0,
    "status": "normal"
  }
}
```

**Output:**
```json
{
  "is_valid": false,
  "error_type": "schema_error",
  "error_detail": "[{'type': 'missing', 'loc': ('quality',), 'msg': 'Field required'}]",
  "device_id": "device-002",
  "group": "water"
}
```

### Ví Dụ 3: Giá Trị Ngoài Range (Range Error)

**Input:**
```json
{
  "event_id": "evt-003",
  "schema_version": "1.0",
  "source": "sensor",
  "device_id": "device-003",
  "device_type": "water_sensor",
  "group": "water",
  "location_id": "loc-001",
  "ts": 1693843200000,
  "ts_iso": "2026-09-04T14:00:00Z",
  "local_hour": 14.0,
  "seq": 3,
  "use_case": "monitoring",
  "alerts": [],
  "metrics": {
    "flow_m3_h": 10.5,
    "pressure_bar": 2.5,
    "cumulative_m3": 1000.0,
    "turbidity_ntu": 0.5,
    "residual_chlorine_mg_l": 0.2,
    "ph": 15.0,
    "conductivity_us_cm": 500.0,
    "temperature_c": 25.0,
    "status": "normal"
  },
  "quality": {
    "rssi_dbm": -60.0,
    "snr_db": 15.0,
    "latency_ms": 50.0
  }
}
```

**Output:**
```json
{
  "is_valid": false,
  "error_type": "range_error",
  "error_detail": "[{'type': 'less_than_equal', 'loc': ('ph',), 'msg': 'Input should be less than or equal to 14', 'input': 15.0}]",
  "device_id": "device-003",
  "group": "water"
}
```

---

## Lưu Ý

1. **Không raise exception:** `validate_event()` trả về `ValidationResult`, không raise exception ra ngoài
2. **Poison pill:** Device lỗi liên tục ≥3 lần → đẩy sang dead letter
3. **Range vật lý:** Chỉ kiểm tra giá trị "vật lý không thể xảy ra", không phải ngưỡng nghiệp vụ
4. **TODO:** Bổ sung `ElectricityMetrics` và `LightMetrics` khi có sample thật
