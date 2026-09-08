# Validate

Mục đích: Kiểm tra mỗi message MQTT có đúng schema và logic business hay không.

## Kiến trúc

```
queue (từ ingestion) → validate() → valid_events (queue) / invalid_events (file)
                                           ↓
                                    detect() → violations (queue) → alert()
```

## 3 Schema Types

Pipeline hỗ trợ 3 loại schema telemetry, mỗi loại đại diện cho một nhóm thiết bị:

### 1. WaterTelemetry

```json
{
  "timestamp": "2025-01-15T10:30:00.000Z",
  "tsunix": 1736955000,
  "dev_id": "WATER_001",
  "Qt": 45.2,
  "V": 120.5
}
```

**Business Logic:**
- `Qt` (Tổng thể tích): Phải > 0
- `V` (Voltage): 100-300V
- Khi `Qt` tăng → Pump ON → `P_yellow` > 600W (nếu có sensor điện)

### 2. LightingTelemetry

```json
{
  "timestamp": "2025-01-15T10:30:00.000Z",
  "tsunix": 1736955000,
  "dev_id": "LIGHT_001",
  "U": 220.5,
  "I": 105.2,
  "Power_kW": 0.023,
  "Energy_kwh": 15.7,
  "Lux": 450.5,
  "Contactor": 1
}
```

**Business Logic:**
- `U` (Voltage): 200-250V
- `I` (Current): 50-150mA
- `Power_kW`: 0-0.05 kW (đèn LED)
- `Contactor`: 0 hoặc 1 (trạng thái đóng/ngắt)

### 3. ElectricityTelemetry (3-Phase)

```json
{
  "timestamp": "2025-01-15T10:30:00.000Z",
  "tsunix": 1736955000,
  "dev_id": "ELECTRIC_001",
  "Uab": 398.5,
  "Ubc": 401.2,
  "Uca": 397.8,
  "Ia": 120.5,
  "Ib": 118.7,
  "Ic": 122.1,
  "P_Total": 85.2,
  "F": 49.98,
  "PFavg": 0.95,
  "THD_V": 2.5,
  "THD_I": 3.2
}
```

**Business Logic:**
- Điện áp: `Uab`, `Ubc`, `Uca` ∈ [380-420]V
- Cường độ: `Ia`, `Ib`, `Ic` ∈ [0-500]A
- Công suất: `P_Total` > 0
- Tần số: `F` ∈ [49.5-50.5] Hz
- Hiệu suất: `PFavg` ∈ [0-1]
- THD: < 8%

### Detect Type

`validate_event(payload)` tự detect schema từ `dev_id`:
- Chứa `WATER` → WaterTelemetry
- Chứa `LIGHT` → LightingTelemetry
- Chứa `ELECTRIC` hoặc mặc định → ElectricityTelemetry

## Quy tắc Validate

### 1. Schema Validation (Pydantic)

Mỗi schema có các trường bắt buộc và kiểu dữ liệu. Nếu thiếu trường hoặc sai kiểu → **FAIL**.

### 2. Poison Pill

**Logic:**
- Theo dõi mỗi `dev_id`
- Nếu 3 tin nhắn liên tiếp fail → đánh dấu `is_poison=True`
- Khi `is_poison=True`: redirect message sang `logs/dead_letter.jsonl`, bỏ qua device 5 phút
- Sau 5 phút: reset counter, cho device gửi lại

**Cấu trúc dead_letter.jsonl:**
```json
{
  "dev_id": "ELECTRIC_001",
  "tsunix": 1736955000,
  "error": "Field 'Uab' value '500.0' >= 450",
  "payload": {...}
}
```

### 3. Invalid Events

Message fail validation nhưng chưa đủ 3 lần → ghi vào `logs/invalid_events.jsonl`.

## Output

### Valid Messages
Đi tiếp sang `detect()` để check threshold.

### Invalid Events
```bash
# Số lượng invalid events
wc -l logs/invalid_events.jsonl

# Xem nội dung
cat logs/invalid_events.jsonl | python3 -m json.tool
```

### Dead Letter
```bash
# Số lượng poison pill
wc -l logs/dead_letter.jsonl

# Xem nội dung
cat logs/dead_letter.jsonl | python3 -m json.tool
```

## Thống kê

Log `validate.log` hiển thị:
- Tổng messages đã validate
- Số valid/invalid
- Số poison pills
- Số dead letters

```bash
cat logs/validate.log | tail -20
```
