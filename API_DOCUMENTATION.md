# 📖 API Documentation for BotTrading Integration

## 🚀 PM2 Commands

### Cài đặt PM2 (nếu chưa có)
```bash
npm install -g pm2
```

### Khởi động tất cả services
```bash
cd D:\projects\SentimentData\SentimentDataSource
pm2 start ecosystem.config.js
```

### Quản lý services
```bash
pm2 list                    # Xem danh sách services
pm2 logs                    # Xem logs realtime
pm2 logs sentiment-worker   # Xem logs của worker
pm2 monit                   # Dashboard monitoring

pm2 stop all               # Dừng tất cả
pm2 restart all            # Restart tất cả
pm2 delete all             # Xóa tất cả

pm2 save                   # Lưu cấu hình
pm2 startup                # Tự khởi động khi Windows boot
```

---

## 📡 API Endpoints

### 1️⃣ Sentiment Analysis API (Flask - Port 5000)

**Purpose:** Phân tích sentiment từ tin nhắn raw - CHỈ DÙNG CHO RISK CONTROL

#### Endpoint
```
POST http://localhost:5000/api/v1/sentiment/analyze
Content-Type: application/json
```

#### Request
```json
{
  "records": [
    {
      "id": "msg_001",
      "asset": "BTC",
      "source": "twitter",
      "text": "Bitcoin is breaking ATH! Very bullish!",
      "timestamp": "2026-01-17T10:30:00Z"
    },
    {
      "id": "msg_002",
      "asset": "BTC",
      "source": "reddit",
      "text": "Market crash incoming, time to sell",
      "timestamp": "2026-01-17T10:31:00Z"
    }
  ]
}
```

#### Response (200 OK)
```json
{
  "meta": {
    "asset": "BTC",
    "record_received": 2,
    "record_processed": 2,
    "record_dropped": 0,
    "timestamp": "2026-01-17T10:32:00Z"
  },
  "results": [
    {
      "id": "msg_001",
      "asset": "BTC",
      "sentiment": {
        "label": 1,
        "confidence": 0.85
      }
    },
    {
      "id": "msg_002",
      "asset": "BTC",
      "sentiment": {
        "label": -1,
        "confidence": 0.78
      }
    }
  ]
}
```

#### Response (422 - Fallback khi không có dữ liệu hợp lệ)
```json
{
  "meta": {
    "asset": "BTC",
    "record_received": 2,
    "record_processed": 0,
    "record_dropped": 2,
    "timestamp": "2026-01-17T10:32:00Z"
  },
  "results": [],
  "risk_flag": {
    "sentiment_unavailable": true,
    "action": "BLOCK_TRADING"
  }
}
```

#### HTTP Status Codes
| Code | Meaning |
|------|---------|
| 200 | Ít nhất 1 record được xử lý thành công |
| 400 | Request structure không hợp lệ |
| 422 | Tất cả records bị drop HOẶC mảng rỗng |
| 500 | Internal error |

---

### 2️⃣ Social Context API (FastAPI - Port 8000)

**Purpose:** Cung cấp Social Context tổng hợp theo time window

#### Endpoint
```
POST http://localhost:8000/api/v1/social/context
Content-Type: application/json
```

#### API Docs (Swagger UI)
```
http://localhost:8000/docs
```

#### Request
```json
{
  "asset": "BTC",
  "since": "2026-01-17T10:25:00Z",
  "until": "2026-01-17T10:30:00Z",
  "sources": ["twitter", "reddit", "telegram"]
}
```

**Constraints:**
- Time window: 30-300 giây (30s đến 5 phút)
- `since` phải trước `until`
- Timestamp phải chính xác đến giây (không có microseconds)
- Sources: `twitter`, `reddit`, `telegram`

#### Response (200 OK)
```json
{
  "meta": {
    "asset": "BTC",
    "window": {
      "since": "2026-01-17T10:25:00Z",
      "until": "2026-01-17T10:30:00Z"
    },
    "generated_at": "2026-01-17T10:30:15Z"
  },
  "social_context": {
    "sentiment": {
      "label": 1,
      "confidence": 0.82
    },
    "risk_indicators": {
      "sentiment_reliability": "normal",
      "fear_greed_index": 65,
      "fear_greed_zone": "normal",
      "social_overheat": false,
      "panic_risk": false,
      "fomo_risk": false
    },
    "data_quality": {
      "overall": "healthy",
      "availability": "ok",
      "time_integrity": "ok",
      "volume": "normal",
      "source_balance": "normal",
      "anomaly_frequency": "normal"
    }
  }
}
```

#### Sentiment Label Values
| Value | Meaning |
|-------|---------|
| -1 | BEARISH |
| 0 | NEUTRAL |
| 1 | BULLISH |

#### Risk Indicators Explanation
| Field | Values | Description |
|-------|--------|-------------|
| sentiment_reliability | low, normal | Độ tin cậy sentiment |
| fear_greed_zone | extreme_fear, extreme_greed, normal, unknown | Vùng fear/greed |
| social_overheat | true/false | Thị trường quá nóng |
| panic_risk | true/false | Rủi ro panic sell |
| fomo_risk | true/false | Rủi ro FOMO buy |

#### Data Quality Levels
| Level | Action |
|-------|--------|
| healthy | ✅ Tiếp tục trade bình thường |
| degraded | ⚠️ Giảm position size |
| critical | 🚫 Dừng trade, chờ data recovery |

#### HTTP Status Codes
| Code | Meaning |
|------|---------|
| 200 | Social context trả về thành công |
| 204 | Không có dữ liệu trong time window |
| 400 | Request schema hoặc time window không hợp lệ |
| 422 | Có data nhưng không đủ để tổng hợp |
| 500 | Internal error |

---

## 🔌 BotTrading Integration Example

### Python Client
```python
import requests
from datetime import datetime, timezone, timedelta

class SentimentClient:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
    
    def get_social_context(self, asset="BTC", window_seconds=60):
        """Lấy social context cho 1 phút gần nhất"""
        now = datetime.now(timezone.utc)
        since = now - timedelta(seconds=window_seconds)
        
        payload = {
            "asset": asset,
            "since": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "until": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sources": ["twitter", "reddit", "telegram"]
        }
        
        response = requests.post(
            f"{self.base_url}/api/v1/social/context",
            json=payload
        )
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 204:
            return None  # No data
        else:
            raise Exception(f"API Error: {response.status_code}")
    
    def should_block_trading(self, context):
        """Kiểm tra xem có nên block trading không"""
        if context is None:
            return True, "No social data available"
        
        dq = context["social_context"]["data_quality"]
        ri = context["social_context"]["risk_indicators"]
        
        # Block nếu data quality critical
        if dq["overall"] == "critical":
            return True, "Data quality is critical"
        
        # Block nếu có panic risk
        if ri["panic_risk"]:
            return True, "Panic risk detected"
        
        # Block nếu extreme fear
        if ri["fear_greed_zone"] == "extreme_fear":
            return True, "Extreme fear zone"
        
        return False, "Trading allowed"


# Usage
client = SentimentClient()
context = client.get_social_context("BTC", window_seconds=60)

if context:
    should_block, reason = client.should_block_trading(context)
    if should_block:
        print(f"🚫 BLOCK TRADING: {reason}")
    else:
        sentiment = context["social_context"]["sentiment"]
        print(f"✅ Trading allowed - Sentiment: {sentiment['label']} ({sentiment['confidence']:.0%})")
```

### cURL Examples

**Sentiment Analysis:**
```bash
curl -X POST http://localhost:5000/api/v1/sentiment/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "records": [
      {"id": "1", "asset": "BTC", "source": "twitter", "text": "Bitcoin to the moon!"}
    ]
  }'
```

**Social Context:**
```bash
curl -X POST http://localhost:8000/api/v1/social/context \
  -H "Content-Type: application/json" \
  -d '{
    "asset": "BTC",
    "since": "2026-01-17T10:00:00Z",
    "until": "2026-01-17T10:01:00Z",
    "sources": ["twitter", "reddit", "telegram"]
  }'
```

---

## ⚠️ Important Rules

1. **RISK CONTROL ONLY** - Sentiment data chỉ dùng để kiểm soát rủi ro, KHÔNG dùng để tạo trading signals
2. **NO MOCKING** - Không bao giờ mock data
3. **NO HALLUCINATION** - Không tạo dữ liệu giả
4. **FAIL-SAFE** - Khi không có data hoặc quality kém → BLOCK TRADING

---

## 📊 Source Reliability

| Source | Reliability Weight |
|--------|-------------------|
| Twitter | 0.5 |
| Reddit | 0.7 |
| Telegram | 0.3 |

Weighted sentiment được tính dựa trên độ tin cậy của từng nguồn.

---

## 🏥 Health Checks

```bash
# Flask API
curl http://localhost:5000/health

# FastAPI  
curl http://localhost:8000/health
```

Response:
```json
{
  "status": "healthy"
}
```
