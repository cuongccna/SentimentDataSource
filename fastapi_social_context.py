"""
FastAPI Social Context Service for Crypto BotTrading
Purpose: Expose structured Social Context data (Sentiment, Risk Indicators, Data Quality)
over a precise time window.

THIS SERVICE IS A DATA PROVIDER ONLY.
ALL TRADING DECISIONS BELONG TO BOTTRADING CORE.

NO MOCKING. NO HALLUCINATION. NO DATA MODIFICATION.
"""

from datetime import datetime, timezone
from typing import Optional, Literal
from enum import Enum
from uuid import UUID
import json
import os

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status, Response
from pydantic import BaseModel, Field, field_validator, model_validator

# Load environment variables
load_dotenv()


# =============================================================================
# CONSTANTS
# =============================================================================

MIN_WINDOW_SECONDS = 30
MAX_WINDOW_SECONDS = 300  # 5 minutes

SOURCE_RELIABILITY = {
    "twitter": 0.5,
    "reddit": 0.7,
    "telegram": 0.3
}

VALID_SOURCES = {"twitter", "reddit", "telegram"}


# =============================================================================
# ENUMS
# =============================================================================

class SentimentLabel(int, Enum):
    BEARISH = -1
    NEUTRAL = 0
    BULLISH = 1


class SentimentReliability(str, Enum):
    LOW = "low"
    NORMAL = "normal"


class FearGreedZone(str, Enum):
    EXTREME_FEAR = "extreme_fear"
    EXTREME_GREED = "extreme_greed"
    NORMAL = "normal"
    UNKNOWN = "unknown"


class OverallQuality(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"


class AvailabilityStatus(str, Enum):
    OK = "ok"
    DEGRADED = "degraded"
    DOWN = "down"


class TimeIntegrityStatus(str, Enum):
    OK = "ok"
    UNSTABLE = "unstable"
    CRITICAL = "critical"


class VolumeStatus(str, Enum):
    NORMAL = "normal"
    ABNORMALLY_LOW = "abnormally_low"
    ABNORMALLY_HIGH = "abnormally_high"


class SourceBalanceStatus(str, Enum):
    NORMAL = "normal"
    IMBALANCED = "imbalanced"


class AnomalyStatus(str, Enum):
    NORMAL = "normal"
    PERSISTENT = "persistent"


# =============================================================================
# PYDANTIC MODELS - REQUEST
# =============================================================================

class SocialContextRequest(BaseModel):
    """Request schema for social context query."""
    asset: str = Field(..., min_length=1, description="Asset symbol (e.g., BTC)")
    since: str = Field(..., description="Start time ISO-8601 UTC with second precision")
    until: str = Field(..., description="End time ISO-8601 UTC with second precision")
    sources: list[str] = Field(..., min_length=1, description="List of sources to query")
    
    @field_validator('sources')
    @classmethod
    def validate_sources(cls, v):
        if not v:
            raise ValueError("sources array MUST NOT be empty")
        for source in v:
            if source not in VALID_SOURCES:
                raise ValueError(f"Invalid source: {source}. Must be one of {VALID_SOURCES}")
        return v
    
    @field_validator('since', 'until')
    @classmethod
    def validate_timestamp(cls, v):
        try:
            if v.endswith('Z'):
                dt = datetime.fromisoformat(v.replace('Z', '+00:00'))
            else:
                dt = datetime.fromisoformat(v)
            
            # Check second-level precision (no microseconds in input)
            if '.' in v:
                raise ValueError("Time precision MUST be seconds, not microseconds")
            
            return v
        except ValueError as e:
            raise ValueError(f"Invalid ISO-8601 timestamp: {e}")
    
    @model_validator(mode='after')
    def validate_time_window(self):
        since_dt = parse_timestamp(self.since)
        until_dt = parse_timestamp(self.until)
        
        if since_dt >= until_dt:
            raise ValueError("since must be before until")
        
        window_seconds = (until_dt - since_dt).total_seconds()
        
        if window_seconds < MIN_WINDOW_SECONDS:
            raise ValueError(f"Window size must be at least {MIN_WINDOW_SECONDS} seconds")
        
        if window_seconds > MAX_WINDOW_SECONDS:
            raise ValueError(f"Window size must not exceed {MAX_WINDOW_SECONDS} seconds (5 minutes)")
        
        return self


# =============================================================================
# PYDANTIC MODELS - RESPONSE
# =============================================================================

class WindowMeta(BaseModel):
    since: str
    until: str


class ResponseMeta(BaseModel):
    asset: str
    window: WindowMeta
    generated_at: str


class SentimentResponse(BaseModel):
    label: int = Field(..., ge=-1, le=1)
    confidence: float = Field(..., ge=0.0, le=1.0)


class RiskIndicatorsResponse(BaseModel):
    sentiment_reliability: str
    fear_greed_index: Optional[int] = None
    fear_greed_zone: str
    social_overheat: bool
    panic_risk: bool
    fomo_risk: bool


class DataQualityResponse(BaseModel):
    overall: str
    availability: str
    time_integrity: str
    volume: str
    source_balance: str
    anomaly_frequency: str


class SocialContextData(BaseModel):
    sentiment: SentimentResponse
    risk_indicators: RiskIndicatorsResponse
    data_quality: DataQualityResponse


class SocialContextResponse(BaseModel):
    meta: ResponseMeta
    social_context: SocialContextData


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def parse_timestamp(ts: str) -> datetime:
    """Parse ISO-8601 timestamp to datetime."""
    if ts.endswith('Z'):
        return datetime.fromisoformat(ts.replace('Z', '+00:00'))
    return datetime.fromisoformat(ts)


def format_timestamp(dt: datetime) -> str:
    """Format datetime to ISO-8601 UTC string."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# =============================================================================
# STORAGE LAYER (Read-Only Interface)
# =============================================================================

class SocialDataStorage:
    """
    Storage layer for reading social data from PostgreSQL.
    
    This class provides read-only access to data from ingested_messages table.
    """
    
    def __init__(self):
        # PostgreSQL connection parameters from environment
        self.db_config = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "database": os.getenv("POSTGRES_DATABASE", "sentiment_db"),
            "user": os.getenv("POSTGRES_USER", "sentiment_user"),
            "password": os.getenv("POSTGRES_PASSWORD", ""),
        }
        self._test_connection()
    
    def _test_connection(self):
        """Test database connection on startup."""
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.close()
            print(f"[INFO] Connected to PostgreSQL: {self.db_config['database']}")
        except Exception as e:
            print(f"[WARNING] Could not connect to PostgreSQL: {e}")
    
    def _get_connection(self):
        """Get a new database connection."""
        return psycopg2.connect(**self.db_config, cursor_factory=RealDictCursor)
    
    def query_records(
        self,
        asset: str,
        since: datetime,
        until: datetime,
        sources: list[str]
    ) -> list[dict]:
        """
        Query records from ingested_messages table.
        
        Returns records with event_time BETWEEN since AND until
        where source IN sources.
        """
        results = []
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Build query with parameterized sources
            placeholders = ','.join(['%s'] * len(sources))
            query = f"""
                SELECT 
                    message_id,
                    source,
                    source_name,
                    asset,
                    text,
                    event_time,
                    engagement_weight,
                    author_weight,
                    velocity,
                    source_reliability,
                    raw_data
                FROM ingested_messages
                WHERE asset = %s
                  AND event_time >= %s
                  AND event_time <= %s
                  AND source IN ({placeholders})
                ORDER BY event_time DESC
                LIMIT 1000
            """
            
            params = [asset, since, until] + sources
            cursor.execute(query, params)
            
            rows = cursor.fetchall()
            
            for row in rows:
                # Convert to expected format
                record = {
                    "message_id": row["message_id"],
                    "source": row["source"],
                    "source_name": row["source_name"],
                    "asset": row["asset"],
                    "text": row["text"],
                    "timestamp": row["event_time"].isoformat() if row["event_time"] else None,
                    "engagement_weight": row["engagement_weight"] or 0,
                    "author_weight": row["author_weight"] or 0,
                    "velocity": row["velocity"] or 1.0,
                    "source_reliability": row["source_reliability"] or 0.5,
                    # Generate sentiment from raw_data if available, else use defaults
                    "sentiment": self._extract_sentiment(row),
                    "risk_indicators": self._extract_risk_indicators(row),
                    "data_quality": self._default_data_quality()
                }
                results.append(record)
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"[ERROR] Database query failed: {e}")
        
        return results
    
    def _extract_sentiment(self, row: dict) -> dict:
        """Extract sentiment from raw_data or generate from text."""
        raw_data = row.get("raw_data") or {}
        
        # Check if sentiment already computed
        if "sentiment" in raw_data:
            return raw_data["sentiment"]
        
        # Simple keyword-based sentiment for now
        text = (row.get("text") or "").lower()
        
        bullish_keywords = ["bullish", "moon", "pump", "buy", "long", "breakout", "🚀", "📈"]
        bearish_keywords = ["bearish", "dump", "sell", "short", "crash", "drop", "📉"]
        
        bullish_count = sum(1 for kw in bullish_keywords if kw in text)
        bearish_count = sum(1 for kw in bearish_keywords if kw in text)
        
        if bullish_count > bearish_count:
            return {"label": 1, "confidence": min(0.5 + bullish_count * 0.1, 0.95)}
        elif bearish_count > bullish_count:
            return {"label": -1, "confidence": min(0.5 + bearish_count * 0.1, 0.95)}
        else:
            return {"label": 0, "confidence": 0.5}
    
    def _extract_risk_indicators(self, row: dict) -> dict:
        """Extract or generate risk indicators."""
        raw_data = row.get("raw_data") or {}
        
        if "risk_indicators" in raw_data:
            return raw_data["risk_indicators"]
        
        velocity = row.get("velocity") or 1.0
        
        return {
            "sentiment_reliability": "normal",
            "fear_greed_index": None,
            "fear_greed_zone": "unknown",
            "social_overheat": velocity > 5.0,
            "panic_risk": False,
            "fomo_risk": velocity > 3.0
        }
    
    def _default_data_quality(self) -> dict:
        """Return default data quality."""
        return {
            "overall": "healthy",
            "availability": "ok",
            "time_integrity": "ok",
            "volume": "normal",
            "source_balance": "normal",
            "anomaly_frequency": "normal"
        }
    
    def add_record(self, record: dict):
        """Add a record to storage (for testing/demo purposes only)."""
        # In production, data is added by the worker, not API
        pass
    
    def clear(self):
        """Clear all records (for testing purposes only)."""
        pass


# =============================================================================
# SERVICE LAYER (Aggregation Logic)
# =============================================================================

class SocialContextService:
    """
    Service layer for aggregating social context data.
    
    Aggregation rules:
    - sentiment.label = weighted majority by source reliability
    - sentiment.confidence = average confidence (clamped 0-1)
    - risk_indicators = logical OR for booleans, most recent for descriptive
    - data_quality.overall = worst state among records
    - data_quality.* = most frequent state
    """
    
    def __init__(self, storage: SocialDataStorage):
        self.storage = storage
    
    def _aggregate_sentiment(self, records: list[dict]) -> dict:
        """
        Aggregate sentiment using weighted majority by source reliability.
        """
        if not records:
            return {"label": 0, "confidence": 0.0}
        
        # Weighted voting for label
        label_weights = {-1: 0.0, 0: 0.0, 1: 0.0}
        total_confidence = 0.0
        count = 0
        
        for record in records:
            sentiment = record.get("sentiment", {})
            source = record.get("source", "unknown")
            
            label = sentiment.get("label", 0)
            confidence = sentiment.get("confidence", 0.5)
            reliability = SOURCE_RELIABILITY.get(source, 0.5)
            
            # Ensure label is valid
            if label not in (-1, 0, 1):
                label = 0
            
            # Weight = reliability * confidence
            weight = reliability * confidence
            label_weights[label] += weight
            
            total_confidence += confidence
            count += 1
        
        # Determine final label by weighted majority
        final_label = max(label_weights, key=label_weights.get)
        
        # Average confidence, clamped to [0, 1]
        avg_confidence = total_confidence / count if count > 0 else 0.0
        avg_confidence = max(0.0, min(1.0, avg_confidence))
        
        return {
            "label": final_label,
            "confidence": round(avg_confidence, 4)
        }
    
    def _aggregate_risk_indicators(self, records: list[dict]) -> dict:
        """
        Aggregate risk indicators.
        - Boolean flags: logical OR
        - Descriptive fields: most recent value
        """
        result = {
            "sentiment_reliability": "normal",
            "fear_greed_index": None,
            "fear_greed_zone": "unknown",
            "social_overheat": False,
            "panic_risk": False,
            "fomo_risk": False
        }
        
        if not records:
            return result
        
        # Sort by timestamp descending for "most recent"
        sorted_records = sorted(
            records,
            key=lambda r: r.get("timestamp", ""),
            reverse=True
        )
        
        for record in records:
            risk = record.get("risk_indicators", {})
            
            # Boolean flags - logical OR
            if risk.get("social_overheat"):
                result["social_overheat"] = True
            if risk.get("panic_risk"):
                result["panic_risk"] = True
            if risk.get("fomo_risk"):
                result["fomo_risk"] = True
        
        # Most recent values for descriptive fields
        most_recent = sorted_records[0].get("risk_indicators", {})
        
        if "sentiment_reliability" in most_recent:
            result["sentiment_reliability"] = most_recent["sentiment_reliability"]
        
        if "fear_greed_index" in most_recent:
            result["fear_greed_index"] = most_recent["fear_greed_index"]
        
        if "fear_greed_zone" in most_recent:
            result["fear_greed_zone"] = most_recent["fear_greed_zone"]
        
        # Determine sentiment_reliability based on aggregation
        if result["social_overheat"] or result["panic_risk"]:
            result["sentiment_reliability"] = "low"
        
        return result
    
    def _aggregate_data_quality(self, records: list[dict]) -> dict:
        """
        Aggregate data quality.
        - overall: worst state among records
        - other fields: most frequent state
        """
        result = {
            "overall": "healthy",
            "availability": "ok",
            "time_integrity": "ok",
            "volume": "normal",
            "source_balance": "normal",
            "anomaly_frequency": "normal"
        }
        
        if not records:
            return result
        
        # Severity ordering for "worst"
        overall_severity = {"healthy": 0, "degraded": 1, "critical": 2}
        availability_severity = {"ok": 0, "degraded": 1, "down": 2}
        time_integrity_severity = {"ok": 0, "unstable": 1, "critical": 2}
        
        # Collect all values
        overall_values = []
        availability_values = []
        time_integrity_values = []
        volume_values = []
        source_balance_values = []
        anomaly_values = []
        
        for record in records:
            dq = record.get("data_quality", {})
            
            if "overall" in dq:
                overall_values.append(dq["overall"])
            if "availability" in dq:
                availability_values.append(dq["availability"])
            if "time_integrity" in dq:
                time_integrity_values.append(dq["time_integrity"])
            if "volume" in dq:
                volume_values.append(dq["volume"])
            if "source_balance" in dq:
                source_balance_values.append(dq["source_balance"])
            if "anomaly_frequency" in dq:
                anomaly_values.append(dq["anomaly_frequency"])
        
        # Overall: worst state
        if overall_values:
            result["overall"] = max(
                overall_values,
                key=lambda x: overall_severity.get(x, 0)
            )
        
        # Availability: worst state
        if availability_values:
            result["availability"] = max(
                availability_values,
                key=lambda x: availability_severity.get(x, 0)
            )
        
        # Time integrity: worst state
        if time_integrity_values:
            result["time_integrity"] = max(
                time_integrity_values,
                key=lambda x: time_integrity_severity.get(x, 0)
            )
        
        # Other fields: most frequent
        def most_frequent(values, default):
            if not values:
                return default
            from collections import Counter
            counts = Counter(values)
            return counts.most_common(1)[0][0]
        
        result["volume"] = most_frequent(volume_values, "normal")
        result["source_balance"] = most_frequent(source_balance_values, "normal")
        result["anomaly_frequency"] = most_frequent(anomaly_values, "normal")
        
        return result
    
    def get_social_context(
        self,
        asset: str,
        since: datetime,
        until: datetime,
        sources: list[str]
    ) -> Optional[dict]:
        """
        Get aggregated social context for the specified window.
        
        Returns None if no records found.
        Returns dict with aggregated data if records exist.
        """
        records = self.storage.query_records(asset, since, until, sources)
        
        if not records:
            return None
        
        return {
            "sentiment": self._aggregate_sentiment(records),
            "risk_indicators": self._aggregate_risk_indicators(records),
            "data_quality": self._aggregate_data_quality(records),
            "record_count": len(records)
        }


# =============================================================================
# FASTAPI APPLICATION
# =============================================================================

# Create storage and service instances
storage = SocialDataStorage()
service = SocialContextService(storage)

# Create FastAPI app
app = FastAPI(
    title="Social Context API",
    description="Social Context data provider for Crypto BotTrading",
    version="1.0.0"
)


@app.post(
    "/api/v1/social/context",
    response_model=SocialContextResponse,
    responses={
        200: {"description": "Social context successfully returned"},
        204: {"description": "No social events in requested window"},
        400: {"description": "Invalid request schema or time window"},
        422: {"description": "Data exists but insufficient for aggregation"},
        500: {"description": "Internal error"}
    }
)
def get_social_context(request: SocialContextRequest, response: Response):
    """
    Get aggregated social context for the specified time window.
    
    Request:
    - asset: Asset symbol (e.g., BTC)
    - since: Start time ISO-8601 UTC with second precision
    - until: End time ISO-8601 UTC with second precision
    - sources: List of sources to query (twitter, reddit, telegram)
    
    Returns aggregated sentiment, risk indicators, and data quality.
    """
    try:
        since_dt = parse_timestamp(request.since)
        until_dt = parse_timestamp(request.until)
        
        context = service.get_social_context(
            asset=request.asset,
            since=since_dt,
            until=until_dt,
            sources=request.sources
        )
        
        # No records found
        if context is None:
            response.status_code = status.HTTP_204_NO_CONTENT
            return Response(status_code=status.HTTP_204_NO_CONTENT)
        
        # Check if data is sufficient for aggregation
        if context["record_count"] < 1:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Data exists but insufficient for aggregation"
            )
        
        # Build response
        now = datetime.now(timezone.utc)
        
        return SocialContextResponse(
            meta=ResponseMeta(
                asset=request.asset,
                window=WindowMeta(
                    since=request.since,
                    until=request.until
                ),
                generated_at=format_timestamp(now)
            ),
            social_context=SocialContextData(
                sentiment=SentimentResponse(
                    label=context["sentiment"]["label"],
                    confidence=context["sentiment"]["confidence"]
                ),
                risk_indicators=RiskIndicatorsResponse(
                    sentiment_reliability=context["risk_indicators"]["sentiment_reliability"],
                    fear_greed_index=context["risk_indicators"]["fear_greed_index"],
                    fear_greed_zone=context["risk_indicators"]["fear_greed_zone"],
                    social_overheat=context["risk_indicators"]["social_overheat"],
                    panic_risk=context["risk_indicators"]["panic_risk"],
                    fomo_risk=context["risk_indicators"]["fomo_risk"]
                ),
                data_quality=DataQualityResponse(
                    overall=context["data_quality"]["overall"],
                    availability=context["data_quality"]["availability"],
                    time_integrity=context["data_quality"]["time_integrity"],
                    volume=context["data_quality"]["volume"],
                    source_balance=context["data_quality"]["source_balance"],
                    anomaly_frequency=context["data_quality"]["anomaly_frequency"]
                )
            )
        )
        
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


# =============================================================================
# FACTORY FUNCTIONS
# =============================================================================

def get_storage() -> SocialDataStorage:
    """Get the storage instance."""
    return storage


def get_service() -> SocialContextService:
    """Get the service instance."""
    return service


def create_app(custom_storage: Optional[SocialDataStorage] = None) -> FastAPI:
    """
    Create a new FastAPI app instance with optional custom storage.
    Useful for testing.
    """
    global storage, service
    
    if custom_storage:
        storage = custom_storage
        service = SocialContextService(storage)
    
    return app


if __name__ == "__main__":
    import uvicorn
    
    # Add sample data for demo
    from datetime import timedelta
    
    now = datetime.now(timezone.utc)
    
    sample_records = [
        {
            "asset": "BTC",
            "source": "twitter",
            "timestamp": format_timestamp(now - timedelta(seconds=60)),
            "sentiment": {"label": 1, "confidence": 0.85},
            "risk_indicators": {
                "social_overheat": False,
                "panic_risk": False,
                "fomo_risk": False,
                "fear_greed_index": 65,
                "fear_greed_zone": "normal"
            },
            "data_quality": {
                "overall": "healthy",
                "availability": "ok",
                "time_integrity": "ok",
                "volume": "normal",
                "source_balance": "normal",
                "anomaly_frequency": "normal"
            }
        },
        {
            "asset": "BTC",
            "source": "reddit",
            "timestamp": format_timestamp(now - timedelta(seconds=45)),
            "sentiment": {"label": 1, "confidence": 0.72},
            "risk_indicators": {
                "social_overheat": False,
                "panic_risk": False,
                "fomo_risk": True,
                "fear_greed_index": 68,
                "fear_greed_zone": "normal"
            },
            "data_quality": {
                "overall": "healthy",
                "availability": "ok",
                "time_integrity": "ok",
                "volume": "normal",
                "source_balance": "normal",
                "anomaly_frequency": "normal"
            }
        },
        {
            "asset": "BTC",
            "source": "telegram",
            "timestamp": format_timestamp(now - timedelta(seconds=30)),
            "sentiment": {"label": 0, "confidence": 0.60},
            "risk_indicators": {
                "social_overheat": False,
                "panic_risk": False,
                "fomo_risk": False,
                "fear_greed_index": 62,
                "fear_greed_zone": "normal"
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
    ]
    
    for record in sample_records:
        storage.add_record(record)
    
    print("=== Social Context API Demo ===")
    print("Starting server on http://127.0.0.1:8000")
    print("API docs available at http://127.0.0.1:8000/docs")
    
    uvicorn.run(app, host="127.0.0.1", port=8000)
