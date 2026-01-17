"""
Risk Indicators Module for Crypto BotTrading
Purpose: Produce RAW Risk Indicators from Social Sentiment data
This module ONLY prepares inputs for BotTrading Risk Gate.
BotTrading Core decides ALL trading actions.

This module NEVER decides trading actions.
"""

from typing import Optional
from dataclasses import dataclass


@dataclass
class FinalSentiment:
    label: int  # -1 | 0 | 1
    confidence: float


@dataclass
class Mentions:
    count_1h: int
    velocity: float
    anomaly: bool


@dataclass
class RiskInputData:
    asset: str
    timestamp: str
    sentiment_final: FinalSentiment
    mentions: Mentions
    fear_greed_index: Optional[float]


def validate_input(data: dict) -> tuple[bool, Optional[RiskInputData], Optional[str]]:
    """
    Validate input data structure.
    
    Required fields:
    - asset, timestamp, sentiment.final.label, sentiment.final.confidence MUST exist
    - mentions MUST exist with all fields (count_1h, velocity, anomaly)
    - fear_greed_index MAY be null
    
    Returns: (is_valid, parsed_data, error_note)
    """
    missing_fields = []
    
    # Check top-level required fields
    if "asset" not in data or data["asset"] is None:
        missing_fields.append("asset")
    
    if "timestamp" not in data or data["timestamp"] is None:
        missing_fields.append("timestamp")
    
    # Check sentiment structure
    if "sentiment" not in data or data["sentiment"] is None:
        missing_fields.append("sentiment")
    elif not isinstance(data["sentiment"], dict):
        missing_fields.append("sentiment")
    else:
        if "final" not in data["sentiment"] or data["sentiment"]["final"] is None:
            missing_fields.append("sentiment.final")
        elif not isinstance(data["sentiment"]["final"], dict):
            missing_fields.append("sentiment.final")
        else:
            final = data["sentiment"]["final"]
            if "label" not in final or final["label"] is None:
                missing_fields.append("sentiment.final.label")
            if "confidence" not in final or final["confidence"] is None:
                missing_fields.append("sentiment.final.confidence")
    
    # Check mentions structure
    if "mentions" not in data or data["mentions"] is None:
        missing_fields.append("mentions")
    elif not isinstance(data["mentions"], dict):
        missing_fields.append("mentions")
    else:
        mentions = data["mentions"]
        if "count_1h" not in mentions or mentions["count_1h"] is None:
            missing_fields.append("mentions.count_1h")
        if "velocity" not in mentions or mentions["velocity"] is None:
            missing_fields.append("mentions.velocity")
        if "anomaly" not in mentions or mentions["anomaly"] is None:
            missing_fields.append("mentions.anomaly")
    
    if missing_fields:
        note = f"Missing required fields: {', '.join(missing_fields)}"
        return False, None, note
    
    # Parse validated data
    try:
        parsed = RiskInputData(
            asset=str(data["asset"]),
            timestamp=str(data["timestamp"]),
            sentiment_final=FinalSentiment(
                label=int(data["sentiment"]["final"]["label"]),
                confidence=float(data["sentiment"]["final"]["confidence"])
            ),
            mentions=Mentions(
                count_1h=int(data["mentions"]["count_1h"]),
                velocity=float(data["mentions"]["velocity"]),
                anomaly=bool(data["mentions"]["anomaly"])
            ),
            fear_greed_index=float(data["fear_greed_index"]) if data.get("fear_greed_index") is not None else None
        )
        
        # Validate label is -1, 0, or 1
        if parsed.sentiment_final.label not in [-1, 0, 1]:
            return False, None, f"Invalid sentiment.final.label: {parsed.sentiment_final.label}"
        
        return True, parsed, None
        
    except (ValueError, TypeError) as e:
        return False, None, f"Data type error: {str(e)}"


def compute_social_overheat(mentions: Mentions) -> bool:
    """
    Social Overheat Indicator:
    IF mentions.velocity >= 3.0 AND mentions.anomaly == true
    → social_overheat = true
    ELSE → social_overheat = false
    """
    if mentions.velocity >= 3.0 and mentions.anomaly is True:
        return True
    return False


def compute_panic_risk(sentiment_label: int, velocity: float) -> bool:
    """
    Panic Risk Indicator:
    IF sentiment.final.label == -1 AND mentions.velocity >= 2.0
    → panic_risk = true
    ELSE → panic_risk = false
    """
    if sentiment_label == -1 and velocity >= 2.0:
        return True
    return False


def compute_fomo_risk(sentiment_label: int, fear_greed_index: Optional[float]) -> bool:
    """
    FOMO Risk Indicator:
    IF sentiment.final.label == +1 AND fear_greed_index IS NOT null AND fear_greed_index >= 70
    → fomo_risk = true
    ELSE → fomo_risk = false
    """
    if sentiment_label == 1 and fear_greed_index is not None and fear_greed_index >= 70:
        return True
    return False


def compute_fear_greed_zone(fear_greed_index: Optional[float]) -> str:
    """
    Fear/Greed Zone (DESCRIPTIVE ONLY):
    IF fear_greed_index IS null → zone = "unknown"
    ELSE IF fear_greed_index <= 20 → zone = "extreme_fear"
    ELSE IF fear_greed_index >= 80 → zone = "extreme_greed"
    ELSE → zone = "normal"
    """
    if fear_greed_index is None:
        return "unknown"
    elif fear_greed_index <= 20:
        return "extreme_fear"
    elif fear_greed_index >= 80:
        return "extreme_greed"
    else:
        return "normal"


def compute_sentiment_reliability(confidence: float) -> str:
    """
    Sentiment Reliability Indicator:
    IF sentiment.final.confidence < 0.6
    → sentiment_reliability = "low"
    ELSE → sentiment_reliability = "normal"
    """
    if confidence < 0.6:
        return "low"
    return "normal"


def compute_risk_indicators(data: dict) -> dict:
    """
    Compute risk indicators from sentiment data.
    
    Input: Step 3 API output with additional mentions and fear_greed_index
    Output: Risk indicators for BotTrading Risk Gate
    
    This function ONLY produces descriptive indicators.
    It NEVER makes trading decisions.
    """
    # Validate input
    is_valid, parsed, error_note = validate_input(data)
    
    if not is_valid:
        # Return output with nulls and insufficient data quality
        # Extract asset and timestamp if available for output
        asset = data.get("asset", "UNKNOWN") if isinstance(data, dict) else "UNKNOWN"
        timestamp = data.get("timestamp", "UNKNOWN") if isinstance(data, dict) else "UNKNOWN"
        
        return {
            "asset": asset if asset else "UNKNOWN",
            "timestamp": timestamp if timestamp else "UNKNOWN",
            "risk_indicators": {
                "sentiment_label": None,
                "sentiment_confidence": None,
                "sentiment_reliability": None,
                "fear_greed_index": None,
                "fear_greed_zone": "unknown",
                "social_overheat": None,
                "panic_risk": None,
                "fomo_risk": None
            },
            "data_quality": {
                "status": "insufficient",
                "notes": error_note
            }
        }
    
    # Compute derived risk indicators (FIXED LOGIC - EXECUTE IN ORDER)
    
    # 1. Social Overheat Indicator
    social_overheat = compute_social_overheat(parsed.mentions)
    
    # 2. Panic Risk Indicator
    panic_risk = compute_panic_risk(
        parsed.sentiment_final.label,
        parsed.mentions.velocity
    )
    
    # 3. FOMO Risk Indicator
    fomo_risk = compute_fomo_risk(
        parsed.sentiment_final.label,
        parsed.fear_greed_index
    )
    
    # 4. Fear/Greed Zone (DESCRIPTIVE ONLY)
    fear_greed_zone = compute_fear_greed_zone(parsed.fear_greed_index)
    
    # 5. Sentiment Reliability Indicator
    sentiment_reliability = compute_sentiment_reliability(
        parsed.sentiment_final.confidence
    )
    
    # Build output (STRICT FORMAT - DO NOT CHANGE)
    return {
        "asset": parsed.asset,
        "timestamp": parsed.timestamp,
        "risk_indicators": {
            "sentiment_label": parsed.sentiment_final.label,
            "sentiment_confidence": parsed.sentiment_final.confidence,
            "sentiment_reliability": sentiment_reliability,
            "fear_greed_index": parsed.fear_greed_index,
            "fear_greed_zone": fear_greed_zone,
            "social_overheat": social_overheat,
            "panic_risk": panic_risk,
            "fomo_risk": fomo_risk
        },
        "data_quality": {
            "status": "ok",
            "notes": None
        }
    }


def process_batch(records: list) -> list:
    """
    Process a batch of records.
    Each record is processed independently.
    """
    return [compute_risk_indicators(record) for record in records]


if __name__ == "__main__":
    import json
    
    # Example usage
    sample_input = {
        "asset": "BTC",
        "timestamp": "2026-01-17T10:30:00Z",
        "sentiment": {
            "final": {
                "label": -1,
                "confidence": 0.85
            }
        },
        "mentions": {
            "count_1h": 1500,
            "velocity": 2.5,
            "anomaly": True
        },
        "fear_greed_index": 25
    }
    
    result = compute_risk_indicators(sample_input)
    print(json.dumps(result, indent=2))
