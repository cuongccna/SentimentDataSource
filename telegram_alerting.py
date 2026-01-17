"""
Telegram Alerting Service for Crypto Social Data & BotTrading Infrastructure.

This service sends timely alerts to a Telegram CHANNEL when critical conditions
are detected in: Social sentiment, Risk indicators, Data quality.

ABSOLUTE RULES:
- NO mocking data
- NO hallucination
- NO trading signals
- NO buy/sell suggestions
- Alerts are INFORMATIONAL ONLY
- BotTrading logic is NOT affected by alerts

This service NEVER triggers trades.
This service NEVER blocks trades.
This service is advisory for humans only.
"""

import os
import time
import logging
import threading
import hashlib
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Callable
from enum import Enum
import urllib.request
import urllib.parse
import urllib.error
import json

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ"
)
logger = logging.getLogger("telegram_alerting")


# =============================================================================
# CONSTANTS
# =============================================================================

# Rate limiting: MAX 1 alert per 10 minutes for same (alert_type + asset + source)
RATE_LIMIT_SECONDS = 600  # 10 minutes

# Retry configuration
MAX_RETRY_ATTEMPTS = 3
INITIAL_RETRY_DELAY = 1.0  # seconds
RETRY_BACKOFF_MULTIPLIER = 2.0

# Telegram API base URL
TELEGRAM_API_BASE = "https://api.telegram.org/bot"


# =============================================================================
# ENUMS
# =============================================================================

class AlertType(str, Enum):
    """Alert types as defined in the locked contract."""
    SOCIAL_OVERHEAT = "SOCIAL_OVERHEAT"
    PANIC_RISK = "PANIC_RISK"
    FOMO_RISK = "FOMO_RISK"
    EXTREME_MARKET_EMOTION = "EXTREME_MARKET_EMOTION"
    DATA_QUALITY_DEGRADED = "DATA_QUALITY_DEGRADED"
    DATA_QUALITY_CRITICAL = "DATA_QUALITY_CRITICAL"
    SOURCE_DELAY = "SOURCE_DELAY"
    SOURCE_DOWN = "SOURCE_DOWN"


class FearGreedZone(str, Enum):
    EXTREME_FEAR = "extreme_fear"
    EXTREME_GREED = "extreme_greed"
    NORMAL = "normal"
    UNKNOWN = "unknown"


class OverallQuality(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"


class WorkerStatus(str, Enum):
    OK = "ok"
    DELAYED = "delayed"
    DOWN = "down"


# =============================================================================
# ALERT RECORD
# =============================================================================

@dataclass
class AlertRecord:
    """Record of a sent alert for deduplication."""
    alert_type: AlertType
    asset: str
    source: Optional[str]
    timestamp: datetime
    key: str  # Deduplication key


@dataclass
class AlertPayload:
    """Payload for an alert to be sent."""
    alert_type: AlertType
    asset: str
    timestamp: datetime
    details: Dict[str, Any]
    source: Optional[str] = None
    
    def get_dedup_key(self) -> str:
        """Generate deduplication key from (alert_type + asset + source)."""
        source_part = self.source if self.source else "none"
        content = f"{self.alert_type.value}|{self.asset}|{source_part}"
        return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


# =============================================================================
# RATE LIMITER
# =============================================================================

class AlertRateLimiter:
    """
    Rate limiter for alerts.
    
    Same alert type for same asset: MAX 1 alert per 10 minutes.
    Suppress repeated alerts unless state changes.
    """
    
    def __init__(self, rate_limit_seconds: int = RATE_LIMIT_SECONDS):
        self.rate_limit_seconds = rate_limit_seconds
        self._last_alerts: Dict[str, datetime] = {}
        self._lock = threading.Lock()
    
    def can_send(self, payload: AlertPayload) -> bool:
        """Check if alert can be sent based on rate limiting."""
        key = payload.get_dedup_key()
        now = datetime.now(timezone.utc)
        
        with self._lock:
            last_sent = self._last_alerts.get(key)
            
            if last_sent is None:
                return True
            
            elapsed = (now - last_sent).total_seconds()
            return elapsed >= self.rate_limit_seconds
    
    def record_sent(self, payload: AlertPayload):
        """Record that an alert was sent."""
        key = payload.get_dedup_key()
        now = datetime.now(timezone.utc)
        
        with self._lock:
            self._last_alerts[key] = now
    
    def get_time_until_allowed(self, payload: AlertPayload) -> float:
        """Get seconds until this alert type can be sent again."""
        key = payload.get_dedup_key()
        now = datetime.now(timezone.utc)
        
        with self._lock:
            last_sent = self._last_alerts.get(key)
            
            if last_sent is None:
                return 0.0
            
            elapsed = (now - last_sent).total_seconds()
            remaining = self.rate_limit_seconds - elapsed
            return max(0.0, remaining)
    
    def clear(self):
        """Clear all rate limit records."""
        with self._lock:
            self._last_alerts.clear()


# =============================================================================
# MESSAGE FORMATTER
# =============================================================================

class AlertMessageFormatter:
    """
    Formats alert messages according to the strict contract.
    
    Plain text only. No markdown tables. No emojis required.
    No trading advice.
    """
    
    @staticmethod
    def format_timestamp(dt: datetime) -> str:
        """Format datetime to ISO-8601 UTC."""
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    def format_alert(self, payload: AlertPayload) -> str:
        """Format alert payload into Telegram message."""
        lines = [
            f"[ALERT] {payload.alert_type.value}",
            f"Asset: {payload.asset}",
            f"Time: {self.format_timestamp(payload.timestamp)}",
            "",
            "Details:"
        ]
        
        # Add source if present
        if payload.source:
            lines.insert(3, f"Source: {payload.source}")
        
        # Add details
        details = self._format_details(payload)
        lines.extend(details)
        
        return "\n".join(lines)
    
    def _format_details(self, payload: AlertPayload) -> List[str]:
        """Format details section based on alert type."""
        details = payload.details
        lines = []
        
        if payload.alert_type == AlertType.SOCIAL_OVERHEAT:
            lines.append("- Social activity spike detected")
            if "velocity" in details:
                lines.append(f"- Velocity: {details['velocity']:.2f}")
            if "sentiment_confidence" in details:
                lines.append(f"- Confidence: {details['sentiment_confidence']:.2f}")
        
        elif payload.alert_type == AlertType.PANIC_RISK:
            lines.append("- Panic indicators elevated")
            if "sentiment_label" in details:
                label = "Bearish" if details["sentiment_label"] == -1 else "Neutral"
                lines.append(f"- Sentiment: {label}")
            if "sentiment_confidence" in details:
                lines.append(f"- Confidence: {details['sentiment_confidence']:.2f}")
        
        elif payload.alert_type == AlertType.FOMO_RISK:
            lines.append("- FOMO indicators elevated")
            if "sentiment_label" in details:
                label = "Bullish" if details["sentiment_label"] == 1 else "Neutral"
                lines.append(f"- Sentiment: {label}")
            if "sentiment_confidence" in details:
                lines.append(f"- Confidence: {details['sentiment_confidence']:.2f}")
        
        elif payload.alert_type == AlertType.EXTREME_MARKET_EMOTION:
            zone = details.get("fear_greed_zone", "unknown")
            if zone == "extreme_fear":
                lines.append("- Market emotion: Extreme Fear")
            elif zone == "extreme_greed":
                lines.append("- Market emotion: Extreme Greed")
            if "fear_greed_index" in details:
                lines.append(f"- Fear/Greed Index: {details['fear_greed_index']}")
        
        elif payload.alert_type == AlertType.DATA_QUALITY_DEGRADED:
            lines.append("- Data quality degraded")
            if "availability" in details:
                lines.append(f"- Availability: {details['availability']}")
            if "time_integrity" in details:
                lines.append(f"- Time Integrity: {details['time_integrity']}")
        
        elif payload.alert_type == AlertType.DATA_QUALITY_CRITICAL:
            lines.append("- Data quality critical")
            if "availability" in details:
                lines.append(f"- Availability: {details['availability']}")
            if "time_integrity" in details:
                lines.append(f"- Time Integrity: {details['time_integrity']}")
        
        elif payload.alert_type == AlertType.SOURCE_DELAY:
            lines.append("- Data source delayed")
            if "lag_seconds" in details:
                lines.append(f"- Lag: {details['lag_seconds']:.1f}s")
            if "source" in details:
                lines.append(f"- Source: {details['source']}")
        
        elif payload.alert_type == AlertType.SOURCE_DOWN:
            lines.append("- Data source down")
            if "source" in details:
                lines.append(f"- Source: {details['source']}")
        
        # Fallback for any unhandled details
        if not lines:
            for key, value in details.items():
                lines.append(f"- {key}: {value}")
        
        return lines


# =============================================================================
# TELEGRAM SENDER
# =============================================================================

class TelegramSender:
    """
    Sends messages to Telegram channel via Bot API.
    
    Uses urllib for HTTP requests (no external dependencies).
    Implements retry with exponential backoff.
    """
    
    def __init__(
        self,
        bot_token: Optional[str] = None,
        channel_id: Optional[str] = None
    ):
        """
        Initialize Telegram sender.
        
        Args:
            bot_token: Telegram bot token (or use TELEGRAM_BOT_TOKEN env var)
            channel_id: Telegram channel ID (or use TELEGRAM_CHANNEL_ID env var)
        """
        self.bot_token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self.channel_id = channel_id or os.environ.get("TELEGRAM_CHANNEL_ID", "")
        self._failed_alerts: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
    
    @property
    def is_configured(self) -> bool:
        """Check if Telegram credentials are configured."""
        return bool(self.bot_token and self.channel_id)
    
    def send_message(self, text: str) -> bool:
        """
        Send message to Telegram channel with retry logic.
        
        Returns True if message was sent successfully.
        Alert send failure MUST NOT block pipeline.
        """
        if not self.is_configured:
            logger.warning("Telegram not configured, skipping alert")
            return False
        
        for attempt in range(MAX_RETRY_ATTEMPTS):
            try:
                success = self._send_request(text)
                if success:
                    return True
                
                # Wait before retry with exponential backoff
                delay = INITIAL_RETRY_DELAY * (RETRY_BACKOFF_MULTIPLIER ** attempt)
                logger.warning(f"Telegram send failed, retrying in {delay:.1f}s (attempt {attempt + 1}/{MAX_RETRY_ATTEMPTS})")
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Telegram send exception: {e}")
                delay = INITIAL_RETRY_DELAY * (RETRY_BACKOFF_MULTIPLIER ** attempt)
                time.sleep(delay)
        
        # Log failed alert for potential retry
        self._log_failed_alert(text)
        return False
    
    def _send_request(self, text: str) -> bool:
        """Send HTTP request to Telegram API."""
        url = f"{TELEGRAM_API_BASE}{self.bot_token}/sendMessage"
        
        data = {
            "chat_id": self.channel_id,
            "text": text,
            "parse_mode": "HTML"  # Optional, allows basic formatting if needed
        }
        
        encoded_data = urllib.parse.urlencode(data).encode("utf-8")
        
        try:
            request = urllib.request.Request(url, data=encoded_data, method="POST")
            request.add_header("Content-Type", "application/x-www-form-urlencoded")
            
            with urllib.request.urlopen(request, timeout=10) as response:
                if response.status == 200:
                    result = json.loads(response.read().decode("utf-8"))
                    return result.get("ok", False)
                return False
                
        except urllib.error.HTTPError as e:
            logger.error(f"Telegram API HTTP error: {e.code} {e.reason}")
            return False
        except urllib.error.URLError as e:
            logger.error(f"Telegram API URL error: {e.reason}")
            return False
        except Exception as e:
            logger.error(f"Telegram API error: {e}")
            return False
    
    def _log_failed_alert(self, text: str):
        """Log failed alert for potential retry."""
        with self._lock:
            self._failed_alerts.append({
                "text": text,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "attempts": MAX_RETRY_ATTEMPTS
            })
            
            # Keep only last 100 failed alerts
            if len(self._failed_alerts) > 100:
                self._failed_alerts = self._failed_alerts[-100:]
        
        logger.error(f"Alert failed after {MAX_RETRY_ATTEMPTS} attempts, logged for retry")
    
    def get_failed_alerts(self) -> List[Dict[str, Any]]:
        """Get list of failed alerts."""
        with self._lock:
            return self._failed_alerts.copy()
    
    def clear_failed_alerts(self):
        """Clear failed alerts log."""
        with self._lock:
            self._failed_alerts.clear()


# =============================================================================
# ALERT TRIGGER EVALUATOR
# =============================================================================

class AlertTriggerEvaluator:
    """
    Evaluates input data against alert trigger rules.
    
    Trigger rules are FIXED - DO NOT CHANGE.
    """
    
    def evaluate(self, data: Dict[str, Any]) -> List[AlertPayload]:
        """
        Evaluate input data and return list of triggered alerts.
        
        Input structure:
        {
            "asset": "BTC",
            "timestamp": "ISO-8601",
            "risk_indicators": {...},
            "data_quality": {...},
            "worker_health": {...}
        }
        """
        alerts = []
        
        asset = data.get("asset", "BTC")
        timestamp = self._parse_timestamp(data.get("timestamp"))
        
        # Evaluate risk indicators
        risk_indicators = data.get("risk_indicators", {})
        alerts.extend(self._evaluate_risk_indicators(asset, timestamp, risk_indicators))
        
        # Evaluate data quality
        data_quality = data.get("data_quality", {})
        alerts.extend(self._evaluate_data_quality(asset, timestamp, data_quality))
        
        # Evaluate worker health
        worker_health = data.get("worker_health", {})
        alerts.extend(self._evaluate_worker_health(asset, timestamp, worker_health))
        
        return alerts
    
    def _parse_timestamp(self, ts: Any) -> datetime:
        """Parse timestamp from various formats."""
        if ts is None:
            return datetime.now(timezone.utc)
        
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                return ts.replace(tzinfo=timezone.utc)
            return ts
        
        if isinstance(ts, str):
            try:
                if ts.endswith("Z"):
                    return datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return datetime.fromisoformat(ts)
            except ValueError:
                pass
        
        return datetime.now(timezone.utc)
    
    def _evaluate_risk_indicators(
        self,
        asset: str,
        timestamp: datetime,
        risk_indicators: Dict[str, Any]
    ) -> List[AlertPayload]:
        """Evaluate risk indicator alert triggers."""
        alerts = []
        
        # 1. SOCIAL OVERHEAT ALERT
        if risk_indicators.get("social_overheat") is True:
            alerts.append(AlertPayload(
                alert_type=AlertType.SOCIAL_OVERHEAT,
                asset=asset,
                timestamp=timestamp,
                details={
                    "velocity": risk_indicators.get("velocity", 0.0),
                    "sentiment_confidence": risk_indicators.get("sentiment_confidence", 0.0)
                }
            ))
        
        # 2. PANIC RISK ALERT
        if risk_indicators.get("panic_risk") is True:
            alerts.append(AlertPayload(
                alert_type=AlertType.PANIC_RISK,
                asset=asset,
                timestamp=timestamp,
                details={
                    "sentiment_label": risk_indicators.get("sentiment_label", 0),
                    "sentiment_confidence": risk_indicators.get("sentiment_confidence", 0.0)
                }
            ))
        
        # 3. FOMO RISK ALERT
        if risk_indicators.get("fomo_risk") is True:
            alerts.append(AlertPayload(
                alert_type=AlertType.FOMO_RISK,
                asset=asset,
                timestamp=timestamp,
                details={
                    "sentiment_label": risk_indicators.get("sentiment_label", 0),
                    "sentiment_confidence": risk_indicators.get("sentiment_confidence", 0.0)
                }
            ))
        
        # 4. EXTREME FEAR / GREED ALERT
        fear_greed_zone = risk_indicators.get("fear_greed_zone", "")
        if fear_greed_zone in ("extreme_fear", "extreme_greed"):
            alerts.append(AlertPayload(
                alert_type=AlertType.EXTREME_MARKET_EMOTION,
                asset=asset,
                timestamp=timestamp,
                details={
                    "fear_greed_zone": fear_greed_zone,
                    "fear_greed_index": risk_indicators.get("fear_greed_index")
                }
            ))
        
        return alerts
    
    def _evaluate_data_quality(
        self,
        asset: str,
        timestamp: datetime,
        data_quality: Dict[str, Any]
    ) -> List[AlertPayload]:
        """Evaluate data quality alert triggers."""
        alerts = []
        overall = data_quality.get("overall", "")
        
        # 5. DATA QUALITY DEGRADED
        if overall == "degraded":
            alerts.append(AlertPayload(
                alert_type=AlertType.DATA_QUALITY_DEGRADED,
                asset=asset,
                timestamp=timestamp,
                details={
                    "overall": overall,
                    "availability": data_quality.get("availability", ""),
                    "time_integrity": data_quality.get("time_integrity", "")
                }
            ))
        
        # 6. DATA QUALITY CRITICAL
        if overall == "critical":
            alerts.append(AlertPayload(
                alert_type=AlertType.DATA_QUALITY_CRITICAL,
                asset=asset,
                timestamp=timestamp,
                details={
                    "overall": overall,
                    "availability": data_quality.get("availability", ""),
                    "time_integrity": data_quality.get("time_integrity", "")
                }
            ))
        
        return alerts
    
    def _evaluate_worker_health(
        self,
        asset: str,
        timestamp: datetime,
        worker_health: Dict[str, Any]
    ) -> List[AlertPayload]:
        """Evaluate worker health alert triggers."""
        alerts = []
        status = worker_health.get("status", "")
        source = worker_health.get("source", "")
        
        # 7. SOURCE DELAY ALERT
        if status == "delayed":
            alerts.append(AlertPayload(
                alert_type=AlertType.SOURCE_DELAY,
                asset=asset,
                timestamp=timestamp,
                source=source,
                details={
                    "source": source,
                    "lag_seconds": worker_health.get("lag_seconds", 0.0),
                    "status": status
                }
            ))
        
        # 8. SOURCE DOWN ALERT
        if status == "down":
            alerts.append(AlertPayload(
                alert_type=AlertType.SOURCE_DOWN,
                asset=asset,
                timestamp=timestamp,
                source=source,
                details={
                    "source": source,
                    "status": status
                }
            ))
        
        return alerts


# =============================================================================
# TELEGRAM ALERTING SERVICE
# =============================================================================

class TelegramAlertingService:
    """
    Main alerting service that coordinates evaluation, formatting, and delivery.
    
    This service is INFORMATIONAL ONLY.
    It NEVER triggers trades.
    It NEVER blocks trades.
    BotTrading logic is NOT affected by alerts.
    """
    
    def __init__(
        self,
        bot_token: Optional[str] = None,
        channel_id: Optional[str] = None,
        rate_limit_seconds: int = RATE_LIMIT_SECONDS
    ):
        self.evaluator = AlertTriggerEvaluator()
        self.formatter = AlertMessageFormatter()
        self.sender = TelegramSender(bot_token, channel_id)
        self.rate_limiter = AlertRateLimiter(rate_limit_seconds)
        
        self._sent_count = 0
        self._suppressed_count = 0
        self._failed_count = 0
        self._lock = threading.Lock()
    
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process input data and send alerts if triggered.
        
        Returns summary of alert processing.
        Alert send failure MUST NOT block pipeline.
        """
        result = {
            "triggered": 0,
            "sent": 0,
            "suppressed": 0,
            "failed": 0,
            "alerts": []
        }
        
        try:
            # Evaluate triggers
            payloads = self.evaluator.evaluate(data)
            result["triggered"] = len(payloads)
            
            for payload in payloads:
                alert_result = self._process_alert(payload)
                result["alerts"].append({
                    "type": payload.alert_type.value,
                    "asset": payload.asset,
                    "status": alert_result
                })
                
                if alert_result == "sent":
                    result["sent"] += 1
                elif alert_result == "suppressed":
                    result["suppressed"] += 1
                elif alert_result == "failed":
                    result["failed"] += 1
                    
        except Exception as e:
            logger.error(f"Alert processing error: {e}")
            # Errors MUST NOT block pipeline
        
        return result
    
    def _process_alert(self, payload: AlertPayload) -> str:
        """
        Process a single alert.
        
        Returns: "sent", "suppressed", or "failed"
        """
        # Check rate limiting
        if not self.rate_limiter.can_send(payload):
            remaining = self.rate_limiter.get_time_until_allowed(payload)
            logger.debug(f"Alert {payload.alert_type.value} suppressed (rate limit, {remaining:.0f}s remaining)")
            with self._lock:
                self._suppressed_count += 1
            return "suppressed"
        
        # Format message
        message = self.formatter.format_alert(payload)
        
        # Send message
        success = self.sender.send_message(message)
        
        if success:
            self.rate_limiter.record_sent(payload)
            with self._lock:
                self._sent_count += 1
            logger.info(f"Alert sent: {payload.alert_type.value} for {payload.asset}")
            return "sent"
        else:
            with self._lock:
                self._failed_count += 1
            logger.error(f"Alert failed: {payload.alert_type.value} for {payload.asset}")
            return "failed"
    
    def get_stats(self) -> Dict[str, int]:
        """Get alerting statistics."""
        with self._lock:
            return {
                "sent": self._sent_count,
                "suppressed": self._suppressed_count,
                "failed": self._failed_count
            }
    
    def reset_stats(self):
        """Reset alerting statistics."""
        with self._lock:
            self._sent_count = 0
            self._suppressed_count = 0
            self._failed_count = 0
    
    @property
    def is_configured(self) -> bool:
        """Check if Telegram is configured."""
        return self.sender.is_configured


# =============================================================================
# FACTORY FUNCTIONS
# =============================================================================

def create_alerting_service(
    bot_token: Optional[str] = None,
    channel_id: Optional[str] = None,
    rate_limit_seconds: int = RATE_LIMIT_SECONDS
) -> TelegramAlertingService:
    """Create a new alerting service instance."""
    return TelegramAlertingService(
        bot_token=bot_token,
        channel_id=channel_id,
        rate_limit_seconds=rate_limit_seconds
    )


def create_test_alerting_service() -> TelegramAlertingService:
    """Create alerting service for testing (no actual Telegram sending)."""
    return TelegramAlertingService(
        bot_token="",  # Empty = disabled
        channel_id="",
        rate_limit_seconds=RATE_LIMIT_SECONDS
    )


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("Telegram Alerting Service Demo")
    print("=" * 70)
    print()
    print("ABSOLUTE RULES:")
    print("  - Alerts are INFORMATIONAL ONLY")
    print("  - This service NEVER triggers trades")
    print("  - This service NEVER blocks trades")
    print("  - BotTrading logic is NOT affected")
    print()
    
    # Create test service (no actual Telegram)
    service = create_test_alerting_service()
    
    print(f"Telegram configured: {service.is_configured}")
    print()
    
    # Sample input data
    sample_data = {
        "asset": "BTC",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "risk_indicators": {
            "social_overheat": True,
            "panic_risk": False,
            "fomo_risk": True,
            "fear_greed_zone": "extreme_greed",
            "sentiment_confidence": 0.85,
            "sentiment_label": 1
        },
        "data_quality": {
            "overall": "healthy",
            "availability": "ok",
            "time_integrity": "ok"
        },
        "worker_health": {
            "source": "twitter",
            "lag_seconds": 5.0,
            "status": "ok"
        }
    }
    
    print("[Sample Input]")
    print(json.dumps(sample_data, indent=2, default=str))
    print()
    
    # Process alerts
    result = service.process(sample_data)
    
    print("[Processing Result]")
    print(f"Triggered: {result['triggered']}")
    print(f"Sent: {result['sent']}")
    print(f"Suppressed: {result['suppressed']}")
    print(f"Failed: {result['failed']}")
    print()
    
    print("[Alert Details]")
    for alert in result["alerts"]:
        print(f"  - {alert['type']}: {alert['status']}")
    
    # Show formatted messages
    print()
    print("[Sample Alert Message Format]")
    print("-" * 40)
    
    formatter = AlertMessageFormatter()
    payload = AlertPayload(
        alert_type=AlertType.PANIC_RISK,
        asset="BTC",
        timestamp=datetime.now(timezone.utc),
        details={
            "sentiment_label": -1,
            "sentiment_confidence": 0.81
        }
    )
    print(formatter.format_alert(payload))
