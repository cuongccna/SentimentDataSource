"""
Demo script for Telegram Alerting Service.

This demonstrates:
1. All 8 alert types and their trigger rules
2. Rate limiting behavior
3. Message formatting
4. Service integration

INFORMATIONAL ONLY - Alerts never trigger trades.
"""

from datetime import datetime, timezone

from telegram_alerting import (
    AlertType,
    AlertPayload,
    AlertRateLimiter,
    AlertMessageFormatter,
    AlertTriggerEvaluator,
    TelegramAlertingService,
    create_test_alerting_service
)


def demo_alert_types():
    """Demonstrate all 8 alert types."""
    print("=" * 60)
    print("DEMO: 8 Alert Types")
    print("=" * 60)
    
    for alert_type in AlertType:
        print(f"  - {alert_type.value}")
    
    print(f"\nTotal: {len(AlertType)} alert types")
    print()


def demo_message_formatting():
    """Demonstrate message formatting for each alert type."""
    print("=" * 60)
    print("DEMO: Message Formatting")
    print("=" * 60)
    
    formatter = AlertMessageFormatter()
    now = datetime.now(timezone.utc)
    
    # Sample payloads for each type
    payloads = [
        AlertPayload(
            alert_type=AlertType.PANIC_RISK,
            asset="BTC",
            timestamp=now,
            details={"sentiment_label": -1, "sentiment_confidence": 0.85}
        ),
        AlertPayload(
            alert_type=AlertType.FOMO_RISK,
            asset="ETH",
            timestamp=now,
            details={"sentiment_label": 1, "sentiment_confidence": 0.78}
        ),
        AlertPayload(
            alert_type=AlertType.SOCIAL_OVERHEAT,
            asset="BTC",
            timestamp=now,
            details={"velocity": 12.5, "sentiment_confidence": 0.82}
        ),
        AlertPayload(
            alert_type=AlertType.EXTREME_MARKET_EMOTION,
            asset="BTC",
            timestamp=now,
            details={"fear_greed_zone": "extreme_fear", "fear_greed_index": 10}
        ),
        AlertPayload(
            alert_type=AlertType.DATA_QUALITY_DEGRADED,
            asset="BTC",
            timestamp=now,
            details={"overall": "degraded", "availability": "degraded"}
        ),
        AlertPayload(
            alert_type=AlertType.SOURCE_DELAY,
            asset="BTC",
            timestamp=now,
            source="twitter",
            details={"source": "twitter", "lag_seconds": 95.5}
        )
    ]
    
    for payload in payloads:
        message = formatter.format_alert(payload)
        print(f"\n--- {payload.alert_type.value} ---")
        print(message)


def demo_rate_limiting():
    """Demonstrate rate limiting behavior."""
    print("\n" + "=" * 60)
    print("DEMO: Rate Limiting")
    print("=" * 60)
    
    # Short rate limit for demo
    limiter = AlertRateLimiter(rate_limit_seconds=60)
    now = datetime.now(timezone.utc)
    
    payload1 = AlertPayload(
        alert_type=AlertType.PANIC_RISK,
        asset="BTC",
        timestamp=now,
        details={}
    )
    
    payload2 = AlertPayload(
        alert_type=AlertType.FOMO_RISK,
        asset="BTC",
        timestamp=now,
        details={}
    )
    
    print(f"\nFirst PANIC_RISK alert: can_send = {limiter.can_send(payload1)}")
    limiter.record_sent(payload1)
    print(f"After sending PANIC_RISK: can_send = {limiter.can_send(payload1)}")
    print(f"Time until allowed: {limiter.get_time_until_allowed(payload1):.0f}s")
    
    print(f"\nDifferent type (FOMO_RISK): can_send = {limiter.can_send(payload2)}")
    print("  (Different alert types are not blocked)")


def demo_trigger_evaluation():
    """Demonstrate trigger evaluation."""
    print("\n" + "=" * 60)
    print("DEMO: Trigger Evaluation")
    print("=" * 60)
    
    evaluator = AlertTriggerEvaluator()
    
    # Scenario 1: Normal market
    print("\n--- Scenario 1: Normal Market ---")
    data1 = {
        "asset": "BTC",
        "risk_indicators": {
            "social_overheat": False,
            "panic_risk": False,
            "fomo_risk": False,
            "fear_greed_zone": "normal"
        },
        "data_quality": {"overall": "healthy"},
        "worker_health": {"status": "ok", "source": "twitter"}
    }
    alerts1 = evaluator.evaluate(data1)
    print(f"Alerts triggered: {len(alerts1)}")
    
    # Scenario 2: Panic conditions
    print("\n--- Scenario 2: Panic Conditions ---")
    data2 = {
        "asset": "BTC",
        "risk_indicators": {
            "panic_risk": True,
            "fear_greed_zone": "extreme_fear"
        }
    }
    alerts2 = evaluator.evaluate(data2)
    print(f"Alerts triggered: {len(alerts2)}")
    for alert in alerts2:
        print(f"  - {alert.alert_type.value}")
    
    # Scenario 3: Multiple issues
    print("\n--- Scenario 3: Multiple Issues ---")
    data3 = {
        "asset": "BTC",
        "risk_indicators": {
            "social_overheat": True,
            "fomo_risk": True
        },
        "data_quality": {"overall": "critical"},
        "worker_health": {"status": "down", "source": "reddit"}
    }
    alerts3 = evaluator.evaluate(data3)
    print(f"Alerts triggered: {len(alerts3)}")
    for alert in alerts3:
        print(f"  - {alert.alert_type.value} (asset: {alert.asset})")


def demo_service_processing():
    """Demonstrate full service processing."""
    print("\n" + "=" * 60)
    print("DEMO: Service Processing")
    print("=" * 60)
    
    service = create_test_alerting_service()
    print(f"\nService configured: {service.is_configured}")
    print("  (Test service has no real Telegram credentials)")
    
    # Process some data
    data = {
        "asset": "ETH",
        "risk_indicators": {
            "panic_risk": True,
            "social_overheat": True
        },
        "data_quality": {"overall": "degraded"}
    }
    
    print("\n--- Processing alert data ---")
    result = service.process(data)
    
    print(f"Triggered: {result['triggered']}")
    print(f"Sent: {result['sent']}")
    print(f"Suppressed: {result['suppressed']}")
    print(f"Failed: {result['failed']} (expected: Telegram not configured)")
    
    print("\nAlert details:")
    for alert in result["alerts"]:
        print(f"  - {alert['type']}: {alert['status']}")
    
    # Show stats
    stats = service.get_stats()
    print(f"\nService stats: {stats}")


def demo_dedup_keys():
    """Demonstrate deduplication key generation."""
    print("\n" + "=" * 60)
    print("DEMO: Deduplication Keys")
    print("=" * 60)
    
    now = datetime.now(timezone.utc)
    
    p1 = AlertPayload(AlertType.PANIC_RISK, "BTC", now, {})
    p2 = AlertPayload(AlertType.PANIC_RISK, "BTC", now, {"extra": "data"})
    p3 = AlertPayload(AlertType.PANIC_RISK, "ETH", now, {})
    p4 = AlertPayload(AlertType.FOMO_RISK, "BTC", now, {})
    p5 = AlertPayload(AlertType.SOURCE_DELAY, "BTC", now, {}, source="twitter")
    p6 = AlertPayload(AlertType.SOURCE_DELAY, "BTC", now, {}, source="reddit")
    
    print(f"\nPANIC_RISK BTC (v1):     {p1.get_dedup_key()}")
    print(f"PANIC_RISK BTC (v2):     {p2.get_dedup_key()}")
    print(f"  Same key? {p1.get_dedup_key() == p2.get_dedup_key()}")
    
    print(f"\nPANIC_RISK ETH:          {p3.get_dedup_key()}")
    print(f"  Different from BTC? {p1.get_dedup_key() != p3.get_dedup_key()}")
    
    print(f"\nFOMO_RISK BTC:           {p4.get_dedup_key()}")
    print(f"  Different type? {p1.get_dedup_key() != p4.get_dedup_key()}")
    
    print(f"\nSOURCE_DELAY twitter:    {p5.get_dedup_key()}")
    print(f"SOURCE_DELAY reddit:     {p6.get_dedup_key()}")
    print(f"  Source affects key? {p5.get_dedup_key() != p6.get_dedup_key()}")


def main():
    print("\n" + "#" * 60)
    print("# TELEGRAM ALERTING SERVICE - DEMO")
    print("# Crypto Social Data Pipeline")
    print("#" * 60)
    print("\nThis service is INFORMATIONAL ONLY.")
    print("It NEVER triggers or blocks trades.")
    print("BotTrading Core owns all trading decisions.")
    
    demo_alert_types()
    demo_message_formatting()
    demo_rate_limiting()
    demo_trigger_evaluation()
    demo_service_processing()
    demo_dedup_keys()
    
    print("\n" + "=" * 60)
    print("DEMO COMPLETE")
    print("=" * 60)
    print("\nTo use in production:")
    print("  1. Set TELEGRAM_BOT_TOKEN environment variable")
    print("  2. Set TELEGRAM_CHANNEL_ID environment variable")
    print("  3. Call create_alerting_service() to get configured service")
    print("\n")


if __name__ == "__main__":
    main()
