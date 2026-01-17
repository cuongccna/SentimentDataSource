"""
Unit tests for Telegram Alerting Service.
All tests use assertAlmostEqual where needed for float comparisons.

NO MOCKING. NO HALLUCINATION.
"""

import unittest
import time
from datetime import datetime, timezone, timedelta

from telegram_alerting import (
    # Constants
    RATE_LIMIT_SECONDS,
    MAX_RETRY_ATTEMPTS,
    INITIAL_RETRY_DELAY,
    RETRY_BACKOFF_MULTIPLIER,
    
    # Enums
    AlertType,
    FearGreedZone,
    OverallQuality,
    WorkerStatus,
    
    # Classes
    AlertRecord,
    AlertPayload,
    AlertRateLimiter,
    AlertMessageFormatter,
    TelegramSender,
    AlertTriggerEvaluator,
    TelegramAlertingService,
    
    # Factory functions
    create_alerting_service,
    create_test_alerting_service
)


class TestConstants(unittest.TestCase):
    """Tests for constants."""
    
    def test_rate_limit_seconds(self):
        self.assertEqual(RATE_LIMIT_SECONDS, 600)  # 10 minutes
    
    def test_max_retry_attempts(self):
        self.assertEqual(MAX_RETRY_ATTEMPTS, 3)
    
    def test_initial_retry_delay(self):
        self.assertAlmostEqual(INITIAL_RETRY_DELAY, 1.0, places=5)
    
    def test_retry_backoff_multiplier(self):
        self.assertAlmostEqual(RETRY_BACKOFF_MULTIPLIER, 2.0, places=5)


class TestAlertTypeEnum(unittest.TestCase):
    """Tests for AlertType enum."""
    
    def test_social_overheat(self):
        self.assertEqual(AlertType.SOCIAL_OVERHEAT.value, "SOCIAL_OVERHEAT")
    
    def test_panic_risk(self):
        self.assertEqual(AlertType.PANIC_RISK.value, "PANIC_RISK")
    
    def test_fomo_risk(self):
        self.assertEqual(AlertType.FOMO_RISK.value, "FOMO_RISK")
    
    def test_extreme_market_emotion(self):
        self.assertEqual(AlertType.EXTREME_MARKET_EMOTION.value, "EXTREME_MARKET_EMOTION")
    
    def test_data_quality_degraded(self):
        self.assertEqual(AlertType.DATA_QUALITY_DEGRADED.value, "DATA_QUALITY_DEGRADED")
    
    def test_data_quality_critical(self):
        self.assertEqual(AlertType.DATA_QUALITY_CRITICAL.value, "DATA_QUALITY_CRITICAL")
    
    def test_source_delay(self):
        self.assertEqual(AlertType.SOURCE_DELAY.value, "SOURCE_DELAY")
    
    def test_source_down(self):
        self.assertEqual(AlertType.SOURCE_DOWN.value, "SOURCE_DOWN")


class TestAlertPayload(unittest.TestCase):
    """Tests for AlertPayload dataclass."""
    
    def test_create_payload(self):
        now = datetime.now(timezone.utc)
        payload = AlertPayload(
            alert_type=AlertType.PANIC_RISK,
            asset="BTC",
            timestamp=now,
            details={"sentiment_label": -1}
        )
        self.assertEqual(payload.alert_type, AlertType.PANIC_RISK)
        self.assertEqual(payload.asset, "BTC")
    
    def test_dedup_key_generation(self):
        now = datetime.now(timezone.utc)
        payload = AlertPayload(
            alert_type=AlertType.PANIC_RISK,
            asset="BTC",
            timestamp=now,
            details={}
        )
        key = payload.get_dedup_key()
        self.assertEqual(len(key), 16)
    
    def test_same_type_asset_same_key(self):
        now = datetime.now(timezone.utc)
        payload1 = AlertPayload(
            alert_type=AlertType.PANIC_RISK,
            asset="BTC",
            timestamp=now,
            details={"a": 1}
        )
        payload2 = AlertPayload(
            alert_type=AlertType.PANIC_RISK,
            asset="BTC",
            timestamp=now + timedelta(minutes=1),
            details={"b": 2}
        )
        self.assertEqual(payload1.get_dedup_key(), payload2.get_dedup_key())
    
    def test_different_type_different_key(self):
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
        self.assertNotEqual(payload1.get_dedup_key(), payload2.get_dedup_key())
    
    def test_different_asset_different_key(self):
        now = datetime.now(timezone.utc)
        payload1 = AlertPayload(
            alert_type=AlertType.PANIC_RISK,
            asset="BTC",
            timestamp=now,
            details={}
        )
        payload2 = AlertPayload(
            alert_type=AlertType.PANIC_RISK,
            asset="ETH",
            timestamp=now,
            details={}
        )
        self.assertNotEqual(payload1.get_dedup_key(), payload2.get_dedup_key())
    
    def test_source_affects_key(self):
        now = datetime.now(timezone.utc)
        payload1 = AlertPayload(
            alert_type=AlertType.SOURCE_DELAY,
            asset="BTC",
            timestamp=now,
            details={},
            source="twitter"
        )
        payload2 = AlertPayload(
            alert_type=AlertType.SOURCE_DELAY,
            asset="BTC",
            timestamp=now,
            details={},
            source="reddit"
        )
        self.assertNotEqual(payload1.get_dedup_key(), payload2.get_dedup_key())


class TestAlertRateLimiter(unittest.TestCase):
    """Tests for AlertRateLimiter."""
    
    def setUp(self):
        self.limiter = AlertRateLimiter(rate_limit_seconds=60)
        self.now = datetime.now(timezone.utc)
    
    def tearDown(self):
        self.limiter.clear()
    
    def test_first_alert_allowed(self):
        payload = AlertPayload(
            alert_type=AlertType.PANIC_RISK,
            asset="BTC",
            timestamp=self.now,
            details={}
        )
        self.assertTrue(self.limiter.can_send(payload))
    
    def test_duplicate_blocked(self):
        payload = AlertPayload(
            alert_type=AlertType.PANIC_RISK,
            asset="BTC",
            timestamp=self.now,
            details={}
        )
        
        self.assertTrue(self.limiter.can_send(payload))
        self.limiter.record_sent(payload)
        self.assertFalse(self.limiter.can_send(payload))
    
    def test_different_type_not_blocked(self):
        payload1 = AlertPayload(
            alert_type=AlertType.PANIC_RISK,
            asset="BTC",
            timestamp=self.now,
            details={}
        )
        payload2 = AlertPayload(
            alert_type=AlertType.FOMO_RISK,
            asset="BTC",
            timestamp=self.now,
            details={}
        )
        
        self.limiter.record_sent(payload1)
        self.assertTrue(self.limiter.can_send(payload2))
    
    def test_time_until_allowed(self):
        payload = AlertPayload(
            alert_type=AlertType.PANIC_RISK,
            asset="BTC",
            timestamp=self.now,
            details={}
        )
        
        self.assertEqual(self.limiter.get_time_until_allowed(payload), 0.0)
        self.limiter.record_sent(payload)
        remaining = self.limiter.get_time_until_allowed(payload)
        self.assertGreater(remaining, 0)
        self.assertLessEqual(remaining, 60)
    
    def test_clear(self):
        payload = AlertPayload(
            alert_type=AlertType.PANIC_RISK,
            asset="BTC",
            timestamp=self.now,
            details={}
        )
        
        self.limiter.record_sent(payload)
        self.assertFalse(self.limiter.can_send(payload))
        
        self.limiter.clear()
        self.assertTrue(self.limiter.can_send(payload))


class TestAlertMessageFormatter(unittest.TestCase):
    """Tests for AlertMessageFormatter."""
    
    def setUp(self):
        self.formatter = AlertMessageFormatter()
        self.now = datetime(2026, 1, 17, 10, 21, 5, tzinfo=timezone.utc)
    
    def test_format_timestamp(self):
        result = self.formatter.format_timestamp(self.now)
        self.assertEqual(result, "2026-01-17T10:21:05Z")
    
    def test_format_panic_risk_alert(self):
        payload = AlertPayload(
            alert_type=AlertType.PANIC_RISK,
            asset="BTC",
            timestamp=self.now,
            details={
                "sentiment_label": -1,
                "sentiment_confidence": 0.81
            }
        )
        
        message = self.formatter.format_alert(payload)
        
        self.assertIn("[ALERT] PANIC_RISK", message)
        self.assertIn("Asset: BTC", message)
        self.assertIn("2026-01-17T10:21:05Z", message)
        self.assertIn("Details:", message)
        self.assertIn("Sentiment: Bearish", message)
    
    def test_format_fomo_risk_alert(self):
        payload = AlertPayload(
            alert_type=AlertType.FOMO_RISK,
            asset="BTC",
            timestamp=self.now,
            details={
                "sentiment_label": 1,
                "sentiment_confidence": 0.75
            }
        )
        
        message = self.formatter.format_alert(payload)
        
        self.assertIn("[ALERT] FOMO_RISK", message)
        self.assertIn("Sentiment: Bullish", message)
    
    def test_format_social_overheat_alert(self):
        payload = AlertPayload(
            alert_type=AlertType.SOCIAL_OVERHEAT,
            asset="BTC",
            timestamp=self.now,
            details={
                "velocity": 15.5,
                "sentiment_confidence": 0.85
            }
        )
        
        message = self.formatter.format_alert(payload)
        
        self.assertIn("[ALERT] SOCIAL_OVERHEAT", message)
        self.assertIn("Social activity spike", message)
        self.assertIn("Velocity: 15.50", message)
    
    def test_format_extreme_fear_alert(self):
        payload = AlertPayload(
            alert_type=AlertType.EXTREME_MARKET_EMOTION,
            asset="BTC",
            timestamp=self.now,
            details={
                "fear_greed_zone": "extreme_fear",
                "fear_greed_index": 15
            }
        )
        
        message = self.formatter.format_alert(payload)
        
        self.assertIn("[ALERT] EXTREME_MARKET_EMOTION", message)
        self.assertIn("Extreme Fear", message)
    
    def test_format_extreme_greed_alert(self):
        payload = AlertPayload(
            alert_type=AlertType.EXTREME_MARKET_EMOTION,
            asset="BTC",
            timestamp=self.now,
            details={
                "fear_greed_zone": "extreme_greed",
                "fear_greed_index": 85
            }
        )
        
        message = self.formatter.format_alert(payload)
        
        self.assertIn("Extreme Greed", message)
    
    def test_format_data_quality_degraded(self):
        payload = AlertPayload(
            alert_type=AlertType.DATA_QUALITY_DEGRADED,
            asset="BTC",
            timestamp=self.now,
            details={
                "availability": "degraded",
                "time_integrity": "unstable"
            }
        )
        
        message = self.formatter.format_alert(payload)
        
        self.assertIn("[ALERT] DATA_QUALITY_DEGRADED", message)
        self.assertIn("Data quality degraded", message)
    
    def test_format_data_quality_critical(self):
        payload = AlertPayload(
            alert_type=AlertType.DATA_QUALITY_CRITICAL,
            asset="BTC",
            timestamp=self.now,
            details={
                "availability": "down",
                "time_integrity": "critical"
            }
        )
        
        message = self.formatter.format_alert(payload)
        
        self.assertIn("[ALERT] DATA_QUALITY_CRITICAL", message)
        self.assertIn("Data quality critical", message)
    
    def test_format_source_delay_alert(self):
        payload = AlertPayload(
            alert_type=AlertType.SOURCE_DELAY,
            asset="BTC",
            timestamp=self.now,
            source="twitter",
            details={
                "source": "twitter",
                "lag_seconds": 120.5
            }
        )
        
        message = self.formatter.format_alert(payload)
        
        self.assertIn("[ALERT] SOURCE_DELAY", message)
        self.assertIn("Source: twitter", message)
        self.assertIn("Lag: 120.5s", message)
    
    def test_format_source_down_alert(self):
        payload = AlertPayload(
            alert_type=AlertType.SOURCE_DOWN,
            asset="BTC",
            timestamp=self.now,
            source="reddit",
            details={
                "source": "reddit"
            }
        )
        
        message = self.formatter.format_alert(payload)
        
        self.assertIn("[ALERT] SOURCE_DOWN", message)
        self.assertIn("Data source down", message)
    
    def test_message_is_plain_text(self):
        """Verify message format is plain text, no markdown tables."""
        payload = AlertPayload(
            alert_type=AlertType.PANIC_RISK,
            asset="BTC",
            timestamp=self.now,
            details={"sentiment_label": -1}
        )
        
        message = self.formatter.format_alert(payload)
        
        # Should not contain markdown table syntax
        self.assertNotIn("|", message)
        self.assertNotIn("---", message)


class TestTelegramSender(unittest.TestCase):
    """Tests for TelegramSender."""
    
    def test_not_configured_without_credentials(self):
        sender = TelegramSender(bot_token="", channel_id="")
        self.assertFalse(sender.is_configured)
    
    def test_configured_with_credentials(self):
        sender = TelegramSender(
            bot_token="test_token",
            channel_id="@test_channel"
        )
        self.assertTrue(sender.is_configured)
    
    def test_send_without_config_returns_false(self):
        sender = TelegramSender(bot_token="", channel_id="")
        result = sender.send_message("test message")
        self.assertFalse(result)
    
    def test_failed_alerts_logging(self):
        sender = TelegramSender(bot_token="", channel_id="")
        self.assertEqual(len(sender.get_failed_alerts()), 0)
        
        sender.clear_failed_alerts()
        self.assertEqual(len(sender.get_failed_alerts()), 0)


class TestAlertTriggerEvaluator(unittest.TestCase):
    """Tests for AlertTriggerEvaluator."""
    
    def setUp(self):
        self.evaluator = AlertTriggerEvaluator()
    
    def test_no_triggers_on_empty_data(self):
        data = {
            "asset": "BTC",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "risk_indicators": {},
            "data_quality": {},
            "worker_health": {}
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(len(alerts), 0)
    
    def test_social_overheat_trigger(self):
        data = {
            "asset": "BTC",
            "risk_indicators": {
                "social_overheat": True
            }
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, AlertType.SOCIAL_OVERHEAT)
    
    def test_panic_risk_trigger(self):
        data = {
            "asset": "BTC",
            "risk_indicators": {
                "panic_risk": True
            }
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, AlertType.PANIC_RISK)
    
    def test_fomo_risk_trigger(self):
        data = {
            "asset": "BTC",
            "risk_indicators": {
                "fomo_risk": True
            }
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, AlertType.FOMO_RISK)
    
    def test_extreme_fear_trigger(self):
        data = {
            "asset": "BTC",
            "risk_indicators": {
                "fear_greed_zone": "extreme_fear"
            }
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, AlertType.EXTREME_MARKET_EMOTION)
    
    def test_extreme_greed_trigger(self):
        data = {
            "asset": "BTC",
            "risk_indicators": {
                "fear_greed_zone": "extreme_greed"
            }
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, AlertType.EXTREME_MARKET_EMOTION)
    
    def test_normal_zone_no_trigger(self):
        data = {
            "asset": "BTC",
            "risk_indicators": {
                "fear_greed_zone": "normal"
            }
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(len(alerts), 0)
    
    def test_data_quality_degraded_trigger(self):
        data = {
            "asset": "BTC",
            "data_quality": {
                "overall": "degraded"
            }
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, AlertType.DATA_QUALITY_DEGRADED)
    
    def test_data_quality_critical_trigger(self):
        data = {
            "asset": "BTC",
            "data_quality": {
                "overall": "critical"
            }
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, AlertType.DATA_QUALITY_CRITICAL)
    
    def test_data_quality_healthy_no_trigger(self):
        data = {
            "asset": "BTC",
            "data_quality": {
                "overall": "healthy"
            }
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(len(alerts), 0)
    
    def test_source_delay_trigger(self):
        data = {
            "asset": "BTC",
            "worker_health": {
                "source": "twitter",
                "status": "delayed",
                "lag_seconds": 120
            }
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, AlertType.SOURCE_DELAY)
        self.assertEqual(alerts[0].source, "twitter")
    
    def test_source_down_trigger(self):
        data = {
            "asset": "BTC",
            "worker_health": {
                "source": "reddit",
                "status": "down"
            }
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, AlertType.SOURCE_DOWN)
    
    def test_source_ok_no_trigger(self):
        data = {
            "asset": "BTC",
            "worker_health": {
                "source": "telegram",
                "status": "ok"
            }
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(len(alerts), 0)
    
    def test_multiple_triggers(self):
        data = {
            "asset": "BTC",
            "risk_indicators": {
                "social_overheat": True,
                "panic_risk": True,
                "fear_greed_zone": "extreme_fear"
            },
            "data_quality": {
                "overall": "critical"
            }
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(len(alerts), 4)
        
        alert_types = {a.alert_type for a in alerts}
        self.assertIn(AlertType.SOCIAL_OVERHEAT, alert_types)
        self.assertIn(AlertType.PANIC_RISK, alert_types)
        self.assertIn(AlertType.EXTREME_MARKET_EMOTION, alert_types)
        self.assertIn(AlertType.DATA_QUALITY_CRITICAL, alert_types)
    
    def test_default_asset(self):
        data = {
            "risk_indicators": {
                "panic_risk": True
            }
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(alerts[0].asset, "BTC")
    
    def test_timestamp_parsing_iso(self):
        data = {
            "asset": "BTC",
            "timestamp": "2026-01-17T10:00:00Z",
            "risk_indicators": {
                "panic_risk": True
            }
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(alerts[0].timestamp.year, 2026)
    
    def test_false_values_no_trigger(self):
        data = {
            "asset": "BTC",
            "risk_indicators": {
                "social_overheat": False,
                "panic_risk": False,
                "fomo_risk": False
            }
        }
        
        alerts = self.evaluator.evaluate(data)
        self.assertEqual(len(alerts), 0)


class TestTelegramAlertingService(unittest.TestCase):
    """Tests for TelegramAlertingService."""
    
    def setUp(self):
        self.service = create_test_alerting_service()
    
    def test_service_not_configured(self):
        self.assertFalse(self.service.is_configured)
    
    def test_process_returns_result(self):
        data = {
            "asset": "BTC",
            "risk_indicators": {
                "panic_risk": True
            }
        }
        
        result = self.service.process(data)
        
        self.assertIn("triggered", result)
        self.assertIn("sent", result)
        self.assertIn("suppressed", result)
        self.assertIn("failed", result)
        self.assertIn("alerts", result)
    
    def test_triggered_count(self):
        data = {
            "asset": "BTC",
            "risk_indicators": {
                "panic_risk": True,
                "fomo_risk": True
            }
        }
        
        result = self.service.process(data)
        self.assertEqual(result["triggered"], 2)
    
    def test_no_triggers_empty_result(self):
        data = {
            "asset": "BTC",
            "risk_indicators": {},
            "data_quality": {},
            "worker_health": {}
        }
        
        result = self.service.process(data)
        self.assertEqual(result["triggered"], 0)
        self.assertEqual(len(result["alerts"]), 0)
    
    def test_rate_limiting_suppresses(self):
        """
        Test rate limiting suppresses duplicate alerts.
        Note: Rate limiting only applies to successfully sent alerts.
        Failed alerts don't trigger rate limiting (users never saw them).
        """
        # Create service with mock-like sender that always succeeds
        service = TelegramAlertingService(
            bot_token="test_token",
            channel_id="@test_channel",
            rate_limit_seconds=600
        )
        
        # Manually record a successful send to test rate limiting
        now = datetime.now(timezone.utc)
        payload = AlertPayload(
            alert_type=AlertType.PANIC_RISK,
            asset="BTC",
            timestamp=now,
            details={}
        )
        
        # Simulate a successful previous send
        service.rate_limiter.record_sent(payload)
        
        # Now process should be suppressed
        data = {
            "asset": "BTC",
            "risk_indicators": {
                "panic_risk": True
            }
        }
        
        result = service.process(data)
        
        self.assertEqual(result["triggered"], 1)
        self.assertEqual(result["suppressed"], 1)
    
    def test_get_stats(self):
        stats = self.service.get_stats()
        self.assertIn("sent", stats)
        self.assertIn("suppressed", stats)
        self.assertIn("failed", stats)
    
    def test_reset_stats(self):
        data = {
            "asset": "BTC",
            "risk_indicators": {
                "panic_risk": True
            }
        }
        
        self.service.process(data)
        self.service.reset_stats()
        
        stats = self.service.get_stats()
        self.assertEqual(stats["sent"], 0)
        self.assertEqual(stats["suppressed"], 0)
        self.assertEqual(stats["failed"], 0)


class TestFactoryFunctions(unittest.TestCase):
    """Tests for factory functions."""
    
    def test_create_alerting_service(self):
        service = create_alerting_service()
        self.assertIsInstance(service, TelegramAlertingService)
    
    def test_create_test_alerting_service(self):
        service = create_test_alerting_service()
        self.assertIsInstance(service, TelegramAlertingService)
        self.assertFalse(service.is_configured)
    
    def test_custom_rate_limit(self):
        service = create_alerting_service(rate_limit_seconds=30)
        self.assertEqual(service.rate_limiter.rate_limit_seconds, 30)


class TestAlertContract(unittest.TestCase):
    """Tests to verify alert contract compliance."""
    
    def test_alert_is_informational(self):
        """Alerts should not contain trading advice."""
        formatter = AlertMessageFormatter()
        now = datetime.now(timezone.utc)
        
        for alert_type in AlertType:
            payload = AlertPayload(
                alert_type=alert_type,
                asset="BTC",
                timestamp=now,
                details={}
            )
            
            message = formatter.format_alert(payload)
            
            # Should not contain trading signals
            self.assertNotIn("buy", message.lower())
            self.assertNotIn("sell", message.lower())
            self.assertNotIn("trade", message.lower())
    
    def test_eight_alert_types(self):
        """Contract specifies exactly 8 alert types."""
        self.assertEqual(len(AlertType), 8)
    
    def test_all_trigger_rules_implemented(self):
        """Verify all 8 trigger rules are implemented."""
        evaluator = AlertTriggerEvaluator()
        
        # Test data for each trigger
        test_cases = [
            ({"risk_indicators": {"social_overheat": True}}, AlertType.SOCIAL_OVERHEAT),
            ({"risk_indicators": {"panic_risk": True}}, AlertType.PANIC_RISK),
            ({"risk_indicators": {"fomo_risk": True}}, AlertType.FOMO_RISK),
            ({"risk_indicators": {"fear_greed_zone": "extreme_fear"}}, AlertType.EXTREME_MARKET_EMOTION),
            ({"data_quality": {"overall": "degraded"}}, AlertType.DATA_QUALITY_DEGRADED),
            ({"data_quality": {"overall": "critical"}}, AlertType.DATA_QUALITY_CRITICAL),
            ({"worker_health": {"status": "delayed", "source": "twitter"}}, AlertType.SOURCE_DELAY),
            ({"worker_health": {"status": "down", "source": "reddit"}}, AlertType.SOURCE_DOWN),
        ]
        
        for data, expected_type in test_cases:
            data["asset"] = "BTC"
            alerts = evaluator.evaluate(data)
            self.assertEqual(len(alerts), 1, f"Failed for {expected_type}")
            self.assertEqual(alerts[0].alert_type, expected_type)


if __name__ == "__main__":
    unittest.main()
