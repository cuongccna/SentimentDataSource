"""
Unit tests for Data Quality Monitor (DQM).
All tests use assertAlmostEqual where needed for float comparisons.

NO MOCKING. NO HALLUCINATION.
"""

import unittest
from datetime import datetime, timezone, timedelta
from data_quality_monitor import (
    DataQualityMonitor,
    AvailabilityMonitor,
    TimeIntegrityMonitor,
    VolumeMonitor,
    SourceBalanceMonitor,
    AnomalyFrequencyMonitor,
    SourceTracker,
    RollingWindow,
    DataQualityReport,
    AvailabilityStatus,
    TimeIntegrityStatus,
    VolumeStatus,
    SourceBalanceStatus,
    AnomalyStatus,
    OverallQuality,
    AVAILABILITY_DEGRADED,
    AVAILABILITY_DOWN,
    VOLUME_LOW_THRESHOLD,
    VOLUME_HIGH_THRESHOLD,
    SOURCE_IMBALANCE_THRESHOLD,
    TIME_UNSTABLE_THRESHOLD,
    TIME_CRITICAL_THRESHOLD,
    create_monitor
)


class TestAvailabilityStatusEnum(unittest.TestCase):
    """Tests for AvailabilityStatus enum."""
    
    def test_ok_value(self):
        self.assertEqual(AvailabilityStatus.OK.value, "ok")
    
    def test_degraded_value(self):
        self.assertEqual(AvailabilityStatus.DEGRADED.value, "degraded")
    
    def test_down_value(self):
        self.assertEqual(AvailabilityStatus.DOWN.value, "down")


class TestTimeIntegrityStatusEnum(unittest.TestCase):
    """Tests for TimeIntegrityStatus enum."""
    
    def test_ok_value(self):
        self.assertEqual(TimeIntegrityStatus.OK.value, "ok")
    
    def test_unstable_value(self):
        self.assertEqual(TimeIntegrityStatus.UNSTABLE.value, "unstable")
    
    def test_critical_value(self):
        self.assertEqual(TimeIntegrityStatus.CRITICAL.value, "critical")


class TestVolumeStatusEnum(unittest.TestCase):
    """Tests for VolumeStatus enum."""
    
    def test_normal_value(self):
        self.assertEqual(VolumeStatus.NORMAL.value, "normal")
    
    def test_abnormally_low_value(self):
        self.assertEqual(VolumeStatus.ABNORMALLY_LOW.value, "abnormally_low")
    
    def test_abnormally_high_value(self):
        self.assertEqual(VolumeStatus.ABNORMALLY_HIGH.value, "abnormally_high")


class TestSourceBalanceStatusEnum(unittest.TestCase):
    """Tests for SourceBalanceStatus enum."""
    
    def test_normal_value(self):
        self.assertEqual(SourceBalanceStatus.NORMAL.value, "normal")
    
    def test_imbalanced_value(self):
        self.assertEqual(SourceBalanceStatus.IMBALANCED.value, "imbalanced")


class TestAnomalyStatusEnum(unittest.TestCase):
    """Tests for AnomalyStatus enum."""
    
    def test_normal_value(self):
        self.assertEqual(AnomalyStatus.NORMAL.value, "normal")
    
    def test_persistent_value(self):
        self.assertEqual(AnomalyStatus.PERSISTENT.value, "persistent")


class TestOverallQualityEnum(unittest.TestCase):
    """Tests for OverallQuality enum."""
    
    def test_healthy_value(self):
        self.assertEqual(OverallQuality.HEALTHY.value, "healthy")
    
    def test_degraded_value(self):
        self.assertEqual(OverallQuality.DEGRADED.value, "degraded")
    
    def test_critical_value(self):
        self.assertEqual(OverallQuality.CRITICAL.value, "critical")


class TestThresholds(unittest.TestCase):
    """Tests for threshold constants."""
    
    def test_twitter_degraded_threshold(self):
        self.assertEqual(AVAILABILITY_DEGRADED["twitter"], 60)
    
    def test_reddit_degraded_threshold(self):
        self.assertEqual(AVAILABILITY_DEGRADED["reddit"], 900)
    
    def test_telegram_degraded_threshold(self):
        self.assertEqual(AVAILABILITY_DEGRADED["telegram"], 120)
    
    def test_twitter_down_threshold(self):
        self.assertEqual(AVAILABILITY_DOWN["twitter"], 300)
    
    def test_reddit_down_threshold(self):
        self.assertEqual(AVAILABILITY_DOWN["reddit"], 3600)
    
    def test_telegram_down_threshold(self):
        self.assertEqual(AVAILABILITY_DOWN["telegram"], 600)
    
    def test_volume_low_threshold(self):
        self.assertAlmostEqual(VOLUME_LOW_THRESHOLD, 0.30, places=5)
    
    def test_volume_high_threshold(self):
        self.assertAlmostEqual(VOLUME_HIGH_THRESHOLD, 3.00, places=5)
    
    def test_source_imbalance_threshold(self):
        self.assertAlmostEqual(SOURCE_IMBALANCE_THRESHOLD, 0.70, places=5)
    
    def test_time_unstable_threshold(self):
        self.assertAlmostEqual(TIME_UNSTABLE_THRESHOLD, 0.05, places=5)
    
    def test_time_critical_threshold(self):
        self.assertAlmostEqual(TIME_CRITICAL_THRESHOLD, 0.15, places=5)


class TestSourceTracker(unittest.TestCase):
    """Tests for SourceTracker dataclass."""
    
    def test_initial_state(self):
        tracker = SourceTracker(source="twitter")
        self.assertEqual(tracker.source, "twitter")
        self.assertIsNone(tracker.last_event_time)
        self.assertEqual(tracker.event_count, 0)
    
    def test_record_event(self):
        tracker = SourceTracker(source="twitter")
        now = datetime.now(timezone.utc)
        tracker.record_event(now)
        self.assertEqual(tracker.last_event_time, now)
        self.assertEqual(tracker.event_count, 1)
    
    def test_multiple_events(self):
        tracker = SourceTracker(source="reddit")
        now = datetime.now(timezone.utc)
        tracker.record_event(now - timedelta(seconds=10))
        tracker.record_event(now)
        self.assertEqual(tracker.event_count, 2)
        self.assertEqual(tracker.last_event_time, now)
    
    def test_seconds_since_last_no_events(self):
        tracker = SourceTracker(source="telegram")
        now = datetime.now(timezone.utc)
        self.assertEqual(tracker.get_seconds_since_last(now), float('inf'))
    
    def test_seconds_since_last_with_event(self):
        tracker = SourceTracker(source="twitter")
        now = datetime.now(timezone.utc)
        tracker.record_event(now - timedelta(seconds=30))
        self.assertAlmostEqual(tracker.get_seconds_since_last(now), 30.0, places=1)


class TestRollingWindow(unittest.TestCase):
    """Tests for RollingWindow dataclass."""
    
    def test_initial_empty(self):
        window = RollingWindow(window_seconds=60)
        self.assertEqual(window.get_count(), 0)
    
    def test_add_event(self):
        window = RollingWindow(window_seconds=60)
        now = datetime.now(timezone.utc)
        window.add_event(now, {"test": True})
        self.assertEqual(window.get_count(now), 1)
    
    def test_prune_old_events(self):
        window = RollingWindow(window_seconds=60)
        now = datetime.now(timezone.utc)
        window.add_event(now - timedelta(seconds=120), {"old": True})
        window.add_event(now, {"new": True})
        self.assertEqual(window.get_count(now), 1)
    
    def test_get_events(self):
        window = RollingWindow(window_seconds=60)
        now = datetime.now(timezone.utc)
        window.add_event(now, {"id": 1})
        window.add_event(now, {"id": 2})
        events = window.get_events(now)
        self.assertEqual(len(events), 2)


class TestAvailabilityMonitor(unittest.TestCase):
    """Tests for AvailabilityMonitor."""
    
    def test_no_events_is_down(self):
        monitor = AvailabilityMonitor()
        now = datetime.now(timezone.utc)
        status = monitor.get_status("twitter", now)
        self.assertEqual(status, AvailabilityStatus.DOWN)
    
    def test_recent_event_is_ok(self):
        monitor = AvailabilityMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_event("twitter", now - timedelta(seconds=10))
        status = monitor.get_status("twitter", now)
        self.assertEqual(status, AvailabilityStatus.OK)
    
    def test_twitter_degraded_after_60s(self):
        monitor = AvailabilityMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_event("twitter", now - timedelta(seconds=65))
        status = monitor.get_status("twitter", now)
        self.assertEqual(status, AvailabilityStatus.DEGRADED)
    
    def test_twitter_down_after_5min(self):
        monitor = AvailabilityMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_event("twitter", now - timedelta(seconds=310))
        status = monitor.get_status("twitter", now)
        self.assertEqual(status, AvailabilityStatus.DOWN)
    
    def test_reddit_degraded_after_15min(self):
        monitor = AvailabilityMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_event("reddit", now - timedelta(seconds=910))
        status = monitor.get_status("reddit", now)
        self.assertEqual(status, AvailabilityStatus.DEGRADED)
    
    def test_reddit_down_after_1hr(self):
        monitor = AvailabilityMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_event("reddit", now - timedelta(seconds=3610))
        status = monitor.get_status("reddit", now)
        self.assertEqual(status, AvailabilityStatus.DOWN)
    
    def test_telegram_degraded_after_120s(self):
        monitor = AvailabilityMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_event("telegram", now - timedelta(seconds=125))
        status = monitor.get_status("telegram", now)
        self.assertEqual(status, AvailabilityStatus.DEGRADED)
    
    def test_telegram_down_after_10min(self):
        monitor = AvailabilityMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_event("telegram", now - timedelta(seconds=610))
        status = monitor.get_status("telegram", now)
        self.assertEqual(status, AvailabilityStatus.DOWN)
    
    def test_get_all_status(self):
        monitor = AvailabilityMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_event("twitter", now)
        monitor.record_event("reddit", now)
        monitor.record_event("telegram", now)
        statuses = monitor.get_all_status(now)
        self.assertEqual(len(statuses), 3)
        self.assertEqual(statuses["twitter"], AvailabilityStatus.OK)
    
    def test_worst_status_down(self):
        monitor = AvailabilityMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_event("twitter", now)
        # reddit and telegram have no events → DOWN
        status = monitor.get_worst_status(now)
        self.assertEqual(status, AvailabilityStatus.DOWN)
    
    def test_worst_status_degraded(self):
        monitor = AvailabilityMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_event("twitter", now - timedelta(seconds=65))
        monitor.record_event("reddit", now)
        monitor.record_event("telegram", now)
        status = monitor.get_worst_status(now)
        self.assertEqual(status, AvailabilityStatus.DEGRADED)
    
    def test_worst_status_ok(self):
        monitor = AvailabilityMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_event("twitter", now)
        monitor.record_event("reddit", now)
        monitor.record_event("telegram", now)
        status = monitor.get_worst_status(now)
        self.assertEqual(status, AvailabilityStatus.OK)


class TestTimeIntegrityMonitor(unittest.TestCase):
    """Tests for TimeIntegrityMonitor."""
    
    def test_no_events_is_ok(self):
        monitor = TimeIntegrityMonitor()
        now = datetime.now(timezone.utc)
        status = monitor.get_status(now)
        self.assertEqual(status, TimeIntegrityStatus.OK)
    
    def test_zero_dropped_is_ok(self):
        monitor = TimeIntegrityMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_batch(now, total=100, dropped_late=0)
        status = monitor.get_status(now)
        self.assertEqual(status, TimeIntegrityStatus.OK)
    
    def test_low_dropped_rate_is_ok(self):
        monitor = TimeIntegrityMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_batch(now, total=100, dropped_late=4)  # 4%
        status = monitor.get_status(now)
        self.assertEqual(status, TimeIntegrityStatus.OK)
    
    def test_above_5_percent_is_unstable(self):
        monitor = TimeIntegrityMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_batch(now, total=100, dropped_late=6)  # 6%
        status = monitor.get_status(now)
        self.assertEqual(status, TimeIntegrityStatus.UNSTABLE)
    
    def test_above_15_percent_is_critical(self):
        monitor = TimeIntegrityMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_batch(now, total=100, dropped_late=16)  # 16%
        status = monitor.get_status(now)
        self.assertEqual(status, TimeIntegrityStatus.CRITICAL)
    
    def test_dropped_rate_calculation(self):
        monitor = TimeIntegrityMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_batch(now, total=100, dropped_late=10)
        rate = monitor.get_dropped_rate(now)
        self.assertAlmostEqual(rate, 0.10, places=5)
    
    def test_record_individual_events(self):
        monitor = TimeIntegrityMonitor()
        now = datetime.now(timezone.utc)
        for i in range(10):
            monitor.record_event(now, was_dropped_late=(i == 0))
        rate = monitor.get_dropped_rate(now)
        self.assertAlmostEqual(rate, 0.10, places=5)


class TestVolumeMonitor(unittest.TestCase):
    """Tests for VolumeMonitor."""
    
    def test_no_baseline_is_normal(self):
        monitor = VolumeMonitor()
        now = datetime.now(timezone.utc)
        status = monitor.get_status(now)
        self.assertEqual(status, VolumeStatus.NORMAL)
    
    def test_set_baseline_rate(self):
        monitor = VolumeMonitor()
        monitor.set_baseline_rate(10.0)
        now = datetime.now(timezone.utc)
        baseline = monitor.get_baseline_volume(now)
        self.assertAlmostEqual(baseline, 10.0, places=5)
    
    def test_below_30_percent_is_low(self):
        monitor = VolumeMonitor()
        monitor.set_baseline_rate(10.0)
        now = datetime.now(timezone.utc)
        # Add 2 events (20% of baseline)
        for i in range(2):
            monitor.record_event(now)
        status = monitor.get_status(now)
        self.assertEqual(status, VolumeStatus.ABNORMALLY_LOW)
    
    def test_normal_volume(self):
        monitor = VolumeMonitor()
        monitor.set_baseline_rate(10.0)
        now = datetime.now(timezone.utc)
        # Add 5 events (50% of baseline)
        for i in range(5):
            monitor.record_event(now)
        status = monitor.get_status(now)
        self.assertEqual(status, VolumeStatus.NORMAL)
    
    def test_above_300_percent_is_high(self):
        monitor = VolumeMonitor()
        monitor.set_baseline_rate(10.0)
        now = datetime.now(timezone.utc)
        # Add 31 events (310% of baseline)
        for i in range(31):
            monitor.record_event(now)
        status = monitor.get_status(now)
        self.assertEqual(status, VolumeStatus.ABNORMALLY_HIGH)
    
    def test_volume_ratio(self):
        monitor = VolumeMonitor()
        monitor.set_baseline_rate(10.0)
        now = datetime.now(timezone.utc)
        for i in range(5):
            monitor.record_event(now)
        ratio = monitor.get_volume_ratio(now)
        self.assertAlmostEqual(ratio, 0.5, places=5)
    
    def test_current_volume_count(self):
        monitor = VolumeMonitor()
        now = datetime.now(timezone.utc)
        for i in range(7):
            monitor.record_event(now)
        count = monitor.get_current_volume(now)
        self.assertEqual(count, 7)


class TestSourceBalanceMonitor(unittest.TestCase):
    """Tests for SourceBalanceMonitor."""
    
    def test_no_events_is_normal(self):
        monitor = SourceBalanceMonitor()
        now = datetime.now(timezone.utc)
        status = monitor.get_status(now)
        self.assertEqual(status, SourceBalanceStatus.NORMAL)
    
    def test_balanced_sources(self):
        monitor = SourceBalanceMonitor()
        now = datetime.now(timezone.utc)
        for source in ["twitter", "reddit", "telegram"]:
            for i in range(10):
                monitor.record_event(source, now)
        status = monitor.get_status(now)
        self.assertEqual(status, SourceBalanceStatus.NORMAL)
    
    def test_single_source_above_70_percent(self):
        monitor = SourceBalanceMonitor()
        now = datetime.now(timezone.utc)
        # Twitter: 8/10 = 80%
        for i in range(8):
            monitor.record_event("twitter", now)
        for i in range(2):
            monitor.record_event("reddit", now)
        status = monitor.get_status(now)
        self.assertEqual(status, SourceBalanceStatus.IMBALANCED)
    
    def test_contribution_ratios(self):
        monitor = SourceBalanceMonitor()
        now = datetime.now(timezone.utc)
        for i in range(6):
            monitor.record_event("twitter", now)
        for i in range(4):
            monitor.record_event("reddit", now)
        ratios = monitor.get_contribution_ratios(now)
        self.assertAlmostEqual(ratios["twitter"], 0.6, places=5)
        self.assertAlmostEqual(ratios["reddit"], 0.4, places=5)
    
    def test_exactly_70_percent_is_normal(self):
        monitor = SourceBalanceMonitor()
        now = datetime.now(timezone.utc)
        # Twitter: 7/10 = 70%
        for i in range(7):
            monitor.record_event("twitter", now)
        for i in range(3):
            monitor.record_event("reddit", now)
        status = monitor.get_status(now)
        self.assertEqual(status, SourceBalanceStatus.NORMAL)


class TestAnomalyFrequencyMonitor(unittest.TestCase):
    """Tests for AnomalyFrequencyMonitor."""
    
    def test_no_events_is_normal(self):
        monitor = AnomalyFrequencyMonitor()
        now = datetime.now(timezone.utc)
        status = monitor.get_status(now)
        self.assertEqual(status, AnomalyStatus.NORMAL)
    
    def test_no_anomalies_is_normal(self):
        monitor = AnomalyFrequencyMonitor()
        now = datetime.now(timezone.utc)
        for i in range(10):
            monitor.record_event(now)
        status = monitor.get_status(now)
        self.assertEqual(status, AnomalyStatus.NORMAL)
    
    def test_low_anomaly_rate_is_normal(self):
        monitor = AnomalyFrequencyMonitor(persistence_threshold=0.5)
        now = datetime.now(timezone.utc)
        for i in range(10):
            monitor.record_event(now, social_overheat=(i < 3))
        status = monitor.get_status(now)
        self.assertEqual(status, AnomalyStatus.NORMAL)
    
    def test_high_anomaly_rate_is_persistent(self):
        monitor = AnomalyFrequencyMonitor(persistence_threshold=0.5)
        now = datetime.now(timezone.utc)
        for i in range(10):
            monitor.record_event(now, social_overheat=(i >= 4))  # 60%
        status = monitor.get_status(now)
        self.assertEqual(status, AnomalyStatus.PERSISTENT)
    
    def test_anomaly_rate_calculation(self):
        monitor = AnomalyFrequencyMonitor()
        now = datetime.now(timezone.utc)
        for i in range(10):
            monitor.record_event(now, panic_risk=(i < 2))
        rate = monitor.get_anomaly_rate(now)
        self.assertAlmostEqual(rate, 0.2, places=5)
    
    def test_multiple_anomaly_types(self):
        monitor = AnomalyFrequencyMonitor()
        now = datetime.now(timezone.utc)
        monitor.record_event(now, social_overheat=True)
        monitor.record_event(now, manipulation_flag=True)
        monitor.record_event(now, panic_risk=True)
        monitor.record_event(now)  # No anomaly
        rate = monitor.get_anomaly_rate(now)
        self.assertAlmostEqual(rate, 0.75, places=5)


class TestDataQualityReport(unittest.TestCase):
    """Tests for DataQualityReport dataclass."""
    
    def test_to_dict_format(self):
        report = DataQualityReport(
            asset="BTC",
            timestamp="2026-01-17T10:00:00Z",
            overall=OverallQuality.HEALTHY,
            availability=AvailabilityStatus.OK,
            time_integrity=TimeIntegrityStatus.OK,
            volume=VolumeStatus.NORMAL,
            source_balance=SourceBalanceStatus.NORMAL,
            anomaly_frequency=AnomalyStatus.NORMAL
        )
        result = report.to_dict()
        
        self.assertEqual(result["asset"], "BTC")
        self.assertEqual(result["timestamp"], "2026-01-17T10:00:00Z")
        self.assertEqual(result["data_quality"]["overall"], "healthy")
        self.assertEqual(result["data_quality"]["availability"], "ok")
        self.assertEqual(result["data_quality"]["time_integrity"], "ok")
        self.assertEqual(result["data_quality"]["volume"], "normal")
        self.assertEqual(result["data_quality"]["source_balance"], "normal")
        self.assertEqual(result["data_quality"]["anomaly_frequency"], "normal")
    
    def test_degraded_report(self):
        report = DataQualityReport(
            asset="BTC",
            timestamp="2026-01-17T10:00:00Z",
            overall=OverallQuality.DEGRADED,
            availability=AvailabilityStatus.DEGRADED,
            time_integrity=TimeIntegrityStatus.UNSTABLE,
            volume=VolumeStatus.ABNORMALLY_LOW,
            source_balance=SourceBalanceStatus.IMBALANCED,
            anomaly_frequency=AnomalyStatus.PERSISTENT
        )
        result = report.to_dict()
        
        self.assertEqual(result["data_quality"]["overall"], "degraded")
        self.assertEqual(result["data_quality"]["availability"], "degraded")


class TestDataQualityMonitorInit(unittest.TestCase):
    """Tests for DataQualityMonitor initialization."""
    
    def test_default_init(self):
        monitor = DataQualityMonitor()
        self.assertEqual(monitor.asset, "BTC")
    
    def test_custom_asset(self):
        monitor = DataQualityMonitor(asset="ETH")
        self.assertEqual(monitor.asset, "ETH")
    
    def test_custom_window(self):
        monitor = DataQualityMonitor(window_seconds=600)
        self.assertEqual(monitor.window_seconds, 600)
    
    def test_monitors_initialized(self):
        monitor = DataQualityMonitor()
        self.assertIsNotNone(monitor.availability_monitor)
        self.assertIsNotNone(monitor.time_integrity_monitor)
        self.assertIsNotNone(monitor.volume_monitor)
        self.assertIsNotNone(monitor.source_balance_monitor)
        self.assertIsNotNone(monitor.anomaly_frequency_monitor)


class TestDataQualityMonitorRecordEvent(unittest.TestCase):
    """Tests for record_event method."""
    
    def test_record_event_updates_availability(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        event = {
            "source": "twitter",
            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sentiment": {},
            "risk_indicators": {}
        }
        monitor.record_event(event)
        status = monitor.availability_monitor.get_status("twitter", now)
        self.assertEqual(status, AvailabilityStatus.OK)
    
    def test_record_event_updates_volume(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        event = {
            "source": "reddit",
            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sentiment": {},
            "risk_indicators": {}
        }
        monitor.record_event(event)
        count = monitor.volume_monitor.get_current_volume(now)
        self.assertEqual(count, 1)
    
    def test_record_event_updates_source_balance(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        for i in range(5):
            monitor.record_event({
                "source": "twitter",
                "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "sentiment": {},
                "risk_indicators": {}
            })
        ratios = monitor.source_balance_monitor.get_contribution_ratios(now)
        self.assertAlmostEqual(ratios["twitter"], 1.0, places=5)
    
    def test_record_event_with_anomalies(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        event = {
            "source": "telegram",
            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sentiment": {},
            "risk_indicators": {"social_overheat": True, "panic_risk": True}
        }
        monitor.record_event(event)
        rate = monitor.anomaly_frequency_monitor.get_anomaly_rate(now)
        self.assertAlmostEqual(rate, 1.0, places=5)
    
    def test_missing_timestamp_skipped(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        event = {
            "source": "twitter",
            "sentiment": {},
            "risk_indicators": {}
        }
        monitor.record_event(event)
        # Should not crash, just skip
        count = monitor.volume_monitor.get_current_volume(now)
        self.assertEqual(count, 0)
    
    def test_invalid_timestamp_skipped(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        event = {
            "source": "twitter",
            "timestamp": "not-a-timestamp",
            "sentiment": {},
            "risk_indicators": {}
        }
        monitor.record_event(event)
        count = monitor.volume_monitor.get_current_volume(now)
        self.assertEqual(count, 0)


class TestDataQualityMonitorReport(unittest.TestCase):
    """Tests for get_report method."""
    
    def test_healthy_report(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        
        # Set baseline for volume check
        monitor.set_volume_baseline(3.0)  # Expect 3 events per window
        
        # Add recent events from all sources
        for source in ["twitter", "reddit", "telegram"]:
            monitor.record_event({
                "source": source,
                "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "sentiment": {},
                "risk_indicators": {}
            })
        
        report = monitor.get_report(now)
        self.assertEqual(report.overall, OverallQuality.HEALTHY)
        self.assertEqual(report.availability, AvailabilityStatus.OK)
    
    def test_critical_when_availability_down(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        # No events from any source → DOWN
        report = monitor.get_report(now)
        self.assertEqual(report.overall, OverallQuality.CRITICAL)
        self.assertEqual(report.availability, AvailabilityStatus.DOWN)
    
    def test_critical_when_time_integrity_critical(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        
        # Add recent events
        for source in ["twitter", "reddit", "telegram"]:
            monitor.record_event({
                "source": source,
                "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "sentiment": {},
                "risk_indicators": {}
            })
        
        # Record critical time sync stats
        monitor.record_time_sync_stats(now, total_events=100, dropped_late_events=20)
        
        report = monitor.get_report(now)
        self.assertEqual(report.time_integrity, TimeIntegrityStatus.CRITICAL)
        self.assertEqual(report.overall, OverallQuality.CRITICAL)
    
    def test_degraded_when_multiple_issues(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        
        # Add events from all sources
        for source in ["twitter", "reddit", "telegram"]:
            monitor.record_event({
                "source": source,
                "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "sentiment": {},
                "risk_indicators": {}
            })
        
        # Add imbalanced source
        for i in range(20):
            monitor.record_event({
                "source": "twitter",
                "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "sentiment": {},
                "risk_indicators": {}
            })
        
        report = monitor.get_report(now)
        self.assertEqual(report.source_balance, SourceBalanceStatus.IMBALANCED)
    
    def test_report_timestamp_format(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        
        for source in ["twitter", "reddit", "telegram"]:
            monitor.record_event({
                "source": source,
                "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "sentiment": {},
                "risk_indicators": {}
            })
        
        report = monitor.get_report(now)
        self.assertIn("T", report.timestamp)
        self.assertTrue(report.timestamp.endswith("Z"))


class TestDataQualityMonitorDetailedStatus(unittest.TestCase):
    """Tests for get_detailed_status method."""
    
    def test_detailed_status_contains_report(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        status = monitor.get_detailed_status(now)
        self.assertIn("report", status)
        self.assertIn("details", status)
    
    def test_detailed_status_availability_details(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        status = monitor.get_detailed_status(now)
        
        self.assertIn("availability", status["details"])
        self.assertIn("twitter", status["details"]["availability"])
        self.assertIn("status", status["details"]["availability"]["twitter"])
    
    def test_detailed_status_time_integrity_details(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        status = monitor.get_detailed_status(now)
        
        self.assertIn("time_integrity", status["details"])
        self.assertIn("dropped_rate", status["details"]["time_integrity"])
    
    def test_detailed_status_volume_details(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        status = monitor.get_detailed_status(now)
        
        self.assertIn("volume", status["details"])
        self.assertIn("current", status["details"]["volume"])
        self.assertIn("baseline", status["details"]["volume"])
        self.assertIn("ratio", status["details"]["volume"])
    
    def test_detailed_status_source_balance_details(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        status = monitor.get_detailed_status(now)
        
        self.assertIn("source_balance", status["details"])
        self.assertIn("ratios", status["details"]["source_balance"])
    
    def test_detailed_status_anomaly_details(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        status = monitor.get_detailed_status(now)
        
        self.assertIn("anomaly_frequency", status["details"])
        self.assertIn("rate", status["details"]["anomaly_frequency"])


class TestOverallQualityAggregation(unittest.TestCase):
    """Tests for overall quality aggregation logic."""
    
    def test_critical_beats_all(self):
        monitor = DataQualityMonitor()
        overall = monitor._compute_overall_quality(
            availability=AvailabilityStatus.OK,
            time_integrity=TimeIntegrityStatus.CRITICAL,
            volume=VolumeStatus.NORMAL,
            source_balance=SourceBalanceStatus.NORMAL,
            anomaly_frequency=AnomalyStatus.NORMAL
        )
        self.assertEqual(overall, OverallQuality.CRITICAL)
    
    def test_down_is_critical(self):
        monitor = DataQualityMonitor()
        overall = monitor._compute_overall_quality(
            availability=AvailabilityStatus.DOWN,
            time_integrity=TimeIntegrityStatus.OK,
            volume=VolumeStatus.NORMAL,
            source_balance=SourceBalanceStatus.NORMAL,
            anomaly_frequency=AnomalyStatus.NORMAL
        )
        self.assertEqual(overall, OverallQuality.CRITICAL)
    
    def test_single_degraded_is_degraded(self):
        monitor = DataQualityMonitor()
        overall = monitor._compute_overall_quality(
            availability=AvailabilityStatus.DEGRADED,
            time_integrity=TimeIntegrityStatus.OK,
            volume=VolumeStatus.NORMAL,
            source_balance=SourceBalanceStatus.NORMAL,
            anomaly_frequency=AnomalyStatus.NORMAL
        )
        self.assertEqual(overall, OverallQuality.DEGRADED)
    
    def test_multiple_degraded_is_degraded(self):
        monitor = DataQualityMonitor()
        overall = monitor._compute_overall_quality(
            availability=AvailabilityStatus.DEGRADED,
            time_integrity=TimeIntegrityStatus.UNSTABLE,
            volume=VolumeStatus.NORMAL,
            source_balance=SourceBalanceStatus.NORMAL,
            anomaly_frequency=AnomalyStatus.NORMAL
        )
        self.assertEqual(overall, OverallQuality.DEGRADED)
    
    def test_all_ok_is_healthy(self):
        monitor = DataQualityMonitor()
        overall = monitor._compute_overall_quality(
            availability=AvailabilityStatus.OK,
            time_integrity=TimeIntegrityStatus.OK,
            volume=VolumeStatus.NORMAL,
            source_balance=SourceBalanceStatus.NORMAL,
            anomaly_frequency=AnomalyStatus.NORMAL
        )
        self.assertEqual(overall, OverallQuality.HEALTHY)
    
    def test_abnormally_low_volume_is_degraded(self):
        monitor = DataQualityMonitor()
        overall = monitor._compute_overall_quality(
            availability=AvailabilityStatus.OK,
            time_integrity=TimeIntegrityStatus.OK,
            volume=VolumeStatus.ABNORMALLY_LOW,
            source_balance=SourceBalanceStatus.NORMAL,
            anomaly_frequency=AnomalyStatus.NORMAL
        )
        self.assertEqual(overall, OverallQuality.DEGRADED)
    
    def test_abnormally_high_volume_is_degraded(self):
        monitor = DataQualityMonitor()
        overall = monitor._compute_overall_quality(
            availability=AvailabilityStatus.OK,
            time_integrity=TimeIntegrityStatus.OK,
            volume=VolumeStatus.ABNORMALLY_HIGH,
            source_balance=SourceBalanceStatus.NORMAL,
            anomaly_frequency=AnomalyStatus.NORMAL
        )
        self.assertEqual(overall, OverallQuality.DEGRADED)
    
    def test_imbalanced_is_degraded(self):
        monitor = DataQualityMonitor()
        overall = monitor._compute_overall_quality(
            availability=AvailabilityStatus.OK,
            time_integrity=TimeIntegrityStatus.OK,
            volume=VolumeStatus.NORMAL,
            source_balance=SourceBalanceStatus.IMBALANCED,
            anomaly_frequency=AnomalyStatus.NORMAL
        )
        self.assertEqual(overall, OverallQuality.DEGRADED)
    
    def test_persistent_anomaly_is_degraded(self):
        monitor = DataQualityMonitor()
        overall = monitor._compute_overall_quality(
            availability=AvailabilityStatus.OK,
            time_integrity=TimeIntegrityStatus.OK,
            volume=VolumeStatus.NORMAL,
            source_balance=SourceBalanceStatus.NORMAL,
            anomaly_frequency=AnomalyStatus.PERSISTENT
        )
        self.assertEqual(overall, OverallQuality.DEGRADED)


class TestFactoryFunction(unittest.TestCase):
    """Tests for create_monitor factory function."""
    
    def test_create_default_monitor(self):
        monitor = create_monitor()
        self.assertIsInstance(monitor, DataQualityMonitor)
        self.assertEqual(monitor.asset, "BTC")
    
    def test_create_with_custom_asset(self):
        monitor = create_monitor(asset="ETH")
        self.assertEqual(monitor.asset, "ETH")
    
    def test_create_with_custom_window(self):
        monitor = create_monitor(window_seconds=600)
        self.assertEqual(monitor.window_seconds, 600)


class TestTimestampParsing(unittest.TestCase):
    """Tests for timestamp parsing."""
    
    def test_z_suffix_parsing(self):
        monitor = DataQualityMonitor()
        now = datetime.now(timezone.utc)
        event = {
            "source": "twitter",
            "timestamp": "2026-01-17T10:00:00Z",
            "sentiment": {},
            "risk_indicators": {}
        }
        monitor.record_event(event)
        # Should not raise
    
    def test_offset_parsing(self):
        monitor = DataQualityMonitor()
        event = {
            "source": "twitter",
            "timestamp": "2026-01-17T10:00:00+00:00",
            "sentiment": {},
            "risk_indicators": {}
        }
        monitor.record_event(event)
        # Should not raise


if __name__ == "__main__":
    unittest.main()
