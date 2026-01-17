"""
Data Quality Monitor (DQM) for Social Data
Purpose: Monitor the QUALITY, HEALTH, and RELIABILITY of social data streams
feeding BotTrading.

DQM ONLY: OBSERVES, FLAGS, and REPORTS
DQM NEVER: Modifies data, blocks pipelines, or triggers trades

Quality Dimensions (FIXED):
1. Data Availability
2. Time Integrity
3. Volume Consistency
4. Source Balance
5. Anomaly Frequency

NO MOCKING. NO HALLUCINATION. NO DATA MODIFICATION.
"""

from datetime import datetime, timezone, timedelta
from typing import Optional
from dataclasses import dataclass, field
from enum import Enum
from collections import deque


class AvailabilityStatus(Enum):
    """Data availability status levels."""
    OK = "ok"
    DEGRADED = "degraded"
    DOWN = "down"


class TimeIntegrityStatus(Enum):
    """Time integrity status levels."""
    OK = "ok"
    UNSTABLE = "unstable"
    CRITICAL = "critical"


class VolumeStatus(Enum):
    """Volume consistency status levels."""
    NORMAL = "normal"
    ABNORMALLY_LOW = "abnormally_low"
    ABNORMALLY_HIGH = "abnormally_high"


class SourceBalanceStatus(Enum):
    """Source balance status levels."""
    NORMAL = "normal"
    IMBALANCED = "imbalanced"


class AnomalyStatus(Enum):
    """Anomaly frequency status levels."""
    NORMAL = "normal"
    PERSISTENT = "persistent"


class OverallQuality(Enum):
    """Overall data quality state."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"


# Availability thresholds (in seconds)
AVAILABILITY_DEGRADED = {
    "twitter": 60,
    "reddit": 900,  # 15 minutes
    "telegram": 120
}

AVAILABILITY_DOWN = {
    "twitter": 300,  # 5 minutes
    "reddit": 3600,  # 1 hour
    "telegram": 600  # 10 minutes
}

# Volume thresholds (percentages of baseline)
VOLUME_LOW_THRESHOLD = 0.30  # 30%
VOLUME_HIGH_THRESHOLD = 3.00  # 300%

# Source balance threshold
SOURCE_IMBALANCE_THRESHOLD = 0.70  # 70%

# Time integrity thresholds
TIME_UNSTABLE_THRESHOLD = 0.05  # 5%
TIME_CRITICAL_THRESHOLD = 0.15  # 15%


@dataclass
class SourceTracker:
    """Tracks events from a single source."""
    source: str
    last_event_time: Optional[datetime] = None
    event_count: int = 0
    
    def record_event(self, timestamp: datetime):
        """Record an event from this source."""
        self.last_event_time = timestamp
        self.event_count += 1
    
    def get_seconds_since_last(self, now: datetime) -> float:
        """Get seconds since last event."""
        if self.last_event_time is None:
            return float('inf')
        return (now - self.last_event_time).total_seconds()


@dataclass
class RollingWindow:
    """Maintains a rolling window of events."""
    window_seconds: int
    events: deque = field(default_factory=deque)
    
    def add_event(self, timestamp: datetime, data: dict):
        """Add an event to the window."""
        self.events.append({"timestamp": timestamp, "data": data})
        self._prune(timestamp)
    
    def _prune(self, now: datetime):
        """Remove events outside the window."""
        cutoff = now - timedelta(seconds=self.window_seconds)
        while self.events and self.events[0]["timestamp"] < cutoff:
            self.events.popleft()
    
    def get_count(self, now: Optional[datetime] = None) -> int:
        """Get event count in current window."""
        if now:
            self._prune(now)
        return len(self.events)
    
    def get_events(self, now: Optional[datetime] = None) -> list[dict]:
        """Get all events in current window."""
        if now:
            self._prune(now)
        return [e["data"] for e in self.events]


class AvailabilityMonitor:
    """
    Monitors data availability per source.
    
    Degraded thresholds:
    - Twitter/X: 60 seconds
    - Telegram: 120 seconds
    - Reddit: 15 minutes
    
    Down thresholds:
    - Twitter/X: 5 minutes
    - Telegram: 10 minutes
    - Reddit: 1 hour
    """
    
    def __init__(self):
        self.trackers: dict[str, SourceTracker] = {}
    
    def record_event(self, source: str, timestamp: datetime):
        """Record an event from a source."""
        if source not in self.trackers:
            self.trackers[source] = SourceTracker(source=source)
        self.trackers[source].record_event(timestamp)
    
    def get_status(self, source: str, now: datetime) -> AvailabilityStatus:
        """Get availability status for a source."""
        if source not in self.trackers:
            return AvailabilityStatus.DOWN
        
        seconds_since = self.trackers[source].get_seconds_since_last(now)
        
        down_threshold = AVAILABILITY_DOWN.get(source, 300)
        degraded_threshold = AVAILABILITY_DEGRADED.get(source, 60)
        
        if seconds_since >= down_threshold:
            return AvailabilityStatus.DOWN
        elif seconds_since >= degraded_threshold:
            return AvailabilityStatus.DEGRADED
        else:
            return AvailabilityStatus.OK
    
    def get_all_status(self, now: datetime) -> dict[str, AvailabilityStatus]:
        """Get availability status for all sources."""
        sources = ["twitter", "reddit", "telegram"]
        return {s: self.get_status(s, now) for s in sources}
    
    def get_worst_status(self, now: datetime) -> AvailabilityStatus:
        """Get worst availability status across all sources."""
        statuses = self.get_all_status(now)
        if any(s == AvailabilityStatus.DOWN for s in statuses.values()):
            return AvailabilityStatus.DOWN
        if any(s == AvailabilityStatus.DEGRADED for s in statuses.values()):
            return AvailabilityStatus.DEGRADED
        return AvailabilityStatus.OK


class TimeIntegrityMonitor:
    """
    Monitors time integrity based on dropped late events rate.
    
    Thresholds:
    - > 5% dropped late → unstable
    - > 15% dropped late → critical
    """
    
    def __init__(self, window_seconds: int = 300):
        self.window_seconds = window_seconds
        self.total_events = RollingWindow(window_seconds=window_seconds)
        self.dropped_late_events = RollingWindow(window_seconds=window_seconds)
    
    def record_event(self, timestamp: datetime, was_dropped_late: bool = False):
        """Record an event and whether it was dropped for being late."""
        self.total_events.add_event(timestamp, {"dropped": was_dropped_late})
        if was_dropped_late:
            self.dropped_late_events.add_event(timestamp, {})
    
    def record_batch(self, timestamp: datetime, total: int, dropped_late: int):
        """Record batch statistics."""
        for _ in range(total):
            self.total_events.add_event(timestamp, {})
        for _ in range(dropped_late):
            self.dropped_late_events.add_event(timestamp, {})
    
    def get_dropped_rate(self, now: datetime) -> float:
        """Get the dropped late events rate."""
        total = self.total_events.get_count(now)
        if total == 0:
            return 0.0
        dropped = self.dropped_late_events.get_count(now)
        return dropped / total
    
    def get_status(self, now: datetime) -> TimeIntegrityStatus:
        """Get time integrity status."""
        rate = self.get_dropped_rate(now)
        
        if rate > TIME_CRITICAL_THRESHOLD:
            return TimeIntegrityStatus.CRITICAL
        elif rate > TIME_UNSTABLE_THRESHOLD:
            return TimeIntegrityStatus.UNSTABLE
        else:
            return TimeIntegrityStatus.OK


class VolumeMonitor:
    """
    Monitors volume consistency against historical baseline.
    
    Thresholds:
    - < 30% of baseline → abnormally_low
    - > 300% of baseline → abnormally_high
    """
    
    def __init__(self, window_seconds: int = 300, baseline_window_seconds: int = 3600):
        self.window_seconds = window_seconds
        self.baseline_window_seconds = baseline_window_seconds
        self.current_window = RollingWindow(window_seconds=window_seconds)
        self.baseline_window = RollingWindow(window_seconds=baseline_window_seconds)
        self.baseline_rate: Optional[float] = None
    
    def record_event(self, timestamp: datetime):
        """Record an event."""
        self.current_window.add_event(timestamp, {})
        self.baseline_window.add_event(timestamp, {})
    
    def set_baseline_rate(self, events_per_window: float):
        """Set baseline rate manually (events per window)."""
        self.baseline_rate = events_per_window
    
    def get_current_volume(self, now: datetime) -> int:
        """Get current event count in window."""
        return self.current_window.get_count(now)
    
    def get_baseline_volume(self, now: datetime) -> float:
        """Get baseline volume (expected events per window)."""
        if self.baseline_rate is not None:
            return self.baseline_rate
        
        # Calculate from historical data
        baseline_count = self.baseline_window.get_count(now)
        if baseline_count == 0:
            return 0.0
        
        # Scale to current window size
        scale = self.window_seconds / self.baseline_window_seconds
        return baseline_count * scale
    
    def get_volume_ratio(self, now: datetime) -> float:
        """Get ratio of current volume to baseline."""
        baseline = self.get_baseline_volume(now)
        if baseline == 0:
            return 1.0  # No baseline, assume normal
        current = self.get_current_volume(now)
        return current / baseline
    
    def get_status(self, now: datetime) -> VolumeStatus:
        """Get volume status."""
        ratio = self.get_volume_ratio(now)
        
        if ratio < VOLUME_LOW_THRESHOLD:
            return VolumeStatus.ABNORMALLY_LOW
        elif ratio > VOLUME_HIGH_THRESHOLD:
            return VolumeStatus.ABNORMALLY_HIGH
        else:
            return VolumeStatus.NORMAL


class SourceBalanceMonitor:
    """
    Monitors balance of events across sources.
    
    Threshold:
    - Any source > 70% of total → imbalanced
    """
    
    def __init__(self, window_seconds: int = 300):
        self.window_seconds = window_seconds
        self.source_windows: dict[str, RollingWindow] = {}
    
    def record_event(self, source: str, timestamp: datetime):
        """Record an event from a source."""
        if source not in self.source_windows:
            self.source_windows[source] = RollingWindow(window_seconds=self.window_seconds)
        self.source_windows[source].add_event(timestamp, {})
    
    def get_contribution_ratios(self, now: datetime) -> dict[str, float]:
        """Get contribution ratio for each source."""
        counts = {}
        total = 0
        
        for source, window in self.source_windows.items():
            count = window.get_count(now)
            counts[source] = count
            total += count
        
        if total == 0:
            return {s: 0.0 for s in counts}
        
        return {s: c / total for s, c in counts.items()}
    
    def get_status(self, now: datetime) -> SourceBalanceStatus:
        """Get source balance status."""
        ratios = self.get_contribution_ratios(now)
        
        if not ratios:
            return SourceBalanceStatus.NORMAL
        
        if any(r > SOURCE_IMBALANCE_THRESHOLD for r in ratios.values()):
            return SourceBalanceStatus.IMBALANCED
        
        return SourceBalanceStatus.NORMAL


class AnomalyFrequencyMonitor:
    """
    Monitors frequency of anomaly indicators.
    
    Tracks:
    - social_overheat = true
    - manipulation_flag = true
    - panic_risk = true
    
    If anomalies persist for extended window → persistent
    """
    
    def __init__(self, window_seconds: int = 300, persistence_threshold: float = 0.5):
        self.window_seconds = window_seconds
        self.persistence_threshold = persistence_threshold
        self.anomaly_window = RollingWindow(window_seconds=window_seconds)
        self.total_window = RollingWindow(window_seconds=window_seconds)
    
    def record_event(
        self,
        timestamp: datetime,
        social_overheat: bool = False,
        manipulation_flag: bool = False,
        panic_risk: bool = False
    ):
        """Record an event with anomaly flags."""
        has_anomaly = social_overheat or manipulation_flag or panic_risk
        
        self.total_window.add_event(timestamp, {"anomaly": has_anomaly})
        if has_anomaly:
            self.anomaly_window.add_event(timestamp, {
                "social_overheat": social_overheat,
                "manipulation_flag": manipulation_flag,
                "panic_risk": panic_risk
            })
    
    def get_anomaly_rate(self, now: datetime) -> float:
        """Get rate of events with anomalies."""
        total = self.total_window.get_count(now)
        if total == 0:
            return 0.0
        anomalies = self.anomaly_window.get_count(now)
        return anomalies / total
    
    def get_status(self, now: datetime) -> AnomalyStatus:
        """Get anomaly frequency status."""
        rate = self.get_anomaly_rate(now)
        
        if rate >= self.persistence_threshold:
            return AnomalyStatus.PERSISTENT
        
        return AnomalyStatus.NORMAL


@dataclass
class DataQualityReport:
    """Data quality report output."""
    asset: str
    timestamp: str
    overall: OverallQuality
    availability: AvailabilityStatus
    time_integrity: TimeIntegrityStatus
    volume: VolumeStatus
    source_balance: SourceBalanceStatus
    anomaly_frequency: AnomalyStatus
    
    def to_dict(self) -> dict:
        """Convert to output format."""
        return {
            "asset": self.asset,
            "timestamp": self.timestamp,
            "data_quality": {
                "overall": self.overall.value,
                "availability": self.availability.value,
                "time_integrity": self.time_integrity.value,
                "volume": self.volume.value,
                "source_balance": self.source_balance.value,
                "anomaly_frequency": self.anomaly_frequency.value
            }
        }


class DataQualityMonitor:
    """
    Data Quality Monitor for Social Data.
    
    Monitors 5 quality dimensions:
    1. Data Availability
    2. Time Integrity
    3. Volume Consistency
    4. Source Balance
    5. Anomaly Frequency
    
    DQM only OBSERVES, FLAGS, and REPORTS.
    DQM NEVER modifies data or triggers trades.
    """
    
    def __init__(
        self,
        asset: str = "BTC",
        window_seconds: int = 300,
        baseline_window_seconds: int = 3600
    ):
        """
        Initialize Data Quality Monitor.
        
        Args:
            asset: Asset being monitored (e.g., "BTC")
            window_seconds: Rolling window for checks (default 5 minutes)
            baseline_window_seconds: Baseline window for volume (default 1 hour)
        """
        self.asset = asset
        self.window_seconds = window_seconds
        
        # Initialize monitors
        self.availability_monitor = AvailabilityMonitor()
        self.time_integrity_monitor = TimeIntegrityMonitor(window_seconds=window_seconds)
        self.volume_monitor = VolumeMonitor(
            window_seconds=window_seconds,
            baseline_window_seconds=baseline_window_seconds
        )
        self.source_balance_monitor = SourceBalanceMonitor(window_seconds=window_seconds)
        self.anomaly_frequency_monitor = AnomalyFrequencyMonitor(window_seconds=window_seconds)
    
    def _parse_timestamp(self, ts: str) -> datetime:
        """Parse ISO-8601 timestamp to datetime."""
        if ts.endswith('Z'):
            ts = ts.replace('Z', '+00:00')
        return datetime.fromisoformat(ts)
    
    def record_event(self, event: dict):
        """
        Record an event from the orchestrator.
        
        Expected format:
        {
            "asset": "BTC",
            "timestamp": "ISO-8601",
            "source": "twitter | reddit | telegram",
            "sentiment": { ... },
            "risk_indicators": { ... }
        }
        """
        source = event.get("source", "unknown")
        timestamp_str = event.get("timestamp", "")
        
        if not timestamp_str:
            return  # Cannot process without timestamp
        
        try:
            timestamp = self._parse_timestamp(timestamp_str)
        except (ValueError, TypeError):
            return  # Invalid timestamp
        
        # Update availability
        self.availability_monitor.record_event(source, timestamp)
        
        # Update volume
        self.volume_monitor.record_event(timestamp)
        
        # Update source balance
        self.source_balance_monitor.record_event(source, timestamp)
        
        # Update anomaly frequency
        risk_indicators = event.get("risk_indicators", {})
        metrics = event.get("metrics", {})
        
        self.anomaly_frequency_monitor.record_event(
            timestamp=timestamp,
            social_overheat=risk_indicators.get("social_overheat", False),
            manipulation_flag=metrics.get("manipulation_flag", False),
            panic_risk=risk_indicators.get("panic_risk", False)
        )
    
    def record_time_sync_stats(
        self,
        timestamp: datetime,
        total_events: int,
        dropped_late_events: int
    ):
        """
        Record time sync statistics.
        
        Args:
            timestamp: Current time
            total_events: Total events processed
            dropped_late_events: Events dropped for being late
        """
        self.time_integrity_monitor.record_batch(
            timestamp=timestamp,
            total=total_events,
            dropped_late=dropped_late_events
        )
    
    def _compute_overall_quality(
        self,
        availability: AvailabilityStatus,
        time_integrity: TimeIntegrityStatus,
        volume: VolumeStatus,
        source_balance: SourceBalanceStatus,
        anomaly_frequency: AnomalyStatus
    ) -> OverallQuality:
        """
        Aggregate individual statuses into overall quality.
        
        Rules:
        - IF any status == "critical" OR availability == "down" → critical
        - ELSE IF multiple status == "degraded" OR "unstable" → degraded
        - ELSE → healthy
        """
        # Check for critical
        if time_integrity == TimeIntegrityStatus.CRITICAL:
            return OverallQuality.CRITICAL
        if availability == AvailabilityStatus.DOWN:
            return OverallQuality.CRITICAL
        
        # Count degraded conditions
        degraded_count = 0
        
        if availability == AvailabilityStatus.DEGRADED:
            degraded_count += 1
        if time_integrity == TimeIntegrityStatus.UNSTABLE:
            degraded_count += 1
        if volume in (VolumeStatus.ABNORMALLY_LOW, VolumeStatus.ABNORMALLY_HIGH):
            degraded_count += 1
        if source_balance == SourceBalanceStatus.IMBALANCED:
            degraded_count += 1
        if anomaly_frequency == AnomalyStatus.PERSISTENT:
            degraded_count += 1
        
        if degraded_count >= 2:
            return OverallQuality.DEGRADED
        
        # Single degraded condition is still healthy with warning
        if degraded_count == 1:
            return OverallQuality.DEGRADED
        
        return OverallQuality.HEALTHY
    
    def get_report(self, now: Optional[datetime] = None) -> DataQualityReport:
        """
        Generate a data quality report.
        
        Args:
            now: Current time (defaults to UTC now)
        
        Returns:
            DataQualityReport with all quality dimensions
        """
        if now is None:
            now = datetime.now(timezone.utc)
        
        # Get individual statuses
        availability = self.availability_monitor.get_worst_status(now)
        time_integrity = self.time_integrity_monitor.get_status(now)
        volume = self.volume_monitor.get_status(now)
        source_balance = self.source_balance_monitor.get_status(now)
        anomaly_frequency = self.anomaly_frequency_monitor.get_status(now)
        
        # Compute overall
        overall = self._compute_overall_quality(
            availability=availability,
            time_integrity=time_integrity,
            volume=volume,
            source_balance=source_balance,
            anomaly_frequency=anomaly_frequency
        )
        
        return DataQualityReport(
            asset=self.asset,
            timestamp=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            overall=overall,
            availability=availability,
            time_integrity=time_integrity,
            volume=volume,
            source_balance=source_balance,
            anomaly_frequency=anomaly_frequency
        )
    
    def get_detailed_status(self, now: Optional[datetime] = None) -> dict:
        """
        Get detailed status with metrics.
        
        Returns dict with all monitor metrics for debugging/dashboards.
        """
        if now is None:
            now = datetime.now(timezone.utc)
        
        report = self.get_report(now)
        
        return {
            "report": report.to_dict(),
            "details": {
                "availability": {
                    source: {
                        "status": self.availability_monitor.get_status(source, now).value,
                        "seconds_since_last": self.availability_monitor.trackers.get(
                            source, SourceTracker(source=source)
                        ).get_seconds_since_last(now)
                    }
                    for source in ["twitter", "reddit", "telegram"]
                },
                "time_integrity": {
                    "dropped_rate": self.time_integrity_monitor.get_dropped_rate(now),
                    "status": self.time_integrity_monitor.get_status(now).value
                },
                "volume": {
                    "current": self.volume_monitor.get_current_volume(now),
                    "baseline": self.volume_monitor.get_baseline_volume(now),
                    "ratio": self.volume_monitor.get_volume_ratio(now),
                    "status": self.volume_monitor.get_status(now).value
                },
                "source_balance": {
                    "ratios": self.source_balance_monitor.get_contribution_ratios(now),
                    "status": self.source_balance_monitor.get_status(now).value
                },
                "anomaly_frequency": {
                    "rate": self.anomaly_frequency_monitor.get_anomaly_rate(now),
                    "status": self.anomaly_frequency_monitor.get_status(now).value
                }
            }
        }
    
    def set_volume_baseline(self, events_per_window: float):
        """Set volume baseline for comparison."""
        self.volume_monitor.set_baseline_rate(events_per_window)


def create_monitor(asset: str = "BTC", window_seconds: int = 300) -> DataQualityMonitor:
    """Factory function to create a Data Quality Monitor."""
    return DataQualityMonitor(asset=asset, window_seconds=window_seconds)


if __name__ == "__main__":
    import json
    
    # Demo
    now = datetime.now(timezone.utc)
    monitor = create_monitor(asset="BTC")
    
    # Simulate events from different sources
    events = [
        {
            "asset": "BTC",
            "source": "twitter",
            "timestamp": (now - timedelta(seconds=10)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sentiment": {"label": 1},
            "risk_indicators": {"social_overheat": False, "panic_risk": False}
        },
        {
            "asset": "BTC",
            "source": "reddit",
            "timestamp": (now - timedelta(seconds=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sentiment": {"label": 0},
            "risk_indicators": {"social_overheat": False, "panic_risk": False}
        },
        {
            "asset": "BTC",
            "source": "telegram",
            "timestamp": (now - timedelta(seconds=20)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sentiment": {"label": -1},
            "risk_indicators": {"social_overheat": True, "panic_risk": True}
        }
    ]
    
    # Record events
    for event in events:
        monitor.record_event(event)
    
    # Record time sync stats
    monitor.record_time_sync_stats(
        timestamp=now,
        total_events=100,
        dropped_late_events=3  # 3% dropped
    )
    
    # Set volume baseline
    monitor.set_volume_baseline(10.0)  # Expect 10 events per window
    
    # Get report
    report = monitor.get_report(now)
    
    print("=== Data Quality Monitor Demo ===\n")
    print("Report:")
    print(json.dumps(report.to_dict(), indent=2))
    
    print("\nDetailed Status:")
    detailed = monitor.get_detailed_status(now)
    print(json.dumps(detailed["details"], indent=2))
