"""
Social Data Orchestrator for Crypto BotTrading
Purpose: Orchestrate the end-to-end flow of social data
from multiple crawlers into BotTrading-safe inputs.

This module coordinates execution order, data flow,
and failure isolation ONLY.

THIS MODULE DOES NOT IMPLEMENT LOGIC OF SUB-MODULES.
IT ONLY CALLS THEM IN ORDER.

Pipeline Order (FIXED):
1. Twitter/X Crawler
2. Reddit Crawler
3. Telegram Crawler
4. Time Sync Guard
5. Sentiment Pipeline
6. Risk Indicators
7. Output Dispatcher

NO MOCKING. NO HALLUCINATION. NO DATA MODIFICATION.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional, Callable
from dataclasses import dataclass, field
from enum import Enum


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("orchestrator")


class SourceType(Enum):
    """Social data source types."""
    TWITTER = "twitter"
    REDDIT = "reddit"
    TELEGRAM = "telegram"


class PipelineStage(Enum):
    """Pipeline processing stages."""
    CRAWL = "crawl"
    TIME_SYNC = "time_sync"
    SENTIMENT = "sentiment"
    RISK = "risk"
    DISPATCH = "dispatch"


@dataclass
class SourceResult:
    """Result from a single source crawler."""
    source: SourceType
    records: list[dict] = field(default_factory=list)
    success: bool = True
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class PipelineMetrics:
    """Metrics for pipeline execution."""
    twitter_records: int = 0
    reddit_records: int = 0
    telegram_records: int = 0
    time_sync_passed: int = 0
    time_sync_dropped: int = 0
    sentiment_processed: int = 0
    risk_computed: int = 0
    dispatched: int = 0
    errors: list[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {
            "twitter_records": self.twitter_records,
            "reddit_records": self.reddit_records,
            "telegram_records": self.telegram_records,
            "time_sync_passed": self.time_sync_passed,
            "time_sync_dropped": self.time_sync_dropped,
            "sentiment_processed": self.sentiment_processed,
            "risk_computed": self.risk_computed,
            "dispatched": self.dispatched,
            "errors": self.errors
        }


class SourceIsolator:
    """
    Isolates source execution to prevent cascade failures.
    
    Failures in one source MUST NOT block others.
    """
    
    @staticmethod
    def execute_isolated(
        source: SourceType,
        fetch_func: Callable[[], list[dict]]
    ) -> SourceResult:
        """
        Execute a source crawler in isolation.
        
        Args:
            source: The source type
            fetch_func: Function that fetches records
        
        Returns:
            SourceResult with records or error
        """
        try:
            records = fetch_func()
            return SourceResult(
                source=source,
                records=records if records else [],
                success=True
            )
        except Exception as e:
            logger.error(f"Source {source.value} failed: {e}")
            return SourceResult(
                source=source,
                records=[],
                success=False,
                error=str(e)
            )


def sort_by_event_time(records: list[dict]) -> list[dict]:
    """
    Sort records by event timestamp (ascending order).
    
    Events MUST be processed in ascending event-time order.
    """
    def get_timestamp(record: dict) -> datetime:
        ts = record.get("timestamp", "")
        if not ts:
            return datetime.min.replace(tzinfo=timezone.utc)
        try:
            if ts.endswith('Z'):
                return datetime.fromisoformat(ts.replace('Z', '+00:00'))
            return datetime.fromisoformat(ts)
        except (ValueError, TypeError):
            return datetime.min.replace(tzinfo=timezone.utc)
    
    return sorted(records, key=get_timestamp)


class SocialDataOrchestrator:
    """
    Social Data Pipeline Orchestrator.
    
    Coordinates:
    1. Twitter/X Crawler
    2. Reddit Crawler
    3. Telegram Crawler
    4. Time Sync Guard
    5. Sentiment Pipeline
    6. Risk Indicators
    7. Output Dispatcher
    
    This module ONLY calls sub-modules in order.
    It does NOT implement their logic.
    """
    
    def __init__(
        self,
        twitter_crawler=None,
        reddit_crawler=None,
        telegram_crawler=None,
        time_sync_guard=None,
        sentiment_pipeline=None,
        risk_indicators=None,
        output_dispatcher=None
    ):
        """
        Initialize orchestrator with pipeline components.
        
        Args:
            twitter_crawler: Twitter/X crawler instance
            reddit_crawler: Reddit crawler instance
            telegram_crawler: Telegram crawler instance
            time_sync_guard: Time synchronization guard
            sentiment_pipeline: Sentiment analysis pipeline
            risk_indicators: Risk indicator calculator
            output_dispatcher: Output dispatcher to BotTrading
        """
        self.twitter_crawler = twitter_crawler
        self.reddit_crawler = reddit_crawler
        self.telegram_crawler = telegram_crawler
        self.time_sync_guard = time_sync_guard
        self.sentiment_pipeline = sentiment_pipeline
        self.risk_indicators = risk_indicators
        self.output_dispatcher = output_dispatcher
        
        self.metrics = PipelineMetrics()
        self._running = False
    
    def _collect_from_twitter(self) -> SourceResult:
        """Collect data from Twitter/X crawler."""
        if self.twitter_crawler is None:
            return SourceResult(
                source=SourceType.TWITTER,
                records=[],
                success=True
            )
        
        def fetch():
            if hasattr(self.twitter_crawler, 'search'):
                return self.twitter_crawler.search()
            elif hasattr(self.twitter_crawler, 'process_tweets'):
                return []  # No tweets to process without input
            return []
        
        return SourceIsolator.execute_isolated(SourceType.TWITTER, fetch)
    
    def _collect_from_reddit(self) -> SourceResult:
        """Collect data from Reddit crawler."""
        if self.reddit_crawler is None:
            return SourceResult(
                source=SourceType.REDDIT,
                records=[],
                success=True
            )
        
        def fetch():
            if hasattr(self.reddit_crawler, 'crawl_all'):
                return self.reddit_crawler.crawl_all()
            return []
        
        return SourceIsolator.execute_isolated(SourceType.REDDIT, fetch)
    
    def _collect_from_telegram(self) -> SourceResult:
        """Collect data from Telegram crawler."""
        if self.telegram_crawler is None:
            return SourceResult(
                source=SourceType.TELEGRAM,
                records=[],
                success=True
            )
        
        def fetch():
            if hasattr(self.telegram_crawler, 'crawl_all'):
                return self.telegram_crawler.crawl_all()
            return []
        
        return SourceIsolator.execute_isolated(SourceType.TELEGRAM, fetch)
    
    def _apply_time_sync(self, records: list[dict]) -> list[dict]:
        """
        Apply Time Sync Guard to validate timestamps.
        
        Time Sync Guard is the ONLY authority on time validity.
        """
        if self.time_sync_guard is None:
            # Passthrough if no guard configured
            self.metrics.time_sync_passed = len(records)
            return records
        
        try:
            validated = self.time_sync_guard.validate_batch(records)
            self.metrics.time_sync_passed = len(validated)
            self.metrics.time_sync_dropped = len(records) - len(validated)
            return validated
        except Exception as e:
            logger.error(f"Time Sync Guard failed: {e}")
            self.metrics.errors.append(f"time_sync: {e}")
            # On failure, drop all records (safety first)
            self.metrics.time_sync_dropped = len(records)
            return []
    
    def _apply_sentiment(self, records: list[dict]) -> list[dict]:
        """
        Apply Sentiment Pipeline to records.
        
        Passthrough - adds sentiment fields to records.
        """
        if self.sentiment_pipeline is None:
            return records
        
        try:
            results = []
            for record in records:
                text = record.get("text", "")
                if hasattr(self.sentiment_pipeline, 'analyze'):
                    sentiment_result = self.sentiment_pipeline.analyze(text)
                    # Merge sentiment into record
                    enriched = record.copy()
                    enriched["sentiment"] = sentiment_result
                    results.append(enriched)
                else:
                    results.append(record)
            
            self.metrics.sentiment_processed = len(results)
            return results
        except Exception as e:
            logger.error(f"Sentiment Pipeline failed: {e}")
            self.metrics.errors.append(f"sentiment: {e}")
            # Return original records on failure
            return records
    
    def _apply_risk_indicators(self, records: list[dict]) -> list[dict]:
        """
        Apply Risk Indicators to records.
        
        Passthrough - adds risk fields to records.
        """
        if self.risk_indicators is None:
            return records
        
        try:
            results = []
            for record in records:
                if hasattr(self.risk_indicators, 'compute_all'):
                    # Extract required inputs
                    sentiment = record.get("sentiment", {})
                    metrics = record.get("metrics", {})
                    
                    risk_result = self.risk_indicators.compute_all(
                        label=sentiment.get("label", 0),
                        velocity=metrics.get("velocity", 1.0),
                        anomaly_flag=False,
                        fear_greed_index=50  # Default neutral
                    )
                    
                    enriched = record.copy()
                    enriched["risk_indicators"] = risk_result
                    results.append(enriched)
                else:
                    results.append(record)
            
            self.metrics.risk_computed = len(results)
            return results
        except Exception as e:
            logger.error(f"Risk Indicators failed: {e}")
            self.metrics.errors.append(f"risk: {e}")
            return records
    
    def _dispatch_output(self, records: list[dict]) -> bool:
        """
        Dispatch records to BotTrading.
        
        Returns True if dispatch succeeded.
        """
        if self.output_dispatcher is None:
            self.metrics.dispatched = len(records)
            return True
        
        try:
            if hasattr(self.output_dispatcher, 'dispatch'):
                success = self.output_dispatcher.dispatch(records)
                if success:
                    self.metrics.dispatched = len(records)
                return success
            elif hasattr(self.output_dispatcher, 'send'):
                for record in records:
                    self.output_dispatcher.send(record)
                self.metrics.dispatched = len(records)
                return True
            else:
                self.metrics.dispatched = len(records)
                return True
        except Exception as e:
            logger.error(f"Output dispatch failed: {e}")
            self.metrics.errors.append(f"dispatch: {e}")
            return False
    
    def process_batch(
        self,
        twitter_data: Optional[list[dict]] = None,
        reddit_data: Optional[list[dict]] = None,
        telegram_data: Optional[list[dict]] = None
    ) -> dict:
        """
        Process a batch of data from all sources.
        
        This is the main entry point for batch processing.
        
        Args:
            twitter_data: Pre-fetched Twitter data (optional)
            reddit_data: Pre-fetched Reddit data (optional)
            telegram_data: Pre-fetched Telegram data (optional)
        
        Returns:
            Processing result with metrics
        """
        self.metrics = PipelineMetrics()
        
        # Step 1-3: Collect from all sources (isolated)
        all_records = []
        
        # Twitter (processed FIRST due to second-level precision)
        if twitter_data is not None:
            self.metrics.twitter_records = len(twitter_data)
            all_records.extend(twitter_data)
        else:
            twitter_result = self._collect_from_twitter()
            self.metrics.twitter_records = len(twitter_result.records)
            all_records.extend(twitter_result.records)
            if not twitter_result.success:
                self.metrics.errors.append(f"twitter: {twitter_result.error}")
        
        # Reddit
        if reddit_data is not None:
            self.metrics.reddit_records = len(reddit_data)
            all_records.extend(reddit_data)
        else:
            reddit_result = self._collect_from_reddit()
            self.metrics.reddit_records = len(reddit_result.records)
            all_records.extend(reddit_result.records)
            if not reddit_result.success:
                self.metrics.errors.append(f"reddit: {reddit_result.error}")
        
        # Telegram
        if telegram_data is not None:
            self.metrics.telegram_records = len(telegram_data)
            all_records.extend(telegram_data)
        else:
            telegram_result = self._collect_from_telegram()
            self.metrics.telegram_records = len(telegram_result.records)
            all_records.extend(telegram_result.records)
            if not telegram_result.success:
                self.metrics.errors.append(f"telegram: {telegram_result.error}")
        
        # Sort by event time (ascending)
        all_records = sort_by_event_time(all_records)
        
        # Step 4: Time Sync Guard
        validated_records = self._apply_time_sync(all_records)
        
        # Step 5: Sentiment Pipeline
        sentiment_records = self._apply_sentiment(validated_records)
        
        # Step 6: Risk Indicators
        risk_records = self._apply_risk_indicators(sentiment_records)
        
        # Step 7: Output Dispatch
        dispatch_success = self._dispatch_output(risk_records)
        
        return {
            "success": dispatch_success and len(self.metrics.errors) == 0,
            "records_processed": len(risk_records),
            "metrics": self.metrics.to_dict(),
            "output": risk_records
        }
    
    def run_cycle(self) -> dict:
        """
        Run one orchestration cycle.
        
        Collects from all crawlers and processes through pipeline.
        """
        logger.info("Starting orchestration cycle")
        result = self.process_batch()
        logger.info(f"Cycle complete: {result['records_processed']} records processed")
        return result
    
    def get_metrics(self) -> dict:
        """Get current pipeline metrics."""
        return self.metrics.to_dict()
    
    def reset_metrics(self):
        """Reset pipeline metrics."""
        self.metrics = PipelineMetrics()


class OutputDispatcher:
    """
    Default output dispatcher for BotTrading.
    
    Outputs data as JSON to configured destination.
    """
    
    def __init__(self, output_callback: Optional[Callable[[list[dict]], bool]] = None):
        """
        Initialize dispatcher.
        
        Args:
            output_callback: Callback function to send records
        """
        self.output_callback = output_callback
        self.last_dispatch: list[dict] = []
    
    def dispatch(self, records: list[dict]) -> bool:
        """
        Dispatch records to output.
        
        Returns True if successful.
        """
        self.last_dispatch = records
        
        if self.output_callback:
            return self.output_callback(records)
        
        # Default: log output
        for record in records:
            logger.info(f"Dispatching: {record.get('source')} - {record.get('timestamp')}")
        
        return True
    
    def get_last_dispatch(self) -> list[dict]:
        """Get last dispatched records."""
        return self.last_dispatch


def create_orchestrator(
    twitter_crawler=None,
    reddit_crawler=None,
    telegram_crawler=None,
    time_sync_guard=None,
    sentiment_pipeline=None,
    risk_indicators=None,
    output_dispatcher=None
) -> SocialDataOrchestrator:
    """Factory function to create an orchestrator."""
    return SocialDataOrchestrator(
        twitter_crawler=twitter_crawler,
        reddit_crawler=reddit_crawler,
        telegram_crawler=telegram_crawler,
        time_sync_guard=time_sync_guard,
        sentiment_pipeline=sentiment_pipeline,
        risk_indicators=risk_indicators,
        output_dispatcher=output_dispatcher
    )


if __name__ == "__main__":
    from datetime import timedelta
    
    # Create orchestrator (standalone demo - no real crawlers)
    orchestrator = create_orchestrator()
    
    # Sample data from crawlers
    now = datetime.now(timezone.utc)
    
    twitter_data = [
        {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": (now - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "text": "$BTC breaking resistance!",
            "metrics": {"engagement_weight": 5.2, "author_weight": 8.1, "velocity": 2.5},
            "source_reliability": 0.5
        }
    ]
    
    reddit_data = [
        {
            "source": "reddit",
            "asset": "BTC",
            "timestamp": (now - timedelta(seconds=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "text": "Bitcoin discussion - bullish sentiment",
            "metrics": {"engagement_weight": 4.0, "author_weight": 6.5, "velocity": 1.2},
            "source_reliability": 0.7
        }
    ]
    
    telegram_data = [
        {
            "source": "telegram",
            "asset": "BTC",
            "timestamp": (now - timedelta(seconds=15)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "text": "Bitcoin alert - price moving",
            "metrics": {"velocity": 3.0, "manipulation_flag": False},
            "source_reliability": 0.3
        }
    ]
    
    # Process batch
    result = orchestrator.process_batch(
        twitter_data=twitter_data,
        reddit_data=reddit_data,
        telegram_data=telegram_data
    )
    
    print("=== Social Data Orchestrator Demo ===\n")
    print(f"Success: {result['success']}")
    print(f"Records Processed: {result['records_processed']}\n")
    
    print("Pipeline Metrics:")
    print(json.dumps(result['metrics'], indent=2))
    
    print("\nProcessed Records (sorted by event time):")
    for record in result['output']:
        print(f"  [{record['source']}] {record['timestamp']}: {record['text'][:40]}...")
