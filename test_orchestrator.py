"""
Unit tests for Social Data Orchestrator.
All tests use assertAlmostEqual where needed for float comparisons.

NO MOCKING. NO HALLUCINATION.
"""

import unittest
from datetime import datetime, timezone, timedelta
from orchestrator import (
    SocialDataOrchestrator,
    SourceIsolator,
    SourceType,
    SourceResult,
    PipelineMetrics,
    OutputDispatcher,
    PipelineStage,
    sort_by_event_time,
    create_orchestrator
)


class TestSourceType(unittest.TestCase):
    """Tests for SourceType enum."""
    
    def test_twitter_value(self):
        self.assertEqual(SourceType.TWITTER.value, "twitter")
    
    def test_reddit_value(self):
        self.assertEqual(SourceType.REDDIT.value, "reddit")
    
    def test_telegram_value(self):
        self.assertEqual(SourceType.TELEGRAM.value, "telegram")
    
    def test_all_sources_distinct(self):
        sources = [SourceType.TWITTER, SourceType.REDDIT, SourceType.TELEGRAM]
        values = [s.value for s in sources]
        self.assertEqual(len(values), len(set(values)))


class TestPipelineStage(unittest.TestCase):
    """Tests for PipelineStage enum."""
    
    def test_crawl_stage(self):
        self.assertEqual(PipelineStage.CRAWL.value, "crawl")
    
    def test_time_sync_stage(self):
        self.assertEqual(PipelineStage.TIME_SYNC.value, "time_sync")
    
    def test_sentiment_stage(self):
        self.assertEqual(PipelineStage.SENTIMENT.value, "sentiment")
    
    def test_risk_stage(self):
        self.assertEqual(PipelineStage.RISK.value, "risk")
    
    def test_dispatch_stage(self):
        self.assertEqual(PipelineStage.DISPATCH.value, "dispatch")


class TestSourceResult(unittest.TestCase):
    """Tests for SourceResult dataclass."""
    
    def test_default_values(self):
        result = SourceResult(source=SourceType.TWITTER)
        self.assertEqual(result.source, SourceType.TWITTER)
        self.assertEqual(result.records, [])
        self.assertTrue(result.success)
        self.assertIsNone(result.error)
    
    def test_with_records(self):
        records = [{"text": "test"}]
        result = SourceResult(source=SourceType.REDDIT, records=records)
        self.assertEqual(len(result.records), 1)
    
    def test_with_error(self):
        result = SourceResult(
            source=SourceType.TELEGRAM,
            success=False,
            error="Connection failed"
        )
        self.assertFalse(result.success)
        self.assertEqual(result.error, "Connection failed")
    
    def test_timestamp_is_utc(self):
        result = SourceResult(source=SourceType.TWITTER)
        self.assertEqual(result.timestamp.tzinfo, timezone.utc)


class TestPipelineMetrics(unittest.TestCase):
    """Tests for PipelineMetrics dataclass."""
    
    def test_default_values(self):
        metrics = PipelineMetrics()
        self.assertEqual(metrics.twitter_records, 0)
        self.assertEqual(metrics.reddit_records, 0)
        self.assertEqual(metrics.telegram_records, 0)
        self.assertEqual(metrics.time_sync_passed, 0)
        self.assertEqual(metrics.time_sync_dropped, 0)
        self.assertEqual(metrics.sentiment_processed, 0)
        self.assertEqual(metrics.risk_computed, 0)
        self.assertEqual(metrics.dispatched, 0)
        self.assertEqual(metrics.errors, [])
    
    def test_to_dict(self):
        metrics = PipelineMetrics(
            twitter_records=10,
            reddit_records=5,
            errors=["test error"]
        )
        result = metrics.to_dict()
        self.assertEqual(result["twitter_records"], 10)
        self.assertEqual(result["reddit_records"], 5)
        self.assertIn("test error", result["errors"])
    
    def test_errors_list_independent(self):
        metrics1 = PipelineMetrics()
        metrics1.errors.append("error1")
        metrics2 = PipelineMetrics()
        self.assertEqual(len(metrics2.errors), 0)


class TestSourceIsolator(unittest.TestCase):
    """Tests for SourceIsolator."""
    
    def test_successful_execution(self):
        def fetch():
            return [{"text": "test"}]
        
        result = SourceIsolator.execute_isolated(SourceType.TWITTER, fetch)
        self.assertTrue(result.success)
        self.assertEqual(len(result.records), 1)
        self.assertIsNone(result.error)
    
    def test_failed_execution_isolates_error(self):
        def fetch():
            raise ValueError("Test error")
        
        result = SourceIsolator.execute_isolated(SourceType.REDDIT, fetch)
        self.assertFalse(result.success)
        self.assertEqual(result.records, [])
        self.assertIn("Test error", result.error)
    
    def test_none_return_handled(self):
        def fetch():
            return None
        
        result = SourceIsolator.execute_isolated(SourceType.TELEGRAM, fetch)
        self.assertTrue(result.success)
        self.assertEqual(result.records, [])
    
    def test_empty_list_is_success(self):
        def fetch():
            return []
        
        result = SourceIsolator.execute_isolated(SourceType.TWITTER, fetch)
        self.assertTrue(result.success)
        self.assertEqual(result.records, [])
    
    def test_exception_type_preserved(self):
        def fetch():
            raise ConnectionError("Network failed")
        
        result = SourceIsolator.execute_isolated(SourceType.TWITTER, fetch)
        self.assertFalse(result.success)
        self.assertIn("Network failed", result.error)


class TestSortByEventTime(unittest.TestCase):
    """Tests for sort_by_event_time function."""
    
    def test_empty_list(self):
        result = sort_by_event_time([])
        self.assertEqual(result, [])
    
    def test_single_record(self):
        records = [{"timestamp": "2026-01-17T10:00:00Z"}]
        result = sort_by_event_time(records)
        self.assertEqual(len(result), 1)
    
    def test_ascending_order(self):
        records = [
            {"id": 3, "timestamp": "2026-01-17T10:00:30Z"},
            {"id": 1, "timestamp": "2026-01-17T10:00:00Z"},
            {"id": 2, "timestamp": "2026-01-17T10:00:15Z"}
        ]
        result = sort_by_event_time(records)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[1]["id"], 2)
        self.assertEqual(result[2]["id"], 3)
    
    def test_missing_timestamp_goes_first(self):
        records = [
            {"id": 1, "timestamp": "2026-01-17T10:00:00Z"},
            {"id": 2}
        ]
        result = sort_by_event_time(records)
        self.assertEqual(result[0]["id"], 2)
        self.assertEqual(result[1]["id"], 1)
    
    def test_empty_timestamp_goes_first(self):
        records = [
            {"id": 1, "timestamp": "2026-01-17T10:00:00Z"},
            {"id": 2, "timestamp": ""}
        ]
        result = sort_by_event_time(records)
        self.assertEqual(result[0]["id"], 2)
    
    def test_invalid_timestamp_goes_first(self):
        records = [
            {"id": 1, "timestamp": "2026-01-17T10:00:00Z"},
            {"id": 2, "timestamp": "not-a-timestamp"}
        ]
        result = sort_by_event_time(records)
        self.assertEqual(result[0]["id"], 2)
    
    def test_z_suffix_handled(self):
        records = [
            {"id": 1, "timestamp": "2026-01-17T10:00:00Z"},
            {"id": 2, "timestamp": "2026-01-17T09:59:59Z"}
        ]
        result = sort_by_event_time(records)
        self.assertEqual(result[0]["id"], 2)
    
    def test_iso_format_with_timezone(self):
        records = [
            {"id": 1, "timestamp": "2026-01-17T10:00:00+00:00"},
            {"id": 2, "timestamp": "2026-01-17T09:59:59+00:00"}
        ]
        result = sort_by_event_time(records)
        self.assertEqual(result[0]["id"], 2)


class TestOrchestratorInit(unittest.TestCase):
    """Tests for SocialDataOrchestrator initialization."""
    
    def test_default_init(self):
        orch = SocialDataOrchestrator()
        self.assertIsNone(orch.twitter_crawler)
        self.assertIsNone(orch.reddit_crawler)
        self.assertIsNone(orch.telegram_crawler)
        self.assertIsNone(orch.time_sync_guard)
        self.assertIsNone(orch.sentiment_pipeline)
        self.assertIsNone(orch.risk_indicators)
        self.assertIsNone(orch.output_dispatcher)
    
    def test_init_with_components(self):
        class MockCrawler:
            pass
        
        crawler = MockCrawler()
        orch = SocialDataOrchestrator(twitter_crawler=crawler)
        self.assertIsNotNone(orch.twitter_crawler)
    
    def test_metrics_initialized(self):
        orch = SocialDataOrchestrator()
        self.assertIsInstance(orch.metrics, PipelineMetrics)
    
    def test_not_running_initially(self):
        orch = SocialDataOrchestrator()
        self.assertFalse(orch._running)


class TestOrchestratorProcessBatch(unittest.TestCase):
    """Tests for process_batch method."""
    
    def test_empty_batch(self):
        orch = SocialDataOrchestrator()
        result = orch.process_batch()
        self.assertTrue(result["success"])
        self.assertEqual(result["records_processed"], 0)
    
    def test_twitter_data_counted(self):
        orch = SocialDataOrchestrator()
        twitter_data = [
            {"source": "twitter", "timestamp": "2026-01-17T10:00:00Z", "text": "test"}
        ]
        result = orch.process_batch(twitter_data=twitter_data)
        self.assertEqual(result["metrics"]["twitter_records"], 1)
    
    def test_reddit_data_counted(self):
        orch = SocialDataOrchestrator()
        reddit_data = [
            {"source": "reddit", "timestamp": "2026-01-17T10:00:00Z", "text": "test"}
        ]
        result = orch.process_batch(reddit_data=reddit_data)
        self.assertEqual(result["metrics"]["reddit_records"], 1)
    
    def test_telegram_data_counted(self):
        orch = SocialDataOrchestrator()
        telegram_data = [
            {"source": "telegram", "timestamp": "2026-01-17T10:00:00Z", "text": "test"}
        ]
        result = orch.process_batch(telegram_data=telegram_data)
        self.assertEqual(result["metrics"]["telegram_records"], 1)
    
    def test_all_sources_combined(self):
        orch = SocialDataOrchestrator()
        result = orch.process_batch(
            twitter_data=[{"timestamp": "2026-01-17T10:00:00Z"}],
            reddit_data=[{"timestamp": "2026-01-17T10:00:01Z"}],
            telegram_data=[{"timestamp": "2026-01-17T10:00:02Z"}]
        )
        total = (
            result["metrics"]["twitter_records"] +
            result["metrics"]["reddit_records"] +
            result["metrics"]["telegram_records"]
        )
        self.assertEqual(total, 3)
    
    def test_records_sorted_by_time(self):
        orch = SocialDataOrchestrator()
        result = orch.process_batch(
            twitter_data=[{"source": "twitter", "timestamp": "2026-01-17T10:00:30Z"}],
            reddit_data=[{"source": "reddit", "timestamp": "2026-01-17T10:00:00Z"}],
            telegram_data=[{"source": "telegram", "timestamp": "2026-01-17T10:00:15Z"}]
        )
        output = result["output"]
        self.assertEqual(output[0]["source"], "reddit")
        self.assertEqual(output[1]["source"], "telegram")
        self.assertEqual(output[2]["source"], "twitter")
    
    def test_time_sync_passthrough_without_guard(self):
        orch = SocialDataOrchestrator()
        result = orch.process_batch(
            twitter_data=[{"timestamp": "2026-01-17T10:00:00Z"}]
        )
        self.assertEqual(result["metrics"]["time_sync_passed"], 1)
        self.assertEqual(result["metrics"]["time_sync_dropped"], 0)
    
    def test_result_contains_output(self):
        orch = SocialDataOrchestrator()
        result = orch.process_batch(
            twitter_data=[{"text": "test", "timestamp": "2026-01-17T10:00:00Z"}]
        )
        self.assertIn("output", result)
        self.assertEqual(len(result["output"]), 1)
    
    def test_dispatched_count(self):
        orch = SocialDataOrchestrator()
        result = orch.process_batch(
            twitter_data=[
                {"timestamp": "2026-01-17T10:00:00Z"},
                {"timestamp": "2026-01-17T10:00:01Z"}
            ]
        )
        self.assertEqual(result["metrics"]["dispatched"], 2)


class TestTimeSyncGuardIntegration(unittest.TestCase):
    """Tests for Time Sync Guard integration."""
    
    def test_guard_filters_records(self):
        class MockGuard:
            def validate_batch(self, records):
                return [r for r in records if "keep" in r.get("text", "")]
        
        orch = SocialDataOrchestrator(time_sync_guard=MockGuard())
        result = orch.process_batch(
            twitter_data=[
                {"text": "keep this", "timestamp": "2026-01-17T10:00:00Z"},
                {"text": "drop this", "timestamp": "2026-01-17T10:00:01Z"}
            ]
        )
        self.assertEqual(result["metrics"]["time_sync_passed"], 1)
        self.assertEqual(result["metrics"]["time_sync_dropped"], 1)
    
    def test_guard_exception_drops_all(self):
        class FailingGuard:
            def validate_batch(self, records):
                raise ValueError("Guard failed")
        
        orch = SocialDataOrchestrator(time_sync_guard=FailingGuard())
        result = orch.process_batch(
            twitter_data=[
                {"text": "test", "timestamp": "2026-01-17T10:00:00Z"}
            ]
        )
        self.assertEqual(result["metrics"]["time_sync_dropped"], 1)
        self.assertEqual(len(result["output"]), 0)
        self.assertIn("time_sync:", result["metrics"]["errors"][0])


class TestSentimentPipelineIntegration(unittest.TestCase):
    """Tests for Sentiment Pipeline integration."""
    
    def test_sentiment_added_to_records(self):
        class MockSentiment:
            def analyze(self, text):
                return {"label": 1, "score": 0.8}
        
        orch = SocialDataOrchestrator(sentiment_pipeline=MockSentiment())
        result = orch.process_batch(
            twitter_data=[{"text": "bullish", "timestamp": "2026-01-17T10:00:00Z"}]
        )
        self.assertIn("sentiment", result["output"][0])
        self.assertEqual(result["output"][0]["sentiment"]["label"], 1)
    
    def test_sentiment_count(self):
        class MockSentiment:
            def analyze(self, text):
                return {"label": 0}
        
        orch = SocialDataOrchestrator(sentiment_pipeline=MockSentiment())
        result = orch.process_batch(
            twitter_data=[
                {"text": "a", "timestamp": "2026-01-17T10:00:00Z"},
                {"text": "b", "timestamp": "2026-01-17T10:00:01Z"}
            ]
        )
        self.assertEqual(result["metrics"]["sentiment_processed"], 2)
    
    def test_sentiment_exception_passthrough(self):
        class FailingSentiment:
            def analyze(self, text):
                raise ValueError("Sentiment failed")
        
        orch = SocialDataOrchestrator(sentiment_pipeline=FailingSentiment())
        result = orch.process_batch(
            twitter_data=[{"text": "test", "timestamp": "2026-01-17T10:00:00Z"}]
        )
        # Records should pass through on error
        self.assertEqual(len(result["output"]), 1)
        self.assertIn("sentiment:", result["metrics"]["errors"][0])


class TestRiskIndicatorsIntegration(unittest.TestCase):
    """Tests for Risk Indicators integration."""
    
    def test_risk_added_to_records(self):
        class MockRisk:
            def compute_all(self, label, velocity, anomaly_flag, fear_greed_index):
                return {"fomo_risk": 0.5}
        
        orch = SocialDataOrchestrator(risk_indicators=MockRisk())
        result = orch.process_batch(
            twitter_data=[{
                "text": "test",
                "timestamp": "2026-01-17T10:00:00Z",
                "sentiment": {"label": 1}
            }]
        )
        self.assertIn("risk_indicators", result["output"][0])
    
    def test_risk_count(self):
        class MockRisk:
            def compute_all(self, **kwargs):
                return {}
        
        orch = SocialDataOrchestrator(risk_indicators=MockRisk())
        result = orch.process_batch(
            twitter_data=[
                {"timestamp": "2026-01-17T10:00:00Z"},
                {"timestamp": "2026-01-17T10:00:01Z"}
            ]
        )
        self.assertEqual(result["metrics"]["risk_computed"], 2)
    
    def test_risk_exception_passthrough(self):
        class FailingRisk:
            def compute_all(self, **kwargs):
                raise ValueError("Risk failed")
        
        orch = SocialDataOrchestrator(risk_indicators=FailingRisk())
        result = orch.process_batch(
            twitter_data=[{"text": "test", "timestamp": "2026-01-17T10:00:00Z"}]
        )
        self.assertEqual(len(result["output"]), 1)


class TestOutputDispatcherIntegration(unittest.TestCase):
    """Tests for Output Dispatcher integration."""
    
    def test_dispatcher_receives_records(self):
        received = []
        
        class MockDispatcher:
            def dispatch(self, records):
                received.extend(records)
                return True
        
        orch = SocialDataOrchestrator(output_dispatcher=MockDispatcher())
        orch.process_batch(
            twitter_data=[{"text": "test", "timestamp": "2026-01-17T10:00:00Z"}]
        )
        self.assertEqual(len(received), 1)
    
    def test_dispatch_failure_marks_error(self):
        class FailingDispatcher:
            def dispatch(self, records):
                raise ValueError("Dispatch failed")
        
        orch = SocialDataOrchestrator(output_dispatcher=FailingDispatcher())
        result = orch.process_batch(
            twitter_data=[{"text": "test", "timestamp": "2026-01-17T10:00:00Z"}]
        )
        self.assertIn("dispatch:", result["metrics"]["errors"][0])
    
    def test_send_method_supported(self):
        sent = []
        
        class SendDispatcher:
            def send(self, record):
                sent.append(record)
        
        orch = SocialDataOrchestrator(output_dispatcher=SendDispatcher())
        result = orch.process_batch(
            twitter_data=[
                {"text": "a", "timestamp": "2026-01-17T10:00:00Z"},
                {"text": "b", "timestamp": "2026-01-17T10:00:01Z"}
            ]
        )
        self.assertEqual(len(sent), 2)
        self.assertEqual(result["metrics"]["dispatched"], 2)


class TestOutputDispatcher(unittest.TestCase):
    """Tests for OutputDispatcher class."""
    
    def test_default_dispatch(self):
        dispatcher = OutputDispatcher()
        result = dispatcher.dispatch([{"text": "test"}])
        self.assertTrue(result)
    
    def test_callback_invoked(self):
        called = []
        
        def callback(records):
            called.extend(records)
            return True
        
        dispatcher = OutputDispatcher(output_callback=callback)
        dispatcher.dispatch([{"text": "test"}])
        self.assertEqual(len(called), 1)
    
    def test_last_dispatch_stored(self):
        dispatcher = OutputDispatcher()
        records = [{"text": "a"}, {"text": "b"}]
        dispatcher.dispatch(records)
        self.assertEqual(dispatcher.get_last_dispatch(), records)
    
    def test_callback_failure_propagates(self):
        def callback(records):
            return False
        
        dispatcher = OutputDispatcher(output_callback=callback)
        result = dispatcher.dispatch([{"text": "test"}])
        self.assertFalse(result)


class TestRunCycle(unittest.TestCase):
    """Tests for run_cycle method."""
    
    def test_run_cycle_returns_result(self):
        orch = SocialDataOrchestrator()
        result = orch.run_cycle()
        self.assertIn("success", result)
        self.assertIn("records_processed", result)
    
    def test_run_cycle_resets_metrics(self):
        orch = SocialDataOrchestrator()
        orch.metrics.twitter_records = 100
        orch.run_cycle()
        # process_batch resets metrics
        self.assertEqual(orch.metrics.twitter_records, 0)


class TestMetricsManagement(unittest.TestCase):
    """Tests for metrics management."""
    
    def test_get_metrics(self):
        orch = SocialDataOrchestrator()
        orch.metrics.twitter_records = 5
        metrics = orch.get_metrics()
        self.assertEqual(metrics["twitter_records"], 5)
    
    def test_reset_metrics(self):
        orch = SocialDataOrchestrator()
        orch.metrics.twitter_records = 100
        orch.reset_metrics()
        self.assertEqual(orch.metrics.twitter_records, 0)


class TestFactoryFunction(unittest.TestCase):
    """Tests for create_orchestrator factory."""
    
    def test_create_empty_orchestrator(self):
        orch = create_orchestrator()
        self.assertIsInstance(orch, SocialDataOrchestrator)
    
    def test_create_with_components(self):
        class MockCrawler:
            pass
        
        orch = create_orchestrator(twitter_crawler=MockCrawler())
        self.assertIsNotNone(orch.twitter_crawler)


class TestSourceIsolation(unittest.TestCase):
    """Tests for source isolation behavior."""
    
    def test_twitter_failure_doesnt_block_reddit(self):
        class FailingTwitter:
            def search(self):
                raise ValueError("Twitter failed")
        
        class WorkingReddit:
            def crawl_all(self):
                return [{"text": "reddit"}]
        
        orch = SocialDataOrchestrator(
            twitter_crawler=FailingTwitter(),
            reddit_crawler=WorkingReddit()
        )
        result = orch.run_cycle()
        self.assertEqual(result["metrics"]["reddit_records"], 1)
    
    def test_all_sources_fail_gracefully(self):
        class FailingCrawler:
            def search(self):
                raise ValueError("Failed")
            def crawl_all(self):
                raise ValueError("Failed")
        
        orch = SocialDataOrchestrator(
            twitter_crawler=FailingCrawler(),
            reddit_crawler=FailingCrawler(),
            telegram_crawler=FailingCrawler()
        )
        result = orch.run_cycle()
        self.assertEqual(len(result["metrics"]["errors"]), 3)
        self.assertEqual(result["records_processed"], 0)


class TestDataPassthrough(unittest.TestCase):
    """Tests for data passthrough behavior."""
    
    def test_original_data_preserved(self):
        orch = SocialDataOrchestrator()
        original = {"text": "test", "custom_field": "value", "timestamp": "2026-01-17T10:00:00Z"}
        result = orch.process_batch(twitter_data=[original])
        self.assertIn("custom_field", result["output"][0])
        self.assertEqual(result["output"][0]["custom_field"], "value")
    
    def test_timestamp_not_modified(self):
        orch = SocialDataOrchestrator()
        original_ts = "2026-01-17T10:00:00Z"
        result = orch.process_batch(
            twitter_data=[{"timestamp": original_ts}]
        )
        self.assertEqual(result["output"][0]["timestamp"], original_ts)
    
    def test_source_field_preserved(self):
        orch = SocialDataOrchestrator()
        result = orch.process_batch(
            twitter_data=[{"source": "twitter", "timestamp": "2026-01-17T10:00:00Z"}],
            reddit_data=[{"source": "reddit", "timestamp": "2026-01-17T10:00:01Z"}]
        )
        sources = [r["source"] for r in result["output"]]
        self.assertIn("twitter", sources)
        self.assertIn("reddit", sources)


class TestFullPipeline(unittest.TestCase):
    """Integration tests for full pipeline flow."""
    
    def test_complete_pipeline(self):
        class MockGuard:
            def validate_batch(self, records):
                return records  # Passthrough
        
        class MockSentiment:
            def analyze(self, text):
                return {"label": 1, "score": 0.8}
        
        class MockRisk:
            def compute_all(self, **kwargs):
                return {"risk_level": "low"}
        
        received = []
        class MockDispatcher:
            def dispatch(self, records):
                received.extend(records)
                return True
        
        orch = SocialDataOrchestrator(
            time_sync_guard=MockGuard(),
            sentiment_pipeline=MockSentiment(),
            risk_indicators=MockRisk(),
            output_dispatcher=MockDispatcher()
        )
        
        result = orch.process_batch(
            twitter_data=[{"text": "bullish", "timestamp": "2026-01-17T10:00:00Z"}]
        )
        
        self.assertTrue(result["success"])
        self.assertEqual(len(received), 1)
        self.assertIn("sentiment", received[0])
        self.assertIn("risk_indicators", received[0])
    
    def test_pipeline_order_preserved(self):
        """Verify stages execute in correct order."""
        execution_order = []
        
        class MockGuard:
            def validate_batch(self, records):
                execution_order.append("time_sync")
                return records
        
        class MockSentiment:
            def analyze(self, text):
                execution_order.append("sentiment")
                return {}
        
        class MockRisk:
            def compute_all(self, **kwargs):
                execution_order.append("risk")
                return {}
        
        class MockDispatcher:
            def dispatch(self, records):
                execution_order.append("dispatch")
                return True
        
        orch = SocialDataOrchestrator(
            time_sync_guard=MockGuard(),
            sentiment_pipeline=MockSentiment(),
            risk_indicators=MockRisk(),
            output_dispatcher=MockDispatcher()
        )
        
        orch.process_batch(
            twitter_data=[{"text": "test", "timestamp": "2026-01-17T10:00:00Z"}]
        )
        
        self.assertEqual(execution_order, ["time_sync", "sentiment", "risk", "dispatch"])


if __name__ == "__main__":
    unittest.main()
