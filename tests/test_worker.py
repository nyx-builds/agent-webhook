"""Tests for the background delivery worker."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx

from agent_webhook.engine import DeliveryEngine
from agent_webhook.models import DeliveryStatus, WebhookEndpoint
from agent_webhook.service import WebhookService
from agent_webhook.store import WebhookStore
from agent_webhook.worker import DeliveryWorker, WorkerConfig, WorkerStats


# ── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def store(tmp_path):
    """JSON store backed by a temp file."""
    return WebhookStore(str(tmp_path / "worker_test.json"))


@pytest.fixture
def engine(store):
    return DeliveryEngine(store)


@pytest.fixture
def service(store):
    return WebhookService(store=store)


@pytest.fixture
def endpoint(service):
    return service.create_endpoint(
        name="Test Endpoint",
        url="https://api.example.com/webhook",
    )


# ── WorkerConfig tests ──────────────────────────────────────────────


class TestWorkerConfig:
    def test_defaults(self):
        cfg = WorkerConfig()
        assert cfg.poll_interval == 5.0
        assert cfg.max_concurrent == 10
        assert cfg.batch_size == 50
        assert cfg.max_retries_per_delivery == 0
        assert cfg.drain_on_stop is True

    def test_custom_config(self):
        cfg = WorkerConfig(
            poll_interval=1.0,
            max_concurrent=5,
            batch_size=20,
            max_retries_per_delivery=3,
            drain_on_stop=False,
        )
        assert cfg.poll_interval == 1.0
        assert cfg.max_concurrent == 5
        assert cfg.batch_size == 20
        assert cfg.max_retries_per_delivery == 3
        assert cfg.drain_on_stop is False

    def test_invalid_poll_interval(self):
        with pytest.raises(ValueError, match="poll_interval"):
            WorkerConfig(poll_interval=0.01)

    def test_invalid_max_concurrent(self):
        with pytest.raises(ValueError, match="max_concurrent"):
            WorkerConfig(max_concurrent=0)

    def test_invalid_batch_size(self):
        with pytest.raises(ValueError, match="batch_size"):
            WorkerConfig(batch_size=-1)

    def test_invalid_max_retries(self):
        with pytest.raises(ValueError, match="max_retries"):
            WorkerConfig(max_retries_per_delivery=-1)


# ── WorkerStats tests ───────────────────────────────────────────────


class TestWorkerStats:
    def test_to_dict_initial(self):
        stats = WorkerStats()
        d = stats.to_dict()
        assert d["running"] is False
        assert d["poll_count"] == 0
        assert d["deliveries_processed"] == 0
        assert d["started_at"] is None

    def test_to_dict_populated(self):
        now = datetime.now(timezone.utc)
        stats = WorkerStats(
            started_at=now,
            poll_count=5,
            deliveries_processed=10,
            deliveries_succeeded=7,
            deliveries_failed=3,
        )
        d = stats.to_dict()
        assert d["running"] is True
        assert d["poll_count"] == 5
        assert d["deliveries_processed"] == 10
        assert d["deliveries_succeeded"] == 7
        assert d["deliveries_failed"] == 3

    def test_reset(self):
        stats = WorkerStats(
            started_at=datetime.now(timezone.utc),
            poll_count=5,
            deliveries_processed=10,
        )
        stats.reset()
        assert stats.started_at is None
        assert stats.poll_count == 0
        assert stats.deliveries_processed == 0


# ── DeliveryWorker basic tests ──────────────────────────────────────


class TestDeliveryWorkerBasic:
    def test_init_defaults(self, service):
        worker = DeliveryWorker(service)
        assert worker.config.poll_interval == 5.0
        assert worker.is_running is False
        assert worker.stats.poll_count == 0

    def test_init_custom_config(self, service):
        cfg = WorkerConfig(poll_interval=2.0, max_concurrent=3)
        worker = DeliveryWorker(service, cfg)
        assert worker.config.poll_interval == 2.0
        assert worker.config.max_concurrent == 3

    def test_is_running_false_before_start(self, service):
        worker = DeliveryWorker(service)
        assert worker.is_running is False

    async def test_start_sets_running(self, service):
        worker = DeliveryWorker(service, WorkerConfig(poll_interval=0.2))
        await worker.start()
        assert worker.is_running is True
        await worker.stop()
        assert worker.is_running is False

    async def test_start_idempotent(self, service):
        worker = DeliveryWorker(service, WorkerConfig(poll_interval=0.2))
        await worker.start()
        await worker.start()  # Should not error
        assert worker.is_running is True
        await worker.stop()

    async def test_stop_when_not_started(self, service):
        worker = DeliveryWorker(service)
        await worker.stop()  # Should not error

    async def test_trigger_manual_poll_empty(self, service):
        worker = DeliveryWorker(service)
        count = await worker.trigger()
        assert count == 0

    async def test_get_stats_not_running(self, service):
        worker = DeliveryWorker(service)
        stats = worker.get_stats()
        assert stats["running"] is False
        assert stats["queue_depth"] == 0
        assert "config" in stats


# ── DeliveryWorker with real deliveries ─────────────────────────────


class TestDeliveryWorkerProcessing:
    @respx.mock
    async def test_worker_delivers_pending(self, service, endpoint):
        """Worker should deliver a pending webhook."""
        respx.post(endpoint.url).mock(return_value=httpx.Response(200))

        # Create a delivery without processing it
        from agent_webhook.models import WebhookDelivery

        delivery = WebhookDelivery(
            endpoint_id=endpoint.id,
            payload={"msg": "hello"},
        )
        service.store.add_delivery(delivery)
        assert delivery.status == DeliveryStatus.PENDING

        worker = DeliveryWorker(service, WorkerConfig(poll_interval=0.1))
        count = await worker.trigger()

        assert count == 1
        result = service.store.get_delivery(delivery.id)
        assert result.status == DeliveryStatus.SUCCESS
        assert worker.stats.deliveries_succeeded == 1
        await worker.stop()

    @respx.mock
    async def test_worker_skips_completed(self, service, endpoint):
        """Worker should not re-process successful deliveries."""
        respx.post(endpoint.url).mock(return_value=httpx.Response(200))

        result = await service.send_webhook(endpoint.id, {"msg": "first"})
        assert result.status == DeliveryStatus.SUCCESS

        worker = DeliveryWorker(service)
        count = await worker.trigger()
        assert count == 0  # Nothing pending

    @respx.mock
    async def test_worker_retries(self, service):
        """Worker should retry failed deliveries when due."""
        # Create an endpoint with 1 retry
        ep = service.create_endpoint(
            name="Retry Endpoint",
            url="https://api.example.com/retry",
            max_retries=1,
            initial_delay_seconds=0.1,
            backoff_multiplier=1.0,  # delay stays at 0.1s
        )

        # First attempt fails with 503 (retryable)
        with respx.mock:
            respx.post(ep.url).mock(return_value=httpx.Response(503))
            result = await service.send_webhook(ep.id, {"msg": "test"})

        assert result.status == DeliveryStatus.RETRYING

        # Wait for the retry to be due
        await asyncio.sleep(0.2)

        # Now worker should pick it up and retry
        with respx.mock:
            respx.post(ep.url).mock(return_value=httpx.Response(200))
            worker = DeliveryWorker(service)
            await worker.trigger()

            final = service.store.get_delivery(result.id)
            assert final.status == DeliveryStatus.SUCCESS

    @respx.mock
    async def test_worker_dead_letters_after_max_retries(self, service):
        """After max retries exhausted, delivery goes to dead letter queue."""
        ep = service.create_endpoint(
            name="Failing Endpoint",
            url="https://api.example.com/fail",
            max_retries=0,  # No retries — immediate dead letter
            initial_delay_seconds=0.1,
        )

        respx.post(ep.url).mock(return_value=httpx.Response(500))

        # Send and process (will fail and go to DLQ since no retries)
        result = await service.send_webhook(ep.id, {"msg": "doomed"})
        assert result.status == DeliveryStatus.DEAD_LETTER

        worker = DeliveryWorker(service)
        count = await worker.trigger()
        assert count == 0  # Nothing pending

    @respx.mock
    async def test_worker_concurrent_deliveries(self, service):
        """Worker should process multiple deliveries concurrently."""
        ep = service.create_endpoint(
            name="Concurrent Endpoint",
            url="https://api.example.com/concurrent",
        )

        # Simulate slow responses
        async def slow_handler(request):
            await asyncio.sleep(0.1)
            return httpx.Response(200)

        respx.post(ep.url).mock(side_effect=slow_handler)

        from agent_webhook.models import WebhookDelivery

        # Create 5 pending deliveries
        delivery_ids = []
        for i in range(5):
            d = WebhookDelivery(
                endpoint_id=ep.id,
                payload={"index": i},
            )
            service.store.add_delivery(d)
            delivery_ids.append(d.id)

        worker = DeliveryWorker(service, WorkerConfig(max_concurrent=5, poll_interval=0.1))
        start = time.monotonic()
        await worker.trigger()
        elapsed = time.monotonic() - start

        # With concurrency 5 and 0.1s each, total should be ~0.1s, not 0.5s
        assert elapsed < 0.35, f"Expected concurrent processing, took {elapsed:.2f}s"

        # All should succeed
        for did in delivery_ids:
            d = service.store.get_delivery(did)
            assert d.status == DeliveryStatus.SUCCESS

        assert worker.stats.deliveries_succeeded == 5


# ── DeliveryWorker lifecycle tests ──────────────────────────────────


class TestDeliveryWorkerLifecycle:
    @respx.mock
    async def test_start_stop_drains_pending(self, service, endpoint):
        """When drain_on_stop=True, worker processes pending before stopping."""
        respx.post(endpoint.url).mock(return_value=httpx.Response(200))

        from agent_webhook.models import WebhookDelivery

        d = WebhookDelivery(endpoint_id=endpoint.id, payload={"x": 1})
        service.store.add_delivery(d)

        worker = DeliveryWorker(service, WorkerConfig(poll_interval=0.1, drain_on_stop=True))
        await worker.start()
        await worker.stop()

        result = service.store.get_delivery(d.id)
        assert result.status == DeliveryStatus.SUCCESS

    @respx.mock
    async def test_start_stop_no_drain(self, service, endpoint):
        """When drain_on_stop=False, pending deliveries remain after stop."""
        respx.post(endpoint.url).mock(return_value=httpx.Response(200))

        from agent_webhook.models import WebhookDelivery

        d = WebhookDelivery(endpoint_id=endpoint.id, payload={"x": 1})
        service.store.add_delivery(d)

        worker = DeliveryWorker(service, WorkerConfig(poll_interval=10.0, drain_on_stop=False))
        await worker.start()
        # Stop immediately without draining
        await worker.stop(drain=False)

        result = service.store.get_delivery(d.id)
        # Should still be pending since we didn't drain and poll_interval is 10s
        assert result.status == DeliveryStatus.PENDING

    @respx.mock
    async def test_worker_runs_continuously(self, service, endpoint):
        """Worker should process deliveries that arrive after starting."""
        respx.post(endpoint.url).mock(return_value=httpx.Response(200))

        worker = DeliveryWorker(service, WorkerConfig(poll_interval=0.2))
        await worker.start()

        # Wait a moment, then add a delivery
        await asyncio.sleep(0.1)

        from agent_webhook.models import WebhookDelivery

        d = WebhookDelivery(endpoint_id=endpoint.id, payload={"x": 1})
        service.store.add_delivery(d)

        # Wait for the worker to pick it up
        await asyncio.sleep(0.5)

        result = service.store.get_delivery(d.id)
        assert result.status == DeliveryStatus.SUCCESS

        await worker.stop()

    async def test_worker_handles_empty_queue(self, service):
        """Worker should handle empty queue gracefully."""
        worker = DeliveryWorker(service, WorkerConfig(poll_interval=0.1))
        await worker.start()
        await asyncio.sleep(0.3)
        assert worker.stats.poll_count >= 2
        assert worker.stats.deliveries_processed == 0
        await worker.stop()

    @respx.mock
    async def test_worker_error_handling(self, service):
        """Worker should continue after a delivery error."""
        # Non-existent endpoint — will cause processing to fail gracefully
        from agent_webhook.models import WebhookDelivery

        d = WebhookDelivery(endpoint_id="nonexistent-id", payload={"x": 1})
        service.store.add_delivery(d)

        worker = DeliveryWorker(service, WorkerConfig(poll_interval=0.1))
        count = await worker.trigger()
        # It will be counted as processed but failed/abandoned
        assert count == 1

        result = service.store.get_delivery(d.id)
        assert result.status == DeliveryStatus.ABANDONED
        await worker.stop()


# ── DeliveryWorker stats tests ──────────────────────────────────────


class TestDeliveryWorkerStats:
    @respx.mock
    async def test_stats_track_processing(self, service, endpoint):
        """Stats should accurately track processed deliveries."""
        respx.post(endpoint.url).mock(return_value=httpx.Response(200))

        from agent_webhook.models import WebhookDelivery

        for i in range(3):
            d = WebhookDelivery(endpoint_id=endpoint.id, payload={"i": i})
            service.store.add_delivery(d)

        worker = DeliveryWorker(service)
        await worker.trigger()

        assert worker.stats.deliveries_processed == 3
        assert worker.stats.deliveries_succeeded == 3
        assert worker.stats.deliveries_failed == 0
        assert worker.stats.last_delivery_at is not None
        await worker.stop()

    @respx.mock
    async def test_stats_track_failures(self, service):
        """Stats should track failed deliveries."""
        ep = service.create_endpoint(
            name="Fail",
            url="https://api.example.com/f",
            max_retries=0,
        )
        respx.post(ep.url).mock(return_value=httpx.Response(500))

        from agent_webhook.models import WebhookDelivery

        d = WebhookDelivery(endpoint_id=ep.id, payload={"x": 1})
        service.store.add_delivery(d)

        worker = DeliveryWorker(service)
        await worker.trigger()

        assert worker.stats.deliveries_processed == 1
        assert worker.stats.deliveries_dead_lettered == 1
        await worker.stop()

    async def test_get_stats_with_config(self, service):
        """get_stats should include configuration."""
        cfg = WorkerConfig(poll_interval=3.0, max_concurrent=7, batch_size=15)
        worker = DeliveryWorker(service, cfg)
        stats = worker.get_stats()
        assert stats["config"]["poll_interval"] == 3.0
        assert stats["config"]["max_concurrent"] == 7
        assert stats["config"]["batch_size"] == 15

    @respx.mock
    async def test_queue_depth_in_stats(self, service, endpoint):
        """Stats should report queue depth."""
        respx.post(endpoint.url).mock(return_value=httpx.Response(200))

        from agent_webhook.models import WebhookDelivery

        d1 = WebhookDelivery(endpoint_id=endpoint.id, payload={"x": 1})
        d2 = WebhookDelivery(endpoint_id=endpoint.id, payload={"x": 2})
        service.store.add_delivery(d1)
        service.store.add_delivery(d2)

        worker = DeliveryWorker(service)
        stats = worker.get_stats()
        assert stats["queue_depth"] == 2

        await worker.trigger()
        stats = worker.get_stats()
        assert stats["queue_depth"] == 0
        await worker.stop()


# ── DeliveryWorker batch size tests ─────────────────────────────────


class TestDeliveryWorkerBatchSize:
    @respx.mock
    async def test_batch_size_limits_per_poll(self, service, endpoint):
        """Batch size should limit deliveries processed per poll."""
        respx.post(endpoint.url).mock(return_value=httpx.Response(200))

        from agent_webhook.models import WebhookDelivery

        # Create 10 pending deliveries
        for i in range(10):
            d = WebhookDelivery(endpoint_id=endpoint.id, payload={"i": i})
            service.store.add_delivery(d)

        worker = DeliveryWorker(service, WorkerConfig(batch_size=3))
        count = await worker.trigger()
        assert count == 3

        # Remaining should still be pending
        stats = worker.get_stats()
        assert stats["queue_depth"] == 7
        await worker.stop()

    @respx.mock
    async def test_batch_size_zero_unlimited(self, service, endpoint):
        """batch_size=0 means process all."""
        respx.post(endpoint.url).mock(return_value=httpx.Response(200))

        from agent_webhook.models import WebhookDelivery

        for i in range(15):
            d = WebhookDelivery(endpoint_id=endpoint.id, payload={"i": i})
            service.store.add_delivery(d)

        worker = DeliveryWorker(service, WorkerConfig(batch_size=0))
        count = await worker.trigger()
        assert count == 15
        assert worker.stats.deliveries_succeeded == 15
        await worker.stop()
