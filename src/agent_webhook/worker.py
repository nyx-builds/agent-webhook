"""Background delivery worker — automatically processes pending and retrying deliveries.

The ``DeliveryWorker`` runs an asyncio loop that periodically polls the store for
deliveries that are PENDING or RETRYING (and past their ``next_retry_at``),
then processes them with configurable concurrency.

Usage::

    import asyncio
    from agent_webhook.service import WebhookService
    from agent_webhook.worker import DeliveryWorker

    service = WebhookService(store_path="webhooks.db")
    worker = DeliveryWorker(service)

    async def main():
        await worker.start()
        # ... send webhooks, they'll be delivered/retried automatically ...
        await asyncio.sleep(60)
        await worker.stop()

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .models import DeliveryStatus
from .service import WebhookService

logger = logging.getLogger(__name__)


@dataclass
class WorkerStats:
    """Runtime statistics for the delivery worker."""

    started_at: datetime | None = None
    poll_count: int = 0
    deliveries_processed: int = 0
    deliveries_succeeded: int = 0
    deliveries_failed: int = 0
    deliveries_retried: int = 0
    deliveries_dead_lettered: int = 0
    errors: int = 0
    last_poll_at: datetime | None = None
    last_delivery_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "running": self.started_at is not None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "poll_count": self.poll_count,
            "deliveries_processed": self.deliveries_processed,
            "deliveries_succeeded": self.deliveries_succeeded,
            "deliveries_failed": self.deliveries_failed,
            "deliveries_retried": self.deliveries_retried,
            "deliveries_dead_lettered": self.deliveries_dead_lettered,
            "errors": self.errors,
            "last_poll_at": self.last_poll_at.isoformat() if self.last_poll_at else None,
            "last_delivery_at": self.last_delivery_at.isoformat() if self.last_delivery_at else None,
        }

    def reset(self) -> None:
        self.started_at = None
        self.poll_count = 0
        self.deliveries_processed = 0
        self.deliveries_succeeded = 0
        self.deliveries_failed = 0
        self.deliveries_retried = 0
        self.deliveries_dead_lettered = 0
        self.errors = 0
        self.last_poll_at = None
        self.last_delivery_at = None


@dataclass
class WorkerConfig:
    """Configuration for the delivery worker.

    Attributes:
        poll_interval: Seconds between polls for pending deliveries (default 5).
        max_concurrent: Maximum concurrent delivery tasks (default 10).
        batch_size: Maximum deliveries to pick up per poll cycle (default 50).
            Set to 0 for unlimited (all pending).
        max_retries_per_delivery: Hard cap on processing attempts per delivery
            in a single worker session, independent of the endpoint retry policy.
            0 = no cap (default).
        drain_on_stop: Process all remaining pending deliveries before stopping (default True).
    """

    poll_interval: float = 5.0
    max_concurrent: int = 10
    batch_size: int = 50
    max_retries_per_delivery: int = 0
    drain_on_stop: bool = True

    def __post_init__(self) -> None:
        if self.poll_interval < 0.1:
            raise ValueError("poll_interval must be >= 0.1")
        if self.max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")
        if self.batch_size < 0:
            raise ValueError("batch_size must be >= 0")
        if self.max_retries_per_delivery < 0:
            raise ValueError("max_retries_per_delivery must be >= 0")


class DeliveryWorker:
    """Async background worker that processes pending and retrying deliveries.

    The worker polls the store at a configurable interval and processes due
    deliveries with bounded concurrency via an ``asyncio.Semaphore``.

    Thread-safety: the worker is designed to run within a single event loop.
    Use ``start()`` / ``stop()`` to control the lifecycle.
    """

    def __init__(
        self,
        service: WebhookService,
        config: WorkerConfig | None = None,
    ) -> None:
        self._service = service
        self._config = config or WorkerConfig()
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._semaphore: asyncio.Semaphore | None = None
        self._stats = WorkerStats()
        self._processed_ids: set[str] = set()  # track IDs processed this session

    @property
    def config(self) -> WorkerConfig:
        return self._config

    @property
    def stats(self) -> WorkerStats:
        return self._stats

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        """Start the worker background loop."""
        if self.is_running:
            return
        self._stop_event.clear()
        self._semaphore = asyncio.Semaphore(self._config.max_concurrent)
        self._stats.started_at = datetime.now(timezone.utc)
        self._task = asyncio.create_task(self._run(), name="webhook-worker")

    async def stop(self, drain: bool | None = None) -> None:
        """Signal the worker to stop and wait for it to finish.

        Args:
            drain: Override config.drain_on_stop. If True, processes remaining
                pending deliveries before shutting down.
        """
        if self._task is None:
            return

        drain = self._config.drain_on_stop if drain is None else drain
        self._stop_event.set()

        if drain:
            # Process remaining pending deliveries
            await self._drain_pending()

        try:
            await asyncio.wait_for(self._task, timeout=30.0)
        except asyncio.TimeoutError:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        finally:
            self._task = None
            self._semaphore = None

    async def trigger(self) -> int:
        """Manually trigger one poll cycle. Returns the number of deliveries processed.

        Useful for testing or when you want immediate processing without waiting
        for the next poll interval.
        """
        return await self._poll_once()

    async def _run(self) -> None:
        """Main worker loop."""
        logger.info("Webhook delivery worker started (poll_interval=%.1fs, max_concurrent=%d)",
                    self._config.poll_interval, self._config.max_concurrent)
        try:
            while not self._stop_event.is_set():
                await self._poll_once()
                # Wait for poll interval or stop signal
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._config.poll_interval,
                    )
                except asyncio.TimeoutError:
                    pass  # Normal — interval elapsed, poll again
        except asyncio.CancelledError:
            logger.info("Webhook delivery worker cancelled")
            raise
        except Exception:
            logger.exception("Webhook delivery worker crashed")
            self._stats.errors += 1
            raise

        logger.info("Webhook delivery worker stopped")

    async def _poll_once(self) -> int:
        """Execute one poll cycle. Returns number of deliveries dispatched."""
        self._stats.poll_count += 1
        self._stats.last_poll_at = datetime.now(timezone.utc)

        # Fire due schedules first (creates pending deliveries)
        schedule_count = 0
        if hasattr(self._service, "process_due_schedules"):
            try:
                created = await self._service.process_due_schedules()
                schedule_count = len(created)
            except Exception:
                logger.debug("Schedule processing skipped (store may not support it)")

        pending = self._service.store.pending_deliveries()
        if not pending:
            return schedule_count

        # Apply batch size limit
        if self._config.batch_size > 0:
            pending = pending[: self._config.batch_size]

        # Create tasks with bounded concurrency
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._config.max_concurrent)

        tasks = [
            asyncio.create_task(self._process_one(d.id))
            for d in pending
        ]

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        return len(pending) + schedule_count

    async def _process_one(self, delivery_id: str) -> None:
        """Process a single delivery with semaphore-controlled concurrency."""
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._config.max_concurrent)

        async with self._semaphore:
            # Skip if already processed in this session beyond retry cap
            if (
                self._config.max_retries_per_delivery > 0
                and delivery_id in self._processed_ids
            ):
                delivery = self._service.store.get_delivery(delivery_id)
                if delivery and delivery.current_attempt_number() >= self._config.max_retries_per_delivery:
                    return

            try:
                result = await self._service.engine.process_delivery(delivery_id)
                if result is None:
                    return

                self._stats.deliveries_processed += 1
                self._stats.last_delivery_at = datetime.now(timezone.utc)
                self._processed_ids.add(delivery_id)

                if result.status == DeliveryStatus.SUCCESS:
                    self._stats.deliveries_succeeded += 1
                elif result.status == DeliveryStatus.RETRYING:
                    self._stats.deliveries_retried += 1
                elif result.status == DeliveryStatus.DEAD_LETTER:
                    self._stats.deliveries_dead_lettered += 1
                elif result.status in (DeliveryStatus.FAILED, DeliveryStatus.ABANDONED):
                    self._stats.deliveries_failed += 1

            except Exception:
                logger.exception("Error processing delivery %s", delivery_id)
                self._stats.errors += 1

    async def _drain_pending(self) -> None:
        """Process all remaining pending deliveries before shutdown."""
        logger.info("Draining pending deliveries before shutdown...")
        max_rounds = 100  # Safety valve to prevent infinite loops
        rounds = 0
        while rounds < max_rounds:
            count = await self._poll_once()
            if count == 0:
                break
            rounds += 1
            # Brief yield between rounds
            await asyncio.sleep(0.1)
        logger.info("Drain complete (rounds=%d)", rounds)

    def get_stats(self) -> dict[str, Any]:
        """Get worker statistics as a dictionary."""
        stats = self._stats.to_dict()
        # Add queue depth
        try:
            pending = self._service.store.pending_deliveries()
            stats["queue_depth"] = len(pending)
        except Exception:
            stats["queue_depth"] = None
        stats["config"] = {
            "poll_interval": self._config.poll_interval,
            "max_concurrent": self._config.max_concurrent,
            "batch_size": self._config.batch_size,
            "max_retries_per_delivery": self._config.max_retries_per_delivery,
            "drain_on_stop": self._config.drain_on_stop,
        }
        return stats
