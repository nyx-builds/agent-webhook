"""CLI for agent-webhook — Rich terminal interface for webhook management."""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import click
from rich.console import Console
from rich.table import Table

from .models import (
    DeadLetterEntry,
    DeliveryStatus,
    EventSubscription,
    Header,
    PayloadTransform,
    RateLimit,
    RateLimitPeriod,
    RelayRule,
    RetryPolicy,
    SigningAlgorithm,
    TransformType,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookMethod,
    WebhookStatus,
)
from .store import WebhookStore

console = Console()

DEFAULT_STORE_PATH = "webhook_store.db"


def _get_store_impl(store_path: str | None = None):
    """Get the best store implementation (SQLite if available)."""
    path = store_path or DEFAULT_STORE_PATH
    if path.endswith(".db"):
        try:
            from .store_sqlite import SQLiteStore
            return SQLiteStore(path)
        except Exception:
            pass
    return WebhookStore(path)


def get_store(store_path: str | None = None) -> WebhookStore:
    return _get_store_impl(store_path)


def format_dt(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ── Endpoint Commands ──────────────────────────────────────────────


@click.group()
@click.option("--store", "-s", default=None, help="Path to store file")
@click.pass_context
def cli(ctx: click.Context, store: str | None) -> None:
    """Webhook management, delivery, and relay for autonomous agents."""
    ctx.ensure_object(dict)
    ctx.obj["store_path"] = store


@cli.group()
def endpoint() -> None:
    """Manage webhook endpoints."""
    pass


@endpoint.command("add")
@click.argument("name")
@click.argument("url")
@click.option("--method", "-m", type=click.Choice(["POST", "PUT", "PATCH", "GET", "DELETE"]), default="POST")
@click.option("--header", "-H", multiple=True, help="Custom header in 'Name: Value' format")
@click.option("--tag", "-t", multiple=True, help="Tags for filtering")
@click.option("--secret", help="HMAC signing secret")
@click.option("--timeout", type=float, default=30.0, help="Request timeout in seconds")
@click.option("--description", "-d", help="Description")
@click.option("--max-retries", type=int, default=3, help="Max retry attempts")
@click.pass_context
def endpoint_add(
    ctx: click.Context,
    name: str,
    url: str,
    method: str,
    header: tuple[str, ...],
    tag: tuple[str, ...],
    secret: str | None,
    timeout: float,
    description: str | None,
    max_retries: int,
) -> None:
    """Register a new webhook endpoint."""
    store = get_store(ctx.obj["store_path"])
    headers = []
    for h in header:
        if ":" not in h:
            console.print(f"[red]Invalid header format: {h}. Use 'Name: Value'[/red]")
            sys.exit(1)
        hname, hvalue = h.split(":", 1)
        headers.append(Header(name=hname.strip(), value=hvalue.strip()))

    retry_policy = RetryPolicy(max_retries=max_retries)

    endpoint_obj = WebhookEndpoint(
        name=name,
        url=url,
        method=WebhookMethod(method),
        headers=headers,
        tags=list(tag),
        secret=secret,
        timeout_seconds=timeout,
        description=description,
        retry_policy=retry_policy,
    )
    store.add_endpoint(endpoint_obj)
    console.print(f"[green]✓[/green] Endpoint created: [bold]{endpoint_obj.name}[/bold] (ID: {endpoint_obj.id})")


@endpoint.command("list")
@click.option("--status", "-s", type=click.Choice(["active", "paused", "disabled"]))
@click.option("--tag", "-t", help="Filter by tag")
@click.pass_context
def endpoint_list(ctx: click.Context, status: str | None, tag: str | None) -> None:
    """List all webhook endpoints."""
    store = get_store(ctx.obj["store_path"])
    ws = WebhookStatus(status) if status else None
    endpoints = store.list_endpoints(status=ws, tag=tag)

    if not endpoints:
        console.print("[yellow]No endpoints found.[/yellow]")
        return

    table = Table(title="Webhook Endpoints")
    table.add_column("ID", style="dim", max_width=12)
    table.add_column("Name", style="bold")
    table.add_column("URL")
    table.add_column("Method")
    table.add_column("Status")
    table.add_column("Tags")
    table.add_column("Created")

    for e in endpoints:
        tags_str = ", ".join(e.tags) if e.tags else "—"
        status_style = {"active": "green", "paused": "yellow", "disabled": "red"}.get(e.status.value, "")
        table.add_row(
            e.id[:8],
            e.name,
            e.url[:50],
            e.method.value,
            f"[{status_style}]{e.status.value}[/{status_style}]",
            tags_str,
            format_dt(e.created_at),
        )

    console.print(table)


@endpoint.command("show")
@click.argument("endpoint_id")
@click.pass_context
def endpoint_show(ctx: click.Context, endpoint_id: str) -> None:
    """Show details of a webhook endpoint."""
    store = get_store(ctx.obj["store_path"])
    ep = store.get_endpoint(endpoint_id)
    if ep is None:
        # Try partial match
        endpoints = store.list_endpoints()
        matches = [e for e in endpoints if e.id.startswith(endpoint_id)]
        if len(matches) == 1:
            ep = matches[0]
        else:
            console.print(f"[red]Endpoint not found: {endpoint_id}[/red]")
            sys.exit(1)

    console.print(f"\n[bold]Endpoint: {ep.name}[/bold]")
    console.print(f"  ID:          {ep.id}")
    console.print(f"  URL:         {ep.url}")
    console.print(f"  Method:      {ep.method.value}")
    console.print(f"  Status:      {ep.status.value}")
    console.print(f"  Secret:      {'configured' if ep.secret else 'none'}")
    console.print(f"  Timeout:     {ep.timeout_seconds}s")
    console.print(f"  Tags:        {', '.join(ep.tags) or '—'}")
    console.print(f"  Description: {ep.description or '—'}")
    console.print(f"  Created:     {format_dt(ep.created_at)}")
    console.print(f"  Updated:     {format_dt(ep.updated_at)}")
    if ep.headers:
        console.print("  Custom Headers:")
        for h in ep.headers:
            console.print(f"    {h.name}: {h.value}")
    console.print(f"  Retry Policy: max={ep.retry_policy.max_retries}, backoff={ep.retry_policy.backoff_multiplier}x")

    # Show subscriptions for this endpoint
    subs = store.list_subscriptions(endpoint_id=ep.id)
    if subs:
        console.print(f"  Subscriptions:")
        for s in subs:
            console.print(f"    {s.id[:8]}: {', '.join(s.event_types)}")


@endpoint.command("pause")
@click.argument("endpoint_id")
@click.pass_context
def endpoint_pause(ctx: click.Context, endpoint_id: str) -> None:
    """Pause a webhook endpoint."""
    store = get_store(ctx.obj["store_path"])
    ep = store.update_endpoint(endpoint_id, status=WebhookStatus.PAUSED)
    if ep is None:
        console.print(f"[red]Endpoint not found: {endpoint_id}[/red]")
        sys.exit(1)
    console.print(f"[yellow]⏸[/yellow] Endpoint '{ep.name}' paused.")


@endpoint.command("resume")
@click.argument("endpoint_id")
@click.pass_context
def endpoint_resume(ctx: click.Context, endpoint_id: str) -> None:
    """Resume a paused webhook endpoint."""
    store = get_store(ctx.obj["store_path"])
    ep = store.update_endpoint(endpoint_id, status=WebhookStatus.ACTIVE)
    if ep is None:
        console.print(f"[red]Endpoint not found: {endpoint_id}[/red]")
        sys.exit(1)
    console.print(f"[green]▶[/green] Endpoint '{ep.name}' resumed.")


@endpoint.command("delete")
@click.argument("endpoint_id")
@click.pass_context
def endpoint_delete(ctx: click.Context, endpoint_id: str) -> None:
    """Delete a webhook endpoint."""
    store = get_store(ctx.obj["store_path"])
    if store.delete_endpoint(endpoint_id):
        console.print(f"[red]✗[/red] Endpoint {endpoint_id} deleted.")
    else:
        console.print(f"[red]Endpoint not found: {endpoint_id}[/red]")
        sys.exit(1)


# ── Delivery Commands ──────────────────────────────────────────────


@cli.group()
def delivery() -> None:
    """Manage webhook deliveries."""
    pass


@delivery.command("list")
@click.option("--endpoint", "-e", help="Filter by endpoint ID")
@click.option("--status", "-s", type=click.Choice(["pending", "in_progress", "success", "failed", "retrying", "abandoned"]))
@click.option("--event-type", help="Filter by event type")
@click.option("--limit", "-n", type=int, default=50)
@click.pass_context
def delivery_list(ctx: click.Context, endpoint: str | None, status: str | None, event_type: str | None, limit: int) -> None:
    """List webhook deliveries."""
    store = get_store(ctx.obj["store_path"])
    ds = DeliveryStatus(status) if status else None
    deliveries = store.list_deliveries(endpoint_id=endpoint, status=ds, event_type=event_type, limit=limit)

    if not deliveries:
        console.print("[yellow]No deliveries found.[/yellow]")
        return

    table = Table(title="Webhook Deliveries")
    table.add_column("ID", style="dim", max_width=12)
    table.add_column("Endpoint", max_width=20)
    table.add_column("Event", max_width=20)
    table.add_column("Status")
    table.add_column("Attempts")
    table.add_column("Created")

    for d in deliveries:
        ep = store.get_endpoint(d.endpoint_id)
        ep_name = ep.name[:18] if ep else d.endpoint_id[:8]
        status_style = {
            "success": "green", "failed": "red", "pending": "yellow",
            "retrying": "cyan", "abandoned": "dim", "in_progress": "blue",
        }.get(d.status.value, "")
        table.add_row(
            d.id[:8],
            ep_name,
            d.event_type or "—",
            f"[{status_style}]{d.status.value}[/{status_style}]",
            str(len(d.attempts)),
            format_dt(d.created_at),
        )

    console.print(table)


@delivery.command("show")
@click.argument("delivery_id")
@click.pass_context
def delivery_show(ctx: click.Context, delivery_id: str) -> None:
    """Show delivery details and attempts."""
    store = get_store(ctx.obj["store_path"])
    d = store.get_delivery(delivery_id)
    if d is None:
        console.print(f"[red]Delivery not found: {delivery_id}[/red]")
        sys.exit(1)

    ep = store.get_endpoint(d.endpoint_id)
    console.print(f"\n[bold]Delivery: {d.id}[/bold]")
    console.print(f"  Endpoint:   {ep.name if ep else d.endpoint_id}")
    console.print(f"  Status:     {d.status.value}")
    console.print(f"  Event:      {d.event_type or '—'}")
    console.print(f"  Created:    {format_dt(d.created_at)}")
    console.print(f"  Next Retry: {format_dt(d.next_retry_at)}")
    console.print(f"  Payload:    {json.dumps(d.payload, default=str)[:200]}")

    if d.attempts:
        table = Table(title="Delivery Attempts")
        table.add_column("#", style="dim")
        table.add_column("Status")
        table.add_column("Code")
        table.add_column("Duration")
        table.add_column("Error")
        table.add_column("Completed")

        for a in d.attempts:
            status_style = {"success": "green", "failed": "red", "in_progress": "blue"}.get(a.status.value, "")
            table.add_row(
                str(a.attempt_number),
                f"[{status_style}]{a.status.value}[/{status_style}]",
                str(a.response_status_code or "—"),
                f"{a.duration_ms:.0f}ms" if a.duration_ms else "—",
                (a.error_message or "—")[:40],
                format_dt(a.completed_at),
            )
        console.print(table)


@delivery.command("cancel")
@click.argument("delivery_id")
@click.pass_context
def delivery_cancel(ctx: click.Context, delivery_id: str) -> None:
    """Cancel a pending or retrying delivery."""
    store = get_store(ctx.obj["store_path"])
    d = store.get_delivery(delivery_id)
    if d is None:
        console.print(f"[red]Delivery not found: {delivery_id}[/red]")
        sys.exit(1)
    if d.status in (DeliveryStatus.PENDING, DeliveryStatus.RETRYING):
        store.update_delivery(d.id, status=DeliveryStatus.ABANDONED)
        console.print(f"[yellow]✗[/yellow] Delivery {delivery_id[:8]} cancelled.")
    else:
        console.print(f"[red]Cannot cancel delivery with status: {d.status.value}[/red]")
        sys.exit(1)


# ── Send Command ───────────────────────────────────────────────────


def _run_async(coro):
    """Run an async coroutine in a fresh event loop."""
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(asyncio.run, coro)
        return future.result()


async def _send_and_close(engine, endpoint_id, payload, event_type, headers):
    """Send a webhook and close the engine client."""
    try:
        result = await engine.send(
            endpoint_id=endpoint_id,
            payload=payload,
            event_type=event_type,
            headers=headers,
        )
        return result
    finally:
        await engine.close()


async def _batch_send_and_close(engine, endpoint_ids, payload, event_type, headers):
    """Send webhooks to multiple endpoints and close the engine client."""
    try:
        results = []
        for eid in endpoint_ids:
            result = await engine.send(
                endpoint_id=eid,
                payload=payload,
                event_type=event_type,
                headers=headers,
            )
            results.append(result)
        return results
    finally:
        await engine.close()


async def _health_check_and_close(engine, store, endpoint_id):
    """Run a health check and close the engine client."""
    try:
        ep = store.get_endpoint(endpoint_id)
        test_payload = {
            "ping": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "agent_webhook_health_check": True,
        }
        delivery = WebhookDelivery(
            endpoint_id=endpoint_id,
            payload=test_payload,
            event_type="health_check",
            metadata={"health_check": True},
        )
        store.add_delivery(delivery)
        attempt = await engine.deliver(delivery)
        store.add_delivery_attempt(delivery.id, attempt)
        if attempt.status == DeliveryStatus.SUCCESS:
            store.update_delivery(delivery.id, status=DeliveryStatus.SUCCESS)
        else:
            store.update_delivery(delivery.id, status=DeliveryStatus.ABANDONED)
        return attempt, ep
    finally:
        await engine.close()


async def _process_pending_and_close(engine):
    """Process pending deliveries and close the engine client."""
    try:
        return await engine.process_pending()
    finally:
        await engine.close()


@cli.command("send")
@click.argument("endpoint_id")
@click.argument("payload", default="-")
@click.option("--event-type", "-e", help="Event type tag")
@click.option("--header", "-H", multiple=True, help="Extra headers in 'Name: Value' format")
@click.pass_context
def send_webhook(ctx: click.Context, endpoint_id: str, payload: str, event_type: str | None, header: tuple[str, ...]) -> None:
    """Send a webhook delivery to an endpoint. Payload is JSON string or stdin."""
    store = get_store(ctx.obj["store_path"])
    ep = store.get_endpoint(endpoint_id)
    if ep is None:
        console.print(f"[red]Endpoint not found: {endpoint_id}[/red]")
        sys.exit(1)

    if payload == "-":
        payload_str = sys.stdin.read()
    else:
        payload_str = payload

    try:
        payload_data = json.loads(payload_str)
    except json.JSONDecodeError as e:
        console.print(f"[red]Invalid JSON payload: {e}[/red]")
        sys.exit(1)

    headers = {}
    for h in header:
        if ":" not in h:
            console.print(f"[red]Invalid header format: {h}[/red]")
            sys.exit(1)
        hname, hvalue = h.split(":", 1)
        headers[hname.strip()] = hvalue.strip()

    from .engine import DeliveryEngine
    engine = DeliveryEngine(store)
    result = _run_async(_send_and_close(
        engine=engine,
        endpoint_id=endpoint_id,
        payload=payload_data,
        event_type=event_type,
        headers=headers,
    ))

    if result.status == DeliveryStatus.SUCCESS:
        console.print(f"[green]✓[/green] Delivered to [bold]{ep.name}[/bold]")
    elif result.status == DeliveryStatus.RETRYING:
        console.print(f"[cyan]↻[/cyan] Delivery scheduled for retry (attempt {result.current_attempt_number()})")
    else:
        console.print(f"[red]✗[/red] Delivery failed: {result.last_attempt().error_message if result.last_attempt() else 'unknown'}")


# ── Batch Send Command ─────────────────────────────────────────────


@cli.command("batch-send")
@click.argument("payload")
@click.option("--endpoint", "-e", multiple=True, required=True, help="Endpoint IDs to send to")
@click.option("--event-type", "-t", help="Event type tag")
@click.option("--header", "-H", multiple=True, help="Extra headers in 'Name: Value' format")
@click.pass_context
def batch_send(ctx: click.Context, payload: str, endpoint: tuple[str, ...], event_type: str | None, header: tuple[str, ...]) -> None:
    """Send a payload to multiple endpoints at once."""
    store = get_store(ctx.obj["store_path"])

    try:
        payload_data = json.loads(payload)
    except json.JSONDecodeError as e:
        console.print(f"[red]Invalid JSON payload: {e}[/red]")
        sys.exit(1)

    headers = {}
    for h in header:
        if ":" not in h:
            console.print(f"[red]Invalid header format: {h}[/red]")
            sys.exit(1)
        hname, hvalue = h.split(":", 1)
        headers[hname.strip()] = hvalue.strip()

    from .engine import DeliveryEngine
    engine = DeliveryEngine(store)

    results = _run_async(_batch_send_and_close(
        engine=engine,
        endpoint_ids=list(endpoint),
        payload=payload_data,
        event_type=event_type,
        headers=headers,
    ))

    success = sum(1 for r in results if r.status == DeliveryStatus.SUCCESS)
    failed = sum(1 for r in results if r.status in (DeliveryStatus.FAILED, DeliveryStatus.ABANDONED))
    retrying = sum(1 for r in results if r.status == DeliveryStatus.RETRYING)

    console.print(f"Batch sent to {len(results)} endpoints: [green]{success} success[/green], [red]{failed} failed[/red], [cyan]{retrying} retrying[/cyan]")


# ── Subscription Commands ──────────────────────────────────────────


@cli.group("subscription")
def subscription_group() -> None:
    """Manage event subscriptions."""
    pass


@subscription_group.command("add")
@click.argument("endpoint_id")
@click.option("--event-type", "-e", multiple=True, required=True, help="Event types to subscribe to")
@click.pass_context
def subscription_add(ctx: click.Context, endpoint_id: str, event_type: tuple[str, ...]) -> None:
    """Subscribe an endpoint to event types."""
    store = get_store(ctx.obj["store_path"])
    ep = store.get_endpoint(endpoint_id)
    if ep is None:
        console.print(f"[red]Endpoint not found: {endpoint_id}[/red]")
        sys.exit(1)

    sub = EventSubscription(
        endpoint_id=endpoint_id,
        event_types=list(event_type),
    )
    store.add_subscription(sub)
    console.print(f"[green]✓[/green] Subscription created: [bold]{', '.join(event_type)}[/bold] → {ep.name} (ID: {sub.id})")


@subscription_group.command("list")
@click.option("--endpoint", "-e", help="Filter by endpoint ID")
@click.pass_context
def subscription_list(ctx: click.Context, endpoint: str | None) -> None:
    """List event subscriptions."""
    store = get_store(ctx.obj["store_path"])
    subs = store.list_subscriptions(endpoint_id=endpoint)

    if not subs:
        console.print("[yellow]No subscriptions found.[/yellow]")
        return

    table = Table(title="Event Subscriptions")
    table.add_column("ID", style="dim", max_width=12)
    table.add_column("Endpoint", max_width=20)
    table.add_column("Event Types")
    table.add_column("Created")

    for s in subs:
        ep = store.get_endpoint(s.endpoint_id)
        ep_name = ep.name[:18] if ep else s.endpoint_id[:8]
        table.add_row(
            s.id[:8],
            ep_name,
            ", ".join(s.event_types),
            format_dt(s.created_at),
        )

    console.print(table)


@subscription_group.command("delete")
@click.argument("subscription_id")
@click.pass_context
def subscription_delete(ctx: click.Context, subscription_id: str) -> None:
    """Delete an event subscription."""
    store = get_store(ctx.obj["store_path"])
    if store.delete_subscription(subscription_id):
        console.print(f"[red]✗[/red] Subscription {subscription_id} deleted.")
    else:
        console.print(f"[red]Subscription not found: {subscription_id}[/red]")
        sys.exit(1)


# ── Health Check Command ───────────────────────────────────────────


@cli.command("health-check")
@click.argument("endpoint_id")
@click.pass_context
def health_check_cmd(ctx: click.Context, endpoint_id: str) -> None:
    """Test endpoint connectivity."""
    store = get_store(ctx.obj["store_path"])
    ep = store.get_endpoint(endpoint_id)
    if ep is None:
        console.print(f"[red]Endpoint not found: {endpoint_id}[/red]")
        sys.exit(1)

    from .engine import DeliveryEngine
    engine = DeliveryEngine(store)

    attempt, ep = _run_async(_health_check_and_close(engine, store, endpoint_id))

    if attempt.status == DeliveryStatus.SUCCESS:
        console.print(f"[green]✓[/green] Endpoint [bold]{ep.name}[/bold] is healthy")
        console.print(f"  Status code: {attempt.response_status_code}")
        console.print(f"  Duration:    {attempt.duration_ms:.0f}ms" if attempt.duration_ms else "  Duration:    —")
    else:
        console.print(f"[red]✗[/red] Endpoint [bold]{ep.name}[/bold] is unhealthy")
        console.print(f"  Error: {attempt.error_message or 'unknown'}")


# ── Relay Commands ─────────────────────────────────────────────────


@cli.group("relay")
def relay_group() -> None:
    """Manage webhook relay rules."""
    pass


@relay_group.command("add")
@click.argument("name")
@click.argument("path_pattern")
@click.option("--target", "-t", multiple=True, required=True, help="Target endpoint IDs")
@click.option("--tag", multiple=True, help="Tags")
@click.pass_context
def relay_add(ctx: click.Context, name: str, path_pattern: str, target: tuple[str, ...], tag: tuple[str, ...]) -> None:
    """Add a relay rule to forward incoming webhooks."""
    store = get_store(ctx.obj["store_path"])
    rule = RelayRule(
        name=name,
        path_pattern=path_pattern,
        target_endpoint_ids=list(target),
        tags=list(tag),
    )
    store.add_relay_rule(rule)
    console.print(f"[green]✓[/green] Relay rule created: [bold]{rule.name}[/bold] (ID: {rule.id})")


@relay_group.command("list")
@click.pass_context
def relay_list(ctx: click.Context) -> None:
    """List all relay rules."""
    store = get_store(ctx.obj["store_path"])
    rules = store.list_relay_rules()

    if not rules:
        console.print("[yellow]No relay rules found.[/yellow]")
        return

    table = Table(title="Relay Rules")
    table.add_column("ID", style="dim", max_width=12)
    table.add_column("Name", style="bold")
    table.add_column("Path Pattern")
    table.add_column("Targets")
    table.add_column("Active")
    table.add_column("Tags")

    for r in rules:
        targets = ", ".join(t[:8] for t in r.target_endpoint_ids)
        active = "[green]Yes[/green]" if r.active else "[red]No[/red]"
        tags = ", ".join(r.tags) or "—"
        table.add_row(r.id[:8], r.name, r.path_pattern, targets, active, tags)

    console.print(table)


@relay_group.command("delete")
@click.argument("rule_id")
@click.pass_context
def relay_delete(ctx: click.Context, rule_id: str) -> None:
    """Delete a relay rule."""
    store = get_store(ctx.obj["store_path"])
    if store.delete_relay_rule(rule_id):
        console.print(f"[red]✗[/red] Relay rule {rule_id} deleted.")
    else:
        console.print(f"[red]Rule not found: {rule_id}[/red]")
        sys.exit(1)


# ── Stats Command ──────────────────────────────────────────────────


@cli.command("stats")
@click.argument("endpoint_id", required=False)
@click.pass_context
def stats_cmd(ctx: click.Context, endpoint_id: str | None) -> None:
    """Show delivery statistics."""
    store = get_store(ctx.obj["store_path"])

    if endpoint_id:
        s = store.get_stats(endpoint_id)
        if s is None:
            console.print(f"[red]Endpoint not found: {endpoint_id}[/red]")
            sys.exit(1)
        stats_list = [s]
    else:
        stats_list = store.get_all_stats()

    if not stats_list:
        console.print("[yellow]No statistics available.[/yellow]")
        return

    table = Table(title="Webhook Statistics")
    table.add_column("Endpoint", style="bold")
    table.add_column("Total")
    table.add_column("Success", style="green")
    table.add_column("Failed", style="red")
    table.add_column("Pending", style="yellow")
    table.add_column("Avg Duration")
    table.add_column("Success Rate")

    for s in stats_list:
        rate = f"{s['successful'] / (s['successful'] + s['failed'] + s['abandoned']) * 100:.1f}%" if (s['successful'] + s['failed'] + s['abandoned']) > 0 else "—"
        avg = f"{s['avg_duration_ms']:.0f}ms" if s['avg_duration_ms'] else "—"
        table.add_row(
            s["endpoint_name"],
            str(s["total_deliveries"]),
            str(s["successful"]),
            str(s["failed"]),
            str(s["pending"]),
            avg,
            rate,
        )

    console.print(table)


# ── Incoming Commands ──────────────────────────────────────────────


@cli.group("incoming")
def incoming_group() -> None:
    """View incoming webhook history."""
    pass


@incoming_group.command("list")
@click.option("--path", "-p", help="Filter by path")
@click.option("--processed/--unprocessed", default=None)
@click.option("--limit", "-n", type=int, default=50)
@click.pass_context
def incoming_list(ctx: click.Context, path: str | None, processed: bool | None, limit: int) -> None:
    """List incoming webhooks."""
    store = get_store(ctx.obj["store_path"])
    incoming = store.list_incoming(path=path, processed=processed, limit=limit)

    if not incoming:
        console.print("[yellow]No incoming webhooks found.[/yellow]")
        return

    table = Table(title="Incoming Webhooks")
    table.add_column("ID", style="dim", max_width=12)
    table.add_column("Path")
    table.add_column("Method")
    table.add_column("Forwarded To")
    table.add_column("Processed")
    table.add_column("Received")

    for i in incoming:
        forwarded = ", ".join(i.forwarded_to) if i.forwarded_to else "—"
        proc = "[green]Yes[/green]" if i.processed else "[yellow]No[/yellow]"
        table.add_row(i.id[:8], i.path, i.method, forwarded, proc, format_dt(i.received_at))

    console.print(table)


# ── Event Log Command ──────────────────────────────────────────────


@cli.command("event-log")
@click.option("--event-type", "-e", help="Filter by event type")
@click.option("--endpoint", "-ep", help="Filter by endpoint ID")
@click.option("--limit", "-n", type=int, default=50)
@click.pass_context
def event_log_cmd(ctx: click.Context, event_type: str | None, endpoint: str | None, limit: int) -> None:
    """Show event audit log."""
    store = get_store(ctx.obj["store_path"])
    entries = store.list_event_log(event_type=event_type, endpoint_id=endpoint, limit=limit)

    if not entries:
        console.print("[yellow]No event log entries found.[/yellow]")
        return

    table = Table(title="Event Log")
    table.add_column("Timestamp")
    table.add_column("Event Type", style="bold")
    table.add_column("Endpoint")
    table.add_column("Details")

    for e in entries:
        ep_name = "—"
        if e.endpoint_id:
            ep = store.get_endpoint(e.endpoint_id)
            ep_name = ep.name[:18] if ep else e.endpoint_id[:8]
        details = json.dumps(e.details, default=str)[:60] if e.details else "—"
        table.add_row(
            format_dt(e.timestamp),
            e.event_type,
            ep_name,
            details,
        )

    console.print(table)


# ── Process Pending ────────────────────────────────────────────────


@cli.command("process-pending")
@click.pass_context
def process_pending_cmd(ctx: click.Context) -> None:
    """Process all pending webhook deliveries."""
    store = get_store(ctx.obj["store_path"])
    from .engine import DeliveryEngine
    engine = DeliveryEngine(store)
    results = _run_async(_process_pending_and_close(engine))

    if not results:
        console.print("[yellow]No pending deliveries.[/yellow]")
        return

    success = sum(1 for r in results if r.status == DeliveryStatus.SUCCESS)
    retrying = sum(1 for r in results if r.status == DeliveryStatus.RETRYING)
    failed = sum(1 for r in results if r.status in (DeliveryStatus.FAILED, DeliveryStatus.ABANDONED))

    console.print(f"Processed {len(results)} deliveries: [green]{success} success[/green], [cyan]{retrying} retrying[/cyan], [red]{failed} failed[/red]")


# ── Transform Commands ─────────────────────────────────────────────


@cli.group("transform")
def transform_group() -> None:
    """Manage payload transformations."""
    pass


@transform_group.command("create")
@click.argument("name")
@click.option("--type", "-t", "transform_type", type=click.Choice(["field_map", "filter", "template"]), required=True, help="Transform type")
@click.option("--mapping", "-m", multiple=True, help="Field mapping 'old:new' (for field_map)")
@click.option("--include", "-i", multiple=True, help="Fields to include (for filter)")
@click.option("--exclude", "-x", multiple=True, help="Fields to exclude (for filter)")
@click.option("--keep-unmapped", is_flag=True, default=True, help="Keep unmapped fields (for field_map)")
@click.option("--field", "-f", multiple=True, help="Template field 'key={{payload.path}}' (for template)")
@click.pass_context
def transform_create(ctx: click.Context, name: str, transform_type: str, mapping: tuple, include: tuple, exclude: tuple, keep_unmapped: bool, field: tuple) -> None:
    """Create a payload transformation."""
    store = get_store(ctx.obj["store_path"])
    if not hasattr(store, "add_transform"):
        console.print("[red]Transforms require SQLite store. Use --store with .db path.[/red]")
        sys.exit(1)

    config: dict[str, Any] = {}
    if transform_type == "field_map":
        config["mapping"] = {m.split(":")[0]: m.split(":", 1)[1] for m in mapping if ":" in m}
        config["keep_unmapped"] = keep_unmapped
    elif transform_type == "filter":
        if include:
            config["include"] = list(include)
        elif exclude:
            config["exclude"] = list(exclude)
        else:
            console.print("[red]Filter requires --include or --exclude[/red]")
            sys.exit(1)
    elif transform_type == "template":
        if field:
            config["fields"] = {f.split("=")[0]: f.split("=", 1)[1] for f in field if "=" in f}
        else:
            console.print("[red]Template requires --field options[/red]")
            sys.exit(1)

    transform = PayloadTransform(name=name, type=TransformType(transform_type), config=config)
    store.add_transform(transform)
    console.print(f"[green]✓[/green] Transform created: [bold]{transform.name}[/bold] (ID: {transform.id}, type: {transform_type})")


@transform_group.command("list")
@click.option("--type", "-t", "transform_type", help="Filter by type")
@click.pass_context
def transform_list(ctx: click.Context, transform_type: str | None) -> None:
    """List payload transforms."""
    store = get_store(ctx.obj["store_path"])
    if not hasattr(store, "list_transforms"):
        console.print("[red]Transforms require SQLite store.[/red]")
        sys.exit(1)
    transforms = store.list_transforms(type=transform_type)
    if not transforms:
        console.print("[yellow]No transforms found.[/yellow]")
        return
    table = Table(title="Payload Transforms")
    table.add_column("ID", style="dim", max_width=12)
    table.add_column("Name", style="bold")
    table.add_column("Type")
    table.add_column("Config", max_width=50)
    table.add_column("Created")
    for t in transforms:
        config_str = json.dumps(t.config, default=str)[:48]
        table.add_row(t.id[:8], t.name, t.type.value, config_str, format_dt(t.created_at))
    console.print(table)


@transform_group.command("delete")
@click.argument("transform_id")
@click.pass_context
def transform_delete(ctx: click.Context, transform_id: str) -> None:
    """Delete a payload transform."""
    store = get_store(ctx.obj["store_path"])
    if not hasattr(store, "delete_transform"):
        console.print("[red]Transforms require SQLite store.[/red]")
        sys.exit(1)
    if store.delete_transform(transform_id):
        console.print(f"[red]✗[/red] Transform {transform_id} deleted.")
    else:
        console.print(f"[red]Transform not found: {transform_id}[/red]")
        sys.exit(1)


@transform_group.command("show")
@click.argument("transform_id")
@click.pass_context
def transform_show(ctx: click.Context, transform_id: str) -> None:
    """Show details of a payload transform."""
    store = get_store(ctx.obj["store_path"])
    if not hasattr(store, "get_transform"):
        console.print("[red]Transforms require SQLite store.[/red]")
        sys.exit(1)
    t = store.get_transform(transform_id)
    if t is None:
        console.print(f"[red]Transform not found: {transform_id}[/red]")
        sys.exit(1)
    console.print(f"\n[bold]Transform: {t.name}[/bold]")
    console.print(f"  ID:      {t.id}")
    console.print(f"  Type:    {t.type.value}")
    console.print(f"  Config:  {json.dumps(t.config, default=str, indent=4)}")
    console.print(f"  Created: {format_dt(t.created_at)}")


@transform_group.command("update")
@click.argument("transform_id")
@click.option("--name", "-n", default=None, help="New transform name")
@click.option("--config", "-c", default=None, help="New config as JSON string")
@click.pass_context
def transform_update(ctx: click.Context, transform_id: str, name: str | None, config: str | None) -> None:
    """Update a payload transform."""
    store = get_store(ctx.obj["store_path"])
    if not hasattr(store, "update_transform"):
        console.print("[red]Transforms require SQLite store.[/red]")
        sys.exit(1)
    updates: dict[str, Any] = {}
    if name is not None:
        updates["name"] = name
    if config is not None:
        try:
            updates["config"] = json.loads(config)
        except json.JSONDecodeError as e:
            console.print(f"[red]Invalid JSON config: {e}[/red]")
            sys.exit(1)
    if not updates:
        console.print("[yellow]No updates specified. Use --name or --config.[/yellow]")
        return
    t = store.update_transform(transform_id, **updates)
    if t is None:
        console.print(f"[red]Transform not found: {transform_id}[/red]")
        sys.exit(1)
    console.print(f"[green]✓[/green] Transform {transform_id[:8]} updated.")


# ── Dead Letter Queue Commands ────────────────────────────────────


@cli.group("dlq")
def dlq_group() -> None:
    """Manage dead letter queue (failed deliveries)."""
    pass


@dlq_group.command("list")
@click.option("--endpoint", "-e", help="Filter by endpoint ID")
@click.option("--replayed/--not-replayed", default=None, help="Filter by replayed status")
@click.option("--limit", "-n", type=int, default=50)
@click.pass_context
def dlq_list(ctx: click.Context, endpoint: str | None, replayed: bool | None, limit: int) -> None:
    """List dead letter queue entries."""
    store = get_store(ctx.obj["store_path"])
    if not hasattr(store, "list_dead_letter"):
        console.print("[red]Dead letter queue requires SQLite store.[/red]")
        sys.exit(1)
    entries = store.list_dead_letter(endpoint_id=endpoint, replayed=replayed, limit=limit)
    if not entries:
        console.print("[yellow]No dead letter entries.[/yellow]")
        return
    table = Table(title="Dead Letter Queue")
    table.add_column("ID", style="dim", max_width=12)
    table.add_column("Endpoint", max_width=18)
    table.add_column("Event")
    table.add_column("Reason", max_width=40)
    table.add_column("Attempts")
    table.add_column("Replayed")
    table.add_column("Created")
    for e in entries:
        ep = store.get_endpoint(e.endpoint_id)
        ep_name = ep.name[:16] if ep else e.endpoint_id[:8]
        reason = (e.reason or "—")[:38]
        replayed_str = "[green]Yes[/green]" if e.replayed else "[red]No[/red]"
        table.add_row(e.id[:8], ep_name, e.event_type or "—", reason, str(e.total_attempts), replayed_str, format_dt(e.created_at))
    console.print(table)


@dlq_group.command("show")
@click.argument("entry_id")
@click.pass_context
def dlq_show(ctx: click.Context, entry_id: str) -> None:
    """Show dead letter entry details."""
    store = get_store(ctx.obj["store_path"])
    if not hasattr(store, "get_dead_letter"):
        console.print("[red]Dead letter queue requires SQLite store.[/red]")
        sys.exit(1)
    entry = store.get_dead_letter(entry_id)
    if entry is None:
        console.print(f"[red]Entry not found: {entry_id}[/red]")
        sys.exit(1)
    console.print(f"\n[bold]Dead Letter Entry: {entry.id}[/bold]")
    console.print(f"  Original Delivery: {entry.delivery_id}")
    console.print(f"  Endpoint:          {entry.endpoint_id}")
    console.print(f"  Event Type:        {entry.event_type or '—'}")
    console.print(f"  Reason:            {entry.reason}")
    console.print(f"  Total Attempts:    {entry.total_attempts}")
    console.print(f"  Last Status Code:  {entry.last_status_code or '—'}")
    console.print(f"  Last Error:        {entry.last_error or '—'}")
    console.print(f"  Replayed:          {'Yes → ' + entry.replayed_delivery_id if entry.replayed else 'No'}")
    console.print(f"  Created:           {format_dt(entry.created_at)}")
    console.print(f"  Payload:           {json.dumps(entry.payload, default=str)[:200]}")


@dlq_group.command("replay")
@click.argument("entry_id")
@click.pass_context
def dlq_replay(ctx: click.Context, entry_id: str) -> None:
    """Replay a dead letter entry (create a new delivery)."""
    store = get_store(ctx.obj["store_path"])
    if not hasattr(store, "get_dead_letter"):
        console.print("[red]Dead letter queue requires SQLite store.[/red]")
        sys.exit(1)
    entry = store.get_dead_letter(entry_id)
    if entry is None:
        console.print(f"[red]Entry not found: {entry_id}[/red]")
        sys.exit(1)
    if entry.replayed:
        console.print(f"[yellow]Already replayed (delivery: {entry.replayed_delivery_id})[/yellow]")
        sys.exit(1)
    from .engine import DeliveryEngine
    engine = DeliveryEngine(store)
    async def _replay():
        result = await engine.send(
            endpoint_id=entry.endpoint_id,
            payload=entry.payload,
            event_type=entry.event_type,
            metadata={"replayed_from_dlq": entry.id, "original_delivery_id": entry.delivery_id},
        )
        await engine.close()
        return result
    result = _run_async(_replay())
    store.update_dead_letter(entry_id, replayed=True, replayed_delivery_id=result.id, replayed_at=datetime.now(timezone.utc))
    if result.status == DeliveryStatus.SUCCESS:
        console.print(f"[green]✓[/green] Replayed: new delivery [bold]{result.id[:8]}[/bold] succeeded")
    elif result.status == DeliveryStatus.RETRYING:
        console.print(f"[cyan]↻[/cyan] Replayed: new delivery [bold]{result.id[:8]}[/bold] scheduled for retry")
    else:
        console.print(f"[red]✗[/red] Replayed: new delivery [bold]{result.id[:8]}[/bold] failed")


@dlq_group.command("delete")
@click.argument("entry_id")
@click.pass_context
def dlq_delete(ctx: click.Context, entry_id: str) -> None:
    """Delete a dead letter entry."""
    store = get_store(ctx.obj["store_path"])
    if not hasattr(store, "delete_dead_letter"):
        console.print("[red]Dead letter queue requires SQLite store.[/red]")
        sys.exit(1)
    if store.delete_dead_letter(entry_id):
        console.print(f"[red]✗[/red] Dead letter entry {entry_id} deleted.")
    else:
        console.print(f"[red]Entry not found: {entry_id}[/red]")
        sys.exit(1)


# ── Migrate Command ────────────────────────────────────────────────


@cli.command("migrate")
@click.argument("json_path")
@click.pass_context
def migrate_cmd(ctx: click.Context, json_path: str) -> None:
    """Migrate data from a JSON store to SQLite."""
    store = get_store(ctx.obj["store_path"])
    if not hasattr(store, "migrate_from_json"):
        console.print("[red]Migration requires SQLite store. Use --store with .db path.[/red]")
        sys.exit(1)
    from pathlib import Path
    if not Path(json_path).exists():
        console.print(f"[red]File not found: {json_path}[/red]")
        sys.exit(1)
    counts = store.migrate_from_json(json_path)
    console.print("[green]✓[/green] Migration complete:")
    for key, count in counts.items():
        console.print(f"  {key}: {count}")


# ── v0.4.0 New Commands ──────────────────────────────────────────


@dlq_group.command("batch-replay")
@click.option("--endpoint", "-e", default=None, help="Filter by endpoint ID")
@click.pass_context
def dlq_batch_replay(ctx: click.Context, endpoint: str | None) -> None:
    """Replay all unreplayed dead letter entries."""
    from .engine import DeliveryEngine
    from .service import WebhookService

    store = get_store(ctx.obj["store_path"])
    if not hasattr(store, "list_dead_letter"):
        console.print("[red]Dead letter queue requires SQLite store.[/red]")
        sys.exit(1)

    service = WebhookService(store_path=ctx.obj["store_path"])
    results = asyncio.run(service.batch_replay_dead_letter(endpoint_id=endpoint))

    if not results:
        console.print("[yellow]No dead letter entries to replay.[/yellow]")
        return

    table = Table(title="Batch Replay Results")
    table.add_column("Delivery ID", style="cyan")
    table.add_column("Endpoint ID", style="magenta")
    table.add_column("Status", style="green")
    for r in results:
        table.add_row(r.id[:8], r.endpoint_id[:8], r.status.value)
    console.print(table)
    console.print(f"[green]✓[/green] Replayed {len(results)} entries.")


@relay_group.command("update")
@click.argument("rule_id")
@click.option("--name", "-n", default=None, help="New rule name")
@click.option("--path-pattern", "-p", default=None, help="New path pattern")
@click.option("--target", "-t", multiple=True, help="New target endpoint IDs")
@click.option("--active/--inactive", default=None, help="Enable or disable the rule")
@click.option("--tag", multiple=True, help="New tags")
@click.pass_context
def relay_update(ctx: click.Context, rule_id: str, name: str | None, path_pattern: str | None, target: tuple[str, ...], active: bool | None, tag: tuple[str, ...]) -> None:
    """Update a relay rule."""
    from .service import WebhookService

    store = get_store(ctx.obj["store_path"])
    if not hasattr(store, "update_relay_rule"):
        console.print("[red]Relay rule updates require SQLite store.[/red]")
        sys.exit(1)

    updates: dict[str, Any] = {}
    if name is not None:
        updates["name"] = name
    if path_pattern is not None:
        updates["path_pattern"] = path_pattern
    if target:
        updates["target_endpoint_ids"] = list(target)
    if active is not None:
        updates["active"] = active
    if tag:
        updates["tags"] = list(tag)

    if not updates:
        console.print("[yellow]No updates specified. Use --name, --path-pattern, --target, --active/--inactive, or --tag.[/yellow]")
        return

    rule = store.update_relay_rule(rule_id, **updates)
    if rule is None:
        console.print(f"[red]Relay rule not found: {rule_id}[/red]")
        sys.exit(1)
    console.print(f"[green]✓[/green] Relay rule {rule_id} updated.")
    console.print(f"  Name: {rule.name}")
    console.print(f"  Pattern: {rule.path_pattern}")
    console.print(f"  Targets: {', '.join(rule.target_endpoint_ids)}")
    console.print(f"  Active: {rule.active}")


@cli.command("metrics")
@click.option("--format", "fmt", type=click.Choice(["json", "prometheus"]), default="json", help="Output format")
@click.pass_context
def metrics_cmd(ctx: click.Context, fmt: str) -> None:
    """Show webhook delivery metrics."""
    from .metrics import get_metrics

    m = get_metrics()
    if fmt == "prometheus":
        console.print(m.generate_prometheus())
    else:
        data = m.get_json()
        console.print_json(json.dumps(data, default=str, indent=2))


@cli.command("rate-limit-status")
@click.argument("endpoint_id")
@click.pass_context
def rate_limit_status_cmd(ctx: click.Context, endpoint_id: str) -> None:
    """Show rate limit status for an endpoint."""
    store = get_store(ctx.obj["store_path"])
    ep = store.get_endpoint(endpoint_id)
    if ep is None:
        console.print(f"[red]Endpoint not found: {endpoint_id}[/red]")
        sys.exit(1)
    if ep.rate_limit is None:
        console.print(f"[yellow]No rate limit configured for endpoint '{ep.name}'[/yellow]")
        return
    from .engine import DeliveryEngine
    engine = DeliveryEngine(store)
    status = engine._rate_limiter.get_status(endpoint_id, ep.rate_limit)
    console.print(f"\n[bold]Rate Limit Status: {ep.name}[/bold]")
    console.print(f"  Limit:      {status['limit']} requests per {status['period']}")
    console.print(f"  Burst:      {status['burst']}")
    console.print(f"  Current:    {status['current_count']}")
    console.print(f"  Remaining:  {status['remaining']}")
    if status.get("reset_at"):
        console.print(f"  Resets at:  {status['reset_at']:.1f}s from now")


@cli.group()
def circuit_breaker() -> None:
    """Manage circuit breakers for endpoints."""
    pass


@circuit_breaker.command("show")
@click.argument("endpoint_id")
@click.pass_context
def circuit_breaker_show(ctx: click.Context, endpoint_id: str) -> None:
    """Show circuit breaker state for an endpoint."""
    store = get_store(ctx.obj["store_path"])
    from .engine import DeliveryEngine
    engine = DeliveryEngine(store)
    state = engine.get_circuit_breaker_state(endpoint_id)

    if state is None:
        console.print(f"[yellow]No circuit breaker tracked yet for endpoint {endpoint_id} (starts in closed state)[/yellow]")
        return

    state_style = {"closed": "green", "open": "red", "half_open": "yellow"}.get(state["state"], "")
    console.print(f"\n[bold]Circuit Breaker: {endpoint_id[:12]}...[/bold]")
    console.print(f"  State:               [{state_style}]{state['state']}[/{state_style}]")
    console.print(f"  Consecutive Failures: {state['consecutive_failures']}")
    console.print(f"  Consecutive Successes: {state['consecutive_successes']}")
    console.print(f"  Total Trips:          {state['total_trips']}")
    if state.get("time_until_half_open_seconds") is not None:
        console.print(f"  Time Until Half-Open: {state['time_until_half_open_seconds']}s")
    console.print(f"  Last Failure:         {format_dt(state['last_failure_at'])}")
    console.print(f"  Last Success:         {format_dt(state['last_success_at'])}")
    config = state.get("config", {})
    console.print(f"  Config:")
    console.print(f"    Failure Threshold:    {config.get('failure_threshold', 5)}")
    console.print(f"    Recovery Timeout:     {config.get('recovery_timeout', 60.0)}s")
    console.print(f"    Half-Open Max Calls:  {config.get('half_open_max_calls', 3)}")
    console.print(f"    Success Threshold:    {config.get('success_threshold', 2)}")


@circuit_breaker.command("list")
@click.pass_context
def circuit_breaker_list(ctx: click.Context) -> None:
    """List all circuit breaker states."""
    store = get_store(ctx.obj["store_path"])
    from .engine import DeliveryEngine
    engine = DeliveryEngine(store)
    states = engine.get_all_circuit_breaker_states()

    if not states:
        console.print("[yellow]No circuit breakers tracked.[/yellow]")
        return

    table = Table(title="Circuit Breakers")
    table.add_column("Endpoint ID", style="dim", max_width=14)
    table.add_column("State")
    table.add_column("Failures")
    table.add_column("Trips")
    table.add_column("Last Failure")

    for s in states:
        state_style = {"closed": "green", "open": "red", "half_open": "yellow"}.get(s["state"], "")
        table.add_row(
            s["endpoint_id"][:12],
            f"[{state_style}]{s['state']}[/{state_style}]",
            str(s["consecutive_failures"]),
            str(s["total_trips"]),
            format_dt(s["last_failure_at"]),
        )

    console.print(table)


@circuit_breaker.command("reset")
@click.argument("endpoint_id")
@click.pass_context
def circuit_breaker_reset_cmd(ctx: click.Context, endpoint_id: str) -> None:
    """Reset (force close) the circuit breaker for an endpoint."""
    store = get_store(ctx.obj["store_path"])
    from .engine import DeliveryEngine
    engine = DeliveryEngine(store)
    result = engine.reset_circuit_breaker(endpoint_id)
    if result is None:
        console.print(f"[red]No circuit breaker found for endpoint: {endpoint_id}[/red]")
        sys.exit(1)
    console.print(f"[green]✓[/green] Circuit breaker reset for endpoint {endpoint_id[:12]}...")


@cli.command("verify-signature")
@click.option("--secret", "-s", required=True, help="HMAC secret")
@click.option("--provider", "-p", type=click.Choice(["generic", "github", "stripe", "slack", "shopify"]), default="generic")
@click.option("--algorithm", "-a", type=click.Choice(["sha256", "sha1"]), default="sha256")
@click.option("--header", "-H", multiple=True, help="Request headers in 'Name: Value' format")
@click.option("--tolerance", type=int, default=300, help="Max timestamp age in seconds")
@click.argument("body")
def verify_signature_cmd(
    secret: str,
    provider: str,
    algorithm: str,
    header: tuple[str, ...],
    tolerance: int,
    body: str,
) -> None:
    """Verify an incoming webhook HMAC signature."""
    from .signature import SignatureVerifier, SignatureError

    headers = {}
    for h in header:
        if ":" not in h:
            console.print(f"[red]Invalid header format: {h}. Use 'Name: Value'[/red]")
            sys.exit(1)
        hname, hvalue = h.split(":", 1)
        headers[hname.strip()] = hvalue.strip()

    verifier = SignatureVerifier(tolerance_seconds=tolerance)
    try:
        verifier.verify_or_raise(
            raw_body=body,
            headers=headers,
            secret=secret,
            provider=provider,
            algorithm=algorithm,
        )
        console.print(f"[green]✓[/green] Signature valid (provider: {provider})")
    except SignatureError as e:
        console.print(f"[red]✗[/red] Signature invalid: {e}")
        sys.exit(1)


@cli.command("detect-provider")
@click.option("--header", "-H", multiple=True, help="Request headers in 'Name: Value' format")
def detect_provider_cmd(header: tuple[str, ...]) -> None:
    """Auto-detect webhook provider from request headers."""
    from .signature import SignatureVerifier

    headers = {}
    for h in header:
        if ":" not in h:
            console.print(f"[red]Invalid header format: {h}. Use 'Name: Value'[/red]")
            sys.exit(1)
        hname, hvalue = h.split(":", 1)
        headers[hname.strip()] = hvalue.strip()

    verifier = SignatureVerifier()
    provider = verifier.detect_provider(headers)
    if provider:
        console.print(f"[green]Detected provider:[/green] [bold]{provider}[/bold]")
    else:
        console.print("[yellow]Could not detect provider from headers.[/yellow]")


# ── Relay Filter Commands ──────────────────────────────────────────


@cli.group("relay-filter")
def relay_filter() -> None:
    """Manage relay rule filters for conditional forwarding."""
    pass


@relay_filter.command("set")
@click.argument("rule_id")
@click.option("--file", "-f", type=click.Path(exists=True), help="JSON file with filter rules")
@click.option("--json", "json_str", type=str, help="Inline JSON filter rules")
@click.pass_context
def relay_filter_set(
    ctx: click.Context,
    rule_id: str,
    file: str | None,
    json_str: str | None,
) -> None:
    """Set filter rules on a relay rule."""
    import json as _json

    store = get_store(ctx.obj["store_path"])

    if file:
        with open(file) as f:
            filter_rules = _json.load(f)
    elif json_str:
        filter_rules = _json.loads(json_str)
    else:
        console.print("[red]Provide filter rules via --file or --json[/red]")
        sys.exit(1)

    # Validate
    from .filters import validate_filter_rules
    errors = validate_filter_rules(filter_rules)
    if errors:
        console.print("[red]Filter validation errors:[/red]")
        for e in errors:
            console.print(f"  • {e}")
        sys.exit(1)

    if not hasattr(store, "update_relay_rule"):
        console.print("[red]This store does not support relay rule updates[/red]")
        sys.exit(1)

    rule = store.update_relay_rule(rule_id, filter_rules=filter_rules)
    if rule is None:
        console.print(f"[red]Relay rule not found: {rule_id}[/red]")
        sys.exit(1)

    console.print(f"[green]✓[/green] Filter rules set for relay rule: {rule.name}")


@relay_filter.command("clear")
@click.argument("rule_id")
@click.pass_context
def relay_filter_clear(ctx: click.Context, rule_id: str) -> None:
    """Clear filter rules from a relay rule."""
    store = get_store(ctx.obj["store_path"])

    if not hasattr(store, "update_relay_rule"):
        console.print("[red]This store does not support relay rule updates[/red]")
        sys.exit(1)

    rule = store.update_relay_rule(rule_id, filter_rules=None)
    if rule is None:
        console.print(f"[red]Relay rule not found: {rule_id}[/red]")
        sys.exit(1)

    console.print(f"[green]✓[/green] Filter rules cleared for relay rule: {rule.name}")


@relay_filter.command("validate")
@click.option("--file", "-f", type=click.Path(exists=True), help="JSON file with filter rules")
@click.option("--json", "json_str", type=str, help="Inline JSON filter rules")
def relay_filter_validate_cmd(file: str | None, json_str: str | None) -> None:
    """Validate filter rules without applying them."""
    import json as _json

    if file:
        with open(file) as f:
            filter_rules = _json.load(f)
    elif json_str:
        filter_rules = _json.loads(json_str)
    else:
        console.print("[red]Provide filter rules via --file or --json[/red]")
        sys.exit(1)

    from .filters import validate_filter_rules
    errors = validate_filter_rules(filter_rules)
    if errors:
        console.print("[red]Validation errors:[/red]")
        for e in errors:
            console.print(f"  • {e}")
        sys.exit(1)

    console.print("[green]✓ Filter rules are valid[/green]")


# ── Import/Export Commands ─────────────────────────────────────────


@cli.command("export")
@click.option("--file", "-f", type=click.Path(), required=True, help="Output JSON file path")
@click.option("--endpoints/--no-endpoints", default=True)
@click.option("--relay-rules/--no-relay-rules", default=True)
@click.option("--transforms/--no-transforms", default=True)
@click.option("--subscriptions/--no-subscriptions", default=True)
@click.pass_context
def export_cmd(
    ctx: click.Context,
    file: str,
    endpoints: bool,
    relay_rules: bool,
    transforms: bool,
    subscriptions: bool,
) -> None:
    """Export configuration to a JSON file."""
    store = get_store(ctx.obj["store_path"])

    from .import_export import export_to_file
    summary = export_to_file(
        store,
        file,
        include_endpoints=endpoints,
        include_relay_rules=relay_rules,
        include_transforms=transforms,
        include_subscriptions=subscriptions,
    )

    console.print(f"[green]✓[/green] Configuration exported to {file}")
    console.print(f"  Endpoints:      {summary.get('endpoints', 0)}")
    console.print(f"  Relay Rules:    {summary.get('relay_rules', 0)}")
    console.print(f"  Transforms:     {summary.get('transforms', 0)}")
    console.print(f"  Subscriptions:  {summary.get('subscriptions', 0)}")


@cli.command("import")
@click.argument("file", type=click.Path(exists=True))
@click.option("--strategy", "-s", type=click.Choice(["skip", "overwrite", "rename"]), default="skip")
@click.option("--restore-secrets/--no-restore-secrets", default=False, help="Restore HMAC secrets from export")
@click.pass_context
def import_cmd(
    ctx: click.Context,
    file: str,
    strategy: str,
    restore_secrets: bool,
) -> None:
    """Import configuration from a JSON file."""
    store = get_store(ctx.obj["store_path"])

    from .import_export import import_from_file
    summary = import_from_file(store, file, conflict_strategy=strategy, restore_secrets=restore_secrets)

    console.print(f"[green]✓[/green] Import complete")
    console.print(f"  Total Imported: {summary.get('total_imported', 0)}")
    console.print(f"  Total Skipped:  {summary.get('total_skipped', 0)}")

    detail = summary.get("imported", {})
    if any(detail.values()):
        console.print("\n[bold]Imported by type:[/bold]")
        for k, v in detail.items():
            if v:
                console.print(f"  {k}: {v}")

    if summary.get("errors"):
        console.print(f"\n[red]Errors ({len(summary['errors'])}):[/red]")
        for e in summary["errors"][:10]:
            console.print(f"  • {e}")
        if len(summary["errors"]) > 10:
            console.print(f"  ... and {len(summary['errors']) - 10} more")


@cli.command("rest-api")
@click.option("--host", default="0.0.0.0", help="Host to bind to")
@click.option("--port", default=8000, type=int, help="Port to bind to")
@click.option("--store", "store_path", default=DEFAULT_STORE_PATH, help="Path to store file")
@click.pass_context
def rest_api_cmd(ctx: click.Context, host: str, port: int, store_path: str) -> None:
    """Start the REST API server."""
    try:
        import uvicorn
    except ImportError:
        console.print("[red]uvicorn is required for the REST API server. Install with: pip install agent-webhook[rest][/red]")
        sys.exit(1)

    from .rest_api import create_app

    console.print(f"[green]Starting REST API server on {host}:{port}[/green]")
    console.print(f"Store: {store_path}")
    console.print("Endpoints:")
    console.print(f"  GET  /health              - Health check")
    console.print(f"  POST /endpoints           - Create endpoint")
    console.print(f"  GET  /endpoints           - List endpoints")
    console.print(f"  GET  /endpoints/:id       - Get endpoint")
    console.print(f"  PATCH /endpoints/:id      - Update endpoint")
    console.print(f"  DELETE /endpoints/:id     - Delete endpoint")
    console.print(f"  POST /deliveries/send     - Send webhook")
    console.print(f"  POST /deliveries/batch-send - Batch send")
    console.print(f"  GET  /deliveries          - List deliveries")
    console.print(f"  POST /dlq/batch-replay    - Batch replay DLQ")
    console.print(f"  GET  /metrics (REST API)  - Service metrics")
    console.print(f"  GET  /stats               - Store stats")

    app = create_app(store_path=store_path)
    uvicorn.run(app, host=host, port=port)


# ── Analytics Commands ───────────────────────────────────────────────


@cli.command("analytics")
@click.option("--endpoint", "endpoint_id", default=None, help="Analytics for a single endpoint")
@click.option("--retry", is_flag=True, help="Show retry analysis instead of overview")
@click.option("--store", "store_path", default=DEFAULT_STORE_PATH, help="Path to store file")
@click.pass_context
def analytics_cmd(ctx: click.Context, endpoint_id: str | None, retry: bool, store_path: str) -> None:
    """View delivery analytics: success rates, latency percentiles, error breakdown, trends."""
    from .analytics import AnalyticsEngine
    from .service import WebhookService

    service = WebhookService(store_path=store_path)
    engine = AnalyticsEngine(service)

    if retry:
        report = engine.retry_analysis()
        console.print("\n[bold cyan]Retry Analysis[/bold cyan]\n")
        console.print(f"  Total Deliveries:    {report['total_deliveries']}")
        console.print(f"  Deliveries Retried:  {report['deliveries_retried']} ({report['retry_rate_pct']}%)")
        console.print(f"  Succeeded on Retry:  {report['succeeded_after_retry']}")
        sr = report.get('retry_success_rate_pct')
        console.print(f"  Retry Success Rate:  {f'{sr}%' if sr is not None else 'N/A'}")
        console.print(f"  Dead Lettered:       {report['dead_lettered']} ({report['dead_letter_rate_pct']}%)")
        console.print(f"  Avg Attempts:        {report['avg_attempts']}")
        if report.get("attempt_distribution"):
            console.print("\n[bold]Attempt Distribution:[/bold]")
            for item in report["attempt_distribution"]:
                console.print(f"    {item['attempts']} attempt(s): {item['count']} deliveries")
        return

    if endpoint_id:
        report = engine.endpoint_report(endpoint_id)
        if report is None:
            console.print(f"[red]Endpoint not found: {endpoint_id}[/red]")
            sys.exit(1)
        console.print(f"\n[bold cyan]Analytics: {report['endpoint_name']}[/bold cyan]\n")
        s = report["summary"]
        console.print(f"  Total Deliveries:  {s['total_deliveries']}")
        console.print(f"  Successful:        {s['successful']}")
        console.print(f"  Failed:            {s['failed']}")
        console.print(f"  Dead Lettered:     {s['dead_letter']}")
        sr = s.get('success_rate_pct')
        console.print(f"  Success Rate:      {f'{sr}%' if sr is not None else 'N/A'}")
        console.print(f"  Health Score:      {s['health_score']}/100")
        lat = report.get("latency", {})
        if lat.get("count"):
            console.print(f"\n[bold]Latency (ms):[/bold]")
            console.print(f"    Avg: {lat.get('avg_ms')}  P50: {lat.get('p50_ms')}  P90: {lat.get('p90_ms')}  P95: {lat.get('p95_ms')}  P99: {lat.get('p99_ms')}")
        eb = report.get("error_breakdown", {})
        if eb.get("by_status_code"):
            console.print(f"\n[bold]Status Codes:[/bold]")
            for item in eb["by_status_code"][:10]:
                console.print(f"    {item['status_code']}: {item['count']}")
        return

    report = engine.overall_report()
    console.print("\n[bold cyan]Delivery Analytics Overview[/bold cyan]\n")
    s = report["summary"]
    console.print(f"  Total Deliveries:  {s['total_deliveries']}")
    console.print(f"  Successful:        {s['successful']}")
    console.print(f"  Failed:            {s['failed']}")
    console.print(f"  Pending:           {s['pending']}")
    console.print(f"  Retrying:          {s['retrying']}")
    console.print(f"  Dead Lettered:     {s['dead_letter']}")
    console.print(f"  Abandoned:         {s['abandoned']}")
    sr = s.get('success_rate_pct')
    console.print(f"  Success Rate:      {f'{sr}%' if sr is not None else 'N/A'}")
    console.print(f"  Total Attempts:    {s['total_attempts']}")
    console.print(f"  Avg Attempts/Delivery: {s['avg_attempts_per_delivery']}")
    console.print(f"\n  [bold green]Health Score: {s['health_score']}/100[/bold green]")

    lat = report.get("latency", {})
    if lat.get("count"):
        console.print(f"\n[bold]Latency (ms):[/bold]")
        console.print(f"    Min: {lat.get('min_ms')}  Max: {lat.get('max_ms')}  Avg: {lat.get('avg_ms')}")
        console.print(f"    P50: {lat.get('p50_ms')}  P90: {lat.get('p90_ms')}  P95: {lat.get('p95_ms')}  P99: {lat.get('p99_ms')}")

    eb = report.get("error_breakdown", {})
    if eb.get("by_status_code"):
        console.print(f"\n[bold]Top Status Codes:[/bold]")
        for item in eb["by_status_code"][:10]:
            console.print(f"    {item['status_code']}: {item['count']}")

    if report.get("top_endpoints_by_volume"):
        console.print(f"\n[bold]Top Endpoints by Volume:[/bold]")
        for item in report["top_endpoints_by_volume"][:5]:
            console.print(f"    {item['endpoint_id'][:8]}... total={item['total']} success={item.get('success_rate', 0)}%")

    if report.get("worst_endpoints_by_failure"):
        console.print(f"\n[bold red]Worst Endpoints by Failure:[/bold red]")
        for item in report["worst_endpoints_by_failure"][:5]:
            console.print(f"    {item['endpoint_id'][:8]}... total={item['total']} failure={item.get('failure_rate', 0)}%")


# ── Template Commands ────────────────────────────────────────────────


@cli.command("template")
@click.argument("action", type=click.Choice(["list", "show", "create"]))
@click.option("--key", default=None, help="Template key (e.g. slack, discord)")
@click.option("--tag", default=None, help="Filter templates by tag")
@click.option("--url", default=None, help="Webhook URL (required for 'create')")
@click.option("--name", default=None, help="Custom endpoint name")
@click.option("--secret", default=None, help="HMAC signing secret")
@click.option("--description", default=None, help="Endpoint description")
@click.option("--header", "extra_headers", multiple=True, help="Extra headers in 'Name: Value' format")
@click.option("--store", "store_path", default=DEFAULT_STORE_PATH, help="Path to store file")
@click.pass_context
def template_cmd(
    ctx: click.Context,
    action: str,
    key: str | None,
    tag: str | None,
    url: str | None,
    name: str | None,
    secret: str | None,
    description: str | None,
    extra_headers: tuple[str, ...],
    store_path: str,
) -> None:
    """List, inspect, or create endpoints from pre-built templates."""
    from .templates import TemplateRegistry
    from .service import WebhookService
    from .models import Header

    registry = TemplateRegistry()

    if action == "list":
        templates = registry.list_templates(tag=tag)
        if not templates:
            console.print("[yellow]No templates found.[/yellow]")
            return
        console.print(f"\n[bold cyan]Available Templates ({len(templates)})[/bold cyan]\n")
        for t in templates:
            tags_str = f" [{', '.join(t['tags'])}]" if t.get("tags") else ""
            console.print(f"  [bold]{t['key']}[/bold] — {t['name']}{tags_str}")
            console.print(f"    {t['description']}")
            if t.get("url_placeholder"):
                console.print(f"    URL: {t['url_placeholder']}")
            console.print()
        return

    if action == "show":
        if not key:
            console.print("[red]--key is required for 'show'[/red]")
            sys.exit(1)
        template = registry.get_template(key)
        if template is None:
            console.print(f"[red]Template not found: {key}[/red]")
            console.print(f"Available: {', '.join(registry.keys)}")
            sys.exit(1)
        d = template.to_dict()
        console.print(f"\n[bold cyan]Template: {d['name']}[/bold cyan]\n")
        for k, v in d.items():
            if k in ("key", "name"):
                continue
            console.print(f"  {k}: {v}")
        return

    if action == "create":
        if not key:
            console.print("[red]--key is required for 'create'[/red]")
            sys.exit(1)
        if not url:
            console.print("[red]--url is required for 'create'[/red]")
            console.print(f"Use 'agent-webhook template show --key {key}' to see URL format")
            sys.exit(1)

        hdrs: dict[str, str] = {}
        for h in extra_headers:
            if ":" in h:
                hn, hv = h.split(":", 1)
                hdrs[hn.strip()] = hv.strip()

        endpoint = registry.create_endpoint(
            key=key,
            url=url,
            name=name,
            secret=secret,
            description=description,
            extra_headers=hdrs if hdrs else None,
        )
        if endpoint is None:
            console.print(f"[red]Template not found: {key}[/red]")
            console.print(f"Available: {', '.join(registry.keys)}")
            sys.exit(1)

        service = WebhookService(store_path=store_path)
        ep = service.create_endpoint(
            name=endpoint.name,
            url=endpoint.url,
            method=endpoint.method.value,
            headers={h.name: h.value for h in endpoint.headers},
            tags=endpoint.tags,
            secret=endpoint.secret,
            timeout_seconds=endpoint.timeout_seconds,
            description=endpoint.description,
            max_retries=endpoint.retry_policy.max_retries,
            initial_delay_seconds=endpoint.retry_policy.initial_delay_seconds,
            max_delay_seconds=endpoint.retry_policy.max_delay_seconds,
            backoff_multiplier=endpoint.retry_policy.backoff_multiplier,
            retry_on_status_codes=endpoint.retry_policy.retry_on_status_codes,
        )
        console.print(f"[green]✓[/green] Created endpoint from '{key}' template")
        console.print(f"  ID:   {ep.id}")
        console.print(f"  Name: {ep.name}")
        console.print(f"  URL:  {ep.url}")


# ── Schedule Command ─────────────────────────────────────────────────


@cli.command("schedule")
@click.argument("endpoint_id")
@click.argument("payload")
@click.option("--at", "scheduled_at", required=True, help="ISO 8601 datetime (e.g. 2025-12-25T10:00:00Z or +1h or +30m)")
@click.option("--event-type", default=None, help="Event type tag")
@click.option("--header", "headers", multiple=True, help="Extra headers 'Name: Value'")
@click.option("--store", "store_path", default=DEFAULT_STORE_PATH, help="Path to store file")
@click.pass_context
def schedule_cmd(
    ctx: click.Context,
    endpoint_id: str,
    payload: str,
    scheduled_at: str,
    event_type: str | None,
    headers: tuple[str, ...],
    store_path: str,
) -> None:
    """Schedule a webhook delivery for a future time.

    Use --at with an ISO datetime or relative time like +1h, +30m, +2d.
    """
    import json as _json
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    from .service import WebhookService

    service = WebhookService(store_path=store_path)

    # Parse scheduled_at
    at: _dt
    if scheduled_at.startswith("+"):
        # Relative time: +1h, +30m, +2d
        unit_map = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}
        match = re.match(r"\+(\d+)([smhd])", scheduled_at)
        if not match:
            console.print(f"[red]Invalid relative time: {scheduled_at}. Use format like +1h, +30m, +2d[/red]")
            sys.exit(1)
        amount = int(match.group(1))
        unit = unit_map[match.group(2)]
        at = _dt.now(_tz.utc) + _td(**{unit: amount})
    else:
        try:
            at = _dt.fromisoformat(scheduled_at.replace("Z", "+00:00"))
        except ValueError:
            console.print(f"[red]Invalid datetime: {scheduled_at}[/red]")
            sys.exit(1)
    if at.tzinfo is None:
        at = at.replace(tzinfo=_tz.utc)

    # Parse payload
    try:
        payload_data = _json.loads(payload)
    except _json.JSONDecodeError:
        console.print(f"[red]Invalid JSON payload[/red]")
        sys.exit(1)

    # Parse headers
    hdrs: dict[str, str] = {}
    for h in headers:
        if ":" in h:
            hn, hv = h.split(":", 1)
            hdrs[hn.strip()] = hv.strip()

    delivery = service.schedule_webhook(
        endpoint_id=endpoint_id,
        payload=payload_data,
        scheduled_at=at,
        event_type=event_type,
        headers=hdrs if hdrs else None,
    )
    if delivery is None:
        console.print(f"[red]Endpoint not found: {endpoint_id}[/red]")
        sys.exit(1)

    console.print(f"[green]✓[/green] Scheduled webhook delivery")
    console.print(f"  Delivery ID:  {delivery.id}")
    console.print(f"  Endpoint:     {endpoint_id}")
    console.print(f"  Scheduled At: {at.isoformat()}")
    console.print(f"  Status:       {delivery.status.value}")


# ── Recurring Schedules ──────────────────────────────────────────────


@cli.group("schedule")
def schedule_group() -> None:
    """Manage recurring webhook delivery schedules."""


@schedule_group.command("create")
@click.option("--name", required=True, help="Schedule name")
@click.option("--endpoint-id", required=True, help="Target endpoint ID")
@click.option("--payload", required=True, help="JSON payload to deliver on each run")
@click.option("--interval", "interval_value", required=True, type=int, help="Interval magnitude")
@click.option(
    "--unit", "interval_unit",
    type=click.Choice(["seconds", "minutes", "hours", "days"]),
    default="minutes",
    help="Interval unit",
)
@click.option("--event-type", default=None, help="Event type tag")
@click.option("--max-runs", default=0, type=int, help="Maximum runs (0 = unlimited)")
@click.option(
    "--store", "store_path",
    envvar="WEBHOOK_STORE",
    default="webhook_store.json",
    help="Store file path",
)
def schedule_create(
    name: str,
    endpoint_id: str,
    payload: str,
    interval_value: int,
    interval_unit: str,
    event_type: str | None,
    max_runs: int,
    store_path: str,
) -> None:
    """Create a recurring webhook delivery schedule."""
    import json as _json
    from .service import WebhookService

    service = WebhookService(store_path=store_path)

    try:
        payload_data = _json.loads(payload)
    except _json.JSONDecodeError:
        console.print("[red]Invalid JSON payload[/red]")
        sys.exit(1)

    schedule = service.create_schedule(
        name=name,
        endpoint_id=endpoint_id,
        payload=payload_data,
        interval_value=interval_value,
        interval_unit=interval_unit,
        event_type=event_type,
        max_runs=max_runs,
    )
    if schedule is None:
        console.print(f"[red]Endpoint not found: {endpoint_id}[/red]")
        sys.exit(1)

    console.print(f"[green]✓[/green] Created recurring schedule")
    console.print(f"  Schedule ID:     {schedule.id}")
    console.print(f"  Name:            {schedule.name}")
    console.print(f"  Endpoint:        {schedule.endpoint_id}")
    console.print(f"  Interval:        Every {schedule.interval_value} {schedule.interval_unit.value}")
    console.print(f"  Max Runs:        {'unlimited' if schedule.max_runs == 0 else schedule.max_runs}")
    console.print(f"  Next Run At:     {format_dt(schedule.next_run_at)}")
    console.print(f"  Active:          {schedule.active}")


@schedule_group.command("list")
@click.option("--endpoint-id", default=None, help="Filter by endpoint")
@click.option("--active-only", is_flag=True, default=False, help="Show only active schedules")
@click.option(
    "--store", "store_path",
    envvar="WEBHOOK_STORE",
    default="webhook_store.json",
    help="Store file path",
)
def schedule_list(
    endpoint_id: str | None,
    active_only: bool,
    store_path: str,
) -> None:
    """List recurring schedules."""
    from .service import WebhookService

    service = WebhookService(store_path=store_path)
    schedules = service.list_schedules(endpoint_id=endpoint_id, active_only=active_only)

    if not schedules:
        console.print("[dim]No schedules found[/dim]")
        return

    table = Table(title="Recurring Schedules")
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Name")
    table.add_column("Endpoint")
    table.add_column("Interval")
    table.add_column("Runs", justify="right")
    table.add_column("Next Run")
    table.add_column("Active")

    for s in schedules:
        max_str = f"{s.run_count}/{s.max_runs}" if s.max_runs > 0 else str(s.run_count)
        table.add_row(
            s.id[:12],
            s.name,
            s.endpoint_id[:12],
            f"{s.interval_value} {s.interval_unit.value}",
            max_str,
            format_dt(s.next_run_at),
            "✓" if s.active else "✗",
        )
    console.print(table)


@schedule_group.command("pause")
@click.argument("schedule_id")
@click.option(
    "--store", "store_path",
    envvar="WEBHOOK_STORE",
    default="webhook_store.json",
    help="Store file path",
)
def schedule_pause(schedule_id: str, store_path: str) -> None:
    """Pause a recurring schedule."""
    from .service import WebhookService

    service = WebhookService(store_path=store_path)
    schedule = service.pause_schedule(schedule_id)
    if schedule is None:
        console.print(f"[red]Schedule not found: {schedule_id}[/red]")
        sys.exit(1)
    console.print(f"[yellow]⏸[/yellow] Paused schedule: {schedule.name}")


@schedule_group.command("resume")
@click.argument("schedule_id")
@click.option(
    "--store", "store_path",
    envvar="WEBHOOK_STORE",
    default="webhook_store.json",
    help="Store file path",
)
def schedule_resume(schedule_id: str, store_path: str) -> None:
    """Resume a paused recurring schedule."""
    from .service import WebhookService

    service = WebhookService(store_path=store_path)
    schedule = service.resume_schedule(schedule_id)
    if schedule is None:
        console.print(f"[red]Schedule not found: {schedule_id}[/red]")
        sys.exit(1)
    console.print(f"[green]▶[/green] Resumed schedule: {schedule.name}")


@schedule_group.command("delete")
@click.argument("schedule_id")
@click.option(
    "--store", "store_path",
    envvar="WEBHOOK_STORE",
    default="webhook_store.json",
    help="Store file path",
)
def schedule_delete(schedule_id: str, store_path: str) -> None:
    """Delete a recurring schedule."""
    from .service import WebhookService

    service = WebhookService(store_path=store_path)
    if service.delete_schedule(schedule_id):
        console.print(f"[red]✗[/red] Deleted schedule: {schedule_id}")
    else:
        console.print(f"[red]Schedule not found: {schedule_id}[/red]")
        sys.exit(1)


@schedule_group.command("show")
@click.argument("schedule_id")
@click.option(
    "--store", "store_path",
    envvar="WEBHOOK_STORE",
    default="webhook_store.json",
    help="Store file path",
)
def schedule_show(schedule_id: str, store_path: str) -> None:
    """Show details of a recurring schedule."""
    import json as _json
    from .service import WebhookService

    service = WebhookService(store_path=store_path)
    schedule = service.get_schedule(schedule_id)
    if schedule is None:
        console.print(f"[red]Schedule not found: {schedule_id}[/red]")
        sys.exit(1)
    console.print_json(_json.dumps(schedule.model_dump(mode="json"), default=str))


@schedule_group.command("fire")
@click.option(
    "--store", "store_path",
    envvar="WEBHOOK_STORE",
    default="webhook_store.json",
    help="Store file path",
)
def schedule_fire(store_path: str) -> None:
    """Manually fire all due schedules (normally handled by the worker)."""
    import asyncio
    from .service import WebhookService

    service = WebhookService(store_path=store_path)

    async def _fire() -> list:
        return await service.process_due_schedules()

    deliveries = asyncio.run(_fire())
    if not deliveries:
        console.print("[dim]No schedules due[/dim]")
        return
    console.print(f"[green]✓[/green] Fired {len(deliveries)} schedule(s)")
    for d in deliveries:
        console.print(f"  Delivery: {d.id}  Endpoint: {d.endpoint_id[:12]}  Status: {d.status.value}")


# ── Bulk Endpoint Operations ─────────────────────────────────────────


@cli.group("bulk")
def bulk_group() -> None:
    """Bulk endpoint operations (pause, resume, disable, delete)."""


def _bulk_resolve(store_path: str, endpoint_ids: tuple[str, ...] | None, tag: str | None) -> list[str]:
    """Resolve bulk targets from IDs or tag."""
    from .service import WebhookService

    service = WebhookService(store_path=store_path)
    if tag:
        return [e.id for e in service.list_endpoints(tag=tag)]
    return list(endpoint_ids) if endpoint_ids else []


@bulk_group.command("pause")
@click.option("--endpoint-id", "endpoint_ids", multiple=True, help="Endpoint IDs to pause")
@click.option("--tag", default=None, help="Pause all endpoints with this tag")
@click.option(
    "--store", "store_path",
    envvar="WEBHOOK_STORE",
    default="webhook_store.json",
    help="Store file path",
)
def bulk_pause(endpoint_ids: tuple[str, ...], tag: str | None, store_path: str) -> None:
    """Pause multiple endpoints by IDs or tag."""
    from .service import WebhookService

    if not endpoint_ids and not tag:
        console.print("[red]Specify --endpoint-id or --tag[/red]")
        sys.exit(1)
    service = WebhookService(store_path=store_path)
    paused = service.bulk_pause(endpoint_ids=list(endpoint_ids) or None, tag=tag)
    console.print(f"[yellow]⏸[/yellow] Paused {len(paused)} endpoint(s): {', '.join(paused)}")


@bulk_group.command("resume")
@click.option("--endpoint-id", "endpoint_ids", multiple=True, help="Endpoint IDs to resume")
@click.option("--tag", default=None, help="Resume all endpoints with this tag")
@click.option(
    "--store", "store_path",
    envvar="WEBHOOK_STORE",
    default="webhook_store.json",
    help="Store file path",
)
def bulk_resume(endpoint_ids: tuple[str, ...], tag: str | None, store_path: str) -> None:
    """Resume multiple endpoints by IDs or tag."""
    from .service import WebhookService

    if not endpoint_ids and not tag:
        console.print("[red]Specify --endpoint-id or --tag[/red]")
        sys.exit(1)
    service = WebhookService(store_path=store_path)
    resumed = service.bulk_resume(endpoint_ids=list(endpoint_ids) or None, tag=tag)
    console.print(f"[green]▶[/green] Resumed {len(resumed)} endpoint(s): {', '.join(resumed)}")


@bulk_group.command("disable")
@click.option("--endpoint-id", "endpoint_ids", multiple=True, help="Endpoint IDs to disable")
@click.option("--tag", default=None, help="Disable all endpoints with this tag")
@click.option(
    "--store", "store_path",
    envvar="WEBHOOK_STORE",
    default="webhook_store.json",
    help="Store file path",
)
def bulk_disable(endpoint_ids: tuple[str, ...], tag: str | None, store_path: str) -> None:
    """Disable multiple endpoints by IDs or tag."""
    from .service import WebhookService

    if not endpoint_ids and not tag:
        console.print("[red]Specify --endpoint-id or --tag[/red]")
        sys.exit(1)
    service = WebhookService(store_path=store_path)
    disabled = service.bulk_disable(endpoint_ids=list(endpoint_ids) or None, tag=tag)
    console.print(f"[red]✗[/red] Disabled {len(disabled)} endpoint(s): {', '.join(disabled)}")


@bulk_group.command("delete")
@click.option("--endpoint-id", "endpoint_ids", multiple=True, help="Endpoint IDs to delete")
@click.option("--tag", default=None, help="Delete all endpoints with this tag")
@click.confirmation_option(prompt="Are you sure you want to delete these endpoints?")
@click.option(
    "--store", "store_path",
    envvar="WEBHOOK_STORE",
    default="webhook_store.json",
    help="Store file path",
)
def bulk_delete(endpoint_ids: tuple[str, ...], tag: str | None, store_path: str) -> None:
    """Delete multiple endpoints by IDs or tag."""
    from .service import WebhookService

    if not endpoint_ids and not tag:
        console.print("[red]Specify --endpoint-id or --tag[/red]")
        sys.exit(1)
    service = WebhookService(store_path=store_path)
    deleted = service.bulk_delete(endpoint_ids=list(endpoint_ids) or None, tag=tag)
    console.print(f"[red]✗[/red] Deleted {len(deleted)} endpoint(s): {', '.join(deleted)}")


# ── Dry-Run / Simulation ─────────────────────────────────────────────


@cli.command("simulate")
@click.argument("endpoint_id")
@click.option("--payload", required=True, help="JSON payload to simulate")
@click.option("--event-type", default=None, help="Event type")
@click.option("--header", "headers", multiple=True, help="Extra headers (format: Key:Value)")
@click.option(
    "--store", "store_path",
    envvar="WEBHOOK_STORE",
    default="webhook_store.json",
    help="Store file path",
)
def simulate_cmd(
    endpoint_id: str,
    payload: str,
    event_type: str | None,
    headers: tuple[str, ...],
    store_path: str,
) -> None:
    """Simulate a webhook delivery without sending it (dry run).

    Shows what would be sent: URL, method, headers, HMAC signature,
    transformed payload, retry config, circuit breaker state.
    """
    import json as _json
    from .service import WebhookService

    service = WebhookService(store_path=store_path)

    try:
        payload_data = _json.loads(payload)
    except _json.JSONDecodeError:
        console.print("[red]Invalid JSON payload[/red]")
        sys.exit(1)

    hdrs: dict[str, str] = {}
    for h in headers:
        if ":" in h:
            hn, hv = h.split(":", 1)
            hdrs[hn.strip()] = hv.strip()

    result = service.simulate_delivery(
        endpoint_id=endpoint_id,
        payload=payload_data,
        event_type=event_type,
        headers=hdrs if hdrs else None,
    )

    if "error" in result:
        console.print(f"[red]{result['error']}[/red]")
        sys.exit(1)

    console.print(f"[cyan]🔍 Dry Run Simulation[/cyan]")
    console.print(f"  Endpoint:     {result['endpoint_name']} ({result['endpoint_id'][:12]})")
    console.print(f"  Status:       {result['endpoint_status']}")
    console.print(f"  URL:          {result['url']}")
    console.print(f"  Method:       {result['method']}")
    console.print(f"  Event Type:   {result['event_type']}")
    console.print(f"  Timeout:      {result['timeout_seconds']}s")
    console.print()

    if result.get("transformed_payload"):
        console.print(f"[yellow]Transformed Payload:[/yellow]")
        console.print_json(_json.dumps(result["transformed_payload"], default=str))
    else:
        console.print(f"[dim]Payload (no transforms):[/dim]")
        console.print_json(_json.dumps(result["original_payload"], default=str))

    console.print(f"\n[yellow]Headers that would be sent:[/yellow]")
    for hn, hv in result["headers"].items():
        display = hv
        if hn.lower() in ("x-webhook-signature",) and len(hv) > 50:
            display = hv[:50] + "..."
        console.print(f"  {hn}: {display}")

    console.print(f"\n[dim]Signature present: {result['signature_present']}[/dim]")
    console.print(f"[dim]Payload size: {result['payload_size_bytes']} bytes[/dim]")
    console.print(f"[dim]Circuit breaker: {result['circuit_breaker_state'] or 'Not tracked'}[/dim]")

    rp = result["retry_policy"]
    console.print(f"\n[cyan]Retry Policy:[/cyan]")
    console.print(f"  Max retries:           {rp['max_retries']}")
    console.print(f"  Initial delay:         {rp['initial_delay_seconds']}s")
    console.print(f"  Backoff multiplier:    {rp['backoff_multiplier']}x")
    console.print(f"  Retry on status codes: {rp['retry_on_status_codes']}")

    if result.get("rate_limit"):
        console.print(f"\n[cyan]Rate Limit:[/cyan]")
        console.print_json(_json.dumps(result["rate_limit"], default=str))

    console.print(f"\n[green]✓[/green] No HTTP request was made (dry run).")


if __name__ == "__main__":
    cli()
