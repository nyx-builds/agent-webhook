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
    DeliveryStatus,
    EventSubscription,
    Header,
    RelayRule,
    RetryPolicy,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookMethod,
    WebhookStatus,
)
from .store import WebhookStore

console = Console()

DEFAULT_STORE_PATH = "webhook_store.json"


def get_store(store_path: str | None = None) -> WebhookStore:
    return WebhookStore(store_path or DEFAULT_STORE_PATH)


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


if __name__ == "__main__":
    cli()
