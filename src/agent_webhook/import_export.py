"""Batch import/export for agent-webhook configuration.

Export endpoints, relay rules, transforms, subscriptions, and circuit breaker
configs to a portable JSON file. Import them back into any store.

This is useful for:
  - Migrating between environments (dev → staging → prod)
  - Backing up configuration
  - Sharing webhook setups with other agents
  - Version-controllable config snapshots
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

from .models import (
    EventSubscription,
    Header,
    PayloadTransform,
    RateLimit,
    RateLimitPeriod,
    RelayRule,
    RetryPolicy,
    TransformType,
    WebhookEndpoint,
    WebhookMethod,
    WebhookStatus,
)


# Schema version for the export format
EXPORT_SCHEMA_VERSION = "1.0"


def export_config(
    store,
    include_endpoints: bool = True,
    include_relay_rules: bool = True,
    include_transforms: bool = True,
    include_subscriptions: bool = True,
) -> dict[str, Any]:
    """Export configuration from a store to a portable dict.

    Args:
        store: A WebhookStore or SQLiteStore instance.
        include_endpoints: Export webhook endpoints.
        include_relay_rules: Export relay rules.
        include_transforms: Export payload transforms.
        include_subscriptions: Export event subscriptions.

    Returns:
        A dict that can be serialized to JSON and later imported.
    """
    export: dict[str, Any] = {
        "schema_version": EXPORT_SCHEMA_VERSION,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "version": "0.5.0",
        "endpoints": [],
        "relay_rules": [],
        "transforms": [],
        "subscriptions": [],
    }

    if include_endpoints:
        for ep in store.list_endpoints():
            ep_data = ep.model_dump(mode="json")
            # Remove sensitive secrets from export by default
            ep_data.pop("secret", None)
            export["endpoints"].append(ep_data)

    if include_relay_rules:
        for rule in store.list_relay_rules():
            rule_data = rule.model_dump(mode="json")
            # Don't export verification secrets
            rule_data.pop("verify_secret", None)
            export["relay_rules"].append(rule_data)

    if include_transforms and hasattr(store, "list_transforms"):
        for t in store.list_transforms():
            export["transforms"].append(t.model_dump(mode="json"))

    if include_subscriptions:
        for sub in store.list_subscriptions():
            export["subscriptions"].append(sub.model_dump(mode="json"))

    export["summary"] = {
        "endpoints": len(export["endpoints"]),
        "relay_rules": len(export["relay_rules"]),
        "transforms": len(export["transforms"]),
        "subscriptions": len(export["subscriptions"]),
    }

    return export


def import_config(
    store,
    data: dict[str, Any],
    conflict_strategy: str = "skip",
    restore_secrets: bool = False,
) -> dict[str, Any]:
    """Import configuration from an export dict into a store.

    Args:
        store: A WebhookStore or SQLiteStore instance.
        data: The export dict (from export_config or JSON file).
        conflict_strategy: How to handle ID conflicts.
            - "skip": Skip items that already exist by ID.
            - "overwrite": Replace existing items.
            - "rename": Create new IDs for items that conflict.
        restore_secrets: If True, restore secrets from the export (default False
            for security).

    Returns:
        A summary dict with counts of imported, skipped, and overwritten items.
    """
    summary: dict[str, Any] = {
        "imported": {"endpoints": 0, "relay_rules": 0, "transforms": 0, "subscriptions": 0},
        "skipped": {"endpoints": 0, "relay_rules": 0, "transforms": 0, "subscriptions": 0},
        "errors": [],
    }

    # Import endpoints
    for ep_data in data.get("endpoints", []):
        try:
            result = _import_endpoint(store, ep_data, conflict_strategy, restore_secrets)
            summary["imported" if result == "imported" else "skipped"]["endpoints"] += 1
        except Exception as e:
            summary["errors"].append(f"Endpoint {ep_data.get('name', '?')}: {e}")

    # Import relay rules
    for rule_data in data.get("relay_rules", []):
        try:
            result = _import_relay_rule(store, rule_data, conflict_strategy)
            summary["imported" if result == "imported" else "skipped"]["relay_rules"] += 1
        except Exception as e:
            summary["errors"].append(f"Relay rule {rule_data.get('name', '?')}: {e}")

    # Import transforms
    for t_data in data.get("transforms", []):
        if not hasattr(store, "add_transform"):
            break
        try:
            result = _import_transform(store, t_data, conflict_strategy)
            summary["imported" if result == "imported" else "skipped"]["transforms"] += 1
        except Exception as e:
            summary["errors"].append(f"Transform {t_data.get('name', '?')}: {e}")

    # Import subscriptions
    for sub_data in data.get("subscriptions", []):
        try:
            result = _import_subscription(store, sub_data, conflict_strategy)
            summary["imported" if result == "imported" else "skipped"]["subscriptions"] += 1
        except Exception as e:
            summary["errors"].append(f"Subscription: {e}")

    summary["total_imported"] = sum(summary["imported"].values())
    summary["total_skipped"] = sum(summary["skipped"].values())
    return summary


def _import_endpoint(
    store,
    ep_data: dict[str, Any],
    conflict_strategy: str,
    restore_secrets: bool,
) -> str:
    """Import a single endpoint. Returns 'imported' or 'skipped'."""
    ep_id = ep_data.get("id")

    # Check for existing
    existing = store.get_endpoint(ep_id) if ep_id else None

    if existing and conflict_strategy == "skip":
        return "skipped"

    if not restore_secrets:
        ep_data = {**ep_data, "secret": None}

    # Convert nested objects
    headers_data = ep_data.get("headers", [])
    headers = [
        Header(name=h["name"], value=h["value"])
        for h in headers_data
    ]
    ep_data["headers"] = headers

    # Retry policy
    rp_data = ep_data.get("retry_policy", {})
    if rp_data:
        ep_data["retry_policy"] = RetryPolicy(**rp_data)

    # Rate limit
    rl_data = ep_data.get("rate_limit")
    if rl_data:
        ep_data["rate_limit"] = RateLimit(
            max_requests=rl_data["max_requests"],
            period=RateLimitPeriod(rl_data.get("period", "minute")),
            burst=rl_data.get("burst", 0),
        )

    # Enum conversions
    if "method" in ep_data and isinstance(ep_data["method"], str):
        ep_data["method"] = WebhookMethod(ep_data["method"])
    if "status" in ep_data and isinstance(ep_data["status"], str):
        ep_data["status"] = WebhookStatus(ep_data["status"])
    if "signing_algorithm" in ep_data and isinstance(ep_data["signing_algorithm"], str):
        ep_data["signing_algorithm"] = ep_data["signing_algorithm"]

    if conflict_strategy == "rename" and existing:
        # Generate new ID by removing the id field
        ep_data.pop("id", None)

    if conflict_strategy == "overwrite" and existing:
        # Update in-place by replacing the model but keeping the ID
        from datetime import datetime as _dt
        safe_updates = {}
        for k, v in ep_data.items():
            if k == "id":
                continue
            if k == "signing_algorithm" and isinstance(v, str):
                from .models import SigningAlgorithm
                safe_updates[k] = SigningAlgorithm(v)
            elif k == "status" and isinstance(v, str):
                safe_updates[k] = WebhookStatus(v)
            elif k == "method" and isinstance(v, str):
                safe_updates[k] = WebhookMethod(v)
            elif k in ("created_at", "updated_at") and isinstance(v, str):
                # Parse ISO datetime strings
                try:
                    safe_updates[k] = _dt.fromisoformat(v.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    pass  # Skip malformed dates
            else:
                safe_updates[k] = v
        store.update_endpoint(ep_id, **safe_updates)
        return "imported"

    endpoint = WebhookEndpoint(**ep_data)
    store.add_endpoint(endpoint)
    return "imported"


def _import_relay_rule(
    store,
    rule_data: dict[str, Any],
    conflict_strategy: str,
) -> str:
    """Import a single relay rule."""
    rule_id = rule_data.get("id")

    existing_rules = store.list_relay_rules()
    existing = any(r.id == rule_id for r in existing_rules) if rule_id else False

    if existing and conflict_strategy == "skip":
        return "skipped"

    if conflict_strategy == "rename" and existing:
        rule_data.pop("id", None)
    elif conflict_strategy == "overwrite" and existing and hasattr(store, "delete_relay_rule"):
        store.delete_relay_rule(rule_id)
        rule_data.pop("id", None)

    rule = RelayRule(**rule_data)
    store.add_relay_rule(rule)
    return "imported"


def _import_transform(
    store,
    t_data: dict[str, Any],
    conflict_strategy: str,
) -> str:
    """Import a single transform."""
    t_id = t_data.get("id")

    existing = None
    if t_id and hasattr(store, "get_transform"):
        existing = store.get_transform(t_id)

    if existing and conflict_strategy == "skip":
        return "skipped"

    if "type" in t_data and isinstance(t_data["type"], str):
        t_data["type"] = TransformType(t_data["type"])

    if conflict_strategy == "rename" and existing:
        t_data.pop("id", None)
    elif conflict_strategy == "overwrite" and existing:
        store.delete_transform(t_id)
        t_data.pop("id", None)

    transform = PayloadTransform(**t_data)
    store.add_transform(transform)
    return "imported"


def _import_subscription(
    store,
    sub_data: dict[str, Any],
    conflict_strategy: str,
) -> str:
    """Import a single subscription."""
    sub_id = sub_data.get("id")

    # Verify endpoint exists
    endpoint_id = sub_data.get("endpoint_id")
    if endpoint_id and store.get_endpoint(endpoint_id) is None:
        raise ValueError(f"Endpoint not found: {endpoint_id}")

    existing_subs = store.list_subscriptions()
    existing = any(s.id == sub_id for s in existing_subs) if sub_id else False

    if existing and conflict_strategy == "skip":
        return "skipped"

    if conflict_strategy == "rename" and existing:
        sub_data.pop("id", None)
    elif conflict_strategy == "overwrite" and existing:
        store.delete_subscription(sub_id)
        sub_data.pop("id", None)

    sub = EventSubscription(**sub_data)
    store.add_subscription(sub)
    return "imported"


# ── Convenience file helpers ────────────────────────────────────────

def export_to_file(
    store,
    file_path: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """Export configuration to a JSON file. Returns the export summary."""
    data = export_config(store, **kwargs)
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    return data.get("summary", {})


def import_from_file(
    store,
    file_path: str,
    conflict_strategy: str = "skip",
    restore_secrets: bool = False,
) -> dict[str, Any]:
    """Import configuration from a JSON file. Returns the import summary."""
    with open(file_path, "r") as f:
        data = json.load(f)
    return import_config(store, data, conflict_strategy=conflict_strategy, restore_secrets=restore_secrets)
