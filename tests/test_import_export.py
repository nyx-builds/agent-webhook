"""Tests for batch import/export functionality."""

import json
import os
import tempfile

import pytest

from agent_webhook.import_export import (
    EXPORT_SCHEMA_VERSION,
    export_config,
    export_to_file,
    import_config,
    import_from_file,
)
from agent_webhook.store_sqlite import SQLiteStore


@pytest.fixture
def store(tmp_path):
    """Create a SQLite store in a temp directory."""
    db_path = str(tmp_path / "test.db")
    return SQLiteStore(db_path)


@pytest.fixture
def populated_store(store):
    """Create a store with endpoints, relay rules, transforms, and subscriptions."""
    from agent_webhook.models import (
        EventSubscription,
        Header,
        PayloadTransform,
        RelayRule,
        WebhookEndpoint,
        WebhookMethod,
        WebhookStatus,
    )

    # Endpoints
    ep1 = WebhookEndpoint(
        name="API Server",
        url="https://api.example.com/webhook",
        method=WebhookMethod.POST,
        tags=["production", "api"],
        secret="secret123",
    )
    store.add_endpoint(ep1)

    ep2 = WebhookEndpoint(
        name="Analytics",
        url="https://analytics.example.com/hook",
        tags=["analytics"],
    )
    store.add_endpoint(ep2)

    # Relay rule
    rule = RelayRule(
        name="Forward Stripe",
        path_pattern="/stripe/*",
        target_endpoint_ids=[ep1.id],
        tags=["payments"],
        verify_signature=True,
        verify_secret="stripe_secret",
    )
    store.add_relay_rule(rule)

    # Transform
    transform = PayloadTransform(
        name="Flatten Data",
        type="field_map",
        config={"mapping": {"old_key": "new_key"}},
    )
    store.add_transform(transform)

    # Subscription
    sub = EventSubscription(
        endpoint_id=ep1.id,
        event_types=["order.created", "order.updated"],
    )
    store.add_subscription(sub)

    return store


class TestExportConfig:
    """Tests for export_config."""

    def test_export_empty_store(self, store):
        result = export_config(store)
        assert result["schema_version"] == EXPORT_SCHEMA_VERSION
        assert result["endpoints"] == []
        assert result["relay_rules"] == []
        assert result["summary"]["endpoints"] == 0

    def test_export_populated_store(self, populated_store):
        result = export_config(populated_store)
        assert len(result["endpoints"]) == 2
        assert len(result["relay_rules"]) == 1
        assert len(result["transforms"]) == 1
        assert len(result["subscriptions"]) == 1
        assert result["summary"]["endpoints"] == 2

    def test_export_excludes_endpoint_secrets(self, populated_store):
        result = export_config(populated_store)
        for ep in result["endpoints"]:
            assert "secret" not in ep or ep["secret"] is None

    def test_export_excludes_relay_secrets(self, populated_store):
        result = export_config(populated_store)
        for rule in result["relay_rules"]:
            assert "verify_secret" not in rule or rule["verify_secret"] is None

    def test_export_partial(self, populated_store):
        result = export_config(populated_store, include_endpoints=True, include_relay_rules=False)
        assert len(result["endpoints"]) == 2
        assert result["relay_rules"] == []
        assert result["summary"]["relay_rules"] == 0

    def test_export_has_schema_version(self, store):
        result = export_config(store)
        assert result["schema_version"] == "1.0"
        assert "exported_at" in result
        assert "version" in result

    def test_export_to_file(self, populated_store, tmp_path):
        file_path = str(tmp_path / "export.json")
        summary = export_to_file(populated_store, file_path)
        assert summary["endpoints"] == 2

        with open(file_path) as f:
            data = json.load(f)
        assert data["schema_version"] == EXPORT_SCHEMA_VERSION
        assert len(data["endpoints"]) == 2


class TestImportConfig:
    """Tests for import_config."""

    def test_import_into_empty_store(self, tmp_path, populated_store):
        # Export from populated, import into a fresh empty store
        empty_store = SQLiteStore(str(tmp_path / "empty.db"))
        export_data = export_config(populated_store)
        summary = import_config(empty_store, export_data)
        assert summary["total_imported"] == 5  # 2 ep + 1 rule + 1 transform + 1 sub
        assert summary["errors"] == []

    def test_import_skip_strategy(self, tmp_path, populated_store):
        export_data = export_config(populated_store)
        # First import
        store = SQLiteStore(str(tmp_path / "skip1.db"))
        import_config(store, export_data)
        # Second import with skip — should skip all
        summary = import_config(store, export_data)
        assert summary["total_imported"] == 0
        assert summary["total_skipped"] == 5

    def test_import_overwrite_strategy(self, tmp_path, populated_store):
        export_data = export_config(populated_store)
        # First import
        store = SQLiteStore(str(tmp_path / "overwrite1.db"))
        import_config(store, export_data)
        # Second import with overwrite
        summary = import_config(store, export_data, conflict_strategy="overwrite")
        assert summary["total_imported"] == 5
        assert summary["errors"] == []

    def test_import_rename_strategy(self, tmp_path, populated_store):
        export_data = export_config(populated_store)
        # First import
        store = SQLiteStore(str(tmp_path / "rename1.db"))
        import_config(store, export_data)
        # Second import with rename — creates new items
        summary = import_config(store, export_data, conflict_strategy="rename")
        assert summary["total_imported"] == 5

    def test_import_from_file(self, tmp_path, populated_store):
        file_path = str(tmp_path / "export.json")
        export_to_file(populated_store, file_path)
        empty_store = SQLiteStore(str(tmp_path / "from_file.db"))
        summary = import_from_file(empty_store, file_path)
        assert summary["total_imported"] == 5

    def test_import_empty_data(self, store):
        summary = import_config(store, {"endpoints": [], "relay_rules": []})
        assert summary["total_imported"] == 0
        assert summary["errors"] == []

    def test_import_handles_bad_endpoint(self, store):
        bad_data = {
            "endpoints": [{"name": "Bad", "url": "not-a-url"}],
        }
        summary = import_config(store, bad_data)
        assert summary["total_imported"] == 0
        assert len(summary["errors"]) == 1

    def test_import_subscription_for_missing_endpoint(self, store):
        bad_data = {
            "subscriptions": [{"endpoint_id": "nonexistent", "event_types": ["test"]}],
        }
        summary = import_config(store, bad_data)
        assert summary["total_imported"] == 0
        assert len(summary["errors"]) == 1

    def test_roundtrip_preserves_structure(self, tmp_path, populated_store):
        """Export and re-import should preserve the same structure."""
        export_data = export_config(populated_store)

        # Import into a fresh store
        new_store = SQLiteStore(str(tmp_path / "roundtrip.db"))
        import_config(new_store, export_data)

        # Re-export and compare
        re_export = export_config(new_store)
        assert len(re_export["endpoints"]) == len(export_data["endpoints"])
        assert len(re_export["relay_rules"]) == len(export_data["relay_rules"])
        assert len(re_export["transforms"]) == len(export_data["transforms"])
        assert len(re_export["subscriptions"]) == len(export_data["subscriptions"])
