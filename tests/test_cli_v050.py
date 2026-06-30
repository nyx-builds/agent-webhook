"""Tests for v0.5.0 CLI commands: relay-filter, export, import."""

import json

import pytest
from click.testing import CliRunner

from agent_webhook.cli import cli
from agent_webhook.models import (
    EventSubscription,
    PayloadTransform,
    RelayRule,
    WebhookEndpoint,
)
from agent_webhook.store import WebhookStore


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def store_path(tmp_path):
    """Use .json so both the test fixture and CLI open the same WebhookStore."""
    return str(tmp_path / "test_cli.json")


@pytest.fixture
def store(store_path):
    return WebhookStore(store_path)


@pytest.fixture
def endpoint(store):
    ep = WebhookEndpoint(name="Test EP", url="https://example.com/hook", tags=["test"])
    store.add_endpoint(ep)
    return ep


@pytest.fixture
def relay_rule(store, endpoint):
    rule = RelayRule(
        name="Test Rule",
        path_pattern="/test/*",
        target_endpoint_ids=[endpoint.id],
    )
    store.add_relay_rule(rule)
    return rule


# ── Relay Filter CLI Tests ──────────────────────────────────────────


class TestRelayFilterCLI:
    """Test relay-filter CLI commands."""

    def test_filter_set_with_inline_json(self, runner, store_path, store, relay_rule):
        filter_json = json.dumps({
            "logic": "all",
            "conditions": [
                {"type": "payload", "field": "status", "operator": "equals", "value": "active"},
            ],
        })
        result = runner.invoke(cli, ["-s", store_path, "relay-filter", "set", relay_rule.id, "--json", filter_json])
        assert result.exit_code == 0
        assert "Filter rules set" in result.output

    def test_filter_set_with_file(self, runner, store_path, store, relay_rule, tmp_path):
        filter_data = {
            "logic": "any",
            "conditions": [
                {"type": "header", "field": "X-Type", "operator": "equals", "value": "a"},
            ],
        }
        file_path = tmp_path / "filter.json"
        file_path.write_text(json.dumps(filter_data))

        result = runner.invoke(cli, ["-s", store_path, "relay-filter", "set", relay_rule.id, "-f", str(file_path)])
        assert result.exit_code == 0
        assert "Filter rules set" in result.output

    def test_filter_set_invalid_json(self, runner, store_path, store, relay_rule):
        result = runner.invoke(cli, ["-s", store_path, "relay-filter", "set", relay_rule.id, "--json", "{invalid}"])
        assert result.exit_code != 0

    def test_filter_set_validation_error(self, runner, store_path, store, relay_rule):
        bad_filter = json.dumps({"logic": "xor", "conditions": []})
        result = runner.invoke(cli, ["-s", store_path, "relay-filter", "set", relay_rule.id, "--json", bad_filter])
        assert result.exit_code != 0
        assert "xor" in result.output

    def test_filter_set_nonexistent_rule(self, runner, store_path):
        filter_json = json.dumps({"logic": "all", "conditions": []})
        result = runner.invoke(cli, ["-s", store_path, "relay-filter", "set", "nonexistent", "--json", filter_json])
        assert result.exit_code != 0

    def test_filter_clear(self, runner, store_path, store, relay_rule):
        # Set first
        filter_json = json.dumps({"logic": "all", "conditions": []})
        runner.invoke(cli, ["-s", store_path, "relay-filter", "set", relay_rule.id, "--json", filter_json])

        # Then clear
        result = runner.invoke(cli, ["-s", store_path, "relay-filter", "clear", relay_rule.id])
        assert result.exit_code == 0
        assert "cleared" in result.output

    def test_filter_validate_valid(self, runner):
        filter_data = {
            "logic": "all",
            "conditions": [
                {"type": "payload", "field": "x", "operator": "equals", "value": "y"},
            ],
        }
        result = runner.invoke(cli, ["relay-filter", "validate", "--json", json.dumps(filter_data)])
        assert result.exit_code == 0
        assert "valid" in result.output

    def test_filter_validate_invalid(self, runner):
        filter_data = {
            "logic": "invalid",
            "conditions": [
                {"type": "payload", "field": "x", "operator": "bad_op", "value": "y"},
            ],
        }
        result = runner.invoke(cli, ["relay-filter", "validate", "--json", json.dumps(filter_data)])
        assert result.exit_code != 0


# ── Export/Import CLI Tests ─────────────────────────────────────────


class TestExportImportCLI:
    """Test export and import CLI commands."""

    def test_export_basic(self, runner, store_path, store, endpoint, tmp_path):
        file_path = str(tmp_path / "export.json")
        result = runner.invoke(cli, ["-s", store_path, "export", "-f", file_path])
        assert result.exit_code == 0
        assert "exported" in result.output
        assert "Endpoints:      1" in result.output

        with open(file_path) as f:
            data = json.load(f)
        assert "schema_version" in data

    def test_export_no_endpoints(self, runner, store_path, store, endpoint, tmp_path):
        file_path = str(tmp_path / "export.json")
        result = runner.invoke(cli, ["-s", store_path, "export", "-f", file_path, "--no-endpoints"])
        assert result.exit_code == 0
        assert "Endpoints:      0" in result.output

    def test_import_into_empty(self, runner, store_path, store, endpoint, tmp_path):
        # Export first
        file_path = str(tmp_path / "export.json")
        runner.invoke(cli, ["-s", store_path, "export", "-f", file_path])

        # Import into a different store
        import_path = str(tmp_path / "import_store.db")
        result = runner.invoke(cli, ["-s", import_path, "import", file_path])
        assert result.exit_code == 0
        assert "Import complete" in result.output

    def test_import_skip_strategy(self, runner, store_path, store, endpoint, tmp_path):
        file_path = str(tmp_path / "export.json")
        runner.invoke(cli, ["-s", store_path, "export", "-f", file_path])

        # First import
        import_path = str(tmp_path / "import_store2.db")
        runner.invoke(cli, ["-s", import_path, "import", file_path])

        # Second import — skip
        result = runner.invoke(cli, ["-s", import_path, "import", file_path, "--strategy", "skip"])
        assert result.exit_code == 0
        assert "Total Skipped:" in result.output

    def test_import_overwrite_strategy(self, runner, store_path, store, endpoint, tmp_path):
        file_path = str(tmp_path / "export.json")
        runner.invoke(cli, ["-s", store_path, "export", "-f", file_path])

        import_path = str(tmp_path / "import_store3.db")
        runner.invoke(cli, ["-s", import_path, "import", file_path])

        result = runner.invoke(cli, ["-s", import_path, "import", file_path, "--strategy", "overwrite"])
        assert result.exit_code == 0

    def test_export_import_roundtrip(self, runner, store_path, tmp_path):
        """Full roundtrip: create data → export → import → verify."""
        file_path = str(tmp_path / "roundtrip.json")

        # Create endpoint in original store
        runner.invoke(cli, [
            "-s", store_path,
            "endpoint", "add", "RoundtripEP",
            "https://rt.example.com/hook",
            "--tag", "test",
        ])

        # Export
        runner.invoke(cli, ["-s", store_path, "export", "-f", file_path])

        # Import into new store (use .json to match the same WebhookStore backend)
        import_path = str(tmp_path / "import_roundtrip.json")
        runner.invoke(cli, ["-s", import_path, "import", file_path])

        # Verify
        result = runner.invoke(cli, ["-s", import_path, "endpoint", "list"])
        assert result.exit_code == 0
        assert "RoundtripEP" in result.output
