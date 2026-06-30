"""Tests for relay rule filters — conditional forwarding based on headers and payload."""

import pytest

from agent_webhook.filters import (
    _MISSING,
    _get_nested,
    _eval_condition,
    _eval_string_condition,
    _eval_numeric_condition,
    _eval_list_condition,
    evaluate_filter,
    validate_filter_rules,
)


class TestNestedPath:
    """Tests for nested path resolution."""

    def test_simple_key(self):
        data = {"name": "test"}
        assert _get_nested(data, "name") == "test"

    def test_nested_key(self):
        data = {"a": {"b": {"c": 42}}}
        assert _get_nested(data, "a.b.c") == 42

    def test_missing_key(self):
        data = {"a": {"b": 1}}
        assert _get_nested(data, "a.c") is _MISSING

    def test_missing_nested(self):
        data = {"a": 1}
        assert _get_nested(data, "a.b.c.d") is _MISSING

    def test_array_index(self):
        data = {"items": [{"name": "first"}, {"name": "second"}]}
        assert _get_nested(data, "items[0].name") == "first"
        assert _get_nested(data, "items[1].name") == "second"

    def test_out_of_bounds_index(self):
        data = {"items": [1, 2]}
        assert _get_nested(data, "items[5]") is _MISSING

    def test_empty_path(self):
        data = {"a": 1}
        assert _get_nested(data, "") == data


class TestStringConditions:
    """Tests for string operators."""

    def test_equals(self):
        assert _eval_string_condition("hello", "equals", "hello") is True
        assert _eval_string_condition("hello", "equals", "world") is False

    def test_not_equals(self):
        assert _eval_string_condition("hello", "not_equals", "world") is True
        assert _eval_string_condition("hello", "not_equals", "hello") is False

    def test_contains(self):
        assert _eval_string_condition("hello world", "contains", "world") is True
        assert _eval_string_condition("hello world", "contains", "xyz") is False

    def test_starts_with(self):
        assert _eval_string_condition("hello world", "starts_with", "hello") is True
        assert _eval_string_condition("hello world", "starts_with", "world") is False

    def test_ends_with(self):
        assert _eval_string_condition("hello world", "ends_with", "world") is True
        assert _eval_string_condition("hello world", "ends_with", "hello") is False

    def test_regex(self):
        assert _eval_string_condition("order_123", "regex", r"order_\d+") is True
        assert _eval_string_condition("order_123", "regex", r"^payment_") is False

    def test_exists(self):
        assert _eval_string_condition("value", "exists", None) is True
        assert _eval_string_condition(_MISSING, "exists", None) is False

    def test_not_exists(self):
        assert _eval_string_condition(_MISSING, "not_exists", None) is True
        assert _eval_string_condition("value", "not_exists", None) is False

    def test_numeric_value_coerced_to_string(self):
        assert _eval_string_condition(123, "equals", "123") is True

    def test_missing_value_fails(self):
        assert _eval_string_condition(_MISSING, "equals", "test") is False


class TestNumericConditions:
    """Tests for numeric operators."""

    def test_eq(self):
        assert _eval_numeric_condition(100, "eq", 100) is True
        assert _eval_numeric_condition(100, "eq", 200) is False

    def test_ne(self):
        assert _eval_numeric_condition(100, "ne", 200) is True
        assert _eval_numeric_condition(100, "ne", 100) is False

    def test_gt(self):
        assert _eval_numeric_condition(100, "gt", 50) is True
        assert _eval_numeric_condition(50, "gt", 100) is False

    def test_gte(self):
        assert _eval_numeric_condition(100, "gte", 100) is True
        assert _eval_numeric_condition(50, "gte", 100) is False

    def test_lt(self):
        assert _eval_numeric_condition(50, "lt", 100) is True
        assert _eval_numeric_condition(100, "lt", 50) is False

    def test_lte(self):
        assert _eval_numeric_condition(100, "lte", 100) is True
        assert _eval_numeric_condition(100, "lte", 50) is False

    def test_string_numbers(self):
        assert _eval_numeric_condition("100", "gt", 50) is True

    def test_missing(self):
        assert _eval_numeric_condition(_MISSING, "gt", 50) is False

    def test_none(self):
        assert _eval_numeric_condition(None, "gt", 50) is False

    def test_non_numeric(self):
        assert _eval_numeric_condition("abc", "gt", 50) is False


class TestListConditions:
    """Tests for list operators."""

    def test_in(self):
        assert _eval_list_condition("apple", "in", ["apple", "banana"]) is True
        assert _eval_list_condition("cherry", "in", ["apple", "banana"]) is False

    def test_not_in(self):
        assert _eval_list_condition("cherry", "not_in", ["apple", "banana"]) is True
        assert _eval_list_condition("apple", "not_in", ["apple", "banana"]) is False

    def test_in_non_list(self):
        assert _eval_list_condition("x", "in", "not_a_list") is False


class TestConditionEvaluation:
    """Tests for _eval_condition with header and payload types."""

    def test_payload_field_equals(self):
        context = {"headers": {}, "payload": {"event": "created"}}
        cond = {"type": "payload", "field": "event", "operator": "equals", "value": "created"}
        assert _eval_condition(cond, context) is True

    def test_payload_nested_field(self):
        context = {"headers": {}, "payload": {"data": {"amount": 500}}}
        cond = {"type": "payload", "field": "data.amount", "operator": "gt", "value": 100}
        assert _eval_condition(cond, context) is True

    def test_header_field_equals(self):
        context = {"headers": {"X-Event-Type": "order.created"}, "payload": {}}
        cond = {"type": "header", "field": "X-Event-Type", "operator": "equals", "value": "order.created"}
        assert _eval_condition(cond, context) is True

    def test_header_case_insensitive(self):
        context = {"headers": {"x-event-type": "order.created"}, "payload": {}}
        cond = {"type": "header", "field": "X-Event-Type", "operator": "equals", "value": "order.created"}
        assert _eval_condition(cond, context) is True

    def test_header_missing(self):
        context = {"headers": {}, "payload": {}}
        cond = {"type": "header", "field": "X-Event-Type", "operator": "exists", "value": None}
        assert _eval_condition(cond, context) is False

    def test_unknown_type(self):
        context = {"headers": {}, "payload": {}}
        cond = {"type": "invalid", "field": "x", "operator": "equals", "value": "y"}
        assert _eval_condition(cond, context) is False


class TestEvaluateFilter:
    """Tests for the top-level evaluate_filter function."""

    def test_empty_filter_always_true(self):
        assert evaluate_filter({}, {}, {}) is True
        assert evaluate_filter(None, {}, {}) is True

    def test_all_logic(self):
        filter_rules = {
            "logic": "all",
            "conditions": [
                {"type": "payload", "field": "status", "operator": "equals", "value": "active"},
                {"type": "payload", "field": "role", "operator": "equals", "value": "admin"},
            ],
        }
        payload_match = {"status": "active", "role": "admin"}
        payload_fail = {"status": "active", "role": "user"}
        assert evaluate_filter(filter_rules, {}, payload_match) is True
        assert evaluate_filter(filter_rules, {}, payload_fail) is False

    def test_any_logic(self):
        filter_rules = {
            "logic": "any",
            "conditions": [
                {"type": "payload", "field": "status", "operator": "equals", "value": "active"},
                {"type": "payload", "field": "role", "operator": "equals", "value": "admin"},
            ],
        }
        payload_one_match = {"status": "inactive", "role": "admin"}
        payload_no_match = {"status": "inactive", "role": "user"}
        assert evaluate_filter(filter_rules, {}, payload_one_match) is True
        assert evaluate_filter(filter_rules, {}, payload_no_match) is False

    def test_none_logic(self):
        filter_rules = {
            "logic": "none",
            "conditions": [
                {"type": "payload", "field": "blocked", "operator": "equals", "value": True},
            ],
        }
        assert evaluate_filter(filter_rules, {}, {"blocked": True}) is False
        assert evaluate_filter(filter_rules, {}, {"blocked": False}) is True

    def test_mixed_header_and_payload(self):
        filter_rules = {
            "logic": "all",
            "conditions": [
                {"type": "header", "field": "X-Source", "operator": "equals", "value": "stripe"},
                {"type": "payload", "field": "amount", "operator": "gt", "value": 1000},
            ],
        }
        headers = {"X-Source": "stripe"}
        payload_match = {"amount": 5000}
        payload_fail = {"amount": 50}
        assert evaluate_filter(filter_rules, headers, payload_match) is True
        assert evaluate_filter(filter_rules, headers, payload_fail) is False

    def test_default_logic_is_all(self):
        filter_rules = {
            "conditions": [
                {"type": "payload", "field": "x", "operator": "equals", "value": "1"},
                {"type": "payload", "field": "y", "operator": "equals", "value": "2"},
            ],
        }
        assert evaluate_filter(filter_rules, {}, {"x": "1", "y": "2"}) is True
        assert evaluate_filter(filter_rules, {}, {"x": "1", "y": "3"}) is False

    def test_empty_conditions_passes(self):
        filter_rules = {"logic": "all", "conditions": []}
        assert evaluate_filter(filter_rules, {}, {"x": 1}) is True

    def test_unknown_logic_defaults_to_all(self):
        filter_rules = {
            "logic": "weird",
            "conditions": [
                {"type": "payload", "field": "x", "operator": "equals", "value": "1"},
            ],
        }
        assert evaluate_filter(filter_rules, {}, {"x": "1"}) is True


class TestValidateFilterRules:
    """Tests for filter rule validation."""

    def test_valid_all(self):
        filter_rules = {
            "logic": "all",
            "conditions": [
                {"type": "payload", "field": "status", "operator": "equals", "value": "active"},
            ],
        }
        assert validate_filter_rules(filter_rules) == []

    def test_invalid_logic(self):
        filter_rules = {"logic": "xor", "conditions": []}
        errors = validate_filter_rules(filter_rules)
        assert len(errors) == 1
        assert "xor" in errors[0]

    def test_invalid_type(self):
        filter_rules = {"conditions": [{"type": "cookie", "field": "x", "operator": "equals", "value": "y"}]}
        errors = validate_filter_rules(filter_rules)
        assert len(errors) == 1
        assert "cookie" in errors[0]

    def test_invalid_operator(self):
        filter_rules = {"conditions": [{"type": "payload", "field": "x", "operator": "similar_to", "value": "y"}]}
        errors = validate_filter_rules(filter_rules)
        assert len(errors) == 1
        assert "similar_to" in errors[0]

    def test_missing_value_for_non_existence(self):
        filter_rules = {"conditions": [{"type": "payload", "field": "x", "operator": "equals"}]}
        errors = validate_filter_rules(filter_rules)
        assert len(errors) == 1
        assert "value" in errors[0]

    def test_exists_does_not_require_value(self):
        filter_rules = {"conditions": [{"type": "payload", "field": "x", "operator": "exists"}]}
        assert validate_filter_rules(filter_rules) == []

    def test_missing_field(self):
        filter_rules = {"conditions": [{"type": "payload", "operator": "equals", "value": "y"}]}
        errors = validate_filter_rules(filter_rules)
        assert len(errors) == 1
        assert "field" in errors[0]

    def test_not_a_dict(self):
        errors = validate_filter_rules("not a dict")  # type: ignore
        assert len(errors) == 1

    def test_empty_dict_is_valid(self):
        assert validate_filter_rules({}) == []
