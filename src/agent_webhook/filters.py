"""Payload filtering for relay rules — conditional forwarding based on payload content.

A RelayRule can have `filter_rules` that determine whether an incoming webhook
should be forwarded to its target endpoints. Filters support:

  - **Header matching**: Match on request headers (exact, contains, regex, exists)
  - **Payload field matching**: Match on JSON body fields (exact, contains, regex,
    numeric comparisons, exists)
  - **Logical combinators**: `all` (AND), `any` (OR), `none` (NOT any)

Filter rule format (stored in RelayRule.filter_rules)::

    {
        "logic": "all",           # "all" | "any" | "none", default "all"
        "conditions": [
            {
                "type": "header",
                "field": "X-Event-Type",
                "operator": "equals",
                "value": "order.created"
            },
            {
                "type": "payload",
                "field": "event",
                "operator": "contains",
                "value": "payment"
            },
            {
                "type": "payload",
                "field": "data.amount",
                "operator": "gt",
                "value": 100
            }
        ]
    }

Operators:
  - **String**: equals, not_equals, contains, starts_with, ends_with, regex, exists, not_exists
  - **Numeric**: eq, ne, gt, gte, lt, lte
  - **List**: in, not_in
"""

from __future__ import annotations

import re
from typing import Any


def _get_nested(obj: Any, path: str) -> Any:
    """Get a value from a nested dict/list by dotted path.

    Supports ``data.amount``, ``items[0].name``, ``tags`` etc.
    Returns ``_MISSING`` sentinel if the path doesn't resolve.
    """
    if not path:
        return obj

    current = obj
    parts = _tokenize_path(path)

    for part in parts:
        if current is _MISSING:
            return _MISSING
        if part.kind == "key":
            if isinstance(current, dict) and part.value in current:
                current = current[part.value]
            else:
                return _MISSING
        elif part.kind == "index":
            if isinstance(current, list) and 0 <= part.value < len(current):
                current = current[part.value]
            else:
                return _MISSING

    return current


class _PathToken:
    __slots__ = ("kind", "value")

    def __init__(self, kind: str, value: Any) -> None:
        self.kind = kind  # "key" or "index"
        self.value = value


def _tokenize_path(path: str) -> list[_PathToken]:
    """Parse a dotted path with optional [n] index accessors."""
    tokens: list[_PathToken] = []
    # Split on dots but allow bracket indexing
    parts = path.replace("]", "").split(".")
    for part in parts:
        if "[" in part:
            key, _, idx_str = part.partition("[")
            if key:
                tokens.append(_PathToken("key", key))
            try:
                tokens.append(_PathToken("index", int(idx_str)))
            except ValueError:
                pass
        else:
            tokens.append(_PathToken("key", part))
    return tokens


class _Missing:
    """Sentinel for missing values."""

    _instance: _Missing | None = None

    def __new__(cls) -> _Missing:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "<MISSING>"


_MISSING = _Missing()


# ── Condition evaluators ────────────────────────────────────────────

def _eval_string_condition(
    actual: Any,
    operator: str,
    expected: Any,
) -> bool:
    """Evaluate a string/string-like condition."""
    if operator in ("exists", "not_exists"):
        exists = actual is not _MISSING and actual is not None
        return exists if operator == "exists" else not exists

    # For remaining operators, value must be present
    if actual is _MISSING:
        return False

    actual_str = str(actual) if not isinstance(actual, str) else actual
    expected_str = str(expected) if not isinstance(expected, str) else expected

    if operator == "equals":
        return actual_str == expected_str
    elif operator == "not_equals":
        return actual_str != expected_str
    elif operator == "contains":
        return expected_str in actual_str
    elif operator == "starts_with":
        return actual_str.startswith(expected_str)
    elif operator == "ends_with":
        return actual_str.endswith(expected_str)
    elif operator == "regex":
        try:
            return bool(re.search(expected_str, actual_str))
        except re.error:
            return False
    else:
        return False


def _eval_numeric_condition(
    actual: Any,
    operator: str,
    expected: Any,
) -> bool:
    """Evaluate a numeric comparison condition."""
    if actual is _MISSING or actual is None:
        return False

    try:
        actual_num = float(actual)
        expected_num = float(expected)
    except (ValueError, TypeError):
        return False

    if operator == "eq":
        return actual_num == expected_num
    elif operator == "ne":
        return actual_num != expected_num
    elif operator == "gt":
        return actual_num > expected_num
    elif operator == "gte":
        return actual_num >= expected_num
    elif operator == "lt":
        return actual_num < expected_num
    elif operator == "lte":
        return actual_num <= expected_num
    else:
        return False


def _eval_list_condition(
    actual: Any,
    operator: str,
    expected: Any,
) -> bool:
    """Evaluate a list membership condition."""
    if operator == "in":
        if not isinstance(expected, list):
            return False
        return actual in expected
    elif operator == "not_in":
        if not isinstance(expected, list):
            return True
        return actual not in expected
    return False


def _eval_condition(condition: dict[str, Any], context: dict[str, Any]) -> bool:
    """Evaluate a single filter condition against the context.

    Args:
        condition: A condition dict with keys: type, field, operator, value.
        context: A dict with 'headers' (dict[str,str]) and 'payload' (dict).
    """
    cond_type = condition.get("type", "payload")
    field = condition.get("field", "")
    operator = condition.get("operator", "equals")
    expected = condition.get("value")

    # Determine the source data
    if cond_type == "header":
        headers = context.get("headers", {})
        actual = headers.get(field.lower())
        if actual is None:
            # Try case-insensitive lookup
            for k, v in headers.items():
                if k.lower() == field.lower():
                    actual = v
                    break
            if actual is None:
                actual = _MISSING
    elif cond_type == "payload":
        payload = context.get("payload", {})
        actual = _get_nested(payload, field)
    else:
        return False

    # Numeric operators
    numeric_ops = {"eq", "ne", "gt", "gte", "lt", "lte"}
    # String operators
    string_ops = {"equals", "not_equals", "contains", "starts_with", "ends_with", "regex", "exists", "not_exists"}
    # List operators
    list_ops = {"in", "not_in"}

    if operator in numeric_ops:
        return _eval_numeric_condition(actual, operator, expected)
    elif operator in string_ops:
        return _eval_string_condition(actual, operator, expected)
    elif operator in list_ops:
        return _eval_list_condition(actual, operator, expected)
    else:
        return False


def evaluate_filter(
    filter_rules: dict[str, Any],
    headers: dict[str, str],
    payload: dict[str, Any],
) -> bool:
    """Evaluate filter rules against headers and payload.

    Args:
        filter_rules: The filter configuration dict. If empty or None, always True.
        headers: Request headers.
        payload: Parsed JSON body.

    Returns:
        True if the webhook should be forwarded (passes the filter).
    """
    if not filter_rules:
        return True

    logic = filter_rules.get("logic", "all")
    conditions = filter_rules.get("conditions", [])

    if not conditions:
        return True

    context = {"headers": headers, "payload": payload}

    if logic == "all":
        # All conditions must be true
        return all(_eval_condition(c, context) for c in conditions)
    elif logic == "any":
        # At least one condition must be true
        return any(_eval_condition(c, context) for c in conditions)
    elif logic == "none":
        # No condition should be true
        return not any(_eval_condition(c, context) for c in conditions)
    else:
        # Default: treat unknown logic as "all"
        return all(_eval_condition(c, context) for c in conditions)


def validate_filter_rules(filter_rules: dict[str, Any]) -> list[str]:
    """Validate filter rules structure. Returns list of error messages (empty = valid)."""
    errors: list[str] = []

    if not isinstance(filter_rules, dict):
        return ["filter_rules must be a dict"]

    logic = filter_rules.get("logic", "all")
    if logic not in ("all", "any", "none"):
        errors.append(f"Invalid logic '{logic}': must be 'all', 'any', or 'none'")

    conditions = filter_rules.get("conditions")
    if conditions is None:
        return errors  # Empty conditions is OK
    if not isinstance(conditions, list):
        errors.append("conditions must be a list")
        return errors

    valid_types = {"header", "payload"}
    valid_operators = {
        "equals", "not_equals", "contains", "starts_with", "ends_with",
        "regex", "exists", "not_exists",
        "eq", "ne", "gt", "gte", "lt", "lte",
        "in", "not_in",
    }

    for i, cond in enumerate(conditions):
        if not isinstance(cond, dict):
            errors.append(f"Condition {i}: must be a dict")
            continue

        cond_type = cond.get("type", "payload")
        if cond_type not in valid_types:
            errors.append(f"Condition {i}: type '{cond_type}' must be one of {valid_types}")

        operator = cond.get("operator", "equals")
        if operator not in valid_operators:
            errors.append(f"Condition {i}: operator '{operator}' must be one of {valid_operators}")

        # exists/not_exists don't need a value
        if operator not in ("exists", "not_exists") and "value" not in cond:
            errors.append(f"Condition {i}: missing 'value' for operator '{operator}'")

        if not cond.get("field"):
            errors.append(f"Condition {i}: missing 'field'")

    return errors
