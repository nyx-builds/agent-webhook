"""Payload transformation engine — transforms webhook payloads before delivery."""

from __future__ import annotations

import re
from typing import Any

from .models import PayloadTransform, TransformType


class TransformEngine:
    """Applies a chain of payload transformations."""

    def apply(self, payload: dict[str, Any], transforms: list[PayloadTransform]) -> dict[str, Any]:
        """Apply a sequence of transforms to a payload. Returns the transformed payload."""
        result = payload
        for transform in transforms:
            result = self.apply_one(result, transform)
        return result

    def apply_one(self, payload: dict[str, Any], transform: PayloadTransform) -> dict[str, Any]:
        """Apply a single transform to a payload."""
        if transform.type == TransformType.FIELD_MAP:
            return self._apply_field_map(payload, transform.config)
        elif transform.type == TransformType.FILTER:
            return self._apply_filter(payload, transform.config)
        elif transform.type == TransformType.TEMPLATE:
            return self._apply_template(payload, transform.config)
        else:
            # Unknown transform type — pass through
            return payload

    @staticmethod
    def _apply_field_map(payload: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """Rename/map fields from old keys to new keys.

        Config: {'mapping': {'old_key': 'new_key', ...}, 'keep_unmapped': True}
        If keep_unmapped is True (default), unmapped keys are preserved.
        If False, only mapped keys are included.
        """
        mapping = config.get("mapping", {})
        keep_unmapped = config.get("keep_unmapped", True)
        result = {}

        for key, value in payload.items():
            if key in mapping:
                new_key = mapping[key]
                result[new_key] = value
            elif keep_unmapped:
                result[key] = value

        return result

    @staticmethod
    def _apply_filter(payload: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """Include or exclude specific fields.

        Config: {'include': ['key1', 'key2']} or {'exclude': ['key1', 'key2']}
        Include takes precedence over exclude.
        """
        if "include" in config:
            include_set = set(config["include"])
            return {k: v for k, v in payload.items() if k in include_set}
        elif "exclude" in config:
            exclude_set = set(config["exclude"])
            return {k: v for k, v in payload.items() if k not in exclude_set}
        return payload

    @staticmethod
    def _apply_template(payload: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """Apply a string template with payload variable substitution.

        Config: {'template': 'key1={{payload.field1}}&key2={{payload.field2}}'}
        This creates a new payload from template substitution. The template is
        parsed as key=value pairs separated by & (like query string format).

        Alternatively: {'fields': {'new_key': '{{payload.old_key}}'}}
        This creates specific new fields using template expressions.
        """
        if "fields" in config:
            result = dict(payload)
            for new_key, template_str in config["fields"].items():
                result[new_key] = _substitute_template(template_str, payload)
            return result

        if "template" in config:
            # Parse as key=value pairs
            template_str = config["template"]
            substituted = _substitute_template(template_str, payload)
            return {"_rendered": substituted}

        return payload


def _substitute_template(template: str, payload: dict[str, Any]) -> str:
    """Replace {{payload.key}} and {{payload.key.nested}} references in a template string."""
    def replacer(match: re.Match) -> str:
        path = match.group(1)
        value = _resolve_path(payload, path)
        return str(value) if value is not None else ""

    return re.sub(r"\{\{payload\.([^}]+)\}\}", replacer, template)


def _resolve_path(obj: Any, path: str) -> Any:
    """Resolve a dotted path like 'data.items.0.name' against an object."""
    current = obj
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
        if current is None:
            return None
    return current
