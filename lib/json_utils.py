import json
import os
import re
from pathlib import Path
from typing import Any


_NON_ALNUM = re.compile(r"[^0-9A-Za-z]+")
_CAMEL_SPLIT = re.compile(r"([a-z0-9])([A-Z])")


def to_camel_case(key: Any) -> Any:
    """Normalize arbitrary keys to lowerCamelCase without underscores/spaces.

    - Removes trailing Salesforce suffix "__c" if present
    - Splits on non-alphanumeric and camel humps (e.g., SalePrice -> Sale Price)
    - Lowercases first token, title-cases subsequent tokens, and joins without separators
    """
    if not isinstance(key, str):
        return key

    s = key.strip()
    # Remove Salesforce-style field suffix
    if s.endswith("__c"):
        s = s[:-3]

    # Insert spaces between camel humps, then replace non-alnum with spaces
    s = _CAMEL_SPLIT.sub(r"\1 \2", s)
    s = _NON_ALNUM.sub(" ", s)

    parts = [p for p in s.split() if p]
    if not parts:
        return ""

    head = parts[0].lower()
    tail = [p[:1].upper() + p[1:].lower() for p in parts[1:]]
    return "".join([head, *tail])


def normalize_keys(obj: Any) -> Any:
    """Recursively convert dict keys to camelCase using to_camel_case.

    Lists are processed element-wise. Scalars are returned unchanged.
    """
    if isinstance(obj, dict):
        return {to_camel_case(k): normalize_keys(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [normalize_keys(v) for v in obj]
    return obj


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def save_json(data: Any, file_path: os.PathLike | str) -> Path:
    """Save data as pretty JSON (UTF-8) and return the Path to the file."""
    path = Path(file_path)
    ensure_parent(path)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    return path


def data_path(*parts: str) -> Path:
    """Convenience to build a path under the workspace 'data' directory."""
    return Path("data", *parts)
