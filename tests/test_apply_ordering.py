"""Tests for apply_ordering with missing (None) and mixed-type values."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from vast_admin_mcp.utils import apply_ordering  # noqa: E402


def _names(rows):
    return [r["name"] for r in rows]


def test_numeric_field_with_none_does_not_raise():
    rows = [
        {"name": "a", "iops": 300},
        {"name": "b", "iops": None},
        {"name": "c", "iops": 100},
    ]

    assert _names(apply_ordering(rows, [{"field": "iops", "direction": "asc"}])) == ["b", "c", "a"]
    assert _names(apply_ordering(rows, [{"field": "iops", "direction": "dec"}])) == ["a", "c", "b"]


def test_capacity_like_field_none_still_sorts_as_zero():
    rows = [
        {"name": "a", "logical_used": 5},
        {"name": "b", "logical_used": None},
        {"name": "c", "logical_used": -1},
    ]

    assert _names(apply_ordering(rows, [{"field": "logical_used", "direction": "asc"}])) == ["c", "b", "a"]


def test_string_field_with_none_sorts_none_first():
    rows = [
        {"name": "x", "tenant": "beta"},
        {"name": "y", "tenant": None},
        {"name": "z", "tenant": "alpha"},
    ]

    assert _names(apply_ordering(rows, [{"field": "tenant", "direction": "asc"}])) == ["y", "z", "x"]


def test_mixed_numbers_and_strings_do_not_raise():
    rows = [
        {"name": "a", "latency": "N/A"},
        {"name": "b", "latency": 2.5},
        {"name": "c", "latency": 1},
    ]

    assert _names(apply_ordering(rows, [{"field": "latency", "direction": "asc"}])) == ["c", "b", "a"]
