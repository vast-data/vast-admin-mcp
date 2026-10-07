#!/usr/bin/env python3
"""Unit tests for GUI-aligned list_clusters capacity mapping."""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from vast_admin_mcp.functions import _cluster_gui_capacity


class TestClusterGuiCapacity(unittest.TestCase):
    """_cluster_gui_capacity mirrors VAST dashboard Usable/Logical rows."""

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1000)
    def test_v99_style_breakdown(self, _mock):
        # Synthetic values matching the GUI screenshot proportions
        row = {
            "data_delete_pending_bytes": 17.957e12,
            "physical_space_in_use_wo_overhead": 43.109e12,  # used+pending usable
            "estore_capacity_in_use_bytes": 44.189e12,       # used+pending logical
            "drr": 1.02506,
            "drr_text": "1.0:1",
            "free_usable_capacity": 509.37e12,
            "free_logical_space": 522.135e12,
            "usable_capacity_bytes": 552.479e12,
            "logical_space": 566.324e12,
        }
        out = _cluster_gui_capacity(row)
        self.assertEqual(out["Usable Used"], "25.15 TB")
        self.assertEqual(out["Logical Used"], "25.78 TB")
        self.assertEqual(out["Usable Pending Deletion"], "17.96 TB")
        self.assertEqual(out["Logical Pending Deletion"], "18.41 TB")
        self.assertEqual(out["Usable Free"], "509.37 TB")
        self.assertEqual(out["Logical Free"], "522.13 TB")
        self.assertEqual(out["DRR"], "1.0:1")

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1000)
    def test_fallback_without_optional_fields(self, _mock):
        row = {
            "physical_space_in_use": 10e12,
            "logical_space_in_use": 12e12,
            "free_physical_space": 90e12,
            "free_logical_space": 100e12,
        }
        out = _cluster_gui_capacity(row)
        self.assertEqual(out["Usable Used"], "10.00 TB")
        self.assertEqual(out["Logical Used"], "12.00 TB")
        self.assertEqual(out["Usable Pending Deletion"], "0 B")
        self.assertEqual(out["Usable Free"], "90.00 TB")
        self.assertEqual(out["Logical Free"], "100.00 TB")


if __name__ == "__main__":
    unittest.main()
