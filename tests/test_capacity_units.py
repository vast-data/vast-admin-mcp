#!/usr/bin/env python3
"""Unit tests for configurable capacity conversion (base 1000 / 1024).

Default base 1000 matches VAST GUI (1 TB = 10^12 bytes).
Config key capacity_unit_base can switch to 1024 (binary / IEC).
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from vast_admin_mcp.utils import pretty_size, parse_capacity_value, get_size_in_bytes
from vast_admin_mcp.command_executor import CommandExecutor
from vast_admin_mcp.config import get_capacity_unit_base, CAPACITY_UNIT_BASE_DEFAULT


class TestGetCapacityUnitBase(unittest.TestCase):
    """get_capacity_unit_base reads and validates config.json."""

    @patch("vast_admin_mcp.config.load_config", side_effect=ValueError("no config"))
    def test_default_when_no_config(self, _mock):
        self.assertEqual(get_capacity_unit_base(), 1000)

    @patch("vast_admin_mcp.config.load_config", return_value={"clusters": []})
    def test_default_when_key_absent(self, _mock):
        self.assertEqual(get_capacity_unit_base(), CAPACITY_UNIT_BASE_DEFAULT)

    @patch("vast_admin_mcp.config.load_config", return_value={"capacity_unit_base": 1024})
    def test_numeric_1024(self, _mock):
        self.assertEqual(get_capacity_unit_base(), 1024)

    @patch("vast_admin_mcp.config.load_config", return_value={"capacity_unit_base": "binary"})
    def test_string_binary(self, _mock):
        self.assertEqual(get_capacity_unit_base(), 1024)

    @patch("vast_admin_mcp.config.load_config", return_value={"capacity_unit_base": "si"})
    def test_string_si(self, _mock):
        self.assertEqual(get_capacity_unit_base(), 1000)

    @patch("vast_admin_mcp.config.load_config", return_value={"capacity_unit_base": 999})
    def test_invalid_falls_back(self, _mock):
        self.assertEqual(get_capacity_unit_base(), 1000)


class TestPrettySizeDecimal(unittest.TestCase):
    """pretty_size with base 1000 (default)."""

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1000)
    def test_exact_100_tb(self, _mock):
        self.assertEqual(pretty_size(100_000_000_000_000), "100.00 TB")

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1000)
    def test_exact_65_tb(self, _mock):
        self.assertEqual(pretty_size(65_000_000_000_000), "65.00 TB")

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1000)
    def test_exact_150_gb(self, _mock):
        self.assertEqual(pretty_size(150_000_000_000), "150.00 GB")

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1000)
    def test_zero(self, _mock):
        self.assertEqual(pretty_size(0), "0 B")


class TestPrettySizeBinary(unittest.TestCase):
    """pretty_size with base 1024 uses IEC labels."""

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1024)
    def test_100e12_as_tib(self, _mock):
        # 1e14 / 1024^4 ≈ 90.95 TiB
        self.assertEqual(pretty_size(100_000_000_000_000), "90.95 TiB")

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1024)
    def test_exact_tib(self, _mock):
        self.assertEqual(pretty_size(1024**4), "1.00 TiB")


class TestConvertUnitsDecimal(unittest.TestCase):
    """CommandExecutor._convert_units with base 1000."""

    def setUp(self):
        self.executor = object.__new__(CommandExecutor)

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1000)
    def test_auto_100_tb(self, _mock):
        self.assertEqual(
            self.executor._convert_units(100_000_000_000_000, "AUTO"),
            "100.00 TB",
        )

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1000)
    def test_auto_150_gb(self, _mock):
        self.assertEqual(
            self.executor._convert_units(150_000_000_000, "AUTO"),
            "150.00 GB",
        )

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1000)
    def test_explicit_tb(self, _mock):
        self.assertEqual(
            self.executor._convert_units(100_000_000_000_000, "TB"),
            "100.00 TB",
        )

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1000)
    def test_not_binary_mislabeled(self, _mock):
        result = self.executor._convert_units(100_000_000_000_000, "AUTO")
        self.assertNotEqual(result, "90.95 TB")
        self.assertEqual(result, "100.00 TB")


class TestConvertUnitsBinary(unittest.TestCase):
    """CommandExecutor._convert_units with base 1024."""

    def setUp(self):
        self.executor = object.__new__(CommandExecutor)

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1024)
    def test_auto_uses_iec_label(self, _mock):
        self.assertEqual(
            self.executor._convert_units(100_000_000_000_000, "AUTO"),
            "90.95 TiB",
        )

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1024)
    def test_explicit_tb_binary_divisor(self, _mock):
        # Explicit SI label kept; divisor is binary when base=1024
        self.assertEqual(
            self.executor._convert_units(100_000_000_000_000, "TB"),
            "90.95 TB",
        )


class TestParseCapacityValue(unittest.TestCase):
    """parse_capacity_value follows configured base for SI labels."""

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1000)
    def test_100_tb_decimal(self, _mock):
        op, value = parse_capacity_value("100TB")
        self.assertEqual(op, "eq")
        self.assertEqual(value, 100_000_000_000_000)

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1024)
    def test_100_tb_binary(self, _mock):
        op, value = parse_capacity_value("100TB")
        self.assertEqual(op, "eq")
        self.assertEqual(value, 100 * (1024**4))

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1000)
    def test_gt_500_gb(self, _mock):
        op, value = parse_capacity_value(">500GB")
        self.assertEqual(op, "gt")
        self.assertEqual(value, 500_000_000_000)


class TestGetSizeInBytes(unittest.TestCase):
    """get_size_in_bytes: SI follows config; IEC always binary."""

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1000)
    def test_si_tb_decimal(self, _mock):
        self.assertEqual(get_size_in_bytes("100TB"), 100_000_000_000_000)

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1024)
    def test_si_tb_binary(self, _mock):
        self.assertEqual(get_size_in_bytes("100TB"), 100 * (1024**4))

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1000)
    def test_iec_tib_always_binary(self, _mock):
        self.assertEqual(get_size_in_bytes("1TiB"), 1024**4)

    @patch("vast_admin_mcp.config.get_capacity_unit_base", return_value=1024)
    def test_iec_gib_always_binary(self, _mock):
        self.assertEqual(get_size_in_bytes("1GiB"), 1024**3)


if __name__ == "__main__":
    unittest.main()
