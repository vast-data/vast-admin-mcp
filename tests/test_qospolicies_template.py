"""Tests that the qospolicies list command is registered from the shipped template."""

import os
import tempfile
import unittest

from vast_admin_mcp.template_parser import TemplateParser

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_TEMPLATE = os.path.join(REPO_ROOT, "mcp_list_cmds_template.yaml")


def _parser():
    # Empty user modifications file so only the shipped template is exercised.
    with tempfile.TemporaryDirectory() as tmp:
        mods = os.path.join(tmp, "mods.yaml")
        with open(mods, "w") as fh:
            fh.write("---\n")
        return TemplateParser(mods, default_template_path=DEFAULT_TEMPLATE)


class TestQosPoliciesTemplate(unittest.TestCase):
    def test_command_registered(self):
        self.assertIn("qospolicies", _parser().get_command_names())

    def test_whitelist_is_get_only(self):
        self.assertEqual(_parser().api_whitelist.get("qospolicies"), ["get"])

    def test_fields_and_cluster_argument(self):
        template = _parser().get_command_template("qospolicies")
        names = [f.get("name") for f in template["fields"]]
        for expected in ("cluster", "name", "mode", "policy type", "static limits",
                         "capacity limits", "is default", "views"):
            self.assertIn(expected, names)
        self.assertEqual(template["api_endpoints"], ["qospolicies", "views"])

    def test_views_whitelist_unchanged(self):
        self.assertEqual(_parser().api_whitelist.get("views"), ["get", "post"])


if __name__ == "__main__":
    unittest.main()
