#!/usr/bin/env python3
"""Regression tests for function-local imports that shadow module-level names.

A local ``import os`` (or ``sys``, ``json``, …) anywhere in a function makes
that name local for the *entire* function. Earlier uses then raise
UnboundLocalError — which broke ``env:`` password refs and CLI validation.
"""

import ast
import os
import sys
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from vast_admin_mcp.utils import retrieve_password_secure


PACKAGE_ROOT = Path(__file__).parent.parent / "src" / "vast_admin_mcp"


class TestEnvPasswordReference(unittest.TestCase):
    """env: refs must resolve without UnboundLocalError on os."""

    def test_env_reference_resolves(self):
        with patch.dict(os.environ, {"VAST_PASSWORD_SHADOW_TEST": "s3cret"}, clear=False):
            self.assertEqual(
                retrieve_password_secure("cluster.example.com", "admin", "env:VAST_PASSWORD_SHADOW_TEST"),
                "s3cret",
            )

    def test_missing_env_variable_raises_value_error(self):
        missing = "VAST_PASSWORD_SHADOW_TEST_MISSING_XYZ"
        os.environ.pop(missing, None)
        with self.assertRaises(ValueError) as ctx:
            retrieve_password_secure("cluster.example.com", "admin", f"env:{missing}")
        self.assertIn(missing, str(ctx.exception))
        self.assertNotIsInstance(ctx.exception, UnboundLocalError)


def _load_cli_module():
    """Load sibling cli.py (package name ``vast_admin_mcp.cli`` is the cli/ package)."""
    import importlib.util

    cli_path = PACKAGE_ROOT / "cli.py"
    spec = importlib.util.spec_from_file_location("vast_admin_mcp.cli_module", cli_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestPerformanceGraphMissingMonitor(unittest.TestCase):
    """Missing --monitor-name must exit 1 with a message, not UnboundLocalError."""

    def test_missing_monitor_name_exits_one(self):
        cli_module = _load_cli_module()

        args = Namespace(
            mcp=False,
            debug=False,
            monitor_name=None,
            cluster="cluster.example.com",
            object_name="cluster",
            timeframe=None,
            instances=None,
            format="table",
            output=None,
        )
        with self.assertRaises(SystemExit) as ctx:
            cli_module.handle_performance_graph_command(args)
        self.assertEqual(ctx.exception.code, 1)


class _ShadowingVisitor(ast.NodeVisitor):
    """Find uses of module-level names that occur before a local re-import."""

    def __init__(self, module_names, filename):
        self.module_names = module_names
        self.filename = filename
        self.violations = []

    def visit_FunctionDef(self, node):
        self._check_function(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node):
        self._check_function(node)
        self.generic_visit(node)

    def _check_function(self, node):
        # Only this function's scope (nested defs/classes are separate scopes).
        local_import_pos = {}
        uses = []

        for stmt in node.body:
            self._scan_stmt(stmt, local_import_pos, uses)

        for name, lineno, _col in uses:
            import_line = local_import_pos.get(name)
            if import_line is not None and lineno < import_line:
                self.violations.append(
                    f"{self.filename}:{lineno}: function '{node.name}' uses "
                    f"'{name}' before local re-import at line {import_line}"
                )

    def _scan_stmt(self, stmt, local_import_pos, uses):
        """Walk a statement tree but skip nested function/class bodies."""

        class ScopeWalker(ast.NodeVisitor):
            def visit_FunctionDef(self, n):
                return  # nested scope — handled by outer visitor

            def visit_AsyncFunctionDef(self, n):
                return

            def visit_ClassDef(self, n):
                return

            def visit_Import(self, n):
                for alias in n.names:
                    bound = alias.asname or alias.name.split(".")[0]
                    if bound in module_names and bound not in local_import_pos:
                        local_import_pos[bound] = n.lineno

            def visit_ImportFrom(self, n):
                for alias in n.names:
                    if alias.name == "*":
                        continue
                    bound = alias.asname or alias.name
                    if bound in module_names and bound not in local_import_pos:
                        local_import_pos[bound] = n.lineno

            def visit_Name(self, n):
                if isinstance(n.ctx, ast.Load) and n.id in module_names:
                    uses.append((n.id, n.lineno, n.col_offset))

        module_names = self.module_names
        ScopeWalker().visit(stmt)


def _module_level_imported_names(tree):
    """Names bound by top-level import statements in a module."""
    names = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == "*":
                    continue
                names.add(alias.asname or alias.name)
        elif isinstance(node, ast.Try):
            # Optional deps: import keyring / cryptography / kubernetes at module level
            for handler_body in [node.body, node.orelse, node.finalbody]:
                for stmt in handler_body:
                    if isinstance(stmt, ast.Import):
                        for alias in stmt.names:
                            names.add(alias.asname or alias.name.split(".")[0])
                    elif isinstance(stmt, ast.ImportFrom):
                        for alias in stmt.names:
                            if alias.name == "*":
                                continue
                            names.add(alias.asname or alias.name)
            for handler in node.handlers:
                for stmt in handler.body:
                    if isinstance(stmt, ast.Import):
                        for alias in stmt.names:
                            names.add(alias.asname or alias.name.split(".")[0])
                    elif isinstance(stmt, ast.ImportFrom):
                        for alias in stmt.names:
                            if alias.name == "*":
                                continue
                            names.add(alias.asname or alias.name)
    return names


class TestNoLocalImportShadowing(unittest.TestCase):
    """Package-wide AST guard against use-before-local-reimport of module names."""

    def test_no_module_name_used_before_local_reimport(self):
        violations = []
        for path in sorted(PACKAGE_ROOT.rglob("*.py")):
            source = path.read_text(encoding="utf-8")
            try:
                tree = ast.parse(source, filename=str(path))
            except SyntaxError as e:
                self.fail(f"Failed to parse {path}: {e}")

            module_names = _module_level_imported_names(tree)
            if not module_names:
                continue

            rel = path.relative_to(PACKAGE_ROOT.parent.parent)
            visitor = _ShadowingVisitor(module_names, str(rel))
            visitor.visit(tree)
            violations.extend(visitor.violations)

        if violations:
            self.fail(
                "Local import shadows module-level name used earlier in the same function:\n"
                + "\n".join(violations)
            )


if __name__ == "__main__":
    unittest.main()
