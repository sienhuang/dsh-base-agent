from __future__ import annotations

import ast
from pathlib import Path

_REMOVED_LEGACY_MODULES = frozenset(
    {
        "dsh_base_agent.agent",
        "dsh_base_agent.auth",
        "dsh_base_agent.gateway",
        "dsh_base_agent.models",
        "dsh_base_agent.profile",
        "dsh_base_agent.runtime",
        "dsh_base_agent.server",
        "dsh_base_agent.tools",
    }
)


def test_canonical_packages_do_not_depend_on_removed_legacy_modules() -> None:
    package = Path(__file__).resolve().parents[1] / "src" / "dsh_base_agent"
    canonical_roots = ("sdk", "control", "adapters", "api", "store")
    violations: list[str] = []

    for root in canonical_roots:
        for path in (package / root).rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module in _REMOVED_LEGACY_MODULES:
                    violations.append(f"{path.relative_to(package)}:{node.lineno} -> {node.module}")
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name in _REMOVED_LEGACY_MODULES:
                            violations.append(
                                f"{path.relative_to(package)}:{node.lineno} -> {alias.name}"
                            )

    assert violations == []
