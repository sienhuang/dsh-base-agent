"""Application-owned Context extension points.

Static, non-secret context is compiled into the Agent prompt. Model-selected
dynamic lookups belong behind a governed Tool; automatic per-Turn retrieval belongs
in a read-only Memory Provider; see ``tools.py`` and ``memory.py``.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class StaticContextSection:
    name: str
    content: str


def static_context_sections() -> tuple[StaticContextSection, ...]:
    """Return stable context shared by every Run of this Agent version."""

    return (
        StaticContextSection(
            name="business-boundary",
            content=(
                "订单数据只能通过已注册的只读工具查询；不得猜测订单状态，也不得访问其他租户。"
            ),
        ),
        StaticContextSection(
            name="response-contract",
            content="回答应简洁，明确给出订单号、当前状态以及数据是否来自工具查询。",
        ),
    )


def render_static_context(sections: tuple[StaticContextSection, ...]) -> str:
    """Render deterministic prompt context without runtime secrets."""

    return "\n\n".join(
        f'<application_context name="{section.name}">\n'
        f"{section.content.strip()}\n"
        "</application_context>"
        for section in sections
    )


__all__ = ["StaticContextSection", "render_static_context", "static_context_sections"]
