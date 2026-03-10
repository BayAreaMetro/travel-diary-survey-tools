"""Base class and helpers for weighting controls.

Contains ``ControlLevel``, ``ControlTarget`` base class, and shared
expression helpers used by the household and person control subclasses.
"""

import logging
from enum import Enum

import polars as pl

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════

_SENTINEL_NAMES = frozenset({"MISSING", "PNTA"})


def _identity_expr(col: str, categories: type[Enum]) -> pl.Expr:
    """Pass-through valid values, null for sentinels or unknown.

    Basically, we use the canonical enum directly, just map sentinels to null.
    """
    sentinels = [m.value for m in categories if m.name in _SENTINEL_NAMES]
    valid = [m.value for m in categories if m.name not in _SENTINEL_NAMES]
    return (
        pl.when(pl.col(col).is_null() | pl.col(col).is_in(sentinels))
        .then(None)
        .when(pl.col(col).is_in(valid))
        .then(pl.col(col))
        .otherwise(None)
        .cast(pl.Int16)
    )


def _breakpoint_expr(col: str, categories: type[Enum]) -> pl.Expr:
    """Build a when/then chain from a LabeledEnum with ``BREAKPOINTS``.

    Zips ``categories.BREAKPOINTS`` with the non-sentinel members so that
    each breakpoint maps ``col < bp`` → the corresponding member value,
    with the final member as the ``otherwise`` catch-all.
    """
    members = [m for m in categories if m.name not in _SENTINEL_NAMES]
    breakpoints: list[int] = categories.BREAKPOINTS  # type: ignore[attr-defined]
    c = pl.col(col)
    expr = pl.when(c.is_null()).then(None)
    for bp, member in zip(breakpoints, members, strict=False):
        expr = expr.when(c < bp).then(member.value)
    return expr.otherwise(members[-1].value).cast(pl.Int16)


# ══════════════════════════════════════════════════════════════════════════
# Base class
# ══════════════════════════════════════════════════════════════════════════


class ControlLevel(str, Enum):
    """Whether a control is at the household or person level."""

    HOUSEHOLD = "household"
    PERSON = "person"


class ControlTarget:
    """Base class for a single weighting control.

    Subclasses set class attributes and override ``survey_expr`` /
    ``pums_expr`` to return native Polars expressions.

    Attributes (set by subclass)
    ----------------------------
    name : str              -- registry key, e.g. ``"h_size"``
    level : ControlLevel    -- HOUSEHOLD or PERSON
    description : str       -- human-readable label
    categories : type       -- IntEnum or LabeledEnum for output bins
    survey_fields : tuple   -- canonical survey column names (metadata)
    pums_fields : tuple     -- PUMS column names (metadata)
    """

    name: str
    level: ControlLevel
    description: str
    categories: type[Enum]
    survey_fields: tuple[str, ...]
    pums_fields: tuple[str, ...]

    def survey_expr(self) -> pl.Expr:
        """Polars expression mapping survey columns → control int (Int16)."""
        msg = f"{type(self).__name__}.survey_expr() not implemented"
        raise NotImplementedError(msg)

    def pums_expr(self) -> pl.Expr:
        """Polars expression mapping PUMS columns → control int (Int16)."""
        msg = f"{type(self).__name__}.pums_expr() not implemented"
        raise NotImplementedError(msg)

    @property
    def valid_members(self) -> list[tuple[int, str]]:
        """``(value, name)`` for each non-sentinel output category."""
        return [(m.value, m.name) for m in self.categories if m.name not in _SENTINEL_NAMES]
