"""Base class and helpers for weighting controls.

Contains ``ControlLevel``, ``ControlTarget`` base class, and shared
expression helpers used by the household and person control subclasses.
"""

import logging
from enum import Enum, StrEnum
from itertools import product
from math import prod

import polars as pl

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════

_SENTINEL_NAMES = frozenset({"MISSING", "PNTA"})

# Cross-tab validation thresholds
MAX_CROSSTAB_CELLS = 500  # Hard limit per individual cross-tab control
INFO_CROSSTAB_CELLS = 100  # Info log threshold

# Total category validation thresholds (across all controls)
MAX_TOTAL_CATEGORIES = 200  # Hard limit for total categories across all controls
INFO_TOTAL_CATEGORIES = 100  # Info log threshold for total categories


def identity_expr(col: str, categories: type[Enum]) -> pl.Expr:
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


def breakpoint_expr(col: str, categories: type[Enum]) -> pl.Expr:
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


class ControlLevel(StrEnum):
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
    structural: bool = False

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


class CrosstabControlTarget(ControlTarget):
    """Base class for N-dimensional cross-tabulated weighting control.

    Cross-tabs are built from the cartesian product of base controls' original
    (unmerged) categories. They then apply their own independent merge specs
    via the balancer's merge mechanism.

    Subclasses set these additional attributes:

    - ``dim_controls`` : tuple of ControlTarget instances to cross-tabulate
    - ``categories`` : IntEnum generated via ``make_crosstab_enum()``

    The survey_expr() and pums_expr() methods are implemented automatically
    to combine dimension expressions into composite integer keys.

    Examples:
    ---------
    >>> class HHIncomeBySizeControl(CrosstabControlTarget):
    ...     name = "h_income_x_size"
    ...     level = ControlLevel.HOUSEHOLD
    ...     description = "Household income by size"
    ...     dim_controls = (HHIncomeControl(), HHSizeControl())
    ...     categories = make_crosstab_enum("IncomeSizeCategory", IncomeBroad, HHSizeCategory)
    ...     survey_fields = tuple()  # Derived from dim_controls
    ...     pums_fields = tuple()    # Derived from dim_controls
    """

    dim_controls: tuple[ControlTarget, ...]

    def __init__(self) -> None:
        """Validate cross-tab dimensions on initialization."""
        # Validate cell count limits
        cell_count = self._compute_cell_count()

        if cell_count > MAX_CROSSTAB_CELLS:
            msg = (
                f"Crosstab '{self.name}' has {cell_count} cells (max: {MAX_CROSSTAB_CELLS}).\n"
                f"Dimensions: {[(c.name, len(c.valid_members)) for c in self.dim_controls]}\n"
                f"Pre-merge dimension categories before creating the crosstab."
            )
            raise ValueError(msg)

        if cell_count > INFO_CROSSTAB_CELLS:
            logger.info(
                "Large crosstab: '%s' has %d cells. Ensure adequate sample size in all zones.\n"
                "Dimensions: %s",
                self.name,
                cell_count,
                [(c.name, len(c.valid_members)) for c in self.dim_controls],
            )

    def _compute_cell_count(self) -> int:
        """Compute total cells in cross-tab (product of dimension sizes)."""
        return prod(len(c.valid_members) for c in self.dim_controls)

    def survey_expr(self) -> pl.Expr:
        """Combine dimension survey expressions into composite key.

        Returns sequential integer (0, 1, 2, ...) corresponding to position
        in cartesian product of dimension categories.
        """
        # Get expressions for each dimension
        dim_exprs = [c.survey_expr() for c in self.dim_controls]

        # Build lookup dict: (dim1_val, dim2_val, ...) -> composite_idx
        member_map = {}
        for idx, member_combo in enumerate(product(*[c.valid_members for c in self.dim_controls])):
            key = tuple(val for val, _ in member_combo)
            member_map[key] = idx

        # Create pl.struct of dimension values, then map to composite index
        struct_expr = pl.struct(*[expr.alias(f"_dim_{i}") for i, expr in enumerate(dim_exprs)])

        # Map struct to composite value using when/then chain
        # Start with FALSE condition to initialize the when/then expression
        result_expr = pl.when(pl.lit(value=False)).then(None)
        for key, composite_val in member_map.items():
            # Build condition: dim0 == key[0] AND dim1 == key[1] AND ...
            condition = pl.lit(value=True)
            for i, val in enumerate(key):
                condition = condition & (struct_expr.struct.field(f"_dim_{i}") == val)
            result_expr = result_expr.when(condition).then(composite_val)

        return result_expr.otherwise(None).cast(pl.Int16)

    def pums_expr(self) -> pl.Expr:
        """Combine dimension PUMS expressions into composite key.

        Uses same mapping as survey_expr() but with PUMS expressions.
        """
        # Get expressions for each dimension
        dim_exprs = [c.pums_expr() for c in self.dim_controls]

        # Build lookup dict: (dim1_val, dim2_val, ...) -> composite_idx
        member_map = {}
        for idx, member_combo in enumerate(product(*[c.valid_members for c in self.dim_controls])):
            key = tuple(val for val, _ in member_combo)
            member_map[key] = idx

        # Create pl.struct of dimension values, then map to composite index
        struct_expr = pl.struct(*[expr.alias(f"_dim_{i}") for i, expr in enumerate(dim_exprs)])

        # Map struct to composite value using when/then chain
        # Start with FALSE condition to initialize the when/then expression
        result_expr = pl.when(pl.lit(value=False)).then(None)
        for key, composite_val in member_map.items():
            # Build condition: dim0 == key[0] AND dim1 == key[1] AND ...
            condition = pl.lit(value=True)
            for i, val in enumerate(key):
                condition = condition & (struct_expr.struct.field(f"_dim_{i}") == val)
            result_expr = result_expr.when(condition).then(composite_val)

        return result_expr.otherwise(None).cast(pl.Int16)
