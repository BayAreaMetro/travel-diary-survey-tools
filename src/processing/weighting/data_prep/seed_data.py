"""Survey seed-data preparation for weighting.

Uses ``ControlTarget.survey_expr()`` to recode canonical survey data into
control-category ints via native Polars expressions (vectorized, no
``map_elements``).  Every control is handled uniformly.
"""

import logging

import polars as pl

from data_canon.codebook.persons import AgeCategory, Employment
from processing.weighting.controls.base import ControlLevel, ControlTarget
from processing.weighting.controls.registry import resolve_targets
from processing.weighting.validation.checksums import check_recode_nulls

logger = logging.getLogger(__name__)


# -- Public API ------------------------------------------------------------
def recode_survey_households(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    targets: list[str],
) -> pl.DataFrame:
    """Recode canonical survey households into control categories.

    Parameters
    ----------
    households, persons : pl.DataFrame
        Canonical tables (both must have ``hh_id``).
    targets : list[str]
        Registry keys to recode.  Person-level keys are ignored.

    Raises:
    ------
    ValueError  -- unknown target.
    KeyError    -- missing required column.
    """
    hh_ctrls = resolve_targets(targets, ControlLevel.HOUSEHOLD)
    if not hh_ctrls:
        return households

    # Pre-aggregate person → household counts for derived HH controls.
    # These become plain columns (_n_persons, _n_workers, _n_children)
    # that the control expressions read directly.
    _person_aggs: dict[str, pl.Expr] = {
        "h_size": pl.len().alias("_n_persons"),
        "h_workers": (
            pl.col("employment")
            .is_in(
                [
                    Employment.EMPLOYED_FULLTIME.value,
                    Employment.EMPLOYED_PARTTIME.value,
                    Employment.EMPLOYED_SELF.value,
                ]
            )
            .sum()
            .cast(pl.Int32)
            .alias("_n_workers")
        ),
        "h_children": (
            pl.col("age")
            .is_in(
                [
                    AgeCategory.AGE_UNDER_5.value,
                    AgeCategory.AGE_5_TO_15.value,
                    AgeCategory.AGE_16_TO_17.value,
                ]
            )
            .sum()
            .cast(pl.Int32)
            .alias("_n_children")
        ),
    }
    requested = {c.name for c in hh_ctrls}
    aggs = [expr for name, expr in _person_aggs.items() if name in requested]

    if aggs:
        counts = persons.group_by("hh_id").agg(aggs)
        hh = households.join(counts, on="hh_id", how="left")
    else:
        hh = households

    for ctrl in hh_ctrls:
        _require_fields(hh, ctrl)
        hh = _apply_recode(hh, ctrl)

    check_recode_nulls(
        hh,
        targets,
        level=ControlLevel.HOUSEHOLD,
        id_col="hh_id",
        source_label="survey",
    )
    return hh


def recode_survey_persons(
    persons: pl.DataFrame,
    targets: list[str],
) -> pl.DataFrame:
    """Recode canonical survey persons into control categories.

    Parameters
    ----------
    persons : pl.DataFrame
        Canonical persons table.
    targets : list[str]
        Registry keys to recode.  Household-level keys are ignored.

    Raises:
    ------
    ValueError  -- unknown target.
    KeyError    -- missing required column.
    """
    p_ctrls = resolve_targets(targets, ControlLevel.PERSON)
    df = persons

    for ctrl in p_ctrls:
        _require_fields(df, ctrl)
        df = _apply_recode(df, ctrl)

    check_recode_nulls(
        df,
        targets,
        level=ControlLevel.PERSON,
        id_col="person_id",
        source_label="survey",
    )
    return df


def build_seed_table(
    households_recoded: pl.DataFrame,
    persons_recoded: pl.DataFrame,
    targets: list[str],
    *,
    geo_col: str = "ctrl_geoid",
) -> pl.DataFrame:
    """Build household-level seed table with person incidence columns.

    Parameters
    ----------
    households_recoded : pl.DataFrame
        Recoded households (from ``recode_survey_households``).
    persons_recoded : pl.DataFrame
        Recoded persons (from ``recode_survey_persons``).
    targets : list[str]
        Registry keys used for recoding.
    geo_col : str
        Geography column on households.
    """
    # Select HH-level control columns by their known target names.
    hh_names = [c.name for c in resolve_targets(targets, ControlLevel.HOUSEHOLD)]
    hh_cols = [n for n in hh_names if n in households_recoded.columns]
    seed = households_recoded.select(["hh_id", geo_col, *hh_cols])

    for ctrl in resolve_targets(targets, ControlLevel.PERSON):
        if ctrl.name not in persons_recoded.columns:
            continue
        for value, member_name in ctrl.valid_members:
            col_name = f"{ctrl.name}__{member_name.lower()}"
            incidence = (
                persons_recoded.filter(pl.col(ctrl.name) == value)
                .group_by("hh_id")
                .agg(pl.len().alias(col_name))
            )
            seed = seed.join(incidence, on="hh_id", how="left").with_columns(
                pl.col(col_name).fill_null(0),
            )

    logger.info("Seed table: %d households, %d columns", len(seed), len(seed.columns))
    return seed


# -- Internals -------------------------------------------------------------


def _apply_recode(df: pl.DataFrame, ctrl: ControlTarget) -> pl.DataFrame:
    """Add ``{name}`` column via ``ctrl.survey_expr()``."""
    # Add null columns for any optional fields absent from the DataFrame
    for f in ctrl.survey_fields:
        if f not in df.columns:
            df = df.with_columns(pl.lit(None).alias(f))
    return df.with_columns(ctrl.survey_expr().alias(ctrl.name))


def _require_fields(df: pl.DataFrame, ctrl: ControlTarget) -> None:
    """Raise ``KeyError`` if the primary survey field is absent."""
    # Structural controls (h_total, p_total) have no survey fields.
    if ctrl.structural:
        return
    if not ctrl.survey_fields:
        msg = (
            f"Target '{ctrl.name}' has no survey_fields defined. "
            f"Set structural=True if this is intentional."
        )
        raise ValueError(msg)
    # Optional secondary fields (e.g., school_type for p_student) may be
    # absent; we only require the first (primary) field.
    if ctrl.survey_fields[0] not in df.columns:
        msg = (
            f"Target '{ctrl.name}' requires column '{ctrl.survey_fields[0]}'. "
            f"Have: {sorted(df.columns)}"
        )
        raise KeyError(msg)
