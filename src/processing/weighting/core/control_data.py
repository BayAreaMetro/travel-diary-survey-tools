"""PUMS control-data transformation.

Recodes raw PUMS variables into the shared control categories defined in
``controls.py``, then aggregates weighted totals by geography (PUMA by
default).

All mapping logic lives in ``controls.py``.  This module only orchestrates
recoding (via ``ctrl.pums_expr()``) and aggregation.
"""

import logging
from dataclasses import dataclass

import polars as pl

from data_canon.codebook.pums import PumsEsr, PumsThresholds
from processing.weighting.core.controls import (
    CONTROLS,
    ControlLevel,
    ControlTarget,
    resolve_targets,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class ControlSpec:
    """Specification for a single weighting control.

    Parameters
    ----------
    name : str
        Registry name (must exist in ``CONTROLS``).
    """

    name: str


@dataclass
class ControlTotals:
    """Result of PUMS control-total aggregation.

    Attributes:
    ----------
    totals : pl.DataFrame
        Tidy frame with columns:
        [geo_id, control_name, category, target_total]
    pums_hh_count : int
        Total PUMS housing unit records (before weighting).
    pums_person_count : int
        Total PUMS person records.
    geo_ids : list[str]
        Unique geography IDs in the totals.
    """

    totals: pl.DataFrame
    pums_hh_count: int
    pums_person_count: int
    geo_ids: list[str]


# ---------------------------------------------------------------------------
# Recoding  (generic loop — all mapping logic lives in controls.py)
# ---------------------------------------------------------------------------


def _apply_pums_recode(df: pl.DataFrame, ctrl: ControlTarget) -> pl.DataFrame:
    """Add ``ctrl_{name}`` column via ``ctrl.pums_expr()``."""
    # Add null columns for any optional PUMS fields absent from the DataFrame
    for f in ctrl.pums_fields:
        if f not in df.columns:
            df = df.with_columns(pl.lit(None).alias(f))
    return df.with_columns(ctrl.pums_expr().alias(f"ctrl_{ctrl.name}"))


def recode_pums_households(
    hh_df: pl.DataFrame,
    person_df: pl.DataFrame,
    targets: list[str] | None = None,
) -> pl.DataFrame:
    """Recode PUMS household records into control categories.

    Derives person-level aggregates (workers, children) then loops over
    household-level controls calling ``ctrl.from_pums_row``.

    Parameters
    ----------
    hh_df : pl.DataFrame
        PUMS household microdata.
    person_df : pl.DataFrame
        PUMS person microdata (used to derive hh-level aggregates).
    targets : list[str] | None
        Registry keys to recode.  ``None`` → all household controls.
    """
    hh_ctrls = (
        resolve_targets(targets, ControlLevel.HOUSEHOLD)
        if targets
        else [c for c in CONTROLS.values() if c.level == ControlLevel.HOUSEHOLD]
    )

    # Pre-aggregate person → household counts for derived HH controls.
    # h_size uses NP directly from the HH table — no person aggregation.
    requested = {c.name for c in hh_ctrls}
    aggs: list[pl.Expr] = []
    if "h_workers" in requested:
        aggs.append(
            pl.col("ESR").is_in(PumsEsr.EMPLOYED).sum().cast(pl.Int32).alias("_n_workers"),
        )
    if "h_children" in requested:
        aggs.append(
            (pl.col("AGEP").is_not_null() & (pl.col("AGEP") <= PumsThresholds.CHILD_MAX_AGE))
            .sum()
            .cast(pl.Int32)
            .alias("_n_children"),
        )

    if aggs:
        person_agg = person_df.group_by("SERIALNO").agg(aggs)
        hh = hh_df.join(person_agg, on="SERIALNO", how="left")
    else:
        hh = hh_df

    for ctrl in hh_ctrls:
        hh = _apply_pums_recode(hh, ctrl)
    return hh


def recode_pums_persons(
    person_df: pl.DataFrame,
    targets: list[str] | None = None,
) -> pl.DataFrame:
    """Recode PUMS person records into control categories.

    Parameters
    ----------
    person_df : pl.DataFrame
        PUMS person microdata.
    targets : list[str] | None
        Registry keys to recode.  ``None`` → all person controls.
    """
    p_ctrls = (
        resolve_targets(targets, ControlLevel.PERSON)
        if targets
        else [c for c in CONTROLS.values() if c.level == ControlLevel.PERSON]
    )

    df = person_df
    for ctrl in p_ctrls:
        df = _apply_pums_recode(df, ctrl)
    return df


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------
def build_control_totals(
    hh_df: pl.DataFrame,
    person_df: pl.DataFrame,
    controls: list[ControlSpec],
    geo_col: str = "PUMA",
) -> ControlTotals:
    """Build weighted control totals from recoded PUMS data.

    Parameters
    ----------
    hh_df : pl.DataFrame
        Recoded PUMS household data (from ``recode_pums_households``).
    person_df : pl.DataFrame
        Recoded PUMS person data (from ``recode_pums_persons``).
    controls : list[ControlSpec]
        Control specifications (which variables to include).
    geo_col : str
        Column name for the geography identifier.  Defaults to ``"PUMA"``.

    Returns:
    -------
    ControlTotals
        Tidy totals frame and metadata.
    """
    all_totals: list[pl.DataFrame] = []

    for spec in controls:
        ctrl = CONTROLS.get(spec.name)
        if ctrl is None:
            msg = f"Unknown control name: {spec.name!r}. Available: {sorted(CONTROLS)}"
            raise ValueError(msg)

        ctrl_col = f"ctrl_{spec.name}"
        weight_col = "WGTP" if ctrl.level == ControlLevel.HOUSEHOLD else "PWGTP"
        source_df = hh_df if ctrl.level == ControlLevel.HOUSEHOLD else person_df

        if ctrl_col not in source_df.columns:
            hh_or_person = "household" if ctrl.level == ControlLevel.HOUSEHOLD else "person"
            msg = (
                f"Control column {ctrl_col!r} not found in "
                f"{hh_or_person} data. Did you run recode first?"
            )
            raise ValueError(msg)

        if geo_col not in source_df.columns:
            msg = f"Geography column {geo_col!r} not found in data."
            raise ValueError(msg)

        # Drop nulls (missing categories)
        working = source_df.filter(pl.col(ctrl_col).is_not_null())

        # Aggregate: sum of weights by (geo, category)
        totals = (
            working.group_by([geo_col, ctrl_col])
            .agg(pl.col(weight_col).sum().alias("target_total"))
            .rename({geo_col: "geo_id", ctrl_col: "category"})
            .with_columns(
                pl.lit(spec.name).alias("control_name"),
                pl.col("geo_id").cast(pl.Utf8),
            )
        )

        all_totals.append(totals.select(["geo_id", "control_name", "category", "target_total"]))

    if not all_totals:
        msg = "No controls specified."
        raise ValueError(msg)

    combined = pl.concat(all_totals)
    geo_ids = combined["geo_id"].unique().sort().to_list()

    return ControlTotals(
        totals=combined,
        pums_hh_count=len(hh_df),
        pums_person_count=len(person_df),
        geo_ids=geo_ids,
    )
