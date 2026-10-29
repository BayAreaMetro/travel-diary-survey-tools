"""PUMS recoding.

Recodes PUMS household and person variables into the shared control categories
defined in ``controls``, through each control's ``pums_expr()``; household
controls that count persons (workers, children) are aggregated from the person
records first. Targets are summed from the recoded records in ``incidence``.
"""

import logging

import polars as pl

from data_canon.codebook.pums import PumsEsr, PumsThresholds
from processing.weighting.controls.base import (
    ControlLevel,
    ControlTarget,
    CrosstabControlTarget,
)
from processing.weighting.controls.registry import CONTROLS, resolve_targets
from processing.weighting.validation.checksums import check_recode_nulls

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Recoding  (generic loop — all mapping logic lives in controls.py)
# ---------------------------------------------------------------------------


def _apply_pums_recode(df: pl.DataFrame, ctrl: ControlTarget) -> pl.DataFrame:
    """Add ``{name}`` column via ``ctrl.pums_expr()``."""
    # Cross-tab controls derive their values from dimension control expressions
    # (no direct PUMS fields)
    if isinstance(ctrl, CrosstabControlTarget):
        # Ensure dimension controls have been recoded first
        for dim_ctrl in ctrl.dim_controls:
            if dim_ctrl.name not in df.columns:
                msg = (
                    f"Cross-tab '{ctrl.name}' requires dimension control '{dim_ctrl.name}' "
                    f"to be recoded first"
                )
                raise ValueError(msg)
        return df.with_columns(ctrl.pums_expr().alias(ctrl.name))

    # Add null columns for any optional PUMS fields absent from the DataFrame
    for f in ctrl.pums_fields:
        if f not in df.columns:
            df = df.with_columns(pl.lit(None).alias(f))
    return df.with_columns(ctrl.pums_expr().alias(ctrl.name))


def recode_pums_households(
    hh_df: pl.DataFrame,
    person_df: pl.DataFrame,
    targets: list[str] | None = None,
) -> pl.DataFrame:
    """Recode PUMS household records into control categories.

    Derives person-level aggregates (workers, children) then loops over
    household-level controls calling ``ctrl.from_pums_row``.

    Args:
        hh_df: PUMS household microdata.
        person_df: PUMS person microdata (used to derive hh-level aggregates).
        targets: Registry keys to recode.  ``None`` → all household controls.
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
        logger.info("Recoding PUMS household control '%s'...", ctrl.name)
        hh = _apply_pums_recode(hh, ctrl)

    check_recode_nulls(
        hh,
        targets or [c.name for c in hh_ctrls],
        level=ControlLevel.HOUSEHOLD,
        id_col="SERIALNO",
        source_label="PUMS",
        strict=True,
    )
    return hh


def recode_pums_persons(
    person_df: pl.DataFrame,
    targets: list[str] | None = None,
) -> pl.DataFrame:
    """Recode PUMS person records into control categories.

    Args:
        person_df: PUMS person microdata.
        targets: Registry keys to recode.  ``None`` → all person controls.
    """
    p_ctrls = (
        resolve_targets(targets, ControlLevel.PERSON)
        if targets
        else [c for c in CONTROLS.values() if c.level == ControlLevel.PERSON]
    )

    df = person_df
    for ctrl in p_ctrls:
        logger.info("Recoding PUMS person control '%s'...", ctrl.name)
        df = _apply_pums_recode(df, ctrl)

    check_recode_nulls(
        df,
        targets or [c.name for c in p_ctrls],
        level=ControlLevel.PERSON,
        id_col="SERIALNO",
        source_label="PUMS",
        strict=True,
    )
    return df
