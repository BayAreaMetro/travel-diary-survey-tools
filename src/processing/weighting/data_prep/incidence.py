"""Incidence-table construction and control-total aggregation.

Unified operations that apply identically to both survey and PUMS datasets
after recoding:

1. **Pivot** — :func:`build_incidence_table` converts encoded control
   columns into the ``{ctrl}__{member}`` incidence layout.
2. **Aggregate** — :func:`aggregate_control_totals` sums the pivoted
   PUMS incidence into per-zone target totals.

Category merging lives in :mod:`processing.weighting.data_prep.merges`.
"""

import logging

import polars as pl

from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import resolve_targets
from processing.weighting.specs import ControlTotals
from processing.weighting.validation.control_validation import validate_total_control_categories

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Incidence pivot
# ---------------------------------------------------------------------------


def build_incidence_table(  # noqa: C901
    hh_recoded: pl.DataFrame,
    per_recoded: pl.DataFrame,
    targets: list[str],
    *,
    hh_id_col: str = "hh_id",
    extra_cols: list[str] | None = None,
) -> pl.DataFrame:
    """Build household-level incidence table with fully-pivoted control columns.

    This is the unified pivoter for both survey and PUMS data.  After
    recoding, both datasets share the same control-column schema
    (``h_size``, ``h_income``, ``p_gender``, …).  This function pivots
    those encoded columns into the incidence layout expected by the
    balancer:

    - HH controls as 0/1 indicators: ``h_size__size_1``, …
    - Person controls as per-household counts: ``p_gender__male``, …
    - Structural controls remain unpivoted: ``h_total``, ``p_total``.

    Non-structural controls use the naming ``{control_name}__{member_name}``.

    Parameters
    ----------
    hh_recoded : pl.DataFrame
        Recoded households (survey or PUMS).
    per_recoded : pl.DataFrame
        Recoded persons (survey or PUMS).
    targets : list[str]
        Control registry names used for recoding.
    hh_id_col : str
        Household identifier column (``"hh_id"`` for survey,
        ``"SERIALNO"`` for PUMS).
    extra_cols : list[str] | None
        Additional columns from *hh_recoded* to carry forward
        (e.g. ``["WGTP", "PUMA"]`` for PUMS).
    """
    base_cols = [hh_id_col]
    for col in extra_cols or []:
        if col in hh_recoded.columns and col not in base_cols:
            base_cols.append(col)
    incidence = hh_recoded.select(base_cols)

    hh_ctrls = resolve_targets(targets, ControlLevel.HOUSEHOLD)
    encoded_cols = [c.name for c in hh_ctrls if c.name in hh_recoded.columns]

    if encoded_cols:
        hh_data = hh_recoded.select([hh_id_col, *encoded_cols])
        incidence = incidence.join(hh_data, on=hh_id_col, how="left")

        for ctrl in hh_ctrls:
            if ctrl.name not in incidence.columns or ctrl.structural:
                continue
            for value, member_name in ctrl.valid_members:
                col_name = f"{ctrl.name}__{member_name.lower()}"
                incidence = incidence.with_columns(
                    (pl.col(ctrl.name) == value).cast(pl.Int32).alias(col_name)
                )
            incidence = incidence.drop(ctrl.name)

    for ctrl in resolve_targets(targets, ControlLevel.PERSON):
        if ctrl.name not in per_recoded.columns:
            continue
        if ctrl.structural:
            agg = per_recoded.group_by(hh_id_col).agg(pl.len().alias(ctrl.name))
            incidence = incidence.join(agg, on=hh_id_col, how="left").with_columns(
                pl.col(ctrl.name).fill_null(0),
            )
        else:
            for value, member_name in ctrl.valid_members:
                col_name = f"{ctrl.name}__{member_name.lower()}"
                agg = (
                    per_recoded.filter(pl.col(ctrl.name) == value)
                    .group_by(hh_id_col)
                    .agg(pl.len().alias(col_name))
                )
                incidence = incidence.join(agg, on=hh_id_col, how="left").with_columns(
                    pl.col(col_name).fill_null(0),
                )

    logger.info(
        "Incidence table: %d households, %d columns", len(incidence), len(incidence.columns)
    )
    return incidence


# ---------------------------------------------------------------------------
# Control-total aggregation from incidence tables
# ---------------------------------------------------------------------------


def aggregate_control_totals(
    pums_incidence: pl.DataFrame,
    targets: list[str],
    *,
    weight_col: str = "WGTP",
    geo_col: str = "ctrl_geoid",
) -> ControlTotals:
    """Aggregate PUMS incidence into per-zone control totals.

    Reads the pivoted ``{control}__{member}`` columns and structural
    columns directly from the incidence table, multiplies each by the
    household weight, and sums by geography.

    Parameters
    ----------
    pums_incidence : pl.DataFrame
        Zone-allocated PUMS incidence table.  Must have *geo_col*,
        *weight_col*, and the ``{control}__{member}`` / structural columns.
    targets : list[str]
        Control registry names.
    weight_col : str
        Weight column (default ``"WGTP"``).
    geo_col : str
        Geography column (default ``"ctrl_geoid"``).
    """
    ctrl_instances = resolve_targets(targets)
    validate_total_control_categories(ctrl_instances)

    all_totals: list[pl.DataFrame] = []

    for ctrl in ctrl_instances:
        if ctrl.structural:
            col = ctrl.name
            if col not in pums_incidence.columns:
                continue
            totals = (
                pums_incidence.group_by(geo_col)
                .agg(
                    (pl.col(col).cast(pl.Float64) * pl.col(weight_col)).sum().alias("target_total")
                )
                .with_columns(
                    pl.col(geo_col).cast(pl.Utf8).alias("geo_id"),
                    pl.lit(ctrl.name).alias("control_name"),
                    pl.lit(ctrl.valid_members[0][1].lower()).alias("category"),
                )
                .select("geo_id", "control_name", "category", "target_total")
            )
            all_totals.append(totals)
        else:
            prefix = f"{ctrl.name}__"
            ctrl_cols = sorted(c for c in pums_incidence.columns if c.startswith(prefix))
            for col in ctrl_cols:
                member = col[len(prefix) :]
                totals = (
                    pums_incidence.group_by(geo_col)
                    .agg(
                        (pl.col(col).cast(pl.Float64) * pl.col(weight_col))
                        .sum()
                        .alias("target_total")
                    )
                    .with_columns(
                        pl.col(geo_col).cast(pl.Utf8).alias("geo_id"),
                        pl.lit(ctrl.name).alias("control_name"),
                        pl.lit(member).alias("category"),
                    )
                    .select("geo_id", "control_name", "category", "target_total")
                )
                all_totals.append(totals)

    if not all_totals:
        msg = "No control columns found in PUMS incidence table."
        raise ValueError(msg)

    combined = pl.concat(all_totals)
    geo_ids = combined["geo_id"].unique().sort().to_list()

    id_col = "SERIALNO" if "SERIALNO" in pums_incidence.columns else pums_incidence.columns[0]
    pums_hh_count = pums_incidence[id_col].n_unique()
    pums_person_count = 0
    if "p_total" in pums_incidence.columns:
        pums_person_count = int(pums_incidence["p_total"].sum())

    return ControlTotals(
        totals=combined,
        pums_hh_count=pums_hh_count,
        pums_person_count=pums_person_count,
        geo_ids=geo_ids,
    )
