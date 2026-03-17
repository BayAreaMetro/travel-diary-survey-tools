"""PUMS control-data transformation.

Recodes raw PUMS variables into the shared control categories defined in
``controls.py``, then aggregates weighted totals by geography (PUMA by
default).

All mapping logic lives in ``controls.py``.  This module only orchestrates
recoding (via ``ctrl.pums_expr()``) and aggregation.

Approach:

1. Load PUMS data; join persons to households to carry PUMA geography.
2. Join crosswalk; multiply PUMS weight by ``allocation_weight`` to
   distribute into custom zones.
3. For each control: apply filter, recode variable into bins/groups,
   aggregate weighted sum by ``(ctrl_geoid, category)``.

Control Variable YAML Configuration Example::

    controls:
      # Simple marginal — household size
      - name: h_size
        table: households
        variable: NP
        bins:
          "1":  [1, 1]
          "2":  [2, 2]
          "3":  [3, 3]
          "4+": [4, 99]

      # Grouped marginal — commute mode
      - name: commute_mode
        table: persons
        variable: JWTRNS
        groups:
          drove_alone: [1]
          carpool:     [2, 3]
          transit:     [4, 5, 6, 7, 8, 9]
          other:       [10, 11, 12]
        filter: "ESR in [1,2,4,5]"   # employed persons only
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
from processing.weighting.data_prep.crosswalk import _build_zone_remap
from processing.weighting.specs import ControlSpec, ControlTotals
from processing.weighting.validation.checksums import check_recode_nulls
from processing.weighting.validation.control_validation import (
    validate_crosstab_margins,
)

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

    check_recode_nulls(
        df,
        targets or [c.name for c in p_ctrls],
        level=ControlLevel.PERSON,
        id_col="SERIALNO",
        source_label="PUMS",
        strict=True,
    )
    return df


# ---------------------------------------------------------------------------
# Zone allocation for PUMS incidence
# ---------------------------------------------------------------------------

# TODO: Should this just be part of crosswalk?
# Or should the internal crosswalk functions become external?
def allocate_pums_zones(
    pums_incidence: pl.DataFrame,
    crosswalk_df: pl.DataFrame,
    *,
    puma_col: str = "PUMA",
    weight_col: str = "WGTP",
) -> pl.DataFrame:
    """Assign target-zone IDs and scale PUMS weights via the crosswalk.

    Takes the pivoted PUMS incidence table (from
    :func:`~processing.weighting.data_prep.seed_data.build_incidence_table`)
    and joins the PUMA → target-zone crosswalk.  Each PUMS household is
    duplicated per overlapping target zone, and its weight is scaled by the
    zone's ``allocation_weight``.

    Parameters
    ----------
    pums_incidence : pl.DataFrame
        Pivoted PUMS incidence table.  Must contain *puma_col* and
        *weight_col*.
    crosswalk_df : pl.DataFrame
        Crosswalk with columns ``puma_id``, ``study_geoid``,
        ``ctrl_geoid``, ``allocation_weight``.
    puma_col : str
        Column in *pums_incidence* identifying the PUMA (default ``"PUMA"``).
    weight_col : str
        Original PUMS weight column to scale (default ``"WGTP"``).

    Returns:
        pl.DataFrame with ``study_geoid`` and ``ctrl_geoid`` added, and
        *weight_col* scaled by ``allocation_weight``.  Rows are multiplied
        per zone overlap.
    """
    xw = crosswalk_df.select("puma_id", "study_geoid", "ctrl_geoid", "allocation_weight")

    result = (
        pums_incidence.join(
            xw,
            left_on=pl.col(puma_col).cast(pl.Utf8),
            right_on="puma_id",
            how="inner",
        )
        .with_columns(
            (pl.col(weight_col).cast(pl.Float64) * pl.col("allocation_weight")).alias(weight_col),
        )
        .drop("allocation_weight")
    )

    # --- Guard: every crosswalk ctrl_geoid must appear in the result ------
    # This is the hard check — a missing control zone means the balancing
    # step will have no seed data for that zone.  If *all* geos are missing
    # it's almost certainly a PUMA ID type mismatch (e.g. leading zeros).
    expected_geos = set(xw["ctrl_geoid"].unique().to_list())
    actual_geos = set(result["ctrl_geoid"].unique().to_list())
    missing_geos = expected_geos - actual_geos

    pums_puma_ids = pums_incidence[puma_col].cast(pl.Utf8).unique()
    xw_puma_ids = xw["puma_id"].unique()

    if missing_geos:
        # Provide extra context when everything is missing — likely a
        # systemic join failure rather than a small-zone issue.
        if actual_geos:
            msg = (
                f"{len(missing_geos)} control zone(s) from the crosswalk received "
                f"zero PUMS allocation: {sorted(missing_geos)}"
            )
        else:
            pums_samples = pums_puma_ids.sort().head(5).to_list()
            xw_samples = xw_puma_ids.sort().head(5).to_list()
            msg = (
                f"ALL {len(missing_geos)} control zone(s) missing after PUMA join — "
                f"no PUMS records matched the crosswalk.  This is almost certainly "
                f"a PUMA ID type mismatch (e.g. integer vs. zero-padded string).  "
                f"PUMS PUMAs (first 5): {pums_samples}.  "
                f"Crosswalk PUMAs (first 5): {xw_samples}."
            )
        raise ValueError(msg)

    logger.info(
        "PUMS zone allocation: %d HHs → %d rows (%d zones)",
        len(pums_incidence),
        len(result),
        result["study_geoid"].n_unique(),
    )
    return result


# ---------------------------------------------------------------------------
# Control-total aggregation from incidence tables
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Aggregation (legacy — kept for tests; prefer aggregate_control_totals)
# ---------------------------------------------------------------------------
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
    # Validate total category count across all controls
    ctrl_instances = [CONTROLS[spec.name] for spec in controls if spec.name in CONTROLS]

    all_totals: list[pl.DataFrame] = []

    for spec in controls:
        ctrl = CONTROLS.get(spec.name)
        if ctrl is None:
            msg = f"Unknown control name: {spec.name!r}. Available: {sorted(CONTROLS)}"
            raise ValueError(msg)

        ctrl_col = spec.name
        raw_wt = "WGTP" if ctrl.level == ControlLevel.HOUSEHOLD else "PWGTP"
        # Prefer crosswalk-scaled weights when available
        xw_wt = f"_xw_{raw_wt}"
        source_df = hh_df if ctrl.level == ControlLevel.HOUSEHOLD else person_df
        weight_col = xw_wt if xw_wt in source_df.columns else raw_wt

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

    # Validate margin consistency for cross-tab controls
    validate_crosstab_margins(combined, ctrl_instances, controls)

    return ControlTotals(
        totals=combined,
        pums_hh_count=len(hh_df),
        pums_person_count=len(person_df),
        geo_ids=geo_ids,
    )


# ---------------------------------------------------------------------------
# Zone grouping (legacy — kept for tests)
# ---------------------------------------------------------------------------


def apply_zone_groups(
    control_totals: ControlTotals,
    seed: pl.DataFrame,
    zone_groups: dict[str, list[str]],
    geo_col: str = "ctrl_geoid",
) -> tuple[ControlTotals, pl.DataFrame]:
    """Merge zones for balancing while preserving crosswalk granularity.

    Zones listed under a group name are remapped to that group in both
    the control totals and the seed table.  Unmapped zones pass through
    unchanged.
    """
    remap = _build_zone_remap(zone_groups)
    remap_expr = pl.col("geo_id").replace(remap)

    merged_totals = (
        control_totals.totals.with_columns(remap_expr)
        .group_by("geo_id", "control_name", "category")
        .agg(pl.col("target_total").sum())
    )
    new_geo_ids = merged_totals["geo_id"].unique().sort().to_list()

    seed_remap_expr = pl.col(geo_col).replace(remap)
    merged_seed = seed.with_columns(
        pl.col(geo_col).alias("_orig_ctrl_geoid"),
        pl.col(geo_col).replace_strict(remap, default=None).alias("zone_group"),
        seed_remap_expr,
    )

    logger.info(
        "Zone groups applied: %d zones → %d zones (%d groups)",
        len(control_totals.geo_ids),
        len(new_geo_ids),
        len(zone_groups),
    )

    return (
        ControlTotals(
            totals=merged_totals,
            pums_hh_count=control_totals.pums_hh_count,
            pums_person_count=control_totals.pums_person_count,
            geo_ids=new_geo_ids,
        ),
        merged_seed,
    )
