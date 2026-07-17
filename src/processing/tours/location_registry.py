"""Build a tall person-location registry (issue #71).

This is the tall replacement for the wide scalar ``home_lat/lon``,
``work_lat/lon``, ``school_lat/lon`` columns produced by
``prepare_person_locations``. Each row is one known location for a person, so a
person can hold multiple locations of the same kind (e.g. a primary workplace
plus a recurring alternate worksite) with per-row provenance and statistics. See
``PersonLocationModel`` for the schema.

Two builders:

- ``build_reported_registry`` unpivots the reported scalar locations into tall
  rows (``source = REPORTED``). This alone reproduces exactly the coordinates the
  scalar model carries today.
- ``derive_observed_locations`` groups observed work (work + work-related) and
  school (school + school-related) travel and records each place the respondent
  spent enough time at as a ``WORK``/``SCHOOL`` row (``source = OBSERVED``).

``build_location_registry`` combines the two, de-duplicates observed locations
that coincide with a reported one, and numbers each person's locations within a
kind (primary first).

Nothing in the pipeline consumes the registry yet: this is migration step 1
(introduce alongside the scalars, no behavior change). Moving classification and
anchor detection onto the registry, and retiring the per-day ``ALTERNATE_WORK``
scalar patch, is a separate follow-up.

The population rule (``RegistryGateConfig``) is a dwell-time cutoff read from the
observed distributions; see ``scripts/dwell_gate_analysis.py`` and
``docs/analysis/location_registry_dwell_distribution.md``.
"""

import logging

import polars as pl
from pydantic import BaseModel, Field

from data_canon.codebook.generic import LocationSource, LocationType
from data_canon.codebook.trips import PurposeCategory

logger = logging.getLogger(__name__)

# Sentinel values of d_activity_duration that are not real dwell times
# (see LinkedTripModel.d_activity_duration): -1 = destination is home,
# -2 = last trip of the person-day (no subsequent departure).
_DWELL_SENTINELS = (-1, -2)

# Registry columns in schema order (see PersonLocationModel). location_num is
# assigned last, so the intermediate builders below omit it.
_REGISTRY_COLUMNS = [
    "person_id",
    "location_type",
    "location_num",
    "is_primary",
    "lat",
    "lon",
    "source",
    "n_days",
    "dwell_minutes",
]

# Reported scalar location columns, mapped to their registry location type.
_REPORTED_SCALARS = [
    (LocationType.HOME, "home_lat", "home_lon"),
    (LocationType.WORK, "work_lat", "work_lon"),
    (LocationType.SCHOOL, "school_lat", "school_lon"),
]

# Observed location kinds and the destination purposes pooled to derive them.
# A work location is any place reached on a work or work-related trip; likewise
# for school. Pooling the "related" purpose with its base captures worksites and
# campuses that are not the person's reported primary one.
_OBSERVED_POOLS = [
    (LocationType.WORK, [PurposeCategory.WORK.value, PurposeCategory.WORK_RELATED.value]),
    (LocationType.SCHOOL, [PurposeCategory.SCHOOL.value, PurposeCategory.SCHOOL_RELATED.value]),
]


class RegistryGateConfig(BaseModel):
    """Rule for promoting observed locations into the registry.

    A destination becomes an observed work/school location when the respondent
    spent at least ``min_dwell_minutes`` there. Dwell is the only filter: it
    separates real worksites and campuses from brief stops. How many distinct
    days the location was visited is recorded on each row as ``n_days`` for
    downstream use, but is not filtered on here, because some survey platforms
    collect only a single travel day (see the analysis doc) and would otherwise
    be excluded outright. ``min_distinct_days`` is retained as an optional knob
    and defaults to 1 (off).
    """

    min_dwell_minutes: float = Field(
        default=30.0,
        ge=0,
        description=(
            "Minimum activity duration in minutes (the location's longest "
            "observed stay) for it to enter the registry."
        ),
    )
    min_distinct_days: int = Field(
        default=1,
        ge=1,
        description=(
            "Optional minimum number of distinct travel days the location was "
            "visited. Defaults to 1 (no filter); n_days is recorded regardless."
        ),
    )
    cluster_decimals: int = Field(
        default=3,
        ge=0,
        le=6,
        description=(
            "Decimal places to round lat/lon to when grouping repeat visits to "
            "the same place (3 dp is roughly 110 m)."
        ),
    )


def build_reported_registry(person_locations: pl.DataFrame) -> pl.DataFrame:
    """Unpivot the reported scalar locations into tall registry rows.

    Args:
        person_locations: Wide per-person table with ``person_id`` and
            ``home_lat/lon``, ``work_lat/lon``, ``school_lat/lon`` columns (as
            produced by ``prepare_person_locations``).

    Returns:
        Tall table with one row per reported (non-null) location, without
        ``location_num`` (assigned by ``build_location_registry``). Every row has
        ``source = REPORTED``, ``is_primary = True``, and null statistics.
    """
    frames = [
        person_locations.filter(
            pl.col(lat_col).is_not_null() & pl.col(lon_col).is_not_null()
        ).select(
            pl.col("person_id"),
            pl.lit(loc_type.value, dtype=pl.Int64).alias("location_type"),
            pl.lit(value=True).alias("is_primary"),
            pl.col(lat_col).alias("lat"),
            pl.col(lon_col).alias("lon"),
            pl.lit(LocationSource.REPORTED.value, dtype=pl.Int64).alias("source"),
            pl.lit(None, dtype=pl.Int64).alias("n_days"),
            pl.lit(None, dtype=pl.Float64).alias("dwell_minutes"),
        )
        for loc_type, lat_col, lon_col in _REPORTED_SCALARS
        if lat_col in person_locations.columns and lon_col in person_locations.columns
    ]
    return pl.concat(frames, how="vertical")


def _derive_pool(
    valid_trips: pl.DataFrame,
    location_type: LocationType,
    purposes: list[int],
    config: RegistryGateConfig,
) -> pl.DataFrame:
    """Cluster one purpose pool's destinations into observed locations."""
    clustered = (
        valid_trips.filter(pl.col("d_purpose_category").is_in(purposes))
        .with_columns(
            pl.col("d_lat").round(config.cluster_decimals).alias("_cell_lat"),
            pl.col("d_lon").round(config.cluster_decimals).alias("_cell_lon"),
        )
        .group_by(["person_id", "_cell_lat", "_cell_lon"])
        .agg(
            pl.col("day_id").n_unique().alias("n_days"),
            pl.col("d_activity_duration").max().cast(pl.Float64).alias("dwell_minutes"),
            pl.col("d_lat").mean().alias("lat"),
            pl.col("d_lon").mean().alias("lon"),
        )
    )
    gated = clustered.filter(
        (pl.col("dwell_minutes") >= config.min_dwell_minutes)
        & (pl.col("n_days") >= config.min_distinct_days)
    )
    return gated.select(
        pl.col("person_id"),
        pl.lit(location_type.value, dtype=pl.Int64).alias("location_type"),
        pl.lit(None, dtype=pl.Boolean).alias("is_primary"),
        pl.col("lat"),
        pl.col("lon"),
        pl.lit(LocationSource.OBSERVED.value, dtype=pl.Int64).alias("source"),
        pl.col("n_days").cast(pl.Int64),
        pl.col("dwell_minutes"),
    )


def derive_observed_locations(
    linked_trips: pl.DataFrame,
    config: RegistryGateConfig | None = None,
) -> pl.DataFrame:
    """Derive observed work and school locations from travel.

    Groups a person's work (work + work-related) and school (school +
    school-related) destinations by rounded coordinates and keeps each place
    where they spent at least the configured dwell time. The number of distinct
    days each place was visited is recorded but not filtered on.

    Args:
        linked_trips: Linked trips with ``person_id``, ``day_id``, ``d_lat``,
            ``d_lon``, ``d_purpose_category`` and ``d_activity_duration``.
        config: Population rule parameters (defaults if not given).

    Returns:
        Tall table of observed WORK/SCHOOL locations without ``location_num``.
        Each row has ``source = OBSERVED``, ``is_primary = None`` (primacy
        unknown), and populated ``n_days``/``dwell_minutes`` statistics.
    """
    config = config or RegistryGateConfig()

    if "d_activity_duration" not in linked_trips.columns:
        msg = (
            "derive_observed_locations requires the d_activity_duration column "
            "(linked-trip field added in #68)."
        )
        raise ValueError(msg)

    valid_trips = linked_trips.filter(
        pl.col("d_activity_duration").is_not_null()
        & ~pl.col("d_activity_duration").is_in(_DWELL_SENTINELS)
    )
    frames = [
        _derive_pool(valid_trips, location_type, purposes, config)
        for location_type, purposes in _OBSERVED_POOLS
    ]
    return pl.concat(frames, how="vertical")


def _drop_observed_duplicating_reported(
    observed: pl.DataFrame,
    reported: pl.DataFrame,
    cluster_decimals: int,
) -> pl.DataFrame:
    """Drop observed rows in the same cell as a reported location of that kind.

    Guards against re-adding a person's reported workplace/school as an
    "observed" location when a work/school trip ends at or near it.
    """
    reported_cells = reported.select(
        "person_id",
        "location_type",
        pl.col("lat").round(cluster_decimals).alias("_cell_lat"),
        pl.col("lon").round(cluster_decimals).alias("_cell_lon"),
    ).unique()
    return (
        observed.with_columns(
            pl.col("lat").round(cluster_decimals).alias("_cell_lat"),
            pl.col("lon").round(cluster_decimals).alias("_cell_lon"),
        )
        .join(
            reported_cells,
            on=["person_id", "location_type", "_cell_lat", "_cell_lon"],
            how="anti",
        )
        .drop("_cell_lat", "_cell_lon")
    )


def _assign_location_num(registry: pl.DataFrame) -> pl.DataFrame:
    """Number locations within each (person, location_type), primary first.

    Reported locations sort ahead of observed; observed sort by descending
    dwell. The first location of each kind is numbered 1.
    """
    return (
        registry.with_columns(
            (pl.col("source") != LocationSource.REPORTED.value).cast(pl.Int8).alias("_source_order")
        )
        .sort(
            ["person_id", "location_type", "_source_order", "dwell_minutes"],
            descending=[False, False, False, True],
            nulls_last=True,
        )
        .with_columns(
            (pl.int_range(0, pl.len()).over(["person_id", "location_type"]) + 1).alias(
                "location_num"
            )
        )
        .drop("_source_order")
    )


def build_location_registry(
    person_locations: pl.DataFrame,
    linked_trips: pl.DataFrame | None = None,
    config: RegistryGateConfig | None = None,
) -> pl.DataFrame:
    """Build the full person-location registry from reported + observed sources.

    Args:
        person_locations: Wide per-person reported locations (see
            ``build_reported_registry``).
        linked_trips: Optional linked trips for deriving observed work/school
            locations. If omitted, only reported locations are included.
        config: Population rule parameters (defaults if not given).

    Returns:
        Tall registry conforming to ``PersonLocationModel``, ordered by
        ``person_id``, ``location_type``, ``location_num``.
    """
    config = config or RegistryGateConfig()
    reported = build_reported_registry(person_locations)

    if linked_trips is not None and linked_trips.height > 0:
        observed = derive_observed_locations(linked_trips, config)
        observed = _drop_observed_duplicating_reported(observed, reported, config.cluster_decimals)
        registry = pl.concat([reported, observed], how="vertical")
    else:
        registry = reported

    registry = _assign_location_num(registry)

    logger.info(
        "Built person-location registry: %d locations (%d reported, %d observed)",
        registry.height,
        registry.filter(pl.col("source") == LocationSource.REPORTED.value).height,
        registry.filter(pl.col("source") == LocationSource.OBSERVED.value).height,
    )

    return registry.select(_REGISTRY_COLUMNS).sort(["person_id", "location_type", "location_num"])
