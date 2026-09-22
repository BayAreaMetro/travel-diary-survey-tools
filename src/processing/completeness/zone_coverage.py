"""Whether a record's locations have a zone in the geography a profile names.

The zone step joins each location to a zone polygon and leaves it null when no
polygon contains it. This asks the one question that follows: does every
location this record needs have a zone in the named geography?

That is a fact about where the zone layer stops, not about how well the record
was reported. A trip to a place outside the layer may be reported perfectly;
a consumer that writes zone IDs simply has nothing to write for it.
"""

import polars as pl

from .profiles import NO_ZONE_COVERAGE, UsabilityProfile

# Zone columns the zone step writes, by table. A record has a zone when
# every one of its locations landed in the named geography.
_ZONE_PREFIXES: dict[str, tuple[str, ...]] = {
    "households": ("home",),
    "tours": ("o", "d"),
    "linked_trips": ("o", "d"),
    "unlinked_trips": ("o", "d"),
}

# The zone step leaves an unmatched location null; formatters have historically
# also written -1 as a missing sentinel, so neither counts as a zone.
_MISSING_ZONE = -1


def _has_zone(column: str) -> pl.Expr:
    """A location has a zone when the zone join actually placed it."""
    col = pl.col(column)
    return (col.is_not_null() & (col != _MISSING_ZONE)).fill_null(value=False)


def _zone_expr(frame: pl.DataFrame, table: str, cols: UsabilityProfile) -> pl.Expr:
    """Addressability of *table*'s own locations under this profile.

    Constant-true when the profile asks for no coverage, so callers combine it
    unconditionally rather than branching on the axis.

    Raises:
        ValueError: If the profile names a geography this frame does not carry,
            which means the zone step either did not run before the cascade or
            was not configured to build it.
    """
    if not cols.requires_zones:
        return pl.lit(value=True)

    wanted = [f"{prefix}_{cols.zone_coverage}" for prefix in _ZONE_PREFIXES.get(table, ())]
    missing = [column for column in wanted if column not in frame.columns]
    if missing:
        msg = (
            f"usability_profile '{cols.name}' sets zone_coverage: "
            f"'{cols.zone_coverage}', but {table} carries no {', '.join(missing)}. "
            f"Run add_zone_ids before cascade_completeness and declare a "
            f"zone_geography named '{cols.zone_coverage}', or set zone_coverage: "
            f"{NO_ZONE_COVERAGE} to ask nothing of geography."
        )
        raise ValueError(msg)

    expr = pl.lit(value=True)
    for column in wanted:
        expr = expr & _has_zone(column)
    return expr


def _home_zone_ok(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> pl.DataFrame | None:
    """Whether each household's home has a zone, or None when unasked.

    The cascade reduces *upward*, so a household-level fact cannot reach its
    descendants by rolling up. It is joined into the tour and day verdicts
    instead, which leaves the cascade's direction alone: with every tour and
    every day of a household whose home has none failing, the person and household
    verdicts follow on their own. Days need the term as well as tours, because a
    genuine no-travel day passes the has-a-usable-tour test by design and would
    otherwise survive in a household that cannot be written at all.
    """
    if not cols.requires_zones:
        return None

    households = tables.get("households")
    if households is None or "hh_id" not in households.columns:
        return None

    return households.select(
        "hh_id", _zone_expr(households, "households", cols).alias("_home_zone_ok")
    )


def _trips_with_zones_per_tour(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> pl.DataFrame | None:
    """Per tour, whether every one of its trips has a zone, or None.

    A tour's own ``o``/``d`` are its anchor and its primary destination, so they
    say nothing about the legs between: a tour can start and end in the model
    area while one intermediate stop falls outside it. CT-RAMP needs a zone for
    every trip it writes, so one leg without a zone makes the whole tour
    unusable -- the same shape as ``survey_complete``, which is an ALL over the tour's
    trips rather than a property of its endpoints.

    Returns None when the profile asks nothing of geography, or when the trips
    are absent or carry no zone columns. That last case is a judgement this
    cannot make rather than one it should guess: a caller supplying tours without
    their trips is not asserting that every leg has one.
    """
    if not cols.requires_zones:
        return None

    trips = tables.get("linked_trips")
    if trips is None or "tour_id" not in trips.columns:
        return None

    wanted = [f"{prefix}_{cols.zone_coverage}" for prefix in _ZONE_PREFIXES["linked_trips"]]
    if any(column not in trips.columns for column in wanted):
        return None

    has_zone = pl.lit(value=True)
    for column in wanted:
        has_zone = has_zone & _has_zone(column)

    return (
        trips.select("tour_id", has_zone.alias("_trip_has_zone"))
        .group_by("tour_id")
        .agg(pl.all("_trip_has_zone"))
    )
