"""One profile's verdict, walked across every table.

Reads the ``survey_complete`` flags the completeness cascade left behind and
walks the hierarchy twice. **Up** counts from the trips, through the tour fuse
-- the one place a verdict is decided rather than read -- to the household.
**Down** carries the household's verdict back to the trips. The joint groupings
are decided last, beside it (see
[`joint_groupings`][processing.completeness.joint_groupings]).

Zone coverage is here too: whether a record's locations have a zone in the
geography a profile names. That is a fact about where the zone layer stops,
not about how well the record was reported.

The functions below are in the order they run.
"""

import polars as pl

from data_canon.codebook.tours import TourCategory

from .joint_groupings import flag_joint
from .profiles import ADMITTED_QUALITY, NO_ZONE_COVERAGE, TourClosesAt, UsabilityProfile
from .reduce import and_reduced
from .survey_complete import (
    reduce_to_household_days,
    with_household_day,
    with_household_day_excluded,
)

# The trip tables: each starts from its own ``survey_complete``, the tour fuse
# needs all of a tour's trips, and they then inherit their tour's verdict.
_TRIP_TABLES = ("linked_trips", "unlinked_trips")


# The down walk, as (table, the table it inherits from, the key joining them),
# in order: each row's parent has already inherited its own by the time it runs.
_DOWN_EDGES = (
    ("household_days", "households", "hh_id"),
    ("persons", "households", "hh_id"),
    ("person_days", "persons", "person_id"),
    ("tours", "person_days", "person_day_id"),
    ("linked_trips", "tours", "tour_id"),
    ("unlinked_trips", "tours", "tour_id"),
)


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


def stamp_usable(
    tables: dict[str, pl.DataFrame | None],
    profile: UsabilityProfile,
) -> None:
    """Derive one usability verdict for every table, in place.

    Reads the ``survey_complete`` flags :func:`cascade_complete` left behind and writes
    the columns *profile* names. It never writes ``survey_complete``, so it can be
    run more than once over the same tables to stamp several verdicts side by
    side, each under its own name.

    Three passes, in the order the package docstring draws them: count
    **up** from the tour fuse to the household, inherit **down** from the
    household to the trips, then decide the **joint** groupings beside them.

    Args:
        tables: Mutable dict of table_name -> DataFrame (or None).
        profile: The standard to apply, named on every axis; its columns are the ones written.
    """
    _cascade_up(tables, profile)
    _cascade_down(tables, profile)
    flag_joint(tables, profile)


# -- Up ------------------------------------------------------------------------


def _cascade_up(tables: dict[str, pl.DataFrame | None], profile: UsabilityProfile) -> None:
    """Count upward from the tour fuse to the household, in place.

    Each level is a count over the one below it: a tour needs every one of its
    trips, a day a usable tour (or is a genuine no-travel day), a person a
    usable day, a household-day every surveyable member's day, a household a
    usable household-day.
    """
    _flag_trips(tables, profile)
    _flag_tours(tables, profile)
    _flag_days(tables, profile)
    _flag_persons(tables, profile)
    _flag_household_days(tables, profile)
    _flag_households(tables, profile)


def _flag_trips(tables: dict[str, pl.DataFrame | None], profile: UsabilityProfile) -> None:
    """Start each trip's usable flag from its own ``survey_complete``, in place.

    The trips have no count of their own to make, so this is their whole verdict
    on the way up; the tour fuse then needs every one of them. On the way down
    they only narrow further, by inheriting their tour's.
    """
    for name in _TRIP_TABLES:
        df = tables.get(name)
        if df is None or "survey_complete" not in df.columns:
            continue
        tables[name] = df.with_columns(
            pl.col("survey_complete").fill_null(value=False).alias(profile.flag)
        )


def _flag_tours(
    tables: dict[str, pl.DataFrame | None],
    profile: UsabilityProfile,
) -> None:
    """Stamp the usable flag on tours, in place.

    A tour is usable when its structure is admissible, it sits on a coherent
    household-date, every trip on it is usable, and -- where the profile names a
    geography -- every one of its trips can be addressed in it, not merely its
    own endpoints. Without the coherence term CT-RAMP (which reads the tour's
    flag) would keep a tour the weighting has zeroed.
    ``household_days.survey_complete`` is available because the completeness
    cascade runs before this.

    Subtours then take their parent's verdict on top of their own: an at-work
    subtour is travel *within* its parent tour, so keeping one whose parent was
    dropped would leave CT-RAMP an AT_WORK tour hanging off a tour that is not
    in the output, and would strand the parent's ``atWork_freq``.
    """
    tours = tables.get("tours")
    if tours is None or "survey_complete" not in tours.columns:
        return

    usable = _tour_usable_expr(
        has_quality="tour_data_quality" in tours.columns,
        has_category="tour_category" in tours.columns,
        closes_at=profile.tour_closes_at,
    ) & _zone_expr(tours, "tours", profile)
    tours = tours.with_columns(usable.alias(profile.flag))

    # ...and every leg of it, not only its anchor and primary destination. See
    # _trips_with_zones_per_tour for why the endpoints are not enough. A tour
    # with no trips has no leg that could be missing a zone.
    trip_zones = _trips_with_zones_per_tour(tables, profile)
    if trip_zones is not None and "tour_id" in tours.columns:
        tours = and_reduced(
            tours,
            trip_zones,
            "tour_id",
            pl.col("_trip_has_zone").all(),
            fill=True,
            into=profile.flag,
        )

    # ...and every trip on it, so a trip dropped on its own takes its tour with it.
    trips_ok = _trips_usable_per_tour(tables, profile)
    if trips_ok is not None and "tour_id" in tours.columns:
        tours = and_reduced(
            tours, trips_ok, "tour_id", pl.col("_trips_usable").all(), fill=True, into=profile.flag
        )

    # The household's own home has to have a zone too, or the tour belongs to
    # a household the consumer cannot write. See _home_zone_ok on why this is
    # joined in rather than rolled down.
    home = _home_zone_ok(tables, profile)
    if home is not None and "hh_id" in tours.columns:
        tours = and_reduced(
            tours, home, "hh_id", pl.col("_home_zone_ok").first(), fill=False, into=profile.flag
        )

    person_days = tables.get("person_days")
    if (
        profile.needs_whole_household_day
        and person_days is not None
        and "person_day_id" in tours.columns
    ):
        coherence = with_household_day(person_days, tables, "survey_complete", "_hh_day_complete")
        tours = and_reduced(
            tours,
            coherence,
            "person_day_id",
            pl.col("_hh_day_complete").first(),
            fill=False,
            into=profile.flag,
        )

    tables["tours"] = _flag_subtours_from_parent(tours, profile)


def _trips_usable_per_tour(
    tables: dict[str, pl.DataFrame | None],
    profile: UsabilityProfile,
) -> pl.DataFrame | None:
    """Each trip's usable flag so far, keyed by tour, or None when no trips carry one.

    Reads both trip tables: a linked trip and the unlinked trips it was built
    from can be narrowed separately (each carries its own weight), and either
    failing fails the tour.

    Raises:
        ValueError: If a trip table carries ``survey_complete`` but has not been
            flagged yet, which would pass every tour silently.
    """
    per_table = []
    for name in _TRIP_TABLES:
        trips = tables.get(name)
        if trips is None or "tour_id" not in trips.columns:
            continue
        if profile.flag not in trips.columns:
            if "survey_complete" not in trips.columns:
                continue
            msg = (
                f"Cannot flag tours: {name} has no {profile.flag} column yet. Flag the "
                "trips first, otherwise every tour silently passes on its trips."
            )
            raise ValueError(msg)
        per_table.append(
            trips.select(
                "tour_id", pl.col(profile.flag).fill_null(value=False).alias("_trips_usable")
            )
        )
    if not per_table:
        return None
    return pl.concat(per_table)


def _flag_subtours_from_parent(
    tours: pl.DataFrame,
    profile: UsabilityProfile,
) -> pl.DataFrame:
    """Reduce each subtour's usable flag by its parent tour's verdict.

    Primary tours self-reference (``parent_tour_id == tour_id``) and so are
    unaffected. A subtour whose parent is missing entirely is left on its own
    verdict rather than silently dropped -- the parent's absence is a different
    defect, and dropping here would hide it.
    """
    if "parent_tour_id" not in tours.columns or "tour_id" not in tours.columns:
        return tours

    parents = tours.select(
        pl.col("tour_id").alias("parent_tour_id"), pl.col(profile.flag).alias("_parent_usable")
    )
    return and_reduced(
        tours,
        parents,
        "parent_tour_id",
        pl.col("_parent_usable").first(),
        fill=True,
        into=profile.flag,
    )


def _tour_usable_expr(
    *,
    has_quality: bool,
    has_category: bool,
    closes_at: str,
) -> pl.Expr:
    """Tour-level usability for one closure setting.

    A tour qualifies when its (cascaded) reporting is complete and its quality
    code is one the setting admits:

    * ``primary_home`` -- VALID only: a whole round trip back to the home it
      left. The anchor is home for a home-based tour and the workplace for an
      at-work subtour, so one criterion admits both.
    * ``any_home`` -- also ``OTHER_HOME``. That tour leaves from or returns to
      another home of this person's, such as a second home, rather than the
      primary one; the trips are whole and only the anchor differs.
    * ``anywhere`` -- also the two open ends, ``PARTIAL_DAY_SPLIT`` and
      ``PARTIAL_DIARY_EDGE``. The tour stops somewhere unexpected and you want
      the trips anyway.

    ``NO_DESTINATION`` and ``SPATIAL_GAP`` are admitted by no setting: they mark
    missing data rather than an open end.

    The ``tour_category`` term only applies at ``primary_home``. Past that the
    admitted codes include tours that are open by construction, so a category
    term would reject what the quality term beside it admits -- the two columns
    state the same fact about where a tour ends.

    Args:
        has_quality: Whether the frame carries ``tour_data_quality``.
        has_category: Whether the frame carries ``tour_category``.
        closes_at: One of :class:`TourClosesAt`.
    """
    admitted = [q.value for q in ADMITTED_QUALITY[closes_at]]

    # Each term is filled before it is combined. A null descriptor means the
    # structure was never established, which is not an admission; leaving the
    # null to propagate would put a three-valued verdict in a boolean column and
    # make every consumer decide separately what an unknown means.
    structural = pl.lit(value=True)
    if has_quality:
        structural = structural & pl.col("tour_data_quality").is_in(admitted).fill_null(value=False)
    if has_category and closes_at == TourClosesAt.PRIMARY_HOME:
        structural = structural & (
            (pl.col("tour_category") == TourCategory.COMPLETE.value).fill_null(value=False)
        )

    return pl.col("survey_complete").fill_null(value=False) & structural


def _flag_days(
    tables: dict[str, pl.DataFrame | None],
    profile: UsabilityProfile,
) -> None:
    """Stamp the usable flag on days, in place: day of week, household-date, usable tour.

    A day is usable only within a complete household-day: even a genuine
    no-travel day is only a clean observation when the whole household was
    observed that date. The household-day's ``survey_complete`` already implies
    the day's own (it is an ALL over the members, which includes this one), so
    a profile admitting incomplete household-days falls back to that own
    reporting rather than dropping the term entirely.

    A day of the week the profile does not take is unusable, no-travel day or
    not; its tours and trips follow it on the way down, and a household-date
    falling on it cannot be usable either.

    Under every profile a day is unusable when the project cleaner excluded it,
    its person or its household (``exclusion_reason`` on any of the three). A
    profile needing every member's day also treats an excluded day as an
    incomplete household-date, so the other members' days on it fall too.

    Requires the flag on tours; a tours table present but unflagged raises
    rather than silently passing every day.

    Raises:
        ValueError: If tours is present but has not been flagged yet.
    """
    person_days = tables.get("person_days")
    if person_days is None or "survey_complete" not in person_days.columns:
        return
    tours = tables.get("tours")
    if tours is not None and profile.flag not in tours.columns:
        msg = (
            f"Cannot flag days: tours has no {profile.flag} column yet. Flag tours "
            "first, otherwise every day silently passes on completeness alone."
        )
        raise ValueError(msg)

    if profile.needs_whole_household_day:
        person_days = with_household_day(person_days, tables, "survey_complete", "_hh_day_complete")
        base = pl.col("_hh_day_complete")
    else:
        base = pl.col("survey_complete").fill_null(value=False)

    # A day the profile does not take is unusable whatever was reported on it,
    # and so is one whose day of the week is unknown.
    if profile.limits_travel_days:
        if "travel_dow" not in person_days.columns:
            msg = (
                f"usability_profile '{profile.name}' takes only some days of the week, "
                "but person_days carries no travel_dow to tell them apart."
            )
            raise ValueError(msg)
        taken = [day.value for day in profile.travel_days]
        base = base & pl.col("travel_dow").is_in(taken).fill_null(value=False)

    person_days = person_days.with_columns(base.alias(profile.flag)).drop(
        "_hh_day_complete", strict=False
    )
    person_days = _without_excluded(person_days, tables, profile)

    # A no-travel day has no tour to carry the household's home zone, so it
    # takes the term directly or it would outlive its own household.
    home = _home_zone_ok(tables, profile)
    if home is not None and "hh_id" in person_days.columns:
        person_days = and_reduced(
            person_days,
            home,
            "hh_id",
            pl.col("_home_zone_ok").first(),
            fill=False,
            into=profile.flag,
        )

    # A day with no tours at all is a legitimate no-travel day.
    if tours is not None and "person_day_id" in tours.columns:
        person_days = and_reduced(
            person_days,
            tours,
            "person_day_id",
            pl.col(profile.flag).any(),
            fill=True,
            into=profile.flag,
        )
    tables["person_days"] = person_days


def _without_excluded(
    person_days: pl.DataFrame,
    tables: dict[str, pl.DataFrame | None],
    profile: UsabilityProfile,
) -> pl.DataFrame:
    """Return *person_days* with excluded days unusable under every profile.

    A day is excluded when the project cleaner gave it, its person or its
    household an ``exclusion_reason``. A profile needing every member's day
    takes the whole household-date down with an excluded day.
    """
    persons = tables.get("persons")
    if "exclusion_reason" in person_days.columns:
        if profile.needs_whole_household_day:
            person_days = with_household_day_excluded(person_days, persons, "_date_excluded")
            own = ~pl.col("_date_excluded")
        else:
            own = pl.col("exclusion_reason").is_null()
        person_days = person_days.with_columns(
            (pl.col(profile.flag) & own).alias(profile.flag)
        ).drop("_date_excluded", strict=False)
    not_excluded = pl.col("exclusion_reason").is_null().first()
    for table, key in (("persons", "person_id"), ("households", "hh_id")):
        df = tables.get(table)
        if df is not None and "exclusion_reason" in df.columns:
            person_days = and_reduced(
                person_days, df, key, not_excluded, fill=True, into=profile.flag
            )
    return person_days


def _flag_persons(
    tables: dict[str, pl.DataFrame | None],
    profile: UsabilityProfile,
) -> None:
    """Set a person's usable flag = has at least one usable day, in place.

    A person kept with no usable day contributes no travel but stays a real
    person; their days simply fall out.

    Requires the flag on days; a days table present but unflagged raises rather
    than silently passing every person.

    Raises:
        ValueError: If days is present but has not been flagged yet.
    """
    persons = tables.get("persons")
    if persons is None or "survey_complete" not in persons.columns:
        return
    person_days = tables.get("person_days")
    if person_days is not None and profile.flag not in person_days.columns:
        msg = (
            f"Cannot flag persons: days has no {profile.flag} column yet. Flag days first, "
            "otherwise every person silently passes on completeness alone."
        )
        raise ValueError(msg)
    if person_days is None or "person_id" not in person_days.columns:
        tables["persons"] = persons.with_columns(
            pl.col("survey_complete").fill_null(value=False).alias(profile.flag)
        )
        return
    tables["persons"] = and_reduced(
        persons.drop(profile.flag, strict=False),
        person_days,
        "person_id",
        pl.col(profile.flag).fill_null(value=False).any(),
        fill=False,
        into=profile.flag,
    )


def _flag_household_days(
    tables: dict[str, pl.DataFrame | None],
    profile: UsabilityProfile,
) -> None:
    """Stamp ``household_days.<flag>``: ALL surveyable members' days usable, in place.

    The usable-side mirror of the completeness cascade's household-day verdict: a household-day
    is *usable* only when every surveyable member's day is usable under the
    profile, not merely complete. Requires the per-record flag on days.
    """
    person_days = tables.get("person_days")
    if person_days is None or profile.flag not in person_days.columns:
        return
    reduce_to_household_days(tables, pl.col(profile.flag), profile.flag)


def _flag_households(
    tables: dict[str, pl.DataFrame | None],
    profile: UsabilityProfile,
) -> None:
    """Stamp the usable flag on households, in place.

    Strictly, a household is admissible only if it has at least one **usable
    household-day** -- a date on which every member's day is model-usable (the
    usable-side mirror of the complete-day rule). Without one there is no
    coherently usable household pattern to weight, so the household is dropped
    rather than left holding weight it cannot pass down. A household the
    project cleaner excluded (``exclusion_reason``) is unusable under every
    profile.

    A profile whose ``household_day_needs`` is ``nothing`` counts usable *days*
    instead. Reading the household-days here regardless would leave the
    household on the strict rule while its tours and days had already relaxed --
    the profile would look like it worked, and the household would still be
    dropped and its weight zeroed.

    Requires the counted table (household-days, or person-days) to carry the flag; one
    present but unflagged raises rather than silently passing every household.

    Raises:
        ValueError: If the counted table is present but has not been flagged yet.
    """
    households = tables.get("households")
    if households is None or "survey_complete" not in households.columns:
        return

    base = pl.col("survey_complete").fill_null(value=False)
    if "exclusion_reason" in households.columns:
        base = base & pl.col("exclusion_reason").is_null()
    # The profile's household_day_needs axis: whole household-days, or any member's day.
    counted_over = "household_days" if profile.needs_whole_household_day else "person_days"
    rows = tables.get(counted_over)
    if rows is not None and profile.flag not in rows.columns:
        msg = (
            f"Cannot flag households: {counted_over} has no {profile.flag} column yet. Flag "
            f"{counted_over} first, otherwise every household silently passes."
        )
        raise ValueError(msg)
    households = households.with_columns(base.alias(profile.flag))
    if rows is None or "hh_id" not in rows.columns:
        tables["households"] = households
        return
    tables["households"] = and_reduced(
        households,
        rows,
        "hh_id",
        pl.col(profile.flag).fill_null(value=False).any(),
        fill=False,
        into=profile.flag,
    )


# -- Down ----------------------------------------------------------------------


def _cascade_down(tables: dict[str, pl.DataFrame | None], profile: UsabilityProfile) -> None:
    """Inherit downward from the household to the trips, in place.

    The upward counts can leave a person usable inside a household that is
    not: a whole-household-day profile judges the household over dates on which
    *every* member passed, while the person only needs one day of their own. A
    consumer takes nothing from a household it does not take, so the household's
    verdict is the ceiling on everything in it. Each level takes its parent's
    verdict in turn -- person from household, day from person, tour from day,
    trips from tour -- and nothing here can change a verdict above it, so one
    walk is the whole of it. The trips start from the verdict
    :func:`_flag_trips` gave them on the way up.
    """
    for table, parent, key in _DOWN_EDGES:
        df, parent_df = tables.get(table), tables.get(parent)
        if df is None or parent_df is None or profile.flag not in df.columns:
            continue
        if (
            profile.flag not in parent_df.columns
            or key not in df.columns
            or key not in parent_df.columns
        ):
            continue
        tables[table] = and_reduced(
            df, parent_df, key, pl.col(profile.flag).first(), fill=False, into=profile.flag
        )


# -- Zone coverage ---------------------------------------------------------------


def _has_zone(column: str) -> pl.Expr:
    """A location has a zone when the zone join actually placed it."""
    col = pl.col(column)
    return (col.is_not_null() & (col != _MISSING_ZONE)).fill_null(value=False)


def _zone_expr(frame: pl.DataFrame, table: str, profile: UsabilityProfile) -> pl.Expr:
    """Addressability of *table*'s own locations under this profile.

    Constant-true when the profile asks for no coverage, so callers combine it
    unconditionally rather than branching on the axis.

    Raises:
        ValueError: If the profile names a geography this frame does not carry,
            which means the zone step either did not run before the cascade or
            was not configured to build it.
    """
    if not profile.requires_zones:
        return pl.lit(value=True)

    wanted = [f"{prefix}_{profile.zone_coverage}" for prefix in _ZONE_PREFIXES.get(table, ())]
    missing = [column for column in wanted if column not in frame.columns]
    if missing:
        msg = (
            f"usability_profile '{profile.name}' sets zone_coverage: "
            f"'{profile.zone_coverage}', but {table} carries no {', '.join(missing)}. "
            f"Run add_zone_ids before cascade_completeness and declare a "
            f"zone_geography named '{profile.zone_coverage}', or set zone_coverage: "
            f"{NO_ZONE_COVERAGE} to ask nothing of geography."
        )
        raise ValueError(msg)

    expr = pl.lit(value=True)
    for column in wanted:
        expr = expr & _has_zone(column)
    return expr


def _home_zone_ok(
    tables: dict[str, pl.DataFrame | None],
    profile: UsabilityProfile,
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
    if not profile.requires_zones:
        return None

    households = tables.get("households")
    if households is None or "hh_id" not in households.columns:
        return None

    return households.select(
        "hh_id", _zone_expr(households, "households", profile).alias("_home_zone_ok")
    )


def _trips_with_zones_per_tour(
    tables: dict[str, pl.DataFrame | None],
    profile: UsabilityProfile,
) -> pl.DataFrame | None:
    """Whether each linked trip has a zone, keyed by tour, or None.

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
    if not profile.requires_zones:
        return None

    trips = tables.get("linked_trips")
    if trips is None or "tour_id" not in trips.columns:
        return None

    wanted = [f"{prefix}_{profile.zone_coverage}" for prefix in _ZONE_PREFIXES["linked_trips"]]
    if any(column not in trips.columns for column in wanted):
        return None

    has_zone = pl.lit(value=True)
    for column in wanted:
        has_zone = has_zone & _has_zone(column)

    return trips.select("tour_id", has_zone.alias("_trip_has_zone"))
