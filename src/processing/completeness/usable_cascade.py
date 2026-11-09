"""One profile's verdict, walked across every table.

Reads the ``survey_complete`` flags the reporting cascade left behind and the
tour fuse's verdict, then walks the hierarchy twice -- **up**, counting from
the tour to the household, and **down**, inheriting from the household to the
trips -- and last decides the joint groupings beside it (see
[`joint_groupings`][processing.completeness.joint_groupings]). The functions
below are in the order they run.
"""

import polars as pl

from .household_day import (
    flag_household_day_usable,
    with_household_day,
    with_household_day_excluded,
)
from .joint_groupings import flag_joint
from .profiles import UsabilityProfile
from .reduce import and_reduced
from .survey_complete import cascade_complete
from .usable_tours import TRIP_TABLES, _flag_tours
from .zone_coverage import _home_zone_ok

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


def stamp_usable(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Derive one usability verdict for every table, in place.

    Reads the ``survey_complete`` flags :func:`cascade_complete` left behind and writes
    the columns *cols* names. It never writes ``survey_complete``, so it can be
    run more than once over the same tables to stamp several verdicts side by
    side, each under its own name.

    Three passes, in the order the package docstring draws them: count
    **up** from the tour fuse to the household, inherit **down** from the
    household to the trips, then decide the **joint** groupings beside them.

    Args:
        tables: Mutable dict of table_name -> DataFrame (or None).
        cols: Column names this pass writes.
    """
    _cascade_up(tables, cols)
    _cascade_down(tables, cols)
    flag_joint(tables, cols)


# -- Up ------------------------------------------------------------------------


def _cascade_up(tables: dict[str, pl.DataFrame | None], cols: UsabilityProfile) -> None:
    """Count upward from the tour fuse to the household, in place.

    Each level is a count over the one below it: a tour needs every one of its
    trips, a day a usable tour (or is a genuine no-travel day), a person a
    usable day, a household-day every surveyable member's day, a household a
    usable household-day.
    """
    _flag_trips(tables, cols)
    _flag_tours(tables, cols)
    _flag_days(tables, cols)
    _flag_persons(tables, cols)
    flag_household_day_usable(tables, cols)
    _flag_households(tables, cols)


def _flag_trips(tables: dict[str, pl.DataFrame | None], cols: UsabilityProfile) -> None:
    """Start each trip's usable flag from its own ``survey_complete``, in place.

    The trips have no count of their own to make, so this is their whole verdict
    on the way up; the tour fuse then needs every one of them. On the way down
    they only narrow further, by inheriting their tour's.
    """
    for name in TRIP_TABLES:
        df = tables.get(name)
        if df is None or "survey_complete" not in df.columns:
            continue
        tables[name] = df.with_columns(
            pl.col("survey_complete").fill_null(value=False).alias(cols.flag)
        )


def _flag_days(  # noqa: C901, PLR0912 # This needs fixing in next issue
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
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
    if tours is not None and cols.flag not in tours.columns:
        msg = (
            f"Cannot flag days: tours has no {cols.flag} column yet. Flag tours "
            "first, otherwise every day silently passes on completeness alone."
        )
        raise ValueError(msg)

    if cols.needs_whole_household_day:
        person_days = with_household_day(person_days, tables, "survey_complete", "_hh_day_complete")
        base = pl.col("_hh_day_complete")
    else:
        base = pl.col("survey_complete").fill_null(value=False)

    # A day the profile does not take is unusable whatever was reported on it,
    # and so is one whose day of the week is unknown.
    if cols.limits_travel_days:
        if "travel_dow" not in person_days.columns:
            msg = (
                f"usability_profile '{cols.name}' takes only some days of the week, "
                "but person_days carries no travel_dow to tell them apart."
            )
            raise ValueError(msg)
        taken = [day.value for day in cols.travel_days]
        base = base & pl.col("travel_dow").is_in(taken).fill_null(value=False)

    # What the cleaner excluded has no usable day under any profile.
    persons = tables.get("persons")
    if "exclusion_reason" in person_days.columns:
        if cols.needs_whole_household_day:
            person_days = with_household_day_excluded(person_days, persons, "_date_excluded")
            base = base & ~pl.col("_date_excluded")
        else:
            base = base & pl.col("exclusion_reason").is_null()
    person_days = person_days.with_columns(base.alias(cols.flag)).drop(
        "_hh_day_complete", "_date_excluded", strict=False
    )
    not_excluded = pl.col("exclusion_reason").is_null().first()
    if persons is not None and "exclusion_reason" in persons.columns:
        person_days = and_reduced(
            person_days, persons, "person_id", not_excluded, fill=True, into=cols.flag
        )
    households = tables.get("households")
    if households is not None and "exclusion_reason" in households.columns:
        person_days = and_reduced(
            person_days, households, "hh_id", not_excluded, fill=True, into=cols.flag
        )

    # A no-travel day has no tour to carry the household's home zone, so it
    # takes the term directly or it would outlive its own household.
    home = _home_zone_ok(tables, cols)
    if home is not None and "hh_id" in person_days.columns:
        person_days = and_reduced(
            person_days, home, "hh_id", pl.col("_home_zone_ok").first(), fill=False, into=cols.flag
        )

    # A day with no tours at all is a legitimate no-travel day.
    if tours is not None and "person_day_id" in tours.columns:
        person_days = and_reduced(
            person_days, tours, "person_day_id", pl.col(cols.flag).any(), fill=True, into=cols.flag
        )
    tables["person_days"] = person_days


def _flag_persons(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
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
    if person_days is not None and cols.flag not in person_days.columns:
        msg = (
            f"Cannot flag persons: days has no {cols.flag} column yet. Flag days first, "
            "otherwise every person silently passes on completeness alone."
        )
        raise ValueError(msg)
    if person_days is None or "person_id" not in person_days.columns:
        tables["persons"] = persons.with_columns(
            pl.col("survey_complete").fill_null(value=False).alias(cols.flag)
        )
        return
    tables["persons"] = and_reduced(
        persons.drop(cols.flag, strict=False),
        person_days,
        "person_id",
        pl.col(cols.flag).fill_null(value=False).any(),
        fill=False,
        into=cols.flag,
    )


def _flag_households(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
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
    counted_over = "household_days" if cols.needs_whole_household_day else "person_days"
    rows = tables.get(counted_over)
    if rows is not None and cols.flag not in rows.columns:
        msg = (
            f"Cannot flag households: {counted_over} has no {cols.flag} column yet. Flag "
            f"{counted_over} first, otherwise every household silently passes."
        )
        raise ValueError(msg)
    households = households.with_columns(base.alias(cols.flag))
    if rows is None or "hh_id" not in rows.columns:
        tables["households"] = households
        return
    tables["households"] = and_reduced(
        households,
        rows,
        "hh_id",
        pl.col(cols.flag).fill_null(value=False).any(),
        fill=False,
        into=cols.flag,
    )


# -- Down ----------------------------------------------------------------------


def _cascade_down(tables: dict[str, pl.DataFrame | None], cols: UsabilityProfile) -> None:
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
        if df is None or parent_df is None or cols.flag not in df.columns:
            continue
        if (
            cols.flag not in parent_df.columns
            or key not in df.columns
            or key not in parent_df.columns
        ):
            continue
        tables[table] = and_reduced(
            df, parent_df, key, pl.col(cols.flag).first(), fill=False, into=cols.flag
        )


# -- Both halves, for a single profile -----------------------------------------


def compute_usability(
    tables: dict[str, pl.DataFrame | None],
    profile: UsabilityProfile,
) -> None:
    """Stamp ``survey_complete`` and one profile's verdict on every table, in place.

    The two halves in order: reporting completeness once, then one usability
    pass over it. A run stamping several profiles calls :func:`cascade_complete`
    once and :func:`stamp_usable` per profile instead.

    Args:
        tables: Mutable dict of table_name -> DataFrame (or None).
        profile: The standard to apply, named on every axis.
    """
    cascade_complete(tables)
    stamp_usable(tables, profile)
