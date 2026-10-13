"""One profile's verdict, walked across every table.

Reads the ``survey_complete`` flags the reporting cascade left behind and the
tour fuse's verdict, then walks the hierarchy twice -- **up**, counting from
the tour to the household, and **down**, inheriting from the household to the
trips -- and last decides the joint groupings beside it (see
[`joint_groupings`][processing.completeness.joint_groupings]). The functions
below are in the order they run.
"""

import polars as pl

from .household_day import flag_household_day_usable
from .joint_groupings import flag_joint
from .profiles import UsabilityProfile
from .survey_complete import cascade_complete
from .usable_tours import _flag_tours
from .zone_coverage import _home_zone_ok

# The down walk, as (table, the table it inherits from, the key joining them),
# in order: each row's parent has already inherited its own by the time it runs.
_DOWN_EDGES = (
    ("persons", "households", "hh_id"),
    ("days", "persons", "person_id"),
    ("tours", "days", "day_id"),
    ("linked_trips", "tours", "tour_id"),
    ("unlinked_trips", "tours", "tour_id"),
)

# Tables whose verdict begins as their own ``survey_complete`` and is then only
# ever narrowed by inheriting: the trips have no count of their own to make.
_INHERIT_ONLY_TABLES = ("linked_trips", "unlinked_trips")


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

    Each level is a count over the one below it: a day needs a usable tour (or
    is a genuine no-travel day), a person a usable day, a household-day every
    surveyable member's day, a household a usable household-day.
    """
    _flag_tours(tables, cols)
    _flag_days(tables, cols)
    _flag_persons(tables, cols)
    flag_household_day_usable(tables, cols)
    _flag_households(tables, cols)


def _flag_days(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Stamp the usable flag on days, in place: a coherent household-date and a usable tour.

    A day is usable only within a complete household-day: even a genuine
    no-travel day is only a clean observation when the whole household was
    observed that date. ``hh_day_survey_complete`` already implies the day's own
    ``survey_complete`` (it is an ALL over the members, which includes this one), so
    a profile admitting incomplete household-days falls back to that own
    reporting rather than dropping the term entirely.

    Requires the flag on tours; a tours table present but unflagged raises
    rather than silently passing every day.

    Raises:
        ValueError: If tours is present but has not been flagged yet.
    """
    days = tables.get("days")
    if days is None or "survey_complete" not in days.columns:
        return
    tours = tables.get("tours")
    if tours is not None and cols.flag not in tours.columns:
        msg = (
            f"Cannot flag days: tours has no {cols.flag} column yet. Flag tours "
            "first, otherwise every day silently passes on completeness alone."
        )
        raise ValueError(msg)

    base = (
        pl.col("hh_day_survey_complete").fill_null(value=False)
        if cols.needs_whole_household_day
        else pl.col("survey_complete").fill_null(value=False)
    )

    # A no-travel day has no tour to carry the household's home zone, so it
    # takes the term directly or it would outlive its own household.
    home = _home_zone_ok(tables, cols)
    if home is not None and "hh_id" in days.columns:
        days = days.join(home, on="hh_id", how="left")
        base = base & pl.col("_home_zone_ok").fill_null(value=False)
    else:
        home = None

    if tours is not None and "day_id" in tours.columns:
        day_has_usable = tours.group_by("day_id").agg(
            pl.col(cols.flag).any().alias("_day_has_usable_tour")
        )
        days = days.join(day_has_usable, on="day_id", how="left")
        # null == the day has no tours at all -> a legitimate no-travel day.
        flagged = days.with_columns(
            (base & pl.col("_day_has_usable_tour").fill_null(value=True)).alias(cols.flag)
        ).drop("_day_has_usable_tour")
    else:
        flagged = days.with_columns(base.alias(cols.flag))

    tables["days"] = flagged.drop("_home_zone_ok") if home is not None else flagged


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
    days = tables.get("days")
    if days is not None and cols.flag not in days.columns:
        msg = (
            f"Cannot flag persons: days has no {cols.flag} column yet. Flag days first, "
            "otherwise every person silently passes on completeness alone."
        )
        raise ValueError(msg)
    if days is None or "person_id" not in days.columns:
        tables["persons"] = persons.with_columns(
            pl.col("survey_complete").fill_null(value=False).alias(cols.flag)
        )
        return
    has_usable_day = (
        days.filter(pl.col(cols.flag).fill_null(value=False))
        .select("person_id")
        .unique()
        .with_columns(pl.lit(value=True).alias("_u"))
    )
    tables["persons"] = (
        persons.join(has_usable_day, on="person_id", how="left")
        .with_columns(pl.col("_u").fill_null(value=False).alias(cols.flag))
        .drop("_u")
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
    rather than left holding weight it cannot pass down.

    A profile whose ``household_day_needs`` is ``nothing`` counts usable *days*
    instead. Reading the household-day column here regardless would leave the
    household on the strict rule while its tours and days had already relaxed --
    the profile would look like it worked, and the household would still be
    dropped and its weight zeroed.

    Requires days to carry the profile's household-day column; a days table
    present but unflagged raises rather than silently passing every household.

    Raises:
        ValueError: If days is present but has not been flagged yet.
    """
    households = tables.get("households")
    if households is None or "survey_complete" not in households.columns:
        return

    base = pl.col("survey_complete").fill_null(value=False)
    # The date-level reduction is only the rule while coherence is required.
    counts = cols.household_day if cols.needs_whole_household_day else cols.flag
    days = tables.get("days")
    if days is not None and counts not in days.columns:
        msg = (
            f"Cannot flag households: days has no {counts} column yet. Flag days "
            "first, otherwise every household silently passes."
        )
        raise ValueError(msg)
    if days is None or "hh_id" not in days.columns:
        tables["households"] = households.with_columns(base.alias(cols.flag))
        return

    hh_has_usable_day = (
        days.filter(pl.col(counts).fill_null(value=False))
        .select("hh_id")
        .unique()
        .with_columns(pl.lit(value=True).alias("_hh_has_usable_day"))
    )
    tables["households"] = (
        households.join(hh_has_usable_day, on="hh_id", how="left")
        .with_columns((base & pl.col("_hh_has_usable_day").fill_null(value=False)).alias(cols.flag))
        .drop("_hh_has_usable_day")
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
    walk is the whole of it.
    """
    # The trips have no count of their own; they start from what the survey
    # collected and then inherit like everything else.
    for name in _INHERIT_ONLY_TABLES:
        df = tables.get(name)
        if df is None or "survey_complete" not in df.columns:
            continue
        tables[name] = df.with_columns(
            pl.col("survey_complete").fill_null(value=False).alias(cols.flag)
        )

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
        verdict = parent_df.select(key, pl.col(cols.flag).alias("_parent_usable"))
        tables[table] = (
            df.join(verdict, on=key, how="left")
            .with_columns(
                (pl.col(cols.flag) & pl.col("_parent_usable").fill_null(value=False)).alias(
                    cols.flag
                )
            )
            .drop("_parent_usable")
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
