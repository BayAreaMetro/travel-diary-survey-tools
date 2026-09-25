"""One profile's verdict, walked across every table.

Reads the ``survey_complete`` flags the reporting cascade left behind and the
tour fuse's verdict, then counts and inherits outwards: days, persons,
household-days, households, member trips, and last the joint groupings, which
read the member tables flagged above them.
"""

import polars as pl

from .household_day import flag_household_day_usable
from .profiles import UsabilityProfile
from .survey_complete import cascade_complete
from .usable_tours import _flag_tours
from .zone_coverage import _home_zone_ok

# Trip tables whose model-usability follows the tour they belong to (tour_id).
_TOUR_MEMBER_TABLES = ("unlinked_trips", "linked_trips")

# A joint entity is only joint while two of its members survive.
MIN_JOINT_PARTICIPANTS = 2

# Joint groupings and the member table they are formed from:
# (joint_table, member_table, shared key).
_JOINT_GROUPINGS = (
    ("joint_trips", "linked_trips", "joint_trip_id"),
    ("joint_tours", "tours", "joint_tour_id"),
)


def _flag_person_usable(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Set a person's usable flag = has at least one usable day, in place.

    Requires the flag on days; a days table present but unflagged raises rather
    than silently passing every person.
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


def _flag_joint_groupings(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Stamp the usable flag on the joint trip / joint tour tables, in place.

    A joint entity is a grouping, so it is usable only while it is still *joint*:
    at least :data:`MIN_JOINT_PARTICIPANTS` of its member records must themselves
    be model-usable. This is what stops a joint trip surviving after the tour it
    belonged to was dropped, and what stops a joint tour reduced to a single
    participant reaching CT-RAMP, where it would violate ``num_participants >= 2``.

    A member table that was never supplied is a legitimate partial call and the
    grouping falls back to its own ``survey_complete``. A member table that *is* present
    but carries no verdict column is a caller ordering error and raises: the
    fallback would silently pass every grouping, which is exactly the behaviour
    this rule exists to replace.

    Raises:
        ValueError: If a member table is present but has not been flagged yet.
    """
    for joint_table, member_table, key in _JOINT_GROUPINGS:
        df = tables.get(joint_table)
        if df is None or "survey_complete" not in df.columns:
            continue

        base = pl.col("survey_complete").fill_null(value=False)
        members = tables.get(member_table)

        if members is not None and cols.flag not in members.columns:
            msg = (
                f"Cannot flag {joint_table}: {member_table} has no {cols.flag} column yet. "
                f"Flag the member table before the groupings that count it, otherwise "
                f"every {joint_table} record silently passes."
            )
            raise ValueError(msg)

        if members is None or key not in members.columns or key not in df.columns:
            tables[joint_table] = df.with_columns(base.alias(cols.flag))
            continue

        usable_members = (
            members.filter(pl.col(key).is_not_null() & pl.col(cols.flag))
            .group_by(key)
            .agg(pl.len().alias("_n_usable_members"))
        )
        tables[joint_table] = (
            df.join(usable_members, on=key, how="left")
            .with_columns(
                (base & (pl.col("_n_usable_members").fill_null(0) >= MIN_JOINT_PARTICIPANTS)).alias(
                    cols.flag
                )
            )
            .drop("_n_usable_members")
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


def stamp_usable(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Derive one usability verdict for every table, in place.

    Reads the ``survey_complete`` flags :func:`cascade_complete` left behind and writes
    the pair of columns *cols* names. It never writes ``survey_complete``, so it can be
    run more than once over the same tables to stamp several verdicts side by
    side, each under its own name.

    See the module docstring for the derivation-order diagram and the per-level
    rule table.

    Args:
        tables: Mutable dict of table_name -> DataFrame (or None).
        cols: Column names this pass writes.
    """
    # -- Tours ---------------------------------------------------------------
    _flag_tours(tables, cols)
    tours = tables.get("tours")

    # -- Days (needs a coherent household-date AND a usable tour) -------------
    # A day is usable only within a complete household-day: even a genuine
    # no-travel day is only a clean observation when the whole household was
    # observed that date. ``hh_day_survey_complete`` already implies the day's own
    # ``survey_complete`` (it is an ALL over the members, which includes this one), so
    # a profile admitting incomplete household-days falls back to that own
    # reporting rather than dropping the term entirely.
    days = tables.get("days")
    if days is not None and "survey_complete" in days.columns:
        base = (
            pl.col("survey_complete").fill_null(value=False)
            if not cols.needs_whole_household_day
            else pl.col("hh_day_survey_complete").fill_null(value=False)
        )
        if tours is not None and cols.flag not in tours.columns:
            msg = (
                f"Cannot flag days: tours has no {cols.flag} column yet. Flag tours "
                "first, otherwise every day silently passes on completeness alone."
            )
            raise ValueError(msg)

        # A no-travel day has no tour to carry the household's home zone, so
        # it takes the term directly or it would outlive its own household.
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

    # -- Persons: usable = has >=1 usable day (mirror of complete) ------------
    # A person kept with no usable day contributes no travel but stays a real
    # person; their days simply fall out.
    _flag_person_usable(tables, cols)

    # -- Household-day usability (lateral: ALL member-days usable) then
    #    household = >=1 usable household-day.
    flag_household_day_usable(tables, cols)
    _flag_households(tables, cols)

    # -- Member trips follow their tour --------------------------------------
    tour_usable = None
    if tours is not None and cols.flag in tours.columns:
        tour_usable = tours.select("tour_id", pl.col(cols.flag).alias("_tour_usable"))
    for name in _TOUR_MEMBER_TABLES:
        df = tables.get(name)
        if df is None or "survey_complete" not in df.columns:
            continue
        base = pl.col("survey_complete").fill_null(value=False)
        if tour_usable is not None and "tour_id" in df.columns:
            df = df.join(tour_usable, on="tour_id", how="left")
            tables[name] = df.with_columns(
                (base & pl.col("_tour_usable").fill_null(value=False)).alias(cols.flag)
            ).drop("_tour_usable")
        else:
            tables[name] = df.with_columns(base.alias(cols.flag))

    # -- Joint groupings (need two surviving members to still be joint) -------
    # Last: they read the member tables flagged above.
    _flag_joint_groupings(tables, cols)


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
        profile: The standard to apply, named on both axes.
    """
    cascade_complete(tables)
    stamp_usable(tables, profile)
