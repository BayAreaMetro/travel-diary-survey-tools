"""One profile's verdict, walked across every table.

Reads the ``survey_complete`` flags the reporting cascade left behind and the
tour fuse's verdict, then counts and inherits outwards: days, persons,
household-days, households, member trips, and last the joint groupings, which
read the member tables flagged above them.
"""

import polars as pl

from data_canon.codebook.trips import PurposeCategory

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

# Purposes a model never draws jointly, so a tour with one is never on a joint tour.
_NEVER_JOINT_PURPOSES = (
    PurposeCategory.WORK.value,
    PurposeCategory.SCHOOL.value,
    PurposeCategory.ESCORT.value,
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

        # The profile's own copy of the id, where decided, is the stricter count:
        # a usable tour that left its group for a leg is not on it.
        own = cols.joint_tour_id if key == "joint_tour_id" else cols.joint_trip_id
        on_it = pl.col(own).is_not_null() if own in members.columns else pl.col(cols.flag)
        usable_members = (
            members.filter(pl.col(key).is_not_null() & on_it)
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


def _flag_joint_ids(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Stamp ``joint_tour_id_<profile>`` and ``joint_trip_id_<profile>``, in place.

    ``joint_tour_id`` and ``joint_trip_id`` record the groups the survey
    detected, once, for every profile. Whether a tour is *on* its joint tour is
    a profile's call, so each profile gets its own copy of the id: the same
    value where the tour is on it, null where it is not. Nothing edits the bare
    id.

    Under ``partial`` a usable tour in a group is on it. Under ``fully_joint``
    the model draws one set of trips for everyone on the tour, so a tour is on
    it only if it is usable, not work, school or escort, and made the same
    joint trips as the others: tours are grouped by the joint trips their legs
    belong to, and the largest such class is the joint tour. A group then needs
    two tours and one purpose, or it is no group and everyone on it travels
    alone. A linked trip is on its joint trip only while its tour is on its
    joint tour, and both trip tables carry the tour's answer too.

    Requires the usable flag on tours and linked trips; a linked trips table
    present but unflagged raises rather than silently passing every leg.

    Raises:
        ValueError: If linked trips are present but not yet flagged.
    """
    tours = tables.get("tours")
    trips = tables.get("linked_trips")
    if tours is None or cols.flag not in tours.columns:
        return
    tour_col = cols.joint_tour_id
    usable = pl.col(cols.flag).fill_null(value=False)

    if "joint_tour_id" not in tours.columns:
        tables["tours"] = tours.with_columns(pl.lit(None, dtype=pl.Int64).alias(tour_col))
    elif not cols.requires_fully_joint:
        tables["tours"] = tours.with_columns(
            pl.when(usable).then(pl.col("joint_tour_id")).otherwise(None).alias(tour_col)
        )
    else:
        if trips is not None and cols.flag not in trips.columns:
            msg = (
                f"Cannot decide joint tours: linked_trips has no {cols.flag} column "
                "yet. Flag linked trips first, otherwise every leg silently counts."
            )
            raise ValueError(msg)
        tables["tours"] = _fully_joint_tours(tours, trips, cols)

    # Both trip tables carry their tour's answer.
    on_tour = tables["tours"].select("tour_id", pl.col(tour_col).alias("_on_tour"))
    for name in _TOUR_MEMBER_TABLES:
        df = tables.get(name)
        if df is None or cols.flag not in df.columns:
            continue
        if "tour_id" in df.columns:
            df = df.join(on_tour, on="tour_id", how="left").rename({"_on_tour": tour_col})
        else:
            df = df.with_columns(pl.lit(None, dtype=pl.Int64).alias(tour_col))
        tables[name] = df

    _flag_joint_trip_ids(tables, cols)


def _flag_joint_trip_ids(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Stamp ``joint_trip_id_<profile>`` on linked trips, in place.

    A usable trip in a joint trip is on it; under ``fully_joint`` only while its
    tour is on its joint tour, which ``joint_tour_id_<profile>`` already says.
    """
    trips = tables.get("linked_trips")
    if trips is None or cols.flag not in trips.columns:
        return
    trip_col = cols.joint_trip_id
    if "joint_trip_id" not in trips.columns:
        tables["linked_trips"] = trips.with_columns(pl.lit(None, dtype=pl.Int64).alias(trip_col))
        return
    on_trip = pl.col(cols.flag).fill_null(value=False)
    if cols.requires_fully_joint and cols.joint_tour_id in trips.columns:
        on_trip = on_trip & pl.col(cols.joint_tour_id).is_not_null()
    tables["linked_trips"] = trips.with_columns(
        pl.when(on_trip).then(pl.col("joint_trip_id")).otherwise(None).alias(trip_col)
    )


def _fully_joint_tours(
    tours: pl.DataFrame,
    trips: pl.DataFrame | None,
    cols: UsabilityProfile,
) -> pl.DataFrame:
    """Return *tours* with ``joint_tour_id_<profile>`` under the fully-joint rule.

    Tours are matched by their **leg signature**: the joint trips a tour's
    usable legs belong to, in order. Tours that made identical trips have the
    same signature, and that is what the model means by a joint tour. Within a
    survey group, the largest class of identical signatures is the joint tour
    (ties go to the class holding the lowest tour id, so the answer is stable);
    everyone outside it travels alone. A tour with any leg that is not a joint
    trip went somewhere alone and is in no class.

    Without a linked trips table there are no legs to compare, so the answer
    stops at the tour's own standing: usable, in a group, and never work,
    school or escort.
    """
    tour_col = cols.joint_tour_id
    on_tour = pl.col(cols.flag).fill_null(value=False) & pl.col("joint_tour_id").is_not_null()
    if "tour_purpose" in tours.columns:
        on_tour = on_tour & ~pl.col("tour_purpose").is_in(_NEVER_JOINT_PURPOSES)

    if trips is not None and {"tour_id", "joint_trip_id"} <= set(trips.columns):
        signatures = (
            trips.filter(pl.col(cols.flag).fill_null(value=False))
            .group_by("tour_id")
            .agg(
                pl.col("joint_trip_id").is_null().any().alias("_solo_leg"),
                pl.col("joint_trip_id").sort().alias("_signature"),
            )
        )
        classes = (
            tours.filter(on_tour)
            .select("tour_id", "joint_tour_id")
            .join(signatures, on="tour_id", how="inner")
            .filter(~pl.col("_solo_leg"))
            .group_by("joint_tour_id", "_signature")
            .agg(pl.col("tour_id").alias("_tours"), pl.len().alias("_n_tours"))
            .with_columns(pl.col("_tours").list.min().alias("_first"))
            .sort("joint_tour_id", "_n_tours", "_first", descending=[False, True, False])
            .unique("joint_tour_id", keep="first", maintain_order=True)
            .filter(pl.col("_n_tours") >= MIN_JOINT_PARTICIPANTS)
        )
        in_class = classes.select(pl.col("_tours").explode().alias("tour_id"))["tour_id"]
        on_tour = on_tour & pl.col("tour_id").is_in(in_class.implode())

    tours = tours.with_columns(
        pl.when(on_tour).then(pl.col("joint_tour_id")).otherwise(None).alias(tour_col)
    )

    # No group of one, and no group of mixed purpose.
    aggs = [pl.len().alias("_n_tours")]
    if "tour_purpose" in tours.columns:
        aggs.append(pl.col("tour_purpose").n_unique().alias("_n_purposes"))
    groups = tours.filter(pl.col(tour_col).is_not_null()).group_by(tour_col).agg(aggs)
    dissolve = pl.col("_n_tours") < MIN_JOINT_PARTICIPANTS
    if "_n_purposes" in groups.columns:
        dissolve = dissolve | (pl.col("_n_purposes") > 1)
    dissolved = groups.filter(dissolve)[tour_col]
    return tours.with_columns(
        pl.when(pl.col(tour_col).is_in(dissolved.implode()))
        .then(None)
        .otherwise(pl.col(tour_col))
        .alias(tour_col)
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


def _carry_verdict_down(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Make every record unusable whose household, person or day is, in place.

    The upward counts above can leave a person usable inside a household that is
    not: a whole-household-day profile judges the household over dates on which
    *every* member passed, while the person only needs one day of their own. A
    consumer takes nothing from a household it does not take, so the household's
    verdict is the ceiling on everything in it. Walked in order, so a day
    inherits a person that already inherited its household.
    """
    for table, parent, key in (
        ("persons", "households", "hh_id"),
        ("days", "persons", "person_id"),
        ("tours", "days", "day_id"),
    ):
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

    # -- Down: nothing is usable inside a household that is not ---------------
    _carry_verdict_down(tables, cols)
    tours = tables.get("tours")

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

    # -- Which joint tour and joint trip each record is on, for this profile ---
    _flag_joint_ids(tables, cols)

    # -- Joint groupings (need two tours or trips on them to still be joint) --
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
