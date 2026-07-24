"""Canonical completeness and model-usability logic.

Two flags roll **up** from two atomic facts -- whether each **trip** was
surveyed, and each tour's structural validity:

* ``complete`` -- was it reported? Rolls up from surveyed trips.
* ``model_usable`` -- can the model consume it? Fuses each tour's structure with
  its household-day coherence, then gates the rest. This is the single gate the
  CT-RAMP/DaySim drop and the weighting read.

Each flag comes from one place in the tree -- its **direction** -- via a counting
**operation** (the ``op`` column below). Naming the direction makes it clear that
``household-day`` and the joint entities are the same kind of thing: cross-person
groupings, not parent/child.

| direction | value comes from               | example                               |
|-----------|--------------------------------|---------------------------------------|
| self      | measured on the record         | a trip never filled out               |
| up        | aggregate your own children    | person with zero complete days        |
| down      | inherit your parent's verdict  | good trip on a dropped tour           |
| lateral   | aggregate a cross-person group | one member skips a day, whole date fails |


The flow, and the rule on each line:

```text
COMPLETE -- rolls up from surveyed trips     op        rule
------------------------------------------------------------------------
trip .................................   direct    the trip was surveyed
 └ person-day ........................   ALL       all its trips complete, or no-travel
    ├ person .........................   >=1       has >=1 complete day
    └ household-day ..................   ALL       all members complete that date
       └ household ...................   >=1       has >=1 complete household-day

USABLE -- rolls down from the tour fuse      op        rule
------------------------------------------------------------------------
household ...........................   >=1       has >=1 usable household-day
 ├ household-day ....................   ALL       all members' days usable that date
 └ person ...........................   >=1       has >=1 usable day
    └ day ...........................   >=1       >=1 usable tour (or a no-travel day)
       └ tour .......................   fuse      complete AND VALID AND hh-day complete
          ├ linked trip .............   inherit   takes its tour's verdict
          |  └ unlinked trip ........   inherit   takes its linked trip's verdict
          ├ joint tour ..............   >=2       >=2 usable member tours
          └ joint trip ..............   >=2       >=2 usable member linked trips

VALID feeds the fuse: trips -> linked trips -> tour, home-to-home, no missing legs.
persons: >=1 complete day; unrelated / unsurveyable persons are counted but have none.
op: ALL / >=1 / >=2 = quantity gate (count members vs threshold);
    direct = measured; inherit = take a neighbour's verdict; fuse = AND of conditions
```

Because each level reads its neighbours, the derivation order is load-bearing;
getting it wrong fails loudly (an unflagged member table raises), never silently.

This module is the one place the logic lives. :func:`compute_model_usable` is
applied once by the ``flag_model_usable`` pipeline step; every downstream
consumer only *reads* the resulting flags.
"""

import logging

import polars as pl

from data_canon.codebook.tours import TourCategory, TourDataQuality
from pipeline.decoration import step

logger = logging.getLogger(__name__)

# Completeness cascade: (parent_table, child_table, join_key). A child is
# effectively complete only if it and its parent are complete; incompleteness
# flows household -> person -> day -> trips/tours.
COMPLETENESS_CASCADE = [
    ("households", "persons", "hh_id"),
    ("persons", "days", "person_id"),
    ("days", "unlinked_trips", "day_id"),
    ("days", "linked_trips", "day_id"),
    ("days", "joint_trips", "day_id"),
    ("days", "tours", "day_id"),
    ("days", "joint_tours", "day_id"),
]

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


def cascade_completeness(tables: dict[str, pl.DataFrame | None]) -> None:
    """Flag reporting completeness from households down through the hierarchy, in place.

    A record is effectively complete only if it *and* every ancestor is
    complete. Incompleteness flows downward: an incomplete household forces all
    its persons, days, trips and tours incomplete; an incomplete person forces
    its days/trips/tours; an incomplete day forces its trips/tours.

    Each child table's ``complete`` column is overwritten with
    ``own_complete AND parent_complete``. A null own flag is treated as
    incomplete; a missing parent (orphan) does not force incompleteness. Tables
    missing the ``complete`` column or the join key are left unchanged. Because
    the cascade runs parent-before-child, each parent is already effective when
    its children are processed. Idempotent: re-running never changes a result.
    """
    for parent, child, key in COMPLETENESS_CASCADE:
        parent_df = tables.get(parent)
        child_df = tables.get(child)
        if parent_df is None or child_df is None:
            continue
        if (
            "complete" not in parent_df.columns
            or "complete" not in child_df.columns
            or key not in child_df.columns
        ):
            continue

        parent_flag = parent_df.select(
            key, pl.col("complete").fill_null(value=False).alias("_parent_complete")
        )
        child_df = child_df.join(parent_flag, on=key, how="left")
        tables[child] = child_df.with_columns(
            (
                pl.col("complete").fill_null(value=False)
                & pl.col("_parent_complete").fill_null(value=True)
            ).alias("complete")
        ).drop("_parent_complete")


def flag_household_day_complete(tables: dict[str, pl.DataFrame | None]) -> None:
    """Stamp ``hh_day_complete`` on the days table, in place (reverse cascade).

    A **household-day** -- the set of person-days sharing one ``hh_id`` and
    ``travel_date`` -- is complete only when *every* member's day is complete
    (an ALL reduction over member-days). The result is written back onto each day
    on that date, so ``hh_day_complete`` marks whether the day belongs to a
    coherently observed household-date.

    This runs after :func:`cascade_completeness`, so ``complete`` already
    reflects ancestry; the reduction then flows the other way, up from members to
    the shared date. Idempotent. Days without ``hh_id`` / ``travel_date`` (e.g.
    schema-only fixtures) fall back to each day's own ``complete``.
    """
    days = tables.get("days")
    if days is None or "complete" not in days.columns:
        return

    own = pl.col("complete").fill_null(value=False)
    if "hh_id" not in days.columns or "travel_date" not in days.columns:
        tables["days"] = days.with_columns(own.alias("hh_day_complete"))
        return

    household_day = days.group_by("hh_id", "travel_date").agg(own.all().alias("_hh_day_complete"))
    tables["days"] = (
        days.join(household_day, on=["hh_id", "travel_date"], how="left")
        .with_columns(pl.col("_hh_day_complete").fill_null(value=False).alias("hh_day_complete"))
        .drop("_hh_day_complete")
    )


def _flag_tours(tables: dict[str, pl.DataFrame | None], *, require_valid_tours: bool) -> None:
    """Stamp ``model_usable`` on tours, in place.

    A tour is usable when its structure is admissible *and* it sits on a coherent
    household-date, so it agrees with its own day. Without the coherence term
    CT-RAMP (which reads ``tour.model_usable``) would keep a tour the weighting
    has zeroed. ``hh_day_complete`` is available because the reverse cascade runs
    before this.
    """
    tours = tables.get("tours")
    if tours is None or "complete" not in tours.columns:
        return

    usable = _tour_model_usable_expr(
        require_valid_tours=require_valid_tours,
        has_quality="tour_data_quality" in tours.columns,
        has_category="tour_category" in tours.columns,
    )
    days = tables.get("days")
    if days is None or "hh_day_complete" not in days.columns or "day_id" not in tours.columns:
        tables["tours"] = tours.with_columns(usable.alias("model_usable"))
        return

    coherence = days.select("day_id", pl.col("hh_day_complete").alias("_hh_day_complete")).unique(
        subset="day_id"
    )
    tables["tours"] = (
        tours.join(coherence, on="day_id", how="left")
        .with_columns(
            (usable & pl.col("_hh_day_complete").fill_null(value=False)).alias("model_usable")
        )
        .drop("_hh_day_complete")
    )


def _tour_model_usable_expr(
    *, require_valid_tours: bool, has_quality: bool, has_category: bool
) -> pl.Expr:
    """model_usable expression for the tours table.

    A tour is model-usable when its (cascaded) reporting is complete and, if
    ``require_valid_tours``, its structure is admissible: VALID quality (not
    single-trip, loop, missing-anchor, change-mode, spatial-gap, indeterminate)
    AND COMPLETE category (starts and ends at home). This matches the CT-RAMP /
    DaySim drop criterion exactly.
    """
    usable = pl.col("complete").fill_null(value=False)
    if require_valid_tours and has_quality:
        usable = usable & (pl.col("tour_data_quality") == TourDataQuality.VALID.value)
    if require_valid_tours and has_category:
        usable = usable & (pl.col("tour_category") == TourCategory.COMPLETE.value)
    return usable


def _flag_joint_groupings(tables: dict[str, pl.DataFrame | None]) -> None:
    """Stamp ``model_usable`` on the joint trip / joint tour tables, in place.

    A joint entity is a grouping, so it is usable only while it is still *joint*:
    at least :data:`MIN_JOINT_PARTICIPANTS` of its member records must themselves
    be model-usable. This is what stops a joint trip surviving after the tour it
    belonged to was dropped, and what stops a joint tour reduced to a single
    participant reaching CT-RAMP, where it would violate ``num_participants >= 2``.

    A member table that was never supplied is a legitimate partial call and the
    grouping falls back to its own ``complete``. A member table that *is* present
    but carries no ``model_usable`` is a caller ordering error and raises: the
    fallback would silently pass every grouping, which is exactly the behaviour
    this rule exists to replace.

    Raises:
        ValueError: If a member table is present but has not been flagged yet.
    """
    for joint_table, member_table, key in _JOINT_GROUPINGS:
        df = tables.get(joint_table)
        if df is None or "complete" not in df.columns:
            continue

        base = pl.col("complete").fill_null(value=False)
        members = tables.get(member_table)

        if members is not None and "model_usable" not in members.columns:
            msg = (
                f"Cannot flag {joint_table}: {member_table} has no model_usable column yet. "
                f"Flag the member table before the groupings that count it, otherwise "
                f"every {joint_table} record silently passes."
            )
            raise ValueError(msg)

        if members is None or key not in members.columns or key not in df.columns:
            tables[joint_table] = df.with_columns(base.alias("model_usable"))
            continue

        usable_members = (
            members.filter(pl.col(key).is_not_null() & pl.col("model_usable"))
            .group_by(key)
            .agg(pl.len().alias("_n_usable_members"))
        )
        tables[joint_table] = (
            df.join(usable_members, on=key, how="left")
            .with_columns(
                (base & (pl.col("_n_usable_members").fill_null(0) >= MIN_JOINT_PARTICIPANTS)).alias(
                    "model_usable"
                )
            )
            .drop("_n_usable_members")
        )


def _flag_households(tables: dict[str, pl.DataFrame | None]) -> None:
    """Stamp ``model_usable`` on households, in place.

    A household is admissible only if it has at least one **complete
    household-day** -- a date on which every member reported a complete day.
    Without one there is no coherently observed household pattern to weight, so
    the household is dropped rather than left holding weight it cannot pass down.

    Requires days to carry ``hh_day_complete`` (from
    :func:`flag_household_day_complete`); a days table present but unflagged
    raises rather than silently passing every household.

    Raises:
        ValueError: If days is present but has no ``hh_day_complete`` column.
    """
    households = tables.get("households")
    if households is None or "complete" not in households.columns:
        return

    base = pl.col("complete").fill_null(value=False)
    days = tables.get("days")
    if days is not None and "hh_day_complete" not in days.columns:
        msg = (
            "Cannot flag households: days has no hh_day_complete column yet. Run "
            "flag_household_day_complete first, otherwise every household silently passes."
        )
        raise ValueError(msg)
    if days is None or "hh_id" not in days.columns:
        tables["households"] = households.with_columns(base.alias("model_usable"))
        return

    hh_has_complete_day = (
        days.filter(pl.col("hh_day_complete").fill_null(value=False))
        .select("hh_id")
        .unique()
        .with_columns(pl.lit(value=True).alias("_hh_has_complete_day"))
    )
    tables["households"] = (
        households.join(hh_has_complete_day, on="hh_id", how="left")
        .with_columns(
            (base & pl.col("_hh_has_complete_day").fill_null(value=False)).alias("model_usable")
        )
        .drop("_hh_has_complete_day")
    )


def compute_model_usable(
    tables: dict[str, pl.DataFrame | None],
    *,
    require_valid_tours: bool = True,
) -> None:
    """Stamp the ``model_usable`` gate on every table, in place.

    Runs the two completeness cascades then derives ``model_usable`` per level.
    See the module docstring for the flag definitions, the derivation-order
    diagram, and the per-level rule table.

    Args:
        tables: Mutable dict of table_name -> DataFrame (or None).
        require_valid_tours: If True (default), fold tour structural validity
            into the gate. If False, ``model_usable`` reduces to reporting
            completeness plus household-day coherence (invalid tours stay
            usable).
    """
    cascade_completeness(tables)

    # -- Household-day coherence (reverse cascade: member-days -> the date) ---
    flag_household_day_complete(tables)

    # -- Tours ---------------------------------------------------------------
    _flag_tours(tables, require_valid_tours=require_valid_tours)
    tours = tables.get("tours")

    # -- Days (needs a coherent household-date AND a usable tour) -------------
    # A day is usable only within a complete household-day: even a genuine
    # no-travel day is only a clean observation when the whole household was
    # observed that date. ``hh_day_complete`` already implies the day's own
    # ``complete`` (it is an ALL over the members, which includes this one).
    days = tables.get("days")
    if days is not None and "complete" in days.columns:
        base = pl.col("hh_day_complete").fill_null(value=False)
        if tours is not None and "model_usable" not in tours.columns:
            msg = (
                "Cannot flag days: tours has no model_usable column yet. Flag tours "
                "first, otherwise every day silently passes on completeness alone."
            )
            raise ValueError(msg)
        if tours is not None and "day_id" in tours.columns:
            day_has_usable = tours.group_by("day_id").agg(
                pl.col("model_usable").any().alias("_day_has_usable_tour")
            )
            days = days.join(day_has_usable, on="day_id", how="left")
            # null == the day has no tours at all -> a legitimate no-travel day.
            tables["days"] = days.with_columns(
                (base & pl.col("_day_has_usable_tour").fill_null(value=True)).alias("model_usable")
            ).drop("_day_has_usable_tour")
        else:
            tables["days"] = days.with_columns(base.alias("model_usable"))

    # -- Persons: model_usable == complete ------------------------------------
    # A person with no usable day is deliberately kept: they are a real person
    # who happens to contribute no travel, and person weights stay calibrated to
    # the person controls. Only their days fall out.
    persons = tables.get("persons")
    if persons is not None and "complete" in persons.columns:
        tables["persons"] = persons.with_columns(
            pl.col("complete").fill_null(value=False).alias("model_usable")
        )

    # -- Households (upward rule: needs a usable person) ----------------------
    _flag_households(tables)

    # -- Member trips follow their tour --------------------------------------
    tour_usable = None
    if tours is not None and "model_usable" in tours.columns:
        tour_usable = tours.select("tour_id", pl.col("model_usable").alias("_tour_usable"))
    for name in _TOUR_MEMBER_TABLES:
        df = tables.get(name)
        if df is None or "complete" not in df.columns:
            continue
        base = pl.col("complete").fill_null(value=False)
        if tour_usable is not None and "tour_id" in df.columns:
            df = df.join(tour_usable, on="tour_id", how="left")
            tables[name] = df.with_columns(
                (base & pl.col("_tour_usable").fill_null(value=False)).alias("model_usable")
            ).drop("_tour_usable")
        else:
            tables[name] = df.with_columns(base.alias("model_usable"))

    # -- Joint groupings (need two surviving members to still be joint) -------
    # Last: they read the member tables flagged above.
    _flag_joint_groupings(tables)


def _log_gate_summary(tables: dict[str, pl.DataFrame | None]) -> None:
    """Log, per table, how many records are complete vs model-usable."""
    lines = [
        "Model-usability gate applied.",
        '  "complete" = survey reporting (partials/overnights included); '
        '"model_usable" = admissible to the tour-based model.',
        "",
        f"  {'table':<16}{'rows':>10}{'complete':>12}{'model_usable':>14}{'net-new':>10}",
    ]
    for name, df in tables.items():
        if df is None or "model_usable" not in df.columns:
            continue
        n = df.height
        n_complete = df.filter(pl.col("complete").fill_null(value=False)).height
        n_usable = df.filter(pl.col("model_usable")).height
        # net-new = valid survey data the model still cannot use
        net_new = n_complete - n_usable
        lines.append(f"  {name:<16}{n:>10,}{n_complete:>12,}{n_usable:>14,}{net_new:>10,}")
    logger.info("\n".join(lines))


@step(
    requires={
        "days": {"day_id", "hh_id", "travel_date", "complete"},
        "tours": {"tour_id", "day_id", "complete"},
    },
)
def flag_model_usable(
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    days: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    joint_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
    joint_tours: pl.DataFrame | None = None,
    require_valid_tours: bool = True,
) -> dict[str, pl.DataFrame]:
    """Stamp the canonical ``model_usable`` gate on every table.

    This is the single place the modelling gate is computed. Downstream steps
    (weighting, the CT-RAMP/DaySim formatters) only *read* ``model_usable``; none
    of them re-derive completeness or tour validity. Because the step is cached,
    the flag is inspectable in the canonical output alongside ``complete``.

    See [`compute_model_usable`][processing.completeness.compute_model_usable]
    for the per-level rules.

    Args:
        households: Canonical households.
        persons: Canonical persons.
        days: Canonical person-days.
        unlinked_trips: Canonical unlinked trips.
        linked_trips: Canonical linked trips.
        joint_trips: Aggregated joint trips.
        tours: Canonical tours (with ``tour_data_quality`` / ``tour_category``).
        joint_tours: Aggregated joint tours.
        require_valid_tours: If True (default), fold strict tour structure into
            the gate. If False, ``model_usable`` reduces to reporting
            completeness, so partial/invalid tours stay model-usable.

    Returns:
        The provided tables, each with a ``model_usable`` column added.
    """
    tables: dict[str, pl.DataFrame | None] = {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "joint_trips": joint_trips,
        "tours": tours,
        "joint_tours": joint_tours,
    }

    compute_model_usable(tables, require_valid_tours=require_valid_tours)
    _log_gate_summary(tables)

    return {name: df for name, df in tables.items() if df is not None}
