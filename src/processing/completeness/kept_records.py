"""Decide which records survive a usability profile, across every table.

A profile's flag is stamped per table, and filtering each table on its own flag
is *not* enough. Most of the cascade does agree -- member trips inherit their
tour's verdict, and the upward reductions to day and person are `>= 1`, so a
usable tour implies a usable day and person. The household is the exception: a
profile requiring a whole household-day reduces over *dates on which every member
was usable*, not over usable days, so a person can be usable while their
household is not. Those records are orphaned by an independent filter and are
removed by following the schema's references, which is why the ledger counts
them separately rather than trusting an invariant that does not hold.

This is the one place that decision is made. The formatters gate on it, and the
weighting must place weight on exactly the records it keeps -- weight left on a
record removed here leaves the output short of the total it was weighted to.
"""

import logging
from dataclasses import dataclass, field

import polars as pl

from data_canon.core.dataclass import CANONICAL_MODELS
from data_canon.validation.relational import foreign_key_edges

from .descriptions import suggest_usability_columns
from .usable_cascade import MIN_JOINT_PARTICIPANTS

logger = logging.getLogger(__name__)

# Every table the gate can apply to; a formatter passes the ones it holds.
# All are stamped by the cascade, so a
# missing verdict is a broken frame rather than a table that opted out.
GATED_TABLES = (
    "households",
    "persons",
    "days",
    "tours",
    "linked_trips",
    "unlinked_trips",
    "joint_trips",
    "joint_tours",
)

# The declared references between canonical tables, derived rather than listed.
# There are three dozen, denormalised across the trip tables, and a
# hand-maintained subset omits whichever one matters next -- which is how a
# person outlived their household, and a trip outlived its joint tour.
_EDGES = foreign_key_edges(CANONICAL_MODELS)

# The joint groupings, as (id column, table whose surviving rows are its members,
# the group's own table).
_GROUPINGS = (
    ("joint_tour_id", "tours", "joint_tours"),
    ("joint_trip_id", "linked_trips", "joint_trips"),
)

# Reconciliation is iterated because each rule can trigger another: dropping a
# household drops its persons, which can leave a joint group short, which voids a
# reference. The bound guards against a cycle rather than counting expected work;
# drops only shrink and voids only remove references, so it settles quickly.
_MAX_RECONCILE_PASSES = 10


@dataclass
class KeepLedger:
    """What each rule removed, per table, accumulated across passes.

    Kept because the per-rule log read alarmingly out of context: a stream of
    "dropped 953 persons" lines invites the reader to think the formatter is
    making judgements of its own, when every one of those persons left because
    their household did not pass the profile. Naming the reason next to the count
    is the difference between a ledger and a list of casualties.

    Voided references are tracked separately from removals because they are not
    losses at all -- the record stays in the output, only its grouping is gone.
    """

    excluded: dict[str, int] = field(default_factory=dict)
    orphaned: dict[str, int] = field(default_factory=dict)
    undersized: dict[str, int] = field(default_factory=dict)
    unjointed: dict[str, int] = field(default_factory=dict)

    def add(self, bucket: dict[str, int], key: str, n: int) -> None:
        """Add *n* to *key* in *bucket*."""
        bucket[key] = bucket.get(key, 0) + n


def keep_records(
    tables: dict[str, pl.DataFrame],
    usability_flag_col: str,
) -> tuple[dict[str, pl.DataFrame], KeepLedger]:
    """Keep the records *usability_flag_col* admits, and restore what that breaks.

    Args:
        tables: Canonical tables, keyed by name.
        usability_flag_col: The stamped verdict to gate on.

    Returns:
        The kept tables, and a ledger of what each rule removed. Tables absent
        from the input stay absent.

    Raises:
        ValueError: If a gated table carries no verdict, which means
            `cascade_completeness` did not run or stamped a different profile.
    """
    missing = [
        name
        for name in GATED_TABLES
        if name in tables and usability_flag_col not in tables[name].columns
    ]
    if missing:
        sample = tables[missing[0]]
        msg = (
            f"{', '.join(missing)} carry no '{usability_flag_col}' column, so there "
            f"is nothing to gate on. Declare it in cascade_completeness's "
            f"usability_profiles. {suggest_usability_columns(sample)}"
        )
        raise ValueError(msg)

    # A null means the cascade never reached this row -- a broken frame, not
    # licence to guess a criterion of our own.
    keep = pl.col(usability_flag_col).fill_null(value=False)

    ledger = KeepLedger()
    kept: dict[str, pl.DataFrame] = dict(tables)
    for name in GATED_TABLES:
        if name not in kept:
            continue
        before = kept[name]
        kept[name] = before.filter(keep)
        ledger.add(ledger.excluded, name, before.height - kept[name].height)

    _reconcile(kept, ledger)
    return kept, ledger


def _reconcile(tables: dict[str, pl.DataFrame], ledger: KeepLedger) -> None:
    """Restore the relationships filtering broke, in place.

    Three rules, each stated once and applied until nothing changes. Iterating
    matters because they feed each other: dropping a household drops its
    persons, which can leave a joint group below quorum, which voids a reference
    on a trip.

    Raises:
        RuntimeError: If the rules have not settled within
            :data:`_MAX_RECONCILE_PASSES`, which would mean two of them disagree
            rather than that the data is large.
    """
    for _ in range(_MAX_RECONCILE_PASSES):
        changed = _cascade_references(tables, ledger)
        changed = _demote_undersized_groups(tables, ledger) or changed
        changed = _unjoint_tours_with_individual_trips(tables, ledger) or changed
        if not changed:
            return

    msg = (
        f"Record reconciliation did not settle in {_MAX_RECONCILE_PASSES} passes. "
        "Two rules are undoing each other rather than converging."
    )
    raise RuntimeError(msg)


def _cascade_references(tables: dict[str, pl.DataFrame], ledger: KeepLedger) -> bool:
    """Follow every declared reference whose target is gone, in place.

    The schema already says what to do. A *required* reference means the child
    cannot exist without its parent, so the child goes: a person with no
    household has no home to report and reaches the model as a null home zone. An
    *optional* reference means the child stands on its own and only the reference
    is void, so it is nulled: a trip that stops being joint is still a trip.

    Returns:
        Whether anything changed, so the caller can iterate to a fixed point.
    """
    changed = False
    for child, column, parent, parent_column, optional in _EDGES:
        c, p = tables.get(child), tables.get(parent)
        if c is None or p is None or column not in c.columns or parent_column not in p.columns:
            continue

        dangling = pl.col(column).is_not_null() & ~pl.col(column).is_in(p[parent_column].implode())
        n_dangling = c.filter(dangling).height
        if not n_dangling:
            continue

        changed = True
        if optional:
            tables[child] = c.with_columns(
                pl.when(dangling).then(None).otherwise(pl.col(column)).alias(column)
            )
            ledger.add(ledger.unjointed, f"{child}.{column}", n_dangling)
            logger.debug("  voided %d %s.%s: no matching %s", n_dangling, child, column, parent)
        else:
            tables[child] = c.filter(~dangling)
            ledger.add(ledger.orphaned, child, n_dangling)
            logger.debug(
                "  dropped %d %s: %s.%s is required and had no matching %s",
                n_dangling,
                child,
                child,
                column,
                parent,
            )
    return changed


def _demote_undersized_groups(tables: dict[str, pl.DataFrame], ledger: KeepLedger) -> bool:
    """Un-joint any grouping left with fewer than two members, in place.

    The cascade already refuses a grouping whose members are not usable, so this
    is not a second opinion on that. It is the consequence: rejecting a member
    leaves the others carrying the group's id, and a group of one is not a party.

    Only the group's own row is removed here. Voiding the members' references to
    it is left to :func:`_cascade_references`, which does that for every table
    carrying a copy rather than the ones remembered at this call site.

    Returns:
        Whether any group was removed.
    """
    changed = False
    for id_col, member_table, group_table in _GROUPINGS:
        members, groups = tables.get(member_table), tables.get(group_table)
        if members is None or groups is None:
            continue
        if id_col not in members.columns or id_col not in groups.columns:
            continue

        surviving = (
            members.filter(pl.col(id_col).is_not_null())
            .group_by(id_col)
            .agg(pl.len().alias("_n"))
            .filter(pl.col("_n") >= MIN_JOINT_PARTICIPANTS)[id_col]
        )
        undersized = groups.filter(~pl.col(id_col).is_in(surviving.implode()))
        if undersized.is_empty():
            continue

        changed = True
        ledger.add(ledger.undersized, group_table, undersized.height)
        logger.debug(
            "  demoted %d %s: fewer than %d members survived",
            undersized.height,
            group_table,
            MIN_JOINT_PARTICIPANTS,
        )
        tables[group_table] = groups.filter(pl.col(id_col).is_in(surviving.implode()))
    return changed


def _unjoint_tours_with_individual_trips(
    tables: dict[str, pl.DataFrame], ledger: KeepLedger
) -> bool:
    """Clear ``joint_tour_id`` where any of a tour's trips is not joint, in place.

    The fully-joint rule: activity-based models represent a joint tour as one
    whose every member shares every trip, so a single non-joint leg disqualifies
    the whole tour. A formatter refuses such a tour outright, because that leg
    would reach neither the individual nor the joint trip output.

    A leg loses its group when the group falls below quorum, which can happen
    while the tour itself still has two members and survives. The tour then
    becomes individual travel: the trips are all still real, they are simply no
    longer recorded as shared.

    Returns:
        Whether any tour was un-jointed.
    """
    tours, trips = tables.get("tours"), tables.get("linked_trips")
    if tours is None or trips is None:
        return False
    if "joint_tour_id" not in trips.columns or "joint_trip_id" not in trips.columns:
        return False

    # A frame recording no trip-level sharing at all is not claiming these tours
    # are partly shared -- it simply never had joint trips. The same judgement
    # _validate_joint_tours_are_wholly_joint makes before refusing one.
    if trips["joint_trip_id"].null_count() == trips.height:
        return False

    partly = (
        trips.filter(pl.col("joint_tour_id").is_not_null() & pl.col("joint_trip_id").is_null())[
            "joint_tour_id"
        ]
        .unique()
        .implode()
    )
    is_partly = pl.col("joint_tour_id").is_in(partly)
    if trips.filter(is_partly).is_empty():
        return False

    n_tours = 0
    for name in ("tours", "linked_trips", "unlinked_trips"):
        df = tables.get(name)
        if df is None or "joint_tour_id" not in df.columns:
            continue
        if name == "tours":
            n_tours = df.filter(is_partly)["joint_tour_id"].n_unique()
        ledger.add(ledger.unjointed, f"{name}.joint_tour_id", df.filter(is_partly).height)
        tables[name] = df.with_columns(
            pl.when(is_partly).then(None).otherwise(pl.col("joint_tour_id")).alias("joint_tour_id")
        )

    logger.debug(
        "  un-jointed %d tours: a leg of each is no longer shared, and a joint "
        "tour must be joint on every leg",
        n_tours,
    )
    return True
