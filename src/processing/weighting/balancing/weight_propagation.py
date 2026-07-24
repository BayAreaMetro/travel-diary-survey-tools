"""Shared weight hierarchy constants and propagation helpers.

Used by both ``weighting`` and ``existing_weights`` (pre-computed)
steps to propagate household weights down through the canonical table hierarchy.

# Weight derivation

|Table      |Weight Column           |Derivation
|-----------|------------------------|-----------------------------------------
|households |``hh_weight``           |Direct from balancer
|persons    |``person_weight``       |Carry forward ``hh_weight`` via ``hh_id``
|days       |``day_weight``          |Carry forward ``person_weight`` via ``person_id``
|unlinked   |``unlinked_trip_weight``| Carry forward ``day_weight`` via ``day_id``
|linked     |``linked_trip_weight``  |Mean of constituent ``unlinked_trip_weight``
|tours      |``tour_weight``         |Mean of constituent ``linked_trip_weight``

# Carry-forward rule

A parent's population is represented by its **usable** children, so one rule
covers the whole carry-forward chain:

    w_child = w_parent x n_children / n_usable   (usable child)
    w_child = 0                                  (unusable child)

which makes the checksum true by construction, per parent and in total:

    sum(child_weight) == parent_weight x n_children

Usability is read from ``model_usable``, stamped once by the
``flag_model_usable`` step (see [`processing.completeness`]
[processing.completeness]), so the weighted sample matches the tours
CT-RAMP/DaySim actually keep.  Pass ``complete`` instead to weight the whole
valid survey, including partial and overnight tours.

A parent with **no** usable child has no denominator of its own.  A household
with no usable person is simply dropped -- there is nothing left to weight.  A
person with no usable day is deliberately *kept*: an unrelated person who
reported no travel is still a real person, and their person weight stays
calibrated to the person controls.  Their day-weight is stood in for by the
household's remaining usable days (``FALLBACK_KEY``), so the identity holds one
scope wider:

    sum(day_weight over a household) == sum(person_weight x n_days) over its persons

Aggregated tables need no rule of their own -- a linked trip, tour or joint
entity is a *grouping* of already-weighted records, so it takes the mean weight
of its usable members.
"""

import logging

import polars as pl

logger = logging.getLogger(__name__)

# Rescale factors within this distance of 1.0 are floating-point drift, not work.
SCALE_TOL = 1e-9

# Canonical mapping: config_key -> (table_name, id_column, weight_column)
WEIGHT_CONFIG_MAPPING: dict[str, tuple[str, str, str]] = {
    "household_weights": ("households", "hh_id", "hh_weight"),
    "person_weights": ("persons", "person_id", "person_weight"),
    "day_weights": ("days", "day_id", "day_weight"),
    "unlinked_trip_weights": ("unlinked_trips", "unlinked_trip_id", "unlinked_trip_weight"),
    "linked_trip_weights": ("linked_trips", "linked_trip_id", "linked_trip_weight"),
    "joint_trip_weights": ("joint_trips", "joint_trip_id", "joint_trip_weight"),
    "tour_weights": ("tours", "tour_id", "tour_weight"),
}

# Derived lookup: table_name -> weight_column
WEIGHT_COLUMNS: dict[str, str] = {
    table: weight for table, _, weight in WEIGHT_CONFIG_MAPPING.values()
}

# Carry-forward: (parent_table, child_table, join_key, child_weight_col)
CARRY_FORWARD = [
    ("households", "persons", "hh_id", "person_weight"),
    ("persons", "days", "person_id", "day_weight"),
    ("days", "unlinked_trips", "day_id", "unlinked_trip_weight"),
]

# Aggregate (mean): (source_table, target_table, group_key, target_weight_col)
AGGREGATE = [
    ("unlinked_trips", "linked_trips", "linked_trip_id", "linked_trip_weight"),
    ("linked_trips", "joint_trips", "joint_trip_id", "joint_trip_weight"),
    ("linked_trips", "tours", "tour_id", "tour_weight"),
]

# Wider scope that absorbs the weight of a parent which kept no usable child.
# A person who reported no usable day (an unrelated person, say) keeps their own
# weight, but their days are stood in for by the household's remaining days --
# so sum(day_weight) over a household still equals what its persons represent.
FALLBACK_KEY: dict[str, str] = {"days": "hh_id"}

# Parent a record's weight is spread within when it is unusable. Households have
# no parent, so a supplied household weight can only be rescaled table-wide.
PARENT_KEY: dict[str, str | None] = {
    "households": None,
    "persons": "hh_id",
    "days": "person_id",
    "unlinked_trips": "day_id",
    "linked_trips": "day_id",
    "joint_trips": "day_id",
    "tours": "day_id",
}

# All canonical table names in hierarchy order
TABLE_NAMES = [
    "households",
    "persons",
    "days",
    "unlinked_trips",
    "linked_trips",
    "joint_trips",
    "tours",
]


def collect_tables(
    *,
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    days: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    joint_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
) -> dict[str, pl.DataFrame | None]:
    """Bundle canonical tables into a dict keyed by table name.

    Kind of silly, but handles Nones and keeps the table names straight in one place.
    """
    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "joint_trips": joint_trips,
        "tours": tours,
    }


def safe_join_weight(
    df: pl.DataFrame,
    weight_df: pl.DataFrame,
    on: str,
) -> pl.DataFrame:
    """Left-join *weight_df* onto *df*, dropping any pre-existing weight columns."""
    new_cols = [c for c in weight_df.columns if c != on]
    for c in new_cols:
        if c in df.columns:
            df = df.drop(c)
    return df.join(weight_df, on=on, how="left")


def is_usable(usability_flag_col: str) -> pl.Expr:
    """True when a record may carry weight; a null flag counts as unusable."""
    return pl.col(usability_flag_col).fill_null(value=False)


def distribute_to_usable(
    df: pl.DataFrame,
    *,
    join_key: str,
    weight_col: str,
    usability_flag_col: str,
    table: str,
    fallback_key: str | None = None,
) -> pl.DataFrame:
    """Spread each parent's weight across its usable children, returning a new frame.

    Applies the carry-forward rule ``w_child = w_parent x n_children / n_usable``
    to usable children and 0 to the rest, so ``sum(child_weight)`` equals
    ``parent_weight x n_children`` for every parent that kept at least one child.

    A parent that kept **no** usable child has no denominator of its own. When
    *fallback_key* names a wider scope, that stranded weight is spread evenly
    over the usable children in that scope instead of being left behind -- this
    is what keeps an unrelated person who reported no usable day from silently
    shrinking their household's travel. Without a fallback the weight is left
    behind and reported as a shortfall by the checksum.

    Args:
        df: Child table already carrying the parent weight in *weight_col*.
        join_key: Foreign key identifying the parent (e.g. ``person_id``).
        weight_col: Weight column, adjusted in place of itself.
        usability_flag_col: Boolean column deciding which records may carry weight.
        table: Table name, for logging.
        fallback_key: Wider grouping (e.g. ``hh_id``) that absorbs the weight of
            parents which kept no usable child. None leaves it behind.

    Returns:
        *df* with *weight_col* distributed across the usable children.
    """
    if usability_flag_col not in df.columns or weight_col not in df.columns:
        return df

    usable = is_usable(usability_flag_col)
    before = df.select(pl.col(weight_col).sum()).item() or 0.0
    n_unusable = df.filter(~usable).height
    if n_unusable == 0:
        return df

    counts = df.group_by(join_key).agg(
        pl.len().alias("_n_children"),
        usable.sum().alias("_n_usable"),
        pl.col(weight_col).first().alias("_parent_weight"),
    )
    share = (
        pl.when(pl.col("_n_usable") > 0)
        .then(pl.col("_n_children") / pl.col("_n_usable"))
        .otherwise(0.0)
    )
    n_emptied = counts.filter(pl.col("_n_usable") == 0).height
    df = df.join(counts, on=join_key, how="left")

    # Weight the parent could not place: it kept no usable child at all.
    stranded = pl.when(pl.col("_n_usable") == 0).then(
        pl.col("_parent_weight") * pl.col("_n_children")
    )

    if fallback_key is not None and fallback_key in df.columns and n_emptied:
        # Escalate to the wider scope: an unrelated person who reported no usable
        # day is still part of the household, so the household's remaining usable
        # days stand in for theirs.
        rescue = (
            df.group_by(fallback_key, join_key)
            .agg(
                stranded.otherwise(0.0).first().alias("_stranded"),
                usable.sum().alias("_usable_here"),
            )
            .group_by(fallback_key)
            .agg(
                pl.col("_stranded").sum().alias("_stranded"),
                pl.col("_usable_here").sum().alias("_usable_in_scope"),
            )
        )
        df = df.join(rescue, on=fallback_key, how="left")
        top_up = (
            pl.when(pl.col("_usable_in_scope") > 0)
            .then(pl.col("_stranded").fill_null(0.0) / pl.col("_usable_in_scope"))
            .otherwise(0.0)
        )
    else:
        top_up = pl.lit(0.0)

    df = df.with_columns(
        pl.when(usable).then(pl.col(weight_col) * share + top_up).otherwise(0.0).alias(weight_col)
    ).drop(
        [
            c
            for c in ("_n_children", "_n_usable", "_parent_weight", "_stranded", "_usable_in_scope")
            if c in df.columns
        ]
    )

    after = df.select(pl.col(weight_col).sum()).item() or 0.0
    logger.info(
        "%s: %d/%d records unusable by %s; total %s %.1f -> %.1f%s",
        table,
        n_unusable,
        df.height,
        usability_flag_col,
        weight_col,
        before,
        after,
        f" ({n_emptied} parent(s) kept no usable child)" if n_emptied else "",
    )
    return df


def propagate_weights(  # noqa: C901, PLR0912
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    *,
    skip: set[str] | None = None,
    usability_flag_col: str | None = "model_usable",
) -> None:
    """Carry forward and aggregate weights through the hierarchy.

    Modifies *tables* and *has_weight* **in place**.

    Args:
        tables: Mutable dict of table_name → DataFrame (or None).
        has_weight: Mutable dict tracking which tables already have a
            weight column and the column name.
            E.g. ``{"households": "hh_weight"}``.
        skip: Table names to skip (e.g. tables that already have
            externally provided weights).
        usability_flag_col: Boolean column deciding which records may carry weight.
            Each parent's weight is spread across its usable children only.
            Defaults to ``model_usable``; pass ``complete`` to weight the whole
            valid survey including partial/overnight tours, or None to give
            every record the parent weight regardless of usability.
    """
    skip = skip or set()

    # Carry forward: parent weight -> child via join
    for parent, child, join_key, weight_col in CARRY_FORWARD:
        if child in skip:
            continue
        child_df = tables.get(child)
        if child_df is None:
            continue

        if parent not in has_weight:
            msg = (
                f"Cannot derive {weight_col} for {child}: "
                f"parent table {parent} has no weight column"
            )
            raise ValueError(msg)

        parent_df = tables.get(parent)
        if parent_df is None:
            msg = f"Cannot derive {weight_col} for {child}: parent table {parent} is None"
            raise ValueError(msg)

        if join_key not in child_df.columns:
            msg = f"Cannot derive weight: {child} missing join key {join_key}"
            raise ValueError(msg)

        parent_weight = has_weight[parent]
        logger.info("Deriving %s from %s via %s", weight_col, parent_weight, join_key)

        w = parent_df.select(join_key, parent_weight).rename({parent_weight: weight_col})
        child_df = safe_join_weight(child_df, w, join_key)

        # Spread each parent's weight across its usable children only.
        if usability_flag_col:
            child_df = distribute_to_usable(
                child_df,
                join_key=join_key,
                weight_col=weight_col,
                usability_flag_col=usability_flag_col,
                table=child,
                fallback_key=FALLBACK_KEY.get(child),
            )

        tables[child] = child_df
        has_weight[child] = weight_col

    # Aggregate: mean weight from source grouped by key
    for source, target, group_key, weight_col in AGGREGATE:
        if target in skip:
            continue
        target_df = tables.get(target)
        if target_df is None:
            continue

        source_df = tables.get(source)
        if source_df is None:
            msg = f"Cannot derive {weight_col} for {target}: source table {source} is None"
            raise ValueError(msg)

        if source not in has_weight:
            msg = (
                f"Cannot derive {weight_col} for {target}: "
                f"source table {source} has no weight column"
            )
            raise ValueError(msg)

        if group_key not in source_df.columns:
            msg = f"Cannot derive {weight_col}: source {source} missing {group_key}"
            raise ValueError(msg)

        src_weight = has_weight[source]
        logger.info("Deriving %s from mean of %s", weight_col, src_weight)

        agg = source_df.group_by(group_key).agg(
            pl.col(src_weight)
            .filter(pl.col(src_weight).is_not_null() & (pl.col(src_weight) != 0))
            .mean()
            .fill_null(0)
            .alias(weight_col),
        )
        target_df = safe_join_weight(target_df, agg, group_key)

        # A grouping is never more usable than its members: an unusable record
        # carries no weight even if the mean came out positive.
        if usability_flag_col and usability_flag_col in target_df.columns:
            target_df = target_df.with_columns(
                pl.when(is_usable(usability_flag_col))
                .then(pl.col(weight_col))
                .otherwise(0.0)
                .alias(weight_col)
            )

        tables[target] = target_df
        has_weight[target] = weight_col


def non_null_tables(tables: dict[str, pl.DataFrame | None]) -> dict[str, pl.DataFrame]:
    """Return only the non-None entries from a tables dict."""
    return {k: v for k, v in tables.items() if v is not None}
