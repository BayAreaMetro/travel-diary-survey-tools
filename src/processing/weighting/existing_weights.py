"""Attach pre-computed weights to survey data tables.

This module provides the ``add_existing_weights`` pipeline step, which loads
weight CSV files and joins them to the corresponding canonical tables.
Optionally, it can derive missing weights by propagating values through the
survey hierarchy.

# Core algorithm

## Phase 1 -- Load and Join Weights

1. For each provided weight config:

    1. Validate the config key against allowed table types.
    2. Load the weight CSV from ``weight_path``.
    3. Validate required ID and weight columns exist.
    4. Handle ID column name mismatches (rename if needed).
    5. Left-join weights to the table on the ID column.

## Phase 2 -- Move weight off dropped records, per profile

Households, then persons within their household, then each person's days, then
each day's trips. See the Existing Weights docs page for the rules and worked
examples.

## Phase 3 -- Derive Missing Weights (when ``derive_missing_weights=True``)

1. *Hierarchical carry-forward* for household → person → day → unlinked_trip:
   validate parent has weights, then join parent weight to child via FK.
2. *Aggregated weights* for linked_trip and tour: the mean of their members'
   final weights (excluding nulls and zeros). The joint levels **sum** instead
   -- see [`Agg`][processing.weighting.core.hierarchy.Agg].

Gap detection: raises an error if a middle-tier weight is missing
(e.g. household + trip weights provided but person/day weights are not).
"""

import logging
from collections.abc import Collection
from pathlib import Path

import polars as pl
from pydantic import BaseModel, Field, field_validator, model_validator

from data_canon.core.dataclass import CanonicalData
from pipeline.decoration import step
from processing.completeness import usable_col_for
from processing.weighting.core.hierarchy import (
    LEVELS,
    WEIGHT_CONFIG_MAPPING,
    Flow,
    describe_weight_columns,
    weight_col_for,
    weight_columns_for,
)
from processing.weighting.core.propagation import (
    collect_tables,
    distribute_within_scope,
    drop_unsuffixed_weights,
    is_usable,
    non_null_tables,
    propagate_weights,
)
from processing.weighting.core.specs import resolve_fitted_profiles
from processing.weighting.validation.weight_checks import (
    _check_hierarchy,
    check_days_carry_their_person,
    check_persons_carry_their_household,
    check_weight_placement,
)

logger = logging.getLogger(__name__)

# Rescale factors within this distance of 1.0 are floating-point drift, not work.
SCALE_TOL = 1e-9


class ExistingWeightConfig(BaseModel):
    """Configuration for a single weight file.

    Most fields use sensible defaults - typically you only need to provide weight_path.
    Override weight_id_col only if your weight file uses different ID column names.
    Override weight_col only if your weight file uses different weight column names.

    Examples:
        Basic usage with defaults:
        >>> WeightConfig(config_key="hh_weight", weight_path="hh_weights.csv")
        # Uses: weight_id_col="hh_id", weight_col="hh_weight"

        Override weight column name:
        >>> WeightConfig(
        ...     config_key="person_weight",
        ...     weight_path="person_weights.csv",
        ...     weight_col="final_weight"  # Weight file uses "final_weight" not "person_weight"
        ... )

        Override ID column name (external weight file with non-canonical IDs):
        >>> WeightConfig(
        ...     config_key="unlinked_trip_weight",
        ...     weight_path="trip_weights.csv",
        ...     weight_id_col="trip_id",  # Weight file uses "trip_id" not "unlinked_trip_id"
        ...     weight_col="trip_weight" # Weight file uses "trip_weight" not "unlinked_trip_weight"
        ... )

    Attributes:
        config_key: Key identifying the weight type (e.g., 'hh_weight')
        weight_path: Path to the CSV file containing weights
        weight_id_col: ID column name in the weight file. Defaults to canonical table ID.
        weight_col: Weight column name in the weight file. Defaults to canonical weight column.
    """

    config_key: str
    weight_path: Path
    weight_id_col: str | None = Field(
        default=None, description="Defaults to canonical table ID if not provided"
    )
    weight_col: str | None = Field(
        default=None, description="Defaults to canonical weight column if not provided"
    )
    keep_name: bool = Field(
        default=False,
        description="If True, keeps original weight_col name instead of renaming to canonical",
    )

    @field_validator("config_key")
    @classmethod
    def validate_config_key(cls, v: str) -> str:
        """Validate that config_key is one of the allowed values."""
        if v not in WEIGHT_CONFIG_MAPPING:
            msg = (
                f"Invalid weight config key: {v}. "
                f"Must be one of {list(WEIGHT_CONFIG_MAPPING.keys())}"
            )
            raise ValueError(msg)
        return v

    @field_validator("weight_path")
    @classmethod
    def validate_path_exists(cls, v: Path) -> Path:
        """Validate that the weight file exists."""
        if not v.exists():
            msg = f"Weight file does not exist: {v}"
            raise FileNotFoundError(msg)
        return v

    @model_validator(mode="after")
    def set_defaults_from_mapping(self) -> "ExistingWeightConfig":
        """Set default values for weight_id_col and weight_col from the mapping."""
        _, table_id_col, canonical_weight_col = WEIGHT_CONFIG_MAPPING[self.config_key]

        if self.weight_id_col is None:
            self.weight_id_col = table_id_col
        if self.weight_col is None:
            self.weight_col = canonical_weight_col

        return self

    @property
    def table_name(self) -> str:
        """Get the table name for this weight config."""
        return WEIGHT_CONFIG_MAPPING[self.config_key][0]

    @property
    def table_id_col(self) -> str:
        """Get the canonical table ID column."""
        return WEIGHT_CONFIG_MAPPING[self.config_key][1]

    @property
    def canonical_weight_col(self) -> str:
        """Get the canonical weight column name for this table."""
        return WEIGHT_CONFIG_MAPPING[self.config_key][2]

    def joined_weight_col(self, profile: str | None = None) -> str:
        """The column the supplied weight actually lands in under *profile*.

        ``keep_name`` suppresses the rename to the canonical name, so under it
        the vendor's own column is the only record of where the weight went --
        which is why it cannot be combined with profiles, there being one column
        and several universes to put in it.
        """
        if self.keep_name and self.weight_col:
            return self.weight_col
        return weight_col_for(self.canonical_weight_col, profile)

    def validate_columns(self, weight_df: pl.DataFrame, table_df: pl.DataFrame) -> None:
        """Validate that required columns exist in weight file and table.

        Args:
            weight_df: The weight DataFrame loaded from weight_path
            table_df: The survey data table to join weights to

        Raises:
            ValueError: If required columns are missing
        """
        # Check weight file has required columns
        if self.weight_id_col not in weight_df.columns:
            msg = f"Weight file {self.weight_path} missing required ID column: {self.weight_id_col}"
            raise ValueError(msg)

        if self.weight_col not in weight_df.columns:
            msg = (
                f"Weight file {self.weight_path} missing required weight column: {self.weight_col}"
            )
            raise ValueError(msg)

        # Check table has required ID column
        if self.table_id_col not in table_df.columns:
            msg = f"Table {self.table_name} missing required ID column: {self.table_id_col}"
            raise ValueError(msg)


# Known populations: always gated and held to their supplied totals.
_POPULATION_TABLES = frozenset({"households", "persons", "days"})

# Travel: unlinked trips follow their day; linked trips and tours are the mean
# of their final trips. ``preserve_travel_totals`` decides only whether a kept
# day's dropped trips hand their weight to its kept ones.
_TRAVEL_TABLES = frozenset({"unlinked_trips", "linked_trips", "tours"})

# Overlays on travel: always the sum of their kept members, derived last.
_JOINT_TABLES = frozenset({"joint_trips", "joint_tours"})


def _gate_travel(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    *,
    usability_flag_col: str,
    preserve_travel_totals: bool,
) -> dict[str, float]:
    """Gate the travel tables, each record starting from its pre-gate claim, in place.

    Used only where a travel weight cannot follow its day: a linked trip or
    tour weight supplied as a file, or unlinked trips supplied without day
    weights. When days and unlinked trips are both supplied, the trips follow
    their day ([`_trips_follow_their_day`]) and this does not see them.

    A claim is the weight a record would carry if every trip and tour were
    usable: the supplied weight, or the weight derived from its usable day.

    With *preserve_travel_totals*, a dropped record's claim moves to the kept
    records on its day and anything left stranded closes region-wide, so each
    table keeps its claim total. Without it, a kept record keeps exactly its
    claim and a dropped one carries nothing.

    Returns:
        table -> the claim total the placement check holds it to; empty when
        the totals are not preserved.
    """
    travel = {t: c for t, c in has_weight.items() if t in _TRAVEL_TABLES}
    if preserve_travel_totals:
        return _apply_usability(tables, travel, usability_flag_col=usability_flag_col)
    for table_name, weight_col in travel.items():
        df = tables.get(table_name)
        if df is None or usability_flag_col not in df.columns:
            continue
        tables[table_name] = df.with_columns(
            pl.when(is_usable(usability_flag_col))
            .then(pl.col(weight_col))
            .otherwise(0.0)
            .alias(weight_col)
        )
    return {}


def _apply_usability(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    *,
    usability_flag_col: str,
) -> dict[str, float]:
    """Redistribute supplied weights onto usable records, preserving each total.

    Unlike the computed path, the vendor's anchor cannot be re-balanced: their
    weights already sum to their population estimate, so dropping records must
    not shrink that total. Each supplied weight is first conserved within its
    declared scope ([`distribute_within_scope`]
    [processing.weighting.core.propagation.distribute_within_scope]);
    whatever that leaves stranded — scopes with no usable record at all — is
    then closed with one table-wide factor. Modifies *tables* in place.

    Args:
        tables: Mutable dict of table_name → DataFrame (or None).
        has_weight: Dict of table_name → weight column name.
        usability_flag_col: Boolean column deciding which records may carry weight
            -- a usability profile stamped upstream, or ``survey_complete`` to weight the
            whole valid survey.

    Returns:
        table -> the supplied total, for the placement check to hold each
        table to.
    """
    supplied_totals: dict[str, float] = {}
    for table_name, weight_col in has_weight.items():
        df = tables.get(table_name)
        if df is None or usability_flag_col not in df.columns:
            continue

        supplied_total = df.select(pl.col(weight_col).sum()).item() or 0.0
        supplied_totals[table_name] = supplied_total
        scope = LEVELS[table_name].scope

        if scope is None:
            # The anchor has no scope to spread within; zero and rescale below.
            df = df.with_columns(
                pl.when(is_usable(usability_flag_col))
                .then(pl.col(weight_col))
                .otherwise(0.0)
                .alias(weight_col)
            )
        else:
            # Raises when the scope column is absent -- quietly conserving the
            # weight over some other grouping would change the numbers silently.
            df = distribute_within_scope(
                df,
                weight_col=weight_col,
                scope=scope,
                usability_flag_col=usability_flag_col,
                table=table_name,
            )

        placed_total = df.select(pl.col(weight_col).sum()).item() or 0.0
        if placed_total > 0 and supplied_total > 0:
            scale = supplied_total / placed_total
            if abs(scale - 1.0) > SCALE_TOL:
                df = df.with_columns((pl.col(weight_col) * scale).alias(weight_col))
                logger.info(
                    "%s: scaled %s by %.4f to preserve the supplied total (%.1f). This covers "
                    "the weight of parents that kept no usable record, which has no sibling "
                    "to receive it.",
                    table_name,
                    weight_col,
                    scale,
                    supplied_total,
                )
        elif supplied_total > 0:
            logger.warning(
                "%s: no usable record carries weight; the supplied total (%.1f) is lost",
                table_name,
                supplied_total,
            )

        tables[table_name] = df
    return supplied_totals


# A verdict that lives only for the household and person passes: a person who
# can pass weight down -- usable, and, where persons and days are supplied,
# carrying a supplied weight onto a usable day the supplier weighted.
_CARRIES = "_can_carry_weight"


def _mark_persons_who_can_carry(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    supplied: Collection[str],
    *,
    usability_flag_col: str,
) -> None:
    """Add the ``_CARRIES`` verdict to persons, in place.

    Days take weight in proportion to the supplier's, so a day weighted 0 stays
    0. A usable person whose supplier-weighted days were all dropped has no day
    to put their weight on, so they count as a dropped person: their weight goes
    to the household's other persons. A household with no person who can carry
    weight counts as a dropped household. Their own verdicts are untouched --
    they stay usable, with weight 0.

    Raises:
        ValueError: If there is no persons table, or it carries no verdict.
            Household size is the number of persons, so there is no size
            without them.
    """
    persons, days = tables.get("persons"), tables.get("days")
    if persons is None or usability_flag_col not in persons.columns:
        msg = (
            f"Household weights are scaled by household size, which is the number of "
            f"persons, so the persons table is needed, carrying {usability_flag_col}."
        )
        raise ValueError(msg)

    can = is_usable(usability_flag_col)
    persons_supplied = "persons" in supplied
    if persons_supplied:
        can = can & (pl.col(has_weight["persons"]).fill_null(0.0) > 0)
    if persons_supplied and "days" in supplied and days is not None:
        weighted = days.filter(
            is_usable(usability_flag_col) & (pl.col(has_weight["days"]).fill_null(0.0) > 0)
        )
        can = can & pl.col("person_id").is_in(weighted["person_id"].unique().implode())
    persons = persons.with_columns(can.alias(_CARRIES))

    if persons_supplied:
        stranded = persons.filter(
            is_usable(usability_flag_col)
            & ~pl.col(_CARRIES)
            & (pl.col(has_weight["persons"]).fill_null(0.0) > 0)
        )
        if stranded.height:
            logger.info(
                "persons: %d usable person(s) carrying %.1f of %s have no usable day the "
                "supplier weighted; their weight goes to their household's other "
                "persons, and a household with no one left to carry it counts as "
                "dropped (sample person_id: %s)",
                stranded.height,
                float(stranded[has_weight["persons"]].sum()),
                has_weight["persons"],
                stranded["person_id"].sort().head(10).to_list(),
            )
    tables["persons"] = persons


def _size_label(group: int, top_group: int) -> str:
    """A size group as a reader would say it: ``3``, or ``6+`` for the top group."""
    return f"{group}+" if group == top_group else str(group)


def _gate_households_by_size(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    *,
    usability_flag_col: str,
    top_group: int,
) -> float:
    """Scale kept households by one factor per household size, in place.

    Size is the number of persons; sizes from *top_group* up share one group.
    Within each group::

        household = supplied x (group's supplied total / group's kept supplied total)

    Because every household in a group has the same size, this holds the
    group's person total as well as its household total -- one factor for all
    sizes would not, when the dropped households are larger or smaller than the
    kept ones. A household is kept when it is usable and has a person who can
    carry weight.

    Returns:
        The supplied household total, for the placement check to hold.

    Raises:
        ValueError: If a size group carries supplied weight but has no kept
            household to put it on.
    """
    households, persons = tables["households"], tables["persons"]
    if households is None or persons is None:  # pragma: no cover - checked by the caller
        return 0.0
    if top_group < 1:
        msg = f"household_size_top_group must be at least 1, got {top_group}"
        raise ValueError(msg)
    col = has_weight["households"]
    weight = pl.col(col).fill_null(0.0)

    sizes = persons.group_by("hh_id").agg(
        pl.len().alias("_size"), pl.col(_CARRIES).any().alias("_carried")
    )
    hh = households.join(sizes, on="hh_id", how="left").with_columns(
        pl.min_horizontal(pl.col("_size").fill_null(0), pl.lit(top_group)).alias("_group"),
        (is_usable(usability_flag_col) & pl.col("_carried").fill_null(value=False)).alias("_kept"),
    )
    groups = (
        hh.group_by("_group")
        .agg(
            pl.len().alias("_n"),
            pl.col("_kept").sum().alias("_n_kept"),
            weight.sum().alias("_supplied"),
            weight.filter(pl.col("_kept")).sum().alias("_kept_supplied"),
        )
        .sort("_group")
    )

    empty = groups.filter((pl.col("_supplied") > 0) & (pl.col("_kept_supplied") <= 0))
    if empty.height:
        row = empty.row(0, named=True)
        msg = (
            f"household size {_size_label(row['_group'], top_group)}: {row['_supplied']:.1f} "
            f"of {col} was supplied over {row['_n']} household(s), but there is no kept "
            f"household of that size to carry it under {usability_flag_col}. Lower "
            "household_size_top_group to group this size with smaller ones."
        )
        logger.error(msg)
        raise ValueError(msg)

    groups = groups.with_columns(
        pl.when(pl.col("_kept_supplied") > 0)
        .then(pl.col("_supplied") / pl.col("_kept_supplied"))
        .otherwise(0.0)
        .alias("_factor")
    )
    for row in groups.iter_rows(named=True):
        logger.info(
            "households of size %s: %d of %d kept; %s scaled by %.4f",
            _size_label(row["_group"], top_group),
            row["_n_kept"],
            row["_n"],
            col,
            row["_factor"],
        )

    # A dropped household goes to 0, and one the supplier never weighted stays null.
    tables["households"] = (
        hh.join(groups.select("_group", "_factor"), on="_group", how="left")
        .with_columns(
            pl.when(pl.col("_kept"))
            .then(pl.col(col) * pl.col("_factor"))
            .otherwise(pl.col(col) * 0.0)
            .alias(col)
        )
        .drop("_size", "_carried", "_group", "_kept", "_factor")
    )
    return float(households.select(weight.sum()).item())


def _persons_follow_household(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
) -> None:
    """Give each household's weight to its persons who can carry it, in place.

    A household stands for all its persons, so its persons together carry its
    weight times its number of persons. Those who can carry share it in
    proportion to their supplied weights::

        person = household x n_persons x supplied / sum(supplied of those who can carry)

    so a dropped person's share stays in the household, and household weight x
    number of persons = the sum of its persons' weights.
    """
    persons, households = tables["persons"], tables["households"]
    if persons is None or households is None:  # pragma: no cover - checked by the caller
        return
    person_col, hh_col = has_weight["persons"], has_weight["households"]
    claim = pl.col(person_col).fill_null(0.0)
    supplied_total = float(persons.select(claim.sum()).item())

    per_hh = persons.group_by("hh_id").agg(
        pl.len().alias("_n"), claim.filter(pl.col(_CARRIES)).sum().alias("_carried")
    )
    persons = (
        persons.join(per_hh, on="hh_id", how="left")
        .join(households.select("hh_id", pl.col(hh_col).alias("_hh")), on="hh_id", how="left")
        .with_columns(
            pl.when(pl.col(_CARRIES) & (pl.col("_carried") > 0))
            .then(pl.col("_hh") * pl.col("_n") * claim / pl.col("_carried"))
            .otherwise(pl.col(person_col) * 0.0)
            .alias(person_col)
        )
        .drop("_n", "_carried", "_hh")
    )
    tables["persons"] = persons

    placed = float(persons.select(pl.col(person_col).fill_null(0.0).sum()).item())
    logger.info(
        "persons: %s copies the household; total %.1f against %.1f supplied (%+.3f%%)",
        person_col,
        placed,
        supplied_total,
        (placed / supplied_total - 1) * 100 if supplied_total else 0.0,
    )


def _check_weight_factors(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    supplied: dict[str, pl.DataFrame],
    limits: dict[str, float],
) -> None:
    """Raise if any record's weight grew past its limit times its supplied weight.

    The factor is a record's final weight over its own supplied weight, so it
    counts everything that moved weight onto it: a household's size factor, a
    person taking a dropped member's share, a day taking a dropped day's.
    """
    for table, limit in limits.items():
        df = tables.get(table)
        if df is None:
            continue
        level, col = LEVELS[table], has_weight[table]
        factors = (
            df.select(level.id_col, col)
            .join(
                supplied[table].select(level.id_col, pl.col(level.weight_col).alias("_supplied")),
                on=level.id_col,
                how="inner",
            )
            .filter(pl.col("_supplied") > 0)
            .with_columns((pl.col(col).fill_null(0.0) / pl.col("_supplied")).alias("_factor"))
        )
        over = factors.filter(pl.col("_factor") > limit + SCALE_TOL).sort(
            ["_factor", level.id_col], descending=[True, False]
        )
        if over.is_empty():
            logger.info(
                "  %s: every record within %.2fx its supplied weight (largest %.4f)",
                table,
                limit,
                float(factors["_factor"].max() or 0.0),  # type: ignore[arg-type]
            )
            continue
        worst = over.head(10)
        msg = (
            f"{over.height} {table} record(s) grew past {limit}x their supplied weight "
            f"(max_weight_factor). Largest, {level.id_col}: {worst[level.id_col].to_list()} "
            f"at {[round(f, 4) for f in worst['_factor'].to_list()]}x."
        )
        logger.error(msg)
        raise ValueError(msg)


def _log_usable_supplied_zero(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    *,
    usability_flag_col: str,
) -> None:
    """Count the usable records the supplier gave no weight.

    Supplied weights are used as given, so such a record stays at 0. The count
    is how far this profile's universe is wider than the supplier's.
    """
    for table, weight_col in has_weight.items():
        df = tables.get(table)
        if df is None or usability_flag_col not in df.columns:
            continue
        usable = df.filter(is_usable(usability_flag_col))
        zero = usable.filter(pl.col(weight_col).fill_null(0.0) <= 0).height
        if zero:
            logger.info(
                "%s: %d of %d usable record(s) were supplied a %s of 0; they stay 0",
                table,
                zero,
                usable.height,
                weight_col,
            )


def _follow_parent(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    table: str,
    *,
    usability_flag_col: str,
) -> None:
    """Share each parent's final weight among *table*'s usable records, in place.

    Each record takes its parent's weight in proportion to its own claim::

        w = claim x parent_final / sum(usable claims under the parent)

    so a parent's usable records sum to the parent, and a record the supplier
    weighted 0 stays 0. A parent whose final weight is positive but that has no
    usable claim under it is stranded: its weight reaches no record here, and is
    logged for the totals check to report.
    """
    level = LEVELS[table]
    df, parent_df = tables[table], tables[level.parent]  # type: ignore[index]
    if df is None or parent_df is None:
        return
    key, weight_col = level.key, has_weight[table]
    usable = is_usable(usability_flag_col)
    claim = pl.col(weight_col).fill_null(0.0)

    usable_claims = df.group_by(key).agg(claim.filter(usable).sum().alias("_usable_claim"))
    parents = parent_df.select(
        key,
        pl.col(has_weight[level.parent]).fill_null(0.0).alias("_parent"),  # type: ignore[index]
    ).join(usable_claims, on=key, how="left")

    stranded = parents.filter(
        (pl.col("_parent") > 0) & (pl.col("_usable_claim").fill_null(0.0) <= 0)
    )
    if stranded.height:
        logger.warning(
            "%s: %d %s(s) carry weight but no usable %s with a supplied weight; %.1f of "
            "%s reaches no record (sample %s: %s)",
            table,
            stranded.height,
            level.parent,
            table,
            float(stranded["_parent"].sum()),
            has_weight[level.parent],  # type: ignore[index]
            key,
            stranded[key].head(10).to_list(),
        )

    tables[table] = (
        df.join(parents, on=key, how="left")
        .with_columns(
            pl.when(usable & (pl.col("_usable_claim") > 0))
            .then(claim * pl.col("_parent") / pl.col("_usable_claim"))
            .otherwise(0.0)
            .alias(weight_col)
        )
        .drop("_parent", "_usable_claim")
    )
    logger.info("%s: %s follows %s", table, weight_col, has_weight[level.parent])  # type: ignore[index]


def _trips_follow_their_day(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    supplied_days: pl.DataFrame,
    *,
    usability_flag_col: str,
    preserve_travel_totals: bool,
) -> None:
    """Scale each unlinked trip by its day's factor, in place.

    A day's factor is its final weight over its supplied weight, so a trip
    moves exactly as its day did -- a dropped day's weight went to the person's
    other days, and the trips on those days carry it::

        w = claim x day_final / day_supplied

    With *preserve_travel_totals*, a kept day's dropped trips also hand their
    claim to its kept trips, in proportion::

        w x sum(claims on the day) / sum(usable claims on the day)

    Raises:
        ValueError: If a trip carries a supplied weight but its day was
            supplied 0 or not at all -- there is no factor to apply, and
            guessing one would invent the supplier's intent.
    """
    trips = tables["unlinked_trips"]
    if trips is None:
        return
    trip_col, day_col = has_weight["unlinked_trips"], has_weight["days"]
    usable = is_usable(usability_flag_col)
    claim = pl.col(trip_col).fill_null(0.0)

    factors = (
        tables["days"]  # type: ignore[union-attr]
        .select("day_id", pl.col(day_col).fill_null(0.0).alias("_day_final"))
        .join(
            supplied_days.select("day_id", pl.col("day_weight").alias("_day_supplied")),
            on="day_id",
            how="left",
        )
    )
    trips = trips.join(factors, on="day_id", how="left")

    orphaned = trips.filter((claim > 0) & (pl.col("_day_supplied").fill_null(0.0) <= 0))
    if orphaned.height:
        msg = (
            f"{orphaned.height} unlinked_trips record(s) carry a supplied {trip_col} but "
            f"their day has a supplied day_weight of 0 or none, so there is no day factor "
            f"to scale them by. Sample unlinked_trip_id: "
            f"{orphaned['unlinked_trip_id'].head(10).to_list()}."
        )
        logger.error(msg)
        raise ValueError(msg)

    move = pl.lit(1.0)
    if preserve_travel_totals:
        within_day = trips.group_by("day_id").agg(
            claim.sum().alias("_claim"), claim.filter(usable).sum().alias("_usable_claim")
        )
        trips = trips.join(within_day, on="day_id", how="left")
        move = pl.col("_claim") / pl.col("_usable_claim")

    trips = trips.with_columns(
        pl.when(usable & (claim > 0))
        .then(claim * pl.col("_day_final") / pl.col("_day_supplied") * move)
        .otherwise(0.0)
        .alias(trip_col)
    )
    tables["unlinked_trips"] = trips.drop(
        "_day_final", "_day_supplied", "_claim", "_usable_claim", strict=False
    )
    logger.info(
        "unlinked_trips: %s follows its day%s",
        trip_col,
        ", with dropped trips' claims moved to the kept trips on the day"
        if preserve_travel_totals
        else "",
    )


def _attach_supplied(
    tables: dict[str, pl.DataFrame | None],
    supplied: dict[str, pl.DataFrame],
    configs: dict[str, ExistingWeightConfig],
    profile: str | None,
) -> dict[str, str]:
    """Join each supplied weight under its column for *profile*, in place.

    Returns:
        table -> the weight column that is actually there, which is what every
        later step reads the weight by.
    """
    by_table = {cfg.table_name: cfg for cfg in configs.values()}
    has_weight: dict[str, str] = {}

    for table_name, weight_df in supplied.items():
        df = tables.get(table_name)
        if df is None:
            continue
        cfg = by_table[table_name]
        joined_col = cfg.joined_weight_col(profile)

        # Drop the target column if it is already there, so a re-run replaces
        # rather than duplicates it.
        if joined_col in df.columns:
            df = df.drop(joined_col)

        renamed = weight_df.rename({cfg.canonical_weight_col: joined_col})
        tables[table_name] = df.join(renamed, on=cfg.table_id_col, how="left")
        has_weight[table_name] = joined_col
        logger.info("Joined %s to %s on %s", joined_col, table_name, cfg.table_id_col)

    return has_weight


def _warn_null_weights(tables: dict[str, pl.DataFrame | None], profile: str | None) -> None:
    """Report weights that came out null -- a record no supplied total covered."""
    for table, weight in weight_columns_for(profile).items():
        df = tables.get(table)
        if df is None or weight not in df.columns:
            continue
        null_count = df.select(pl.col(weight).is_null().sum()).item()
        if null_count:
            logger.warning(
                "Table %s has %d / %d NULL values in weight column %s",
                table,
                null_count,
                len(df),
                weight,
            )


@step()
def add_existing_weights(  # noqa: C901, PLR0912, PLR0913, PLR0915
    weights: dict[str, ExistingWeightConfig | dict],
    preserve_travel_totals: bool,
    household_size_top_group: int,
    max_weight_factor: dict[str, float] | None = None,
    usability_profile: str | None = None,
    weight_profiles: list[str] | None = None,
    derive_missing_weights: bool = False,
    canonical_data: CanonicalData | None = None,
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    days: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
    joint_trips: pl.DataFrame | None = None,
    joint_tours: pl.DataFrame | None = None,
) -> dict[str, pl.DataFrame]:
    """Attach existing weights to the data.

    Loads weights from provided files and attaches them to the data.

    For any tables that exist, and do not have weights provided, we can optionally
    derive missing weights by carrying forward weights from the next logical upstream table.

    For example, if only household weights exist, all subsequent tables (persons, days, trips)
    will receive the household weight for each member record. If trip (unlinked) weights exist
    but not linked trips or tours, the unlinked trip weights will be carried forward using
    appropriate logic.

    If a "middle" weight is missing, an error will be raised if derive_missing_weights is True.
    For example, if household and trip weights are provided, but not person or day weights,
    an error will be raised as this likely indicates a misconfiguration.

    # Weight hierarchy logic

        hh_weight
          └─ person_weight        (carry forward via hh_id)
              └─ day_weight        (split: person_weight / n_usable_days)
                  └─ unlinked_trip_weight  (carry forward via day_id)
                      ├─ linked_trip_weight   (mean agg via linked_trip_id)
                      │   └─ joint_trip_weight (SUM agg via joint_trip_id)
                      └─ tour_weight          (mean agg via tour_id)
                          └─ joint_tour_weight (SUM agg via joint_tour_id)

    The shape is declared once in
    [`HIERARCHY`][processing.weighting.core.hierarchy.HIERARCHY]; this
    step only supplies the weights it is given and derives the rest from it.

    Supplied weights move household -> person -> day -> trip, each level only
    within the level above it; the rules, the arithmetic and worked examples
    are on the Existing Weights docs page. In short: households scale by one
    factor per household size; persons copy their household, a dropped
    person's share staying inside it; a person's days carry exactly that
    person's weight; a trip is scaled by its day's factor; linked trips and
    tours are the mean of their final trips.

    Checked before the step returns, per profile, raising on failure:

    - the household total equals the supplied total;
    - ``hh_weight x num_persons == sum(person_weight)`` for every household;
    - each person's days sum to that person's weight, when both are supplied;
    - each derived level against the identity it was derived by;
    - no record grew past its ``max_weight_factor``;
    - no dropped record carries weight, and the formatter's own record
      selection keeps every weight.

    The person total is logged against the supplied total rather than held:
    size groups hold it exactly, except where the top group joins sizes. Not
    held, by design: the trip, linked trip and tour totals.

    Args:
        weights: A dict mapping config keys to weight file paths.
            Each entry specifies a weight CSV to load.  Supported config
            keys: ``hh_weight``, ``person_weight``,
            ``day_weight``, ``unlinked_trip_weight``,
            ``linked_trip_weight``, ``joint_trip_weight``,
            ``tour_weight``.

            Config options per entry:

            - ``weight_path``: Path to CSV file containing weights (required).
            - ``weight_id_col``: ID column name in the weight file
              (optional, defaults to canonical table ID).
            - ``weight_col``: Weight column name in the weight file
              (optional, defaults to canonical weight column).

        household_size_top_group: Required; the config states it. Households
            are scaled by one factor per household size (number of persons), so
            dropping more large households than small ones does not move the
            person total. Sizes from this one up share a factor, which keeps a
            size with few households from carrying an extreme one.
        max_weight_factor: Optional limits, table -> the most a record's final
            weight may be, as a multiple of its own supplied weight. A table
            left out has no limit. The step raises naming the records past it.
            Each table named must have a supplied weight.
        preserve_travel_totals: Required; the config states it. Households
            are always held to their total and persons to their household, as
            known populations, and a person's days carry that person's weight.
            Every unlinked trip
            moves with its day, by the day's final weight over its supplied
            weight. Trips and tours are not targets, so whether a dropped
            trip's weight moves is a choice. ``true``: a kept day's dropped
            trips hand their weight to its kept trips, in proportion, so the
            day's trips keep their total. ``false``: every kept trip keeps its
            own weight times its day's factor, and a dropped one's weight is
            gone. Either way, linked trips and tours are the mean of their final
            trips, and joint trips and joint tours the sum of their kept
            members.
        derive_missing_weights: Whether to derive weights for tables
            without provided weight files (default: False).
        usability_profile: The usability profile deciding which records may carry
            weight -- one stamped upstream by the
            ``cascade_completeness`` step, matching the tours that profile's
            consumer keeps; pass ``survey_complete`` to weight the whole valid survey
            including partial and overnight tours. Supplied weights are
            moved onto the usable records rather than simply zeroed, so the
            household and person totals are preserved -- the vendor's anchor
            cannot be re-balanced from here. Mutually exclusive with
            *weight_profiles*.
        weight_profiles: Profiles to weight, each writing its own column set
            suffixed with the profile name. The supplied files are read once and
            moved onto each profile's usable records, so every set holds the
            same household and person totals over a different universe.
        canonical_data: Canonical data container. When present, the suffixed
            weight columns are registered on it, since names that come from
            config cannot be model fields.
        households: Households DataFrame.
        persons: Persons DataFrame.
        days: Days DataFrame.
        unlinked_trips: Unlinked trips DataFrame.
        linked_trips: Linked trips DataFrame.
        tours: Tours DataFrame.
        joint_trips: Joint trips DataFrame.
        joint_tours: Joint tours DataFrame.

    Returns:
        Dict of tables with attached weights.

    # Example config

    ```yaml
        - name: add_existing_weights
          params:
            preserve_travel_totals: true
            derive_missing_weights: true
            weights:
              hh_weight:
                weight_path: "weights/hh_weights.csv"
                # defaults: id_col='hh_id', weight_col='hh_weight'
              person_weight:
                weight_path: "weights/person_weights.csv"
              unlinked_trip_weight:
                weight_path: "weights/trip_weights.csv"
                weight_id_col: "trip_id"
                weight_col: "trip_weight"
    ```
    """
    # Collect all provided tables
    tables = collect_tables(
        households=households,
        persons=persons,
        days=days,
        unlinked_trips=unlinked_trips,
        linked_trips=linked_trips,
        joint_trips=joint_trips,
        joint_tours=joint_tours,
        tours=tours,
    )

    profiles = resolve_fitted_profiles(usability_profile, weight_profiles)

    # Track which weights are provided, and the supplied numbers themselves --
    # read once, then redistributed per profile.
    provided_weights = set()
    supplied: dict[str, pl.DataFrame] = {}

    # Validate and convert weight configs to WeightConfig objects
    validated_weights: dict[str, ExistingWeightConfig] = {}
    for config_key, value in weights.items():
        if isinstance(value, ExistingWeightConfig):
            validated_weights[config_key] = value
        elif isinstance(value, dict):
            # Auto-infer config_key from dict key if not provided
            validated_weights[config_key] = ExistingWeightConfig(config_key=config_key, **value)
        else:
            msg = (
                f"Weight config for {config_key} must be a ExistingWeightConfig or dict, "
                f"got {type(value)}"
            )
            raise TypeError(msg)

    # Load and join provided weight files
    for cfg in validated_weights.values():
        table_name = cfg.table_name
        provided_weights.add(table_name)

        df = tables.get(table_name)
        if df is None:
            logger.warning("Weight file provided for %s but table not found in data", table_name)
            continue

        # Load weight file
        logger.info("Loading weights from %s for %s", cfg.weight_path, table_name)
        weight_df = pl.read_csv(cfg.weight_path)

        # Validate columns exist before selecting
        cfg.validate_columns(weight_df, df)

        # Select only needed columns
        weight_df = weight_df.select([cfg.weight_id_col, cfg.weight_col])

        # Handle potential column name mismatches
        rename_map = {}

        # Rename weight file ID column to match table if needed
        if cfg.weight_id_col != cfg.table_id_col:
            if cfg.weight_id_col is None:
                msg = f"weight_id_col should never be None due to model_validator for {table_name}"
                raise ValueError(msg)
            rename_map[cfg.weight_id_col] = cfg.table_id_col

        if cfg.keep_name and profiles != (None,):
            msg = (
                f"{cfg.config_key}: keep_name cannot be combined with weight_profiles. "
                f"The vendor's column {cfg.weight_col!r} is one name, and there are "
                f"{len(profiles)} universes to write; suffixing it would defeat the only "
                "reason to keep it. Drop keep_name, or weight a single profile."
            )
            raise ValueError(msg)

        # Land on the canonical name here so what was read is uniform whatever the
        # vendor called it; `_attach_supplied` renames to the column each pass
        # writes, which under keep_name is the vendor's name again.
        if cfg.weight_col != cfg.canonical_weight_col:
            if cfg.weight_col is None:
                msg = f"weight_col should never be None due to model_validator for {table_name}"
                raise ValueError(msg)
            rename_map[cfg.weight_col] = cfg.canonical_weight_col

        if rename_map:
            weight_df = weight_df.rename(rename_map)

        logger.info("Loaded %d %s weights from %s", weight_df.height, table_name, cfg.weight_path)

        supplied[table_name] = weight_df

    # A limit compares a record's final weight to its supplied one, so it needs one.
    limits = max_weight_factor or {}
    for table, limit in limits.items():
        if table not in supplied:
            msg = (
                f"max_weight_factor names {table}, which has no supplied weight to compare "
                f"a final weight against. Tables with one: {sorted(supplied)}."
            )
            raise ValueError(msg)
        if limit <= 0:
            msg = f"max_weight_factor for {table} must be positive, got {limit}"
            raise ValueError(msg)

    # One column set per profile, all from the same supplied numbers: the files
    # are read once above, and each pass starts from those values rather than
    # from the previous profile's redistributed ones.
    for profile in profiles:
        named = profile if profile is not None else usability_profile
        if named is None:  # pragma: no cover - resolve_fitted_profiles forbids it
            msg = "No usability profile to weight against"
            raise ValueError(msg)
        flag = usable_col_for(named)

        has_weight = _attach_supplied(tables, supplied, validated_weights, profile)
        _log_usable_supplied_zero(tables, has_weight, usability_flag_col=flag)

        # Populations first. Usability is stamped upstream by
        # ``cascade_completeness``. Households scale by size; persons copy their
        # household; days take their person's weight.
        population = {t: c for t, c in has_weight.items() if t in _POPULATION_TABLES}
        supplied_totals: dict[str, float] = {}
        households_df = tables.get("households")
        by_size = (
            "households" in population
            and households_df is not None
            and flag in households_df.columns
        )
        days_follow = "days" in population and "persons" in population
        if by_size or days_follow:
            _mark_persons_who_can_carry(
                tables, has_weight, supplied.keys(), usability_flag_col=flag
            )
        if by_size:
            population.pop("households")
            supplied_totals["households"] = _gate_households_by_size(
                tables, has_weight, usability_flag_col=flag, top_group=household_size_top_group
            )
            if "persons" in population:
                population.pop("persons")
                _persons_follow_household(tables, has_weight)
        if days_follow:
            # Held person by person below, which holds the day total to the
            # person total rather than to the supplier's day total.
            population.pop("days")
            if "persons" in population:
                # Persons supplied without households: the older rule, within the
                # household then region-wide.
                supplied_totals |= _apply_usability(
                    tables, {"persons": population.pop("persons")}, usability_flag_col=_CARRIES
                )
        supplied_totals |= _apply_usability(tables, population, usability_flag_col=flag)
        persons_df = tables.get("persons")
        if persons_df is not None and _CARRIES in persons_df.columns:
            tables["persons"] = persons_df.drop(_CARRIES)
        if days_follow:
            _follow_parent(tables, has_weight, "days", usability_flag_col=flag)

        if derive_missing_weights:
            propagate_weights(
                tables,
                has_weight,
                skip=provided_weights | _TRAVEL_TABLES | _JOINT_TABLES,
                usability_flag_col=flag,
                profile=profile,
            )

        if {"unlinked_trips", "days"} <= supplied.keys():
            # Unlinked trips move with their day. Their total is not a target: a
            # dropped day's weight lands on the person's other days, whose trip
            # counts differ, so it is not held.
            _trips_follow_their_day(
                tables,
                has_weight,
                supplied["days"],
                usability_flag_col=flag,
                preserve_travel_totals=preserve_travel_totals,
            )
            if derive_missing_weights:
                # Linked trips and tours: the mean of their final trips, so each
                # agrees with its own trips.
                propagate_weights(
                    tables,
                    has_weight,
                    skip=provided_weights | _POPULATION_TABLES | _JOINT_TABLES,
                    usability_flag_col=flag,
                    profile=profile,
                )
            # A linked trip or tour weight supplied as a file is gated on its own.
            to_gate = {
                t: c
                for t, c in has_weight.items()
                if t in supplied.keys() & _TRAVEL_TABLES - {"unlinked_trips"}
            }
        else:
            if derive_missing_weights:
                # Travel claims: derived from the gated days, but not gated themselves.
                propagate_weights(
                    tables,
                    has_weight,
                    skip=provided_weights | _POPULATION_TABLES | _JOINT_TABLES,
                    usability_flag_col=None,
                    profile=profile,
                )
            to_gate = has_weight

        supplied_totals |= _gate_travel(
            tables,
            to_gate,
            usability_flag_col=flag,
            preserve_travel_totals=preserve_travel_totals,
        )

        if derive_missing_weights:
            # Joint tables last: the sum of the members that kept their weight.
            propagate_weights(
                tables,
                has_weight,
                skip=provided_weights | _POPULATION_TABLES | _TRAVEL_TABLES,
                usability_flag_col=flag,
                profile=profile,
            )

        _warn_null_weights(tables, profile)
        # Each level against the rule that set it: persons against their
        # household, supplied days against their person, derived levels against
        # the identity they were derived by.
        if by_size and "persons" in has_weight:
            check_persons_carry_their_household(non_null_tables(tables), profile)
        if days_follow:
            check_days_carry_their_person(non_null_tables(tables), profile)
        derived = {t for t in has_weight if t not in supplied and LEVELS[t].flow is Flow.DOWN}
        if derived:
            _check_hierarchy(non_null_tables(tables), flag, profile, only=derived)
        _check_weight_factors(tables, has_weight, supplied, limits)
        # Every supplied total still there, and all of it on records the
        # formatter will keep -- or the run stops here, not in the model.
        check_weight_placement(
            non_null_tables(tables), flag, profile, supplied_totals=supplied_totals
        )

    drop_unsuffixed_weights(tables, profiles)

    if canonical_data is not None:
        for table, described in describe_weight_columns(profiles).items():
            canonical_data.register_generated_columns(table, described)

    logger.info(
        "Weight attachment complete for %s. Tables with weights: %s",
        ", ".join(p or "the whole survey" for p in profiles),
        sorted(supplied),
    )

    return {name: df for name, df in tables.items() if df is not None}
