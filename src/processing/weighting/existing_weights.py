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

import polars as pl

from data_canon.core.dataclass import CanonicalData
from pipeline.decoration import step
from processing.completeness import usable_col_for
from processing.weighting.core.hierarchy import (
    LEVELS,
    Flow,
    describe_weight_columns,
)
from processing.weighting.core.propagation import (
    collect_tables,
    drop_unsuffixed_weights,
    non_null_tables,
    propagate_weights,
)
from processing.weighting.core.specs import resolve_fitted_profiles
from processing.weighting.existing.config import (
    ExistingWeightConfig,
    ExistingWeightsConfig,
    attach_supplied,
)
from processing.weighting.existing.population import (
    CARRIES,
    POPULATION_TABLES,
    apply_usability,
    follow_parent,
    gate_households_by_size,
    mark_persons_who_can_carry,
    persons_follow_household,
)
from processing.weighting.existing.travel import (
    JOINT_TABLES,
    TRAVEL_TABLES,
    check_weight_factors,
    gate_travel,
    log_usable_supplied_zero,
    trips_follow_their_day,
    warn_null_weights,
)
from processing.weighting.validation.weight_checks import (
    _check_hierarchy,
    check_days_carry_their_person,
    check_persons_carry_their_household,
    check_weight_placement,
)

logger = logging.getLogger(__name__)


@step()
def add_existing_weights(  # noqa: C901, PLR0912, PLR0915
    weights: dict[str, ExistingWeightConfig | dict],
    existing: ExistingWeightsConfig,
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

        existing: How supplied weights move onto usable records, per
            profile. See ``ExistingWeightsConfig``.
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

    profiles = resolve_fitted_profiles(existing.weight_profiles)

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

        # Land on the canonical name here so what was read is uniform whatever the
        # vendor called it; `attach_supplied` renames to the column each pass
        # writes.
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
    limits = existing.max_weight_factor or {}
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
        flag = usable_col_for(profile)

        has_weight = attach_supplied(tables, supplied, validated_weights, profile)
        log_usable_supplied_zero(tables, has_weight, usability_flag_col=flag)

        # Populations first. Usability is stamped upstream by
        # ``cascade_completeness``. Households scale by size; persons copy their
        # household; days take their person's weight.
        population = {t: c for t, c in has_weight.items() if t in POPULATION_TABLES}
        supplied_totals: dict[str, float] = {}
        households_df = tables.get("households")
        by_size = (
            "households" in population
            and households_df is not None
            and flag in households_df.columns
        )
        days_follow = "days" in population and "persons" in population
        if by_size or days_follow:
            mark_persons_who_can_carry(tables, has_weight, supplied.keys(), usability_flag_col=flag)
        if by_size:
            population.pop("households")
            supplied_totals["households"] = gate_households_by_size(
                tables,
                has_weight,
                usability_flag_col=flag,
                top_group=existing.household_size_top_group,
            )
            if "persons" in population:
                population.pop("persons")
                persons_follow_household(tables, has_weight)
        if days_follow:
            # Held person by person below, which holds the day total to the
            # person total rather than to the supplier's day total.
            population.pop("days")
            if "persons" in population:
                # Persons supplied without households: the older rule, within the
                # household then region-wide.
                supplied_totals |= apply_usability(
                    tables, {"persons": population.pop("persons")}, usability_flag_col=CARRIES
                )
        supplied_totals |= apply_usability(tables, population, usability_flag_col=flag)
        persons_df = tables.get("persons")
        if persons_df is not None and CARRIES in persons_df.columns:
            tables["persons"] = persons_df.drop(CARRIES)
        if days_follow:
            follow_parent(tables, has_weight, "days", usability_flag_col=flag)

        if existing.derive_missing_weights:
            propagate_weights(
                tables,
                has_weight,
                skip=provided_weights | TRAVEL_TABLES | JOINT_TABLES,
                usability_flag_col=flag,
                profile=profile,
            )

        if {"unlinked_trips", "days"} <= supplied.keys():
            # Unlinked trips move with their day. Their total is not a target: a
            # dropped day's weight lands on the person's other days, whose trip
            # counts differ, so it is not held.
            trips_follow_their_day(
                tables,
                has_weight,
                supplied["days"],
                usability_flag_col=flag,
                preserve_travel_totals=existing.preserve_travel_totals,
            )
            if existing.derive_missing_weights:
                # Linked trips and tours: the mean of their final trips, so each
                # agrees with its own trips.
                propagate_weights(
                    tables,
                    has_weight,
                    skip=provided_weights | POPULATION_TABLES | JOINT_TABLES,
                    usability_flag_col=flag,
                    profile=profile,
                )
            # A linked trip or tour weight supplied as a file is gated on its own.
            to_gate = {
                t: c
                for t, c in has_weight.items()
                if t in supplied.keys() & TRAVEL_TABLES - {"unlinked_trips"}
            }
        else:
            if existing.derive_missing_weights:
                # Travel claims: derived from the gated days, but not gated themselves.
                propagate_weights(
                    tables,
                    has_weight,
                    skip=provided_weights | POPULATION_TABLES | JOINT_TABLES,
                    usability_flag_col=None,
                    profile=profile,
                )
            to_gate = has_weight

        supplied_totals |= gate_travel(
            tables,
            to_gate,
            usability_flag_col=flag,
            preserve_travel_totals=existing.preserve_travel_totals,
        )

        if existing.derive_missing_weights:
            # Joint tables last: the sum of the members that kept their weight.
            propagate_weights(
                tables,
                has_weight,
                skip=provided_weights | POPULATION_TABLES | TRAVEL_TABLES,
                usability_flag_col=flag,
                profile=profile,
            )

        warn_null_weights(tables, profile)
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
        check_weight_factors(tables, has_weight, supplied, limits)
        # Every supplied total still there, and all of it on records the
        # formatter will keep -- or the run stops here, not in the model.
        check_weight_placement(
            non_null_tables(tables), flag, profile, supplied_totals=supplied_totals
        )

    drop_unsuffixed_weights(tables)

    if canonical_data is not None:
        for table, described in describe_weight_columns(profiles).items():
            canonical_data.register_generated_columns(table, described)

    logger.info(
        "Weight attachment complete for %s. Tables with weights: %s",
        ", ".join(profiles),
        sorted(supplied),
    )

    return {name: df for name, df in tables.items() if df is not None}
