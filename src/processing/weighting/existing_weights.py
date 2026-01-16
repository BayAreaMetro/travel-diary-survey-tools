"""Simply concatenate existing weights to the data."""

import logging

import polars as pl

from pipeline.decoration import step

logger = logging.getLogger(__name__)

# Strict mapping of config keys to (table_name, id_column, weight_column)
DEFAULT_WEIGHT_CONFIG_MAPPING = {
    "household_weights": ("households", "hh_id", "hh_weight"),
    "person_weights": ("persons", "person_id", "person_weight"),
    "day_weights": ("days", "day_id", "day_weight"),
    "unlinked_trip_weights": ("unlinked_trips", "unlinked_trip_id", "unlinked_trip_weight"),
    "linked_trip_weights": ("linked_trips", "linked_trip_id", "linked_trip_weight"),
    "joint_trip_weights": ("joint_trips", "joint_trip_id", "joint_trip_weight"),
    "tour_weights": ("tours", "tour_id", "tour_weight"),
}


def _derive_missing_weights(
    tables: dict[str, pl.DataFrame | None],
    provided_weights: set[str],
    has_weight: dict[str, str],
) -> None:
    """Derive missing weights from upstream tables. Modifies tables and has_weight in place."""
    # Get weight column names from mapping
    weight_cols = {table: col for table, _, col in DEFAULT_WEIGHT_CONFIG_MAPPING.values()}

    # Hierarchy for simple carry-forward derivation
    hierarchy = [
        ("persons", "households", "hh_id"),
        ("days", "persons", "person_id"),
        ("unlinked_trips", "days", "day_id"),
    ]

    # Carry forward weights through hierarchy
    for child_table, parent_table, join_key in hierarchy:
        child_df = tables.get(child_table)
        parent_df = tables.get(parent_table)

        # If child table is missing or already has weight, skip
        if child_df is None or child_table in provided_weights:
            continue

        # Check that parent has weight to carry forward
        if parent_table not in has_weight:
            msg = (
                f"Cannot derive {weight_cols[child_table]} for {child_table}: "
                f"parent table {parent_table} does not have {weight_cols[parent_table]}"
            )
            raise ValueError(msg)

        # Check that parent table exists
        if parent_df is None:
            msg = (
                f"Cannot derive {weight_cols[child_table]} for {child_table}: "
                f"parent table {parent_table} is None"
            )
            raise ValueError(msg)

        # Get weight column names, allow for different names in weight files
        parent_weight = has_weight[parent_table]
        child_weight = weight_cols[child_table]

        # Carry forward weight from parent to child via join
        logger.info("Deriving %s from %s via %s", child_weight, parent_weight, join_key)

        if join_key not in child_df.columns:
            msg = f"Cannot derive weight: {child_table} missing join key {join_key}"
            raise ValueError(msg)

        weight_to_carry = parent_df.select([join_key, parent_weight]).rename(
            {parent_weight: child_weight}
        )
        tables[child_table] = child_df.join(weight_to_carry, on=join_key, how="left")
        has_weight[child_table] = child_weight

    # Derive aggregated weights
    aggregations = [
        ("linked_trips", "unlinked_trips", "linked_trip_id"),
        ("joint_trips", "linked_trips", "joint_trip_id"),
        ("tours", "linked_trips", "tour_id"),
    ]

    for target_table, source_table, group_key in aggregations:
        target_df = tables.get(target_table)
        source_df = tables.get(source_table)

        if target_df is None or target_table in provided_weights:
            continue

        if source_table not in has_weight or source_df is None:
            continue

        source_weight = has_weight[source_table]
        target_weight = weight_cols[target_table]

        # Derive weight by aggregating (mean) from source, excluding nulls and zeros
        logger.info("Deriving %s from mean of %s", target_weight, source_weight)

        if group_key not in source_df.columns:
            msg = f"Cannot derive {target_weight}: source missing {group_key}"
            raise ValueError(msg)

        aggregated = source_df.group_by(group_key).agg(
            pl.col(source_weight)
            .filter(pl.col(source_weight).is_not_null() & (pl.col(source_weight) != 0))
            .mean()
            .alias(target_weight)
        )
        tables[target_table] = target_df.join(aggregated, on=group_key, how="left")
        has_weight[target_table] = target_weight


@step()
def add_existing_weights(
    weights: dict[str, dict[str, str]],
    derive_missing_weights: bool = False,
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    days: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
    joint_trips: pl.DataFrame | None = None,
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

    Weight hierarchy logic:
     - household_weight
        - person_weight <- household_weight for each person in household
            - day_weight <- person_weight for day for each person
                - unlinked_trip_weight <- day_weight for each trip for each person-day
                    - linked_trip_weight <- Average weight of unlinked_trips
                    - joint_trip_weight <- Average weight of linked joint trips
                        - tour_weight <- Average of linked trip weights

    Note that if there are no "adjustments" made to sub-table weights (e.g., person or trip), then
    all weights should actually be exactly same from household through tour.

    If sub-table weights do vary, a checksum can validate integrity:
    - sum(person_weight) == sum(household_weight * num_persons)
    - sum(day_weight) == sum(person_weight * num_complete_days)
    - sum(unlinked_trip_weight) == sum(day_weight * num_trips)
    - sum(linked_trip_weight) == sum(unlinked_trip_weight)
    - sum(tour_weight) == sum(linked_trip_weight)

    Args:
        weights: A dict mapping config keys to weight file paths.
        households: Households DataFrame
        persons: Persons DataFrame
        days: Days DataFrame
        unlinked_trips: Unlinked trips DataFrame
        linked_trips: Linked trips DataFrame
        tours: Tours DataFrame
        joint_trips: Joint trips DataFrame
        derive_missing_weights: Whether to derive missing weights from upstream tables
    Returns:
        Dict of tables with attached weights.
    """
    # Collect all provided tables
    tables = {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "joint_trips": joint_trips,
        "tours": tours,
    }

    # Track which weights are provided and which tables have weights
    provided_weights = set()
    has_weight = {}

    # Load and join provided weight files
    for config_key, cfg in weights.items():
        if config_key not in DEFAULT_WEIGHT_CONFIG_MAPPING:
            msg = (
                f"Invalid weight config key: {config_key}. "
                f"Must be one of {list(DEFAULT_WEIGHT_CONFIG_MAPPING.keys())}"
            )
            raise ValueError(msg)

        # Get defaults from mapping
        table_name, default_id_col, default_weight_col = DEFAULT_WEIGHT_CONFIG_MAPPING[config_key]

        # Override with config values if provided
        file_path = cfg.get("weight_path")
        table_id_col = cfg.get("id_col", default_id_col)  # ID column in main table
        weight_id_col = cfg.get("weight_id_col", table_id_col)  # ID column in weight file
        weight_col = cfg.get("weight_col", default_weight_col)

        if file_path is None:
            msg = f"Missing required 'weight_path' for {config_key}"
            raise ValueError(msg)

        provided_weights.add(table_name)

        df = tables.get(table_name)
        if df is None:
            logger.warning("Weight file provided for %s but table not found in data", table_name)
            continue

        # Load and join weight file
        logger.info("Loading weights from %s for %s", file_path, table_name)
        weight_df = pl.read_csv(file_path)

        # Validate required columns exist
        if weight_id_col not in weight_df.columns:
            msg = f"Weight file {file_path} missing required ID column: {weight_id_col}"
            raise ValueError(msg)
        if weight_col not in weight_df.columns:
            msg = f"Weight file {file_path} missing required weight column: {weight_col}"
            raise ValueError(msg)
        if table_id_col not in df.columns:
            msg = f"Table {table_name} missing required ID column: {table_id_col}"
            raise ValueError(msg)

        # Join, handling potential ID column name mismatch
        if weight_id_col != table_id_col:
            # Rename weight file ID column to match table
            weight_df = weight_df.rename({weight_id_col: table_id_col})

        logger.info("Joined %s to %s on %s", weight_col, table_name, table_id_col)
        tables[table_name] = df.join(weight_df, on=table_id_col, how="left")
        has_weight[table_name] = weight_col

    # Derive missing weights if requested
    if derive_missing_weights:
        _derive_missing_weights(tables, provided_weights, has_weight)

    # Build results dict, excluding None values and internal tables
    results = {name: df for name, df in tables.items() if df is not None}

    logger.info("Weight attachment complete. Tables with weights: %s", list(has_weight.keys()))

    return results
