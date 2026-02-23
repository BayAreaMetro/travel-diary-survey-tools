"""Generic imputation step using KNN and MICE methods."""

import logging
from typing import Any, Literal

import polars as pl

from pipeline.decoration import step
from processing.imputation.flags import create_flag_columns
from processing.imputation.knn import impute_knn
from processing.imputation.mice import impute_mice
from processing.imputation.validation import (
    log_validation_results,
    validate_knn_imputation,
    validate_mice_imputation,
)

from .impute_utils import (
    add_household_agg_features,
    aggregate_from_children,
    join_parent_tables,
    log_imputation_stats,
    prepare_column_for_imputation,
    strip_joined_columns,
)

logger = logging.getLogger(__name__)


def _enrich_dataframe(
    df: pl.DataFrame,
    table_name: str,
    tables: dict[str, pl.DataFrame] | None,
    join_tables_list: list[str],
    target_columns: list[str],
    categorical_features: list[str] | None,
    aggregate_from_config: dict[str, dict[str, list[str]]] | None = None,
) -> tuple[pl.DataFrame, list[str], list[str] | None]:
    """Enrich a DataFrame with parent joins and child aggregations.

    Handles both directions:
      - Parent→child joins via ``join_tables`` config
      - Child→parent aggregations via ``aggregate_from`` config

    Returns:
        Tuple of (enriched_df, added_column_names, updated_categorical_features)
    """
    added_columns: list[str] = []

    # Parent joins (e.g. persons joining household columns)
    if join_tables_list and tables:
        df, joined_cols = join_parent_tables(df, table_name, tables, join_tables_list)
        added_columns.extend(joined_cols)

        df, agg_cols = add_household_agg_features(df, target_columns)
        added_columns.extend(agg_cols)
        if agg_cols:
            categorical_features = list(categorical_features or []) + agg_cols

    # Child aggregations (e.g. households aggregating from persons)
    if aggregate_from_config and tables:
        df, child_cols = aggregate_from_children(df, table_name, tables, aggregate_from_config)
        added_columns.extend(child_cols)
        if child_cols:
            categorical_features = list(categorical_features or []) + child_cols

    return df, added_columns, categorical_features


def _process_imputation(
    df: pl.DataFrame,
    original_df: pl.DataFrame,
    table_name: str,
    configs: list[dict[str, Any]],
    method: Literal["knn", "mice"],
    validate_imputation: dict[str, Any] | None,
    random_state: int | None,
    tables: dict[str, pl.DataFrame] | None = None,
) -> tuple[pl.DataFrame, list[str]]:
    """Process imputation for a table using the specified method.

    Shared scaffold: enrich → prepare missing → impute → strip → validate.
    The *method* parameter selects the imputation strategy (KNN or MICE).

    Args:
        df: Current DataFrame to impute
        original_df: Original DataFrame (for validation)
        table_name: Name of the table
        configs: List of column/group configurations
        method: Imputation method - ``"knn"`` or ``"mice"``
        validate_imputation: Optional validation config
        random_state: Random seed for reproducibility
        tables: Dict of all canonical tables (for cross-table joins)

    Returns:
        Tuple of (imputed_df, list of imputed column names)
    """
    imputed_columns: list[str] = []

    for config in configs:
        # Normalise target columns: KNN uses 'column', MICE uses 'columns'
        target_columns = [config["column"]] if method == "knn" else config["columns"]

        # Common config extraction
        numeric_features = config.get("numeric_features")
        categorical_features = config.get("categorical_features")
        join_tables_list = config.get("join_tables", [])
        aggregate_from_config = config.get("aggregate_from")
        missing_values_config = config.get("missing_values", [] if method == "knn" else {})

        if not numeric_features and not categorical_features:
            label = (
                f"Column '{target_columns[0]}'" if method == "knn" else f"Columns {target_columns}"
            )
            msg = f"{label}: At least one of numeric_features or categorical_features required"
            raise ValueError(msg)

        # 1. Enrich (parent joins + child aggregations)
        df, added_columns, categorical_features = _enrich_dataframe(
            df,
            table_name,
            tables,
            join_tables_list,
            target_columns,
            categorical_features,
            aggregate_from_config,
        )

        # 2. Prepare missing values (replace enum labels with null)
        for column in target_columns:
            labels = (
                missing_values_config.get(column, [])
                if isinstance(missing_values_config, dict)
                else missing_values_config
            )
            if labels:
                df, _ = prepare_column_for_imputation(df, table_name, column, labels)

        # 3. Impute (strategy dispatch)
        # NOTE: This is kind of hacky to hard-code, but its hard to conform the interfaces
        # Plus we may not need and infinite number of impute methods.
        if method == "knn":
            df, stats = impute_knn(
                df,
                target_columns[0],
                config.get("n_neighbors", 5),
                config.get("neighbor_weights", "distance"),
                numeric_features,
                categorical_features,
            )
        else:
            df, stats = impute_mice(
                df,
                target_columns,
                config.get("max_iter", 10),
                random_state,
                numeric_features,
                categorical_features,
            )

        log_imputation_stats(method.upper(), target_columns, stats, len(df))
        imputed_columns.extend(target_columns)

        # 4. Strip temporary joined columns
        if added_columns:
            df = strip_joined_columns(df, added_columns)

        # 5. Optional validation
        if validate_imputation and validate_imputation.get("enabled", False):
            _validate_config(
                original_df,
                table_name,
                target_columns,
                config,
                method,
                missing_values_config,
                validate_imputation,
                random_state,
                tables,
                categorical_features,
            )

    return df, imputed_columns


def _validate_config(
    original_df: pl.DataFrame,
    table_name: str,
    target_columns: list[str],
    config: dict[str, Any],
    method: Literal["knn", "mice"],
    missing_values_config: dict | list,
    validate_imputation: dict[str, Any],
    random_state: int | None,
    tables: dict[str, pl.DataFrame] | None,
    categorical_features: list[str] | None,
) -> None:
    """Run k-fold cross-validation for a single imputation config block."""
    n_folds = validate_imputation.get("n_folds", 5)
    sample_pct = validate_imputation.get("sample_pct", 5.0)
    join_tables_list = config.get("join_tables", [])
    aggregate_from_config = config.get("aggregate_from")
    numeric_features = config.get("numeric_features")

    # Prepare the original for validation (same enrichment as imputation)
    prepared = original_df.clone()
    for column in target_columns:
        labels = (
            missing_values_config.get(column, [])
            if isinstance(missing_values_config, dict)
            else missing_values_config
        )
        if labels:
            prepared, _ = prepare_column_for_imputation(prepared, table_name, column, labels)

    prepared, _, val_cat_features = _enrich_dataframe(
        prepared,
        table_name,
        tables,
        join_tables_list,
        target_columns,
        categorical_features,
        aggregate_from_config,
    )

    if method == "knn":
        logger.info("Validating KNN imputation for %s.%s", table_name, target_columns[0])
        metrics = validate_knn_imputation(
            prepared,
            target_columns[0],
            n_folds,
            sample_pct,
            config.get("n_neighbors", 5),
            config.get("neighbor_weights", "distance"),
            random_state,
            numeric_features,
            val_cat_features,
        )
    else:
        logger.info("Validating MICE imputation for %s.%s", table_name, target_columns)
        metrics = validate_mice_imputation(
            prepared,
            target_columns,
            n_folds,
            sample_pct,
            config.get("max_iter", 10),
            random_state,
            numeric_features,
            val_cat_features,
        )

    log_validation_results(metrics)


@step()
def imputation(
    # Optional canonical tables
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    days: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
    # Config parameters
    knn_columns: dict[str, list[dict[str, Any]]] | None = None,
    mice_groups: dict[str, list[dict[str, Any]]] | None = None,
    create_flags: bool = True,
    random_state: int | None = None,
    validate_imputation: dict[str, Any] | None = None,
) -> dict[str, pl.DataFrame]:
    """Impute missing values using KNN and/or MICE methods.

    Args:
        households: Households table (optional)
        persons: Persons table (optional)
        days: Days table (optional)
        unlinked_trips: Unlinked trips table (optional)
        linked_trips: Linked trips table (optional)
        tours: Tours table (optional)
        knn_columns: Dict mapping table names to list of KNN column configs.
            Each config can include:
            - column: Column name to impute
            - missing_values: List of enum labels to treat as missing (e.g., ['MISSING', 'PNTA'])
            - n_neighbors: Number of neighbors (default: 5)
            - neighbor_weights: 'uniform' or 'distance' (default: 'distance')
            - numeric_features: List of numeric/continuous feature columns (required)
            - categorical_features: Optional list of categorical feature columns to one-hot encode
                       Recommended to specify for performance with many columns
        mice_groups: Dict mapping table names to list of MICE column group configs.
            Each config can include:
            - columns: List of column names to impute together
            - missing_values: Dict mapping column names to lists of enum labels,
                            or a single list to apply to all columns
            - max_iter: Maximum iterations (default: 10)
        create_flags: Whether to create imputation flag columns (default: True)
        random_state: Random seed for reproducibility across all imputation (default: None)
        validate_imputation: Optional validation config with keys:
            - enabled: Whether to run validation (default: False)
            - n_folds: Number of k-folds (default: 5)
            - sample_pct: % of non-missing values to test (default: 5.0)

    Returns:
        Dictionary of imputed tables

    Example config:
        knn_columns:
          households:
            - column: income_broad
              missing_values: [MISSING, PNTA]  # Enum labels
              n_neighbors: 5
              neighbor_weights: distance
              numeric_features: [num_persons, num_vehicles, num_workers]  # Continuous features
          persons:
            - column: gender
              missing_values: [MISSING]
              n_neighbors: 5
              numeric_features: [age]  # Continuous features only
              categorical_features: [relationship, employment, occupation, education]
        mice_groups:
          persons:
            - columns: [race, ethnicity]
              missing_values:
                race: [MISSING]
                ethnicity: [MISSING, PNTA]
              max_iter: 10
        random_state: 42
        create_flags: true
        validate_imputation:
          enabled: true
          n_folds: 5
          sample_pct: 5.0
    """
    # Collect input tables
    tables = {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "tours": tours,
    }

    # Remove None tables
    tables = {name: df for name, df in tables.items() if df is not None}

    if not tables:
        logger.warning("No tables provided for imputation")
        return {}

    logger.info("Starting imputation for tables: %s", list(tables.keys()))

    # Clone all tables and track originals for validation/flags
    originals = {name: df.clone() for name, df in tables.items()}
    current_dfs = dict(tables.items())
    all_imputed_columns = {name: [] for name in tables}

    # Process ALL KNN imputation first (across all tables)
    if knn_columns:
        logger.info("Phase 1: KNN imputation")
        for table_name in tables:
            if table_name in knn_columns:
                current_dfs[table_name], knn_cols = _process_imputation(
                    current_dfs[table_name],
                    originals[table_name],
                    table_name,
                    knn_columns[table_name],
                    "knn",
                    validate_imputation,
                    random_state,
                    tables=current_dfs,
                )
                all_imputed_columns[table_name].extend(knn_cols)

    # Then process ALL MICE imputation (across all tables)
    if mice_groups:
        logger.info("Phase 2: MICE imputation")
        for table_name in tables:
            if table_name in mice_groups:
                logger.info("  MICE imputation for %s", table_name)
                current_dfs[table_name], mice_cols = _process_imputation(
                    current_dfs[table_name],
                    originals[table_name],
                    table_name,
                    mice_groups[table_name],
                    "mice",
                    validate_imputation,
                    random_state,
                    tables=current_dfs,
                )
                all_imputed_columns[table_name].extend(mice_cols)

    # Create flag columns for all tables
    result_tables = {}
    for table_name in tables:
        current_df = current_dfs[table_name]
        imputed_columns = all_imputed_columns[table_name]

        if create_flags and imputed_columns:
            logger.info(
                "Creating imputation flags for %s (%d columns)",
                table_name,
                len(imputed_columns),
            )
            current_df = create_flag_columns(current_df, originals[table_name], imputed_columns)

        result_tables[table_name] = current_df

    logger.info("Imputation complete for %d tables", len(result_tables))
    return result_tables
