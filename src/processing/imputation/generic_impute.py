"""Generic imputation step using KNN and MICE methods."""

import logging
from typing import Any

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
from utils.enum_helpers import resolve_enum_labels

logger = logging.getLogger(__name__)


def _process_knn_imputation(
    df: pl.DataFrame,
    original_df: pl.DataFrame,
    table_name: str,
    knn_configs: list[dict[str, Any]],
    validate_imputation: dict[str, Any] | None,
    random_state: int | None,
) -> tuple[pl.DataFrame, list[str]]:
    """Process KNN imputation for a table.

    Args:
        df: Current DataFrame to impute
        original_df: Original DataFrame (for validation)
        table_name: Name of the table
        knn_configs: List of KNN column configurations
        validate_imputation: Optional validation config
        random_state: Random seed for reproducibility

    Returns:
        Tuple of (imputed_df, list of imputed column names)
    """
    imputed_columns = []

    for col_config in knn_configs:
        column = col_config["column"]
        missing_value_labels = col_config.get("missing_values", [])
        n_neighbors = col_config.get("n_neighbors", 5)
        neighbor_weights = col_config.get("neighbor_weights", "distance")
        numeric_features = col_config.get("numeric_features")
        categorical_features = col_config.get("categorical_features")

        if not numeric_features and not categorical_features:
            msg = (
                f"Column '{column}': At least one of numeric_features "
                "or categorical_features required"
            )
            raise ValueError(msg)

        # Prepare column: replace enum-labeled missing values with null
        df, _ = _prepare_column_for_imputation(df, table_name, column, missing_value_labels)

        # Perform KNN imputation
        df, _ = impute_knn(
            df, column, n_neighbors, neighbor_weights, numeric_features, categorical_features
        )
        imputed_columns.append(column)

        # Optional validation
        if validate_imputation and validate_imputation.get("enabled", False):
            logger.info("Validating KNN imputation for %s.%s", table_name, column)
            n_folds = validate_imputation.get("n_folds", 5)
            sample_pct = validate_imputation.get("sample_pct", 5.0)

            # Prepare original data for validation
            original_prepared, _ = _prepare_column_for_imputation(
                original_df, table_name, column, missing_value_labels
            )

            metrics = validate_knn_imputation(
                original_prepared,
                column,
                n_folds,
                sample_pct,
                n_neighbors,
                neighbor_weights,
                random_state,
                numeric_features,
                categorical_features,
            )
            log_validation_results(metrics)

    return df, imputed_columns


def _process_mice_imputation(
    df: pl.DataFrame,
    original_df: pl.DataFrame,
    table_name: str,
    mice_configs: list[dict[str, Any]],
    validate_imputation: dict[str, Any] | None,
    random_state: int | None,
) -> tuple[pl.DataFrame, list[str]]:
    """Process MICE imputation for a table.

    Args:
        df: Current DataFrame to impute
        original_df: Original DataFrame (for validation)
        table_name: Name of the table
        mice_configs: List of MICE group configurations
        validate_imputation: Optional validation config
        random_state: Random seed for reproducibility

    Returns:
        Tuple of (imputed_df, list of imputed column names)
    """
    imputed_columns = []

    for group_config in mice_configs:
        columns = group_config["columns"]
        max_iter = group_config.get("max_iter", 10)
        missing_values_config = group_config.get("missing_values", {})

        # Prepare columns: replace enum-labeled missing values with null
        for column in columns:
            if isinstance(missing_values_config, dict):
                missing_value_labels = missing_values_config.get(column, [])
            else:
                missing_value_labels = missing_values_config

            if missing_value_labels:
                df, _ = _prepare_column_for_imputation(df, table_name, column, missing_value_labels)

        # Perform MICE imputation
        df, _ = impute_mice(df, columns, max_iter, random_state)
        imputed_columns.extend(columns)

        # Optional validation
        if validate_imputation and validate_imputation.get("enabled", False):
            n_folds = validate_imputation.get("n_folds", 5)
            sample_pct = validate_imputation.get("sample_pct", 5.0)

            # Prepare original data for validation
            original_prepared = original_df.clone()
            for column in columns:
                if isinstance(missing_values_config, dict):
                    missing_value_labels = missing_values_config.get(column, [])
                else:
                    missing_value_labels = missing_values_config

                if missing_value_labels:
                    original_prepared, _ = _prepare_column_for_imputation(
                        original_prepared, table_name, column, missing_value_labels
                    )

            metrics = validate_mice_imputation(
                original_prepared, columns, n_folds, sample_pct, max_iter, random_state
            )
            log_validation_results(metrics)

    return df, imputed_columns


def _prepare_column_for_imputation(
    df: pl.DataFrame,
    table_name: str,
    column: str,
    missing_value_labels: list[str] | None = None,
) -> tuple[pl.DataFrame, list[Any]]:
    """Prepare a column for imputation by replacing missing values with null.

    This function resolves enum labels to their numeric values and replaces
    them with null, making the column ready for imputation algorithms.

    Args:
        df: DataFrame containing the column
        table_name: Name of the table (for enum resolution)
        column: Column name to prepare
        missing_value_labels: Optional list of enum labels to treat as missing
                            (e.g., ['MISSING', 'PNTA'])

    Returns:
        Tuple of (prepared_df, resolved_values) where resolved_values is the list
        of numeric/string values that were replaced with null

    Example:
        >>> df = pl.DataFrame({'income_broad': [1, 2, 995, 999, 3]})
        >>> df_prep, values = prepare_column_for_imputation(
        ...     df, 'households', 'income_broad', ['MISSING', 'PNTA']
        ... )
        >>> values
        [995, 999]
    """
    if not missing_value_labels:
        return df, []

    # Resolve enum labels to values
    missing_values = resolve_enum_labels(table_name, column, missing_value_labels)

    if not missing_values:
        logger.warning(
            "No missing values resolved for column '%s' in table '%s'",
            column,
            table_name,
        )
        return df, []

    # Log what we're doing
    logger.info(
        "Replacing missing values %s with null for column '%s' (from labels %s)",
        missing_values,
        column,
        missing_value_labels,
    )

    # Replace missing values with null inline
    expr = pl.col(column)
    for value in missing_values:
        expr = expr.replace(value, None)
    df_prepared = df.with_columns(expr)

    return df_prepared, missing_values


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
                current_dfs[table_name], knn_cols = _process_knn_imputation(
                    current_dfs[table_name],
                    originals[table_name],
                    table_name,
                    knn_columns[table_name],
                    validate_imputation,
                    random_state,
                )
                all_imputed_columns[table_name].extend(knn_cols)

    # Then process ALL MICE imputation (across all tables)
    if mice_groups:
        logger.info("Phase 2: MICE imputation")
        for table_name in tables:
            if table_name in mice_groups:
                logger.info("  MICE imputation for %s", table_name)
                current_dfs[table_name], mice_cols = _process_mice_imputation(
                    current_dfs[table_name],
                    originals[table_name],
                    table_name,
                    mice_groups[table_name],
                    validate_imputation,
                    random_state,
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
