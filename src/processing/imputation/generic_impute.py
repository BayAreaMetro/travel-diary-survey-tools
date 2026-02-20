"""Generic imputation step using KNN and MICE methods."""

import logging
from typing import Any

import polars as pl

from pipeline.decoration import step

from .flags import create_flag_columns
from .knn import impute_knn
from .mice import impute_mice
from .validation import (
    log_validation_results,
    validate_knn_imputation,
    validate_mice_imputation,
)

logger = logging.getLogger(__name__)


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
        knn_columns: Dict mapping table names to list of KNN column configs
        mice_groups: Dict mapping table names to list of MICE column group configs
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
          unlinked_trips:
            - column: mode
              n_neighbors: 5
              neighbor_weights: distance
        mice_groups:
          unlinked_trips:
            - columns: [depart_hour, arrive_hour, duration]
              max_iter: 10
            - columns: [origin_lat, origin_lon]
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

    # Process each table
    result_tables = {}

    for table_name, df in tables.items():
        logger.info("Processing table: %s", table_name)
        original_df = df.clone()
        current_df = df
        imputed_columns = []

        # KNN imputation
        if knn_columns and table_name in knn_columns:
            logger.info("  KNN imputation for %s", table_name)
            for col_config in knn_columns[table_name]:
                column = col_config["column"]
                n_neighbors = col_config.get("n_neighbors", 5)
                neighbor_weights = col_config.get("neighbor_weights", "distance")

                current_df, _ = impute_knn(
                    current_df, column, n_neighbors, neighbor_weights, random_state
                )
                imputed_columns.append(column)

                # Optional validation
                if validate_imputation and validate_imputation.get("enabled", False):
                    n_folds = validate_imputation.get("n_folds", 5)
                    sample_pct = validate_imputation.get("sample_pct", 5.0)

                    metrics = validate_knn_imputation(
                        original_df,
                        column,
                        n_folds,
                        sample_pct,
                        n_neighbors,
                        neighbor_weights,
                        random_state,
                    )
                    log_validation_results(metrics)

        # MICE imputation
        if mice_groups and table_name in mice_groups:
            logger.info("  MICE imputation for %s", table_name)
            for group_config in mice_groups[table_name]:
                columns = group_config["columns"]
                max_iter = group_config.get("max_iter", 10)

                current_df, _ = impute_mice(current_df, columns, max_iter, random_state)
                imputed_columns.extend(columns)

                # Optional validation
                if validate_imputation and validate_imputation.get("enabled", False):
                    n_folds = validate_imputation.get("n_folds", 5)
                    sample_pct = validate_imputation.get("sample_pct", 5.0)

                    metrics = validate_mice_imputation(
                        original_df, columns, n_folds, sample_pct, max_iter, random_state
                    )
                    log_validation_results(metrics)

        # Create flag columns if requested
        if create_flags and imputed_columns:
            logger.info("  Creating imputation flags for %d columns", len(imputed_columns))
            current_df = create_flag_columns(current_df, original_df, imputed_columns)

        result_tables[table_name] = current_df

    logger.info("Imputation complete for %d tables", len(result_tables))
    return result_tables
