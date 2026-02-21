"""Internal validation utilities for imputation methods."""

import logging
from typing import Any

import polars as pl

from utils.enum_helpers import resolve_enum_labels

logger = logging.getLogger(__name__)


def validate_features_exist(
    df: pl.DataFrame,
    numeric_features: list[str] | None = None,
    categorical_features: list[str] | None = None,
) -> None:
    """Validate that at least one feature type is specified and all features exist.

    Args:
        df: DataFrame to validate against
        numeric_features: Optional list of numeric feature column names
        categorical_features: Optional list of categorical feature column names

    Raises:
        ValueError: If no features are specified or if any features are missing
    """
    if not numeric_features and not categorical_features:
        msg = "At least one of numeric_features or categorical_features must be specified"
        raise ValueError(msg)

    # Validate that all specified features exist in DataFrame
    all_features = set(numeric_features or []) | set(categorical_features or [])
    missing_features = all_features - set(df.columns)
    if missing_features:
        msg = f"Features not found in DataFrame: {missing_features}"
        raise ValueError(msg)


def prepare_column_for_imputation(
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
