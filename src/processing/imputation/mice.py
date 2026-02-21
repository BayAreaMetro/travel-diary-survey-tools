"""MICE-based imputation for missing values."""

import logging
from typing import Any

import numpy as np
import polars as pl
from sklearn.experimental import enable_iterative_imputer  # noqa: F401
from sklearn.impute import IterativeImputer

from .impute_utils import validate_features_exist

logger = logging.getLogger(__name__)


def impute_mice(
    df: pl.DataFrame,
    columns: list[str],
    max_iter: int = 10,
    random_state: int | None = None,
    numeric_features: list[str] | None = None,
    categorical_features: list[str] | None = None,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Impute missing values in multiple correlated columns using MICE.

    MICE (Multiple Imputation by Chained Equations) imputes multiple correlated
    variables together (e.g., race, ethnicity, or time-related columns).

    Args:
        df: DataFrame containing the columns to impute
        columns: List of column names to impute together
        max_iter: Maximum number of imputation rounds
        random_state: Random state for reproducibility
        numeric_features: List of numeric/continuous feature columns
        categorical_features: List of categorical features (one-hot encoded)

    Returns:
        Tuple of (imputed_df, stats_dict) with per-column statistics
    """
    # Validate inputs
    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        msg = f"Columns not found in DataFrame: {missing_cols}"
        raise ValueError(msg)

    validate_features_exist(df, numeric_features, categorical_features)

    # Initialize statistics
    stats = {
        col: {"n_missing": df[col].null_count(), "n_imputed": 0, "pct_imputed": 0.0}
        for col in columns
    }
    total_missing = sum(s["n_missing"] for s in stats.values())

    if total_missing == 0:
        logger.info("Columns %s: No missing values, skipping imputation", columns)
        return df, stats

    # Build and impute feature matrix
    feature_matrix, column_indices = _build_feature_matrix(
        df, columns, numeric_features or [], categorical_features or []
    )
    imputer = IterativeImputer(max_iter=max_iter, random_state=random_state, verbose=0)
    imputed_matrix = imputer.fit_transform(feature_matrix)

    # Update DataFrame with imputed values
    df_imputed = df
    for col, col_idx in column_indices.items():
        imputed_values = imputed_matrix[:, col_idx]
        original_dtype = df[col].dtype

        imputed_series = pl.Series(col, imputed_values)
        if original_dtype.is_integer():
            imputed_series = imputed_series.round().cast(original_dtype)

        df_imputed = df_imputed.with_columns(imputed_series)

        n_missing = stats[col]["n_missing"]
        stats[col]["n_imputed"] = n_missing
        stats[col]["pct_imputed"] = (n_missing / len(df)) * 100

    # Log results
    _log_imputation_results(columns, stats, len(df), max_iter)
    return df_imputed, stats


def _build_feature_matrix(
    df: pl.DataFrame,
    columns: list[str],
    numeric_features: list[str],
    categorical_features: list[str],
) -> tuple[np.ndarray, dict[str, int]]:
    """Build feature matrix for MICE imputation."""
    matrices = []

    # Add continuous features
    continuous = [f for f in numeric_features if f in df.columns and df[f].dtype.is_numeric()]
    continuous.extend(
        [
            col
            for col in columns
            if col not in continuous and col in df.columns and df[col].dtype.is_numeric()
        ]
    )

    if continuous:
        matrices.append(df.select(continuous).to_numpy())

    column_indices = {col: continuous.index(col) for col in columns if col in continuous}

    # One-hot encode categorical features (excluding target columns)
    categorical = [
        f
        for f in categorical_features
        if f in df.columns and f not in columns and df[f].dtype.is_numeric()
    ]

    if categorical:
        for cat_col in categorical:
            unique_vals = df[cat_col].drop_nulls().unique().sort().to_list()
            matrices.extend(
                [
                    (df[cat_col] == val).cast(pl.Float64).to_numpy().reshape(-1, 1)
                    for val in unique_vals
                ]
            )

    return np.hstack(matrices) if len(matrices) > 1 else matrices[0], column_indices


def _log_imputation_results(
    columns: list[str], stats: dict[str, Any], n_total: int, max_iter: int
) -> None:
    """Log MICE imputation results."""
    imputed_cols = [col for col in columns if stats[col]["n_imputed"] > 0]
    if imputed_cols:
        logger.info("Columns %s: Imputed using MICE (max_iter=%d)", imputed_cols, max_iter)
        for col in imputed_cols:
            s = stats[col]
            logger.info("  - %s: %d/%d (%.1f%%)", col, s["n_imputed"], n_total, s["pct_imputed"])
