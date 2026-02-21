"""KNN-based imputation for missing values."""

import logging
from typing import Any, Literal

import numpy as np
import polars as pl
from sklearn.impute import KNNImputer

from .impute_utils import validate_features_exist

logger = logging.getLogger(__name__)


def impute_knn(
    df: pl.DataFrame,
    column: str,
    n_neighbors: int = 5,
    neighbor_weights: Literal["uniform", "distance"] = "distance",
    numeric_features: list[str] | None = None,
    categorical_features: list[str] | None = None,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Impute missing values in a single column using K-Nearest Neighbors.

    Args:
        df: DataFrame containing the column to impute
        column: Name of the column to impute
        n_neighbors: Number of neighbors to use for imputation
        neighbor_weights: How to weight neighbors ('uniform' or 'distance')
        numeric_features: List of numeric/continuous feature columns
        categorical_features: List of categorical features (one-hot encoded for distance)

    Returns:
        Tuple of (imputed_df, stats_dict) with imputation statistics
    """
    if column not in df.columns:
        msg = f"Column '{column}' not found in DataFrame"
        raise ValueError(msg)

    # Validate features
    validate_features_exist(df, numeric_features, categorical_features)
    numeric_features = numeric_features or []
    categorical_features = categorical_features or []

    # Count missing values and handle edge cases
    n_missing = df[column].null_count()
    n_total = len(df)
    pct_imputed = (n_missing / n_total) * 100

    if n_missing == 0:
        logger.info("Column '%s': No missing values, skipping imputation", column)
        return df, {"n_missing": 0, "n_imputed": 0, "pct_imputed": 0.0}

    if n_missing == n_total:
        logger.warning("Column '%s': All values are missing, cannot impute", column)
        return df, {"n_missing": n_missing, "n_imputed": 0, "pct_imputed": 100.0}

    original_dtype = df[column].dtype

    # Build feature matrix: continuous features
    continuous = [f for f in numeric_features if f in df.columns and df[f].dtype.is_numeric()]
    if column not in continuous:
        continuous.append(column)

    matrices = [df.select(continuous).to_numpy()]
    target_idx = continuous.index(column)

    # Add one-hot encoded categorical features
    categorical = [
        f
        for f in categorical_features
        if f in df.columns and df[f].dtype.is_numeric() and f != column
    ]

    for cat_col in categorical:
        unique_vals = df[cat_col].drop_nulls().unique().sort().to_list()
        one_hot_cols = [
            (df[cat_col] == val).cast(pl.Float64).to_numpy().reshape(-1, 1) for val in unique_vals
        ]
        matrices.extend(one_hot_cols)

    feature_matrix = np.hstack(matrices) if len(matrices) > 1 else matrices[0]

    # Run KNN imputation and cast back to original dtype
    imputer = KNNImputer(n_neighbors=n_neighbors, weights=neighbor_weights)
    imputed_values = imputer.fit_transform(feature_matrix)[:, target_idx]

    imputed_series = (
        pl.Series(column, imputed_values).round().cast(original_dtype)
        if original_dtype.is_integer()
        else pl.Series(column, imputed_values)
    )

    return df.with_columns(imputed_series), {
        "n_missing": n_missing,
        "n_imputed": n_missing,
        "pct_imputed": pct_imputed,
    }
