"""KNN-based imputation for missing values."""

import logging
from typing import Any, Literal

import polars as pl
from sklearn.impute import KNNImputer

from .impute_utils import (
    build_feature_matrix,
    decode_dense_to_integer,
    encode_integer_categoricals,
    validate_features_exist,
)

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

    validate_features_exist(df, numeric_features, categorical_features)

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

    # Encode non-contiguous integer codes to dense 0..N for distance calc
    df_work, int_encodings = encode_integer_categoricals(df, [column])

    # Build feature matrix (shared helper)
    feature_matrix, column_indices = build_feature_matrix(
        df_work, [column], numeric_features or [], categorical_features or []
    )
    target_idx = column_indices[column]

    # Run KNN imputation
    imputer = KNNImputer(n_neighbors=n_neighbors, weights=neighbor_weights)
    imputed_values = imputer.fit_transform(feature_matrix)[:, target_idx]

    # Decode back to original codes
    if column in int_encodings:
        decoded = decode_dense_to_integer(imputed_values, int_encodings[column])
        imputed_series = pl.Series(column, decoded).cast(original_dtype)
    elif original_dtype.is_integer():
        imputed_series = pl.Series(column, imputed_values).round().cast(original_dtype)
    else:
        imputed_series = pl.Series(column, imputed_values)

    return df.with_columns(imputed_series), {
        "n_missing": n_missing,
        "n_imputed": n_missing,
        "pct_imputed": pct_imputed,
    }
