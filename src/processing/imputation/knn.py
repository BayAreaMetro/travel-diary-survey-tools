"""KNN-based imputation for missing values."""

import logging
from typing import Any, Literal

import polars as pl
from sklearn.impute import KNNImputer

logger = logging.getLogger(__name__)


def impute_knn(
    df: pl.DataFrame,
    column: str,
    n_neighbors: int = 5,
    neighbor_weights: Literal["uniform", "distance"] = "distance",
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Impute missing values in a single column using K-Nearest Neighbors.

    Args:
        df: DataFrame containing the column to impute
        column: Name of the column to impute
        n_neighbors: Number of neighbors to use for imputation
        neighbor_weights: How to weight neighbors ('uniform' or 'distance')

    Returns:
        Tuple of (imputed_df, stats_dict) where stats_dict contains:
            - n_missing: Number of missing values
            - n_imputed: Number of values imputed
            - pct_imputed: Percentage of values imputed
    """
    if column not in df.columns:
        msg = f"Column '{column}' not found in DataFrame"
        raise ValueError(msg)

    # Count missing values before imputation
    n_missing = df[column].null_count()
    n_total = len(df)

    if n_missing == 0:
        logger.info("Column '%s': No missing values, skipping imputation", column)
        return df, {"n_missing": 0, "n_imputed": 0, "pct_imputed": 0.0}

    if n_missing == n_total:
        logger.warning("Column '%s': All values are missing, cannot impute", column)
        return df, {"n_missing": n_missing, "n_imputed": 0, "pct_imputed": 100.0}

    # Convert to numpy for sklearn (use all numeric columns as features)
    numeric_cols = [col for col in df.columns if df[col].dtype.is_numeric()]
    if not numeric_cols:
        msg = "No numeric columns available for KNN imputation"
        raise ValueError(msg)

    # Create feature matrix (use only numeric columns)
    feature_matrix = df.select(numeric_cols).to_numpy()

    # Get index of target column in feature matrix
    target_idx = numeric_cols.index(column)

    # Perform KNN imputation
    imputer = KNNImputer(n_neighbors=n_neighbors, weights=neighbor_weights)
    imputed_matrix = imputer.fit_transform(feature_matrix)

    # Extract imputed column
    imputed_values = imputed_matrix[:, target_idx]

    # Update DataFrame with imputed values
    df_imputed = df.with_columns(pl.Series(column, imputed_values))

    # Calculate statistics
    n_imputed = n_missing
    pct_imputed = (n_imputed / n_total) * 100

    stats = {
        "n_missing": n_missing,
        "n_imputed": n_imputed,
        "pct_imputed": pct_imputed,
    }

    logger.info(
        "Column '%s': Imputed %d/%d (%.1f%%) missing values using KNN (n_neighbors=%d, neighbor_weights=%s)",  # noqa: E501
        column,
        n_imputed,
        n_total,
        pct_imputed,
        n_neighbors,
        neighbor_weights,
    )

    return df_imputed, stats
