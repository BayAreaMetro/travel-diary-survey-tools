"""MICE-based imputation for missing values."""

import logging
from typing import Any

import polars as pl
from sklearn.experimental import enable_iterative_imputer  # noqa: F401
from sklearn.impute import IterativeImputer

logger = logging.getLogger(__name__)


def impute_mice(
    df: pl.DataFrame,
    columns: list[str],
    max_iter: int = 10,
    random_state: int | None = None,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Impute missing values in multiple correlated columns using MICE.

    MICE (Multiple Imputation by Chained Equations) is particularly useful for
    imputing multiple correlated variables together (e.g., depart_time, arrive_time, duration).

    Args:
        df: DataFrame containing the columns to impute
        columns: List of column names to impute together
        max_iter: Maximum number of imputation rounds
        random_state: Random state for reproducibility

    Returns:
        Tuple of (imputed_df, stats_dict) where stats_dict contains per-column:
            - n_missing: Number of missing values
            - n_imputed: Number of values imputed
            - pct_imputed: Percentage of values imputed
    """
    # Validate columns exist
    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        msg = f"Columns not found in DataFrame: {missing_cols}"
        raise ValueError(msg)

    # Count missing values before imputation
    stats = {}
    n_total = len(df)
    total_missing = 0

    for col in columns:
        n_missing = df[col].null_count()
        stats[col] = {
            "n_missing": n_missing,
            "n_imputed": 0,
            "pct_imputed": 0.0,
        }
        total_missing += n_missing

    if total_missing == 0:
        logger.info("Columns %s: No missing values, skipping imputation", columns)
        return df, stats

    # Get all numeric columns for feature matrix
    numeric_cols = [col for col in df.columns if df[col].dtype.is_numeric()]
    if not numeric_cols:
        msg = "No numeric columns available for MICE imputation"
        raise ValueError(msg)

    # Create feature matrix
    feature_matrix = df.select(numeric_cols).to_numpy()

    # Perform MICE imputation
    imputer = IterativeImputer(
        max_iter=max_iter,
        random_state=random_state,
        verbose=0,
    )
    imputed_matrix = imputer.fit_transform(feature_matrix)

    # Update DataFrame with imputed values for target columns
    df_imputed = df
    for col in columns:
        if col in numeric_cols:
            col_idx = numeric_cols.index(col)
            imputed_values = imputed_matrix[:, col_idx]
            df_imputed = df_imputed.with_columns(pl.Series(col, imputed_values))

            # Update statistics
            n_missing = stats[col]["n_missing"]
            if n_missing > 0:
                stats[col]["n_imputed"] = n_missing
                stats[col]["pct_imputed"] = (n_missing / n_total) * 100

    # Log summary
    imputed_cols = [col for col in columns if stats[col]["n_imputed"] > 0]
    if imputed_cols:
        logger.info(
            "Columns %s: Imputed missing values using MICE (max_iter=%d, random_state=%s)",
            imputed_cols,
            max_iter,
            random_state,
        )
        for col in imputed_cols:
            s = stats[col]
            logger.info("  - %s: %d/%d (%.1f%%)", col, s["n_imputed"], n_total, s["pct_imputed"])

    return df_imputed, stats
