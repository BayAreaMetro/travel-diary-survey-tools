"""K-fold cross-validation for imputation quality assessment."""

import logging
from collections.abc import Callable
from typing import Any, Literal

import numpy as np
import polars as pl
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    r2_score,
    recall_score,
)
from sklearn.model_selection import KFold

from .knn import impute_knn
from .mice import impute_mice

logger = logging.getLogger(__name__)


def is_categorical(df: pl.DataFrame, column: str) -> bool:
    """Determine if a column should be treated as categorical.

    Args:
        df: DataFrame containing the column
        column: Column name to check

    Returns:
        True if column is categorical (non-float numeric or string), False otherwise
    """
    dtype = df[column].dtype
    return dtype in (
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
        pl.Utf8,
    )


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _run_kfold(
    df: pl.DataFrame,
    target_columns: list[str],
    n_folds: int,
    sample_pct: float,
    random_state: int | None,
    impute_fn: Callable[[pl.DataFrame], pl.DataFrame],
) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray], int]:
    """Run k-fold cross-validation, masking *target_columns* in each fold.

    Args:
        df: DataFrame with complete values for the target columns
        target_columns: Columns to mask and evaluate
        n_folds: Number of KFold splits
        sample_pct: Percentage of complete rows to sample (0-100)
        random_state: Random seed
        impute_fn: ``df_masked -> df_imputed`` callback (KNN or MICE)

    Returns:
        ``(predictions, true_vals, n_sample)`` - dicts keyed by column name
        containing numpy arrays; *n_sample* is 0 when no complete rows exist.
    """
    # Filter to rows where every target column is non-null
    df_complete = df
    for col in target_columns:
        df_complete = df_complete.filter(pl.col(col).is_not_null())

    n_complete = len(df_complete)
    if n_complete == 0:
        return {}, {}, 0

    n_sample = min(max(1, int(n_complete * sample_pct / 100)), n_complete)

    rng = np.random.default_rng(random_state)
    sample_indices = rng.choice(n_complete, size=n_sample, replace=False)
    df_sample = df_complete[sample_indices]

    true_values = {col: df_sample[col].to_numpy() for col in target_columns}

    kf = KFold(n_splits=n_folds, shuffle=True, random_state=random_state)
    predictions: dict[str, list] = {col: [] for col in target_columns}
    true_vals: dict[str, list] = {col: [] for col in target_columns}

    for _, test_idx in kf.split(df_sample.to_numpy()):
        # Mask test rows for every target column
        df_masked = df_sample.clone()
        mask = pl.Series([i in test_idx for i in range(len(df_masked))])
        for col in target_columns:
            df_masked = df_masked.with_columns(
                pl.when(mask).then(None).otherwise(pl.col(col)).alias(col)
            )

        df_imputed = impute_fn(df_masked)

        for col in target_columns:
            predictions[col].extend(df_imputed[test_idx][col].to_numpy())
            true_vals[col].extend(true_values[col][test_idx])

    return (
        {col: np.array(v) for col, v in predictions.items()},
        {col: np.array(v) for col, v in true_vals.items()},
        n_sample,
    )


def _compute_metrics(
    true_vals: np.ndarray,
    predictions: np.ndarray,
    categorical: bool,
    column: str,
    n_sample: int,
    n_folds: int,
) -> dict[str, Any]:
    """Compute classification or regression metrics for one column."""
    metrics: dict[str, Any] = {
        "column": column,
        "n_samples": n_sample,
        "n_folds": n_folds,
    }

    if categorical:
        rounded = predictions.round()
        metrics["type"] = "categorical"
        metrics["accuracy"] = accuracy_score(true_vals, rounded)
        metrics["precision"] = precision_score(
            true_vals,
            rounded,
            average="weighted",
            zero_division=0,
        )
        metrics["recall"] = recall_score(true_vals, rounded, average="weighted", zero_division=0)
        metrics["f1"] = f1_score(true_vals, rounded, average="weighted", zero_division=0)
    else:
        metrics["type"] = "continuous"
        metrics["rmse"] = np.sqrt(mean_squared_error(true_vals, predictions))
        metrics["mae"] = mean_absolute_error(true_vals, predictions)
        metrics["r2"] = r2_score(true_vals, predictions)

    return metrics


# ---------------------------------------------------------------------------
# Public validation entry-points
# ---------------------------------------------------------------------------


def validate_knn_imputation(
    df: pl.DataFrame,
    column: str,
    n_folds: int,
    sample_pct: float,
    n_neighbors: int,
    neighbor_weights: Literal["uniform", "distance"],
    random_state: int | None = None,
    numeric_features: list[str] | None = None,
    categorical_features: list[str] | None = None,
) -> dict[str, Any]:
    """Validate KNN imputation quality using k-fold cross-validation."""
    if not numeric_features and not categorical_features:
        return {"error": "At least one of numeric_features or categorical_features required"}

    def _impute(df_masked: pl.DataFrame) -> pl.DataFrame:
        result, _ = impute_knn(
            df_masked,
            column,
            n_neighbors,
            neighbor_weights,
            numeric_features,
            categorical_features,
        )
        return result

    preds, trues, n_sample = _run_kfold(
        df,
        [column],
        n_folds,
        sample_pct,
        random_state,
        _impute,
    )

    if n_sample == 0:
        return {"error": "No complete values to validate"}

    return _compute_metrics(
        trues[column],
        preds[column],
        is_categorical(df, column),
        column,
        n_sample,
        n_folds,
    )


def validate_mice_imputation(
    df: pl.DataFrame,
    columns: list[str],
    n_folds: int,
    sample_pct: float,
    max_iter: int,
    random_state: int | None = None,
    numeric_features: list[str] | None = None,
    categorical_features: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Validate MICE imputation quality using k-fold cross-validation."""

    def _impute(df_masked: pl.DataFrame) -> pl.DataFrame:
        result, _ = impute_mice(
            df_masked,
            columns,
            max_iter=max_iter,
            random_state=random_state,
            numeric_features=numeric_features,
            categorical_features=categorical_features,
            verbose=False,
        )
        return result

    preds, trues, n_sample = _run_kfold(
        df,
        columns,
        n_folds,
        sample_pct,
        random_state,
        _impute,
    )

    if n_sample == 0:
        return {col: {"error": "No complete values to validate"} for col in columns}

    return {
        col: _compute_metrics(
            trues[col],
            preds[col],
            is_categorical(df, col),
            col,
            n_sample,
            n_folds,
        )
        for col in columns
    }


def log_validation_results(metrics: dict[str, Any] | dict[str, dict[str, Any]]) -> None:
    """Log validation metrics in a readable format.

    Args:
        metrics: Dictionary of metrics (single column or per-column dict)
    """
    logger.info("\n%s", "=" * 60)
    logger.info("Imputation Validation Results")
    logger.info("%s", "=" * 60)

    # Handle single column metrics
    if "column" in metrics:
        metrics = {str(metrics["column"]): metrics}

    for col, col_metrics in metrics.items():
        if "error" in col_metrics:
            logger.warning("Column: %s - %s", col, col_metrics["error"])
            continue

        data_type = col_metrics.get("type", "unknown")
        n_samples = col_metrics.get("n_samples", 0)

        logger.info("\nColumn: %s (%s, n=%s test samples)", col, data_type, n_samples)

        if data_type == "categorical":
            logger.info("  Accuracy:  %.3f", col_metrics["accuracy"])
            logger.info("  Precision: %.3f", col_metrics["precision"])
            logger.info("  Recall:    %.3f", col_metrics["recall"])
            logger.info("  F1-Score:  %.3f", col_metrics["f1"])
        elif data_type == "continuous":
            logger.info("  RMSE: %.4f", col_metrics["rmse"])
            logger.info("  MAE:  %.4f", col_metrics["mae"])
            logger.info("  R²:   %.4f", col_metrics["r2"])

    logger.info("%s", "=" * 60)
