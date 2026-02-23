"""K-fold cross-validation for imputation quality assessment."""

import logging
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
    # Treat integers and strings as categorical
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
    """Validate KNN imputation quality using k-fold cross-validation.

    Args:
        df: DataFrame with complete (non-missing) values
        column: Column to validate
        n_folds: Number of cross-validation folds
        sample_pct: Percentage of values to mask and test (0-100)
        n_neighbors: Number of neighbors for KNN
        neighbor_weights: How to weight neighbors ('uniform' or 'distance')
        random_state: Random state for reproducibility
        numeric_features: Optional list of numeric feature columns to use
        categorical_features: Optional list of categorical feature columns to use

    Returns:
        Dictionary with validation metrics
    """
    # Filter to non-missing values
    df_complete = df.filter(pl.col(column).is_not_null())
    n_complete = len(df_complete)

    if n_complete == 0:
        return {"error": "No complete values to validate"}

    if not numeric_features and not categorical_features:
        return {"error": "At least one of numeric_features or categorical_features required"}

    # Sample rows to test
    n_sample = max(1, int(n_complete * sample_pct / 100))
    n_sample = min(n_sample, n_complete)

    rng = np.random.default_rng(random_state)
    sample_indices = rng.choice(n_complete, size=n_sample, replace=False)
    df_sample = df_complete[sample_indices]

    # Extract true values
    true_values = df_sample[column].to_numpy()

    # K-fold validation
    kf = KFold(n_splits=n_folds, shuffle=True, random_state=random_state)
    predictions = []
    true_vals = []

    for _, test_idx in kf.split(df_sample.to_numpy()):
        # Create masked version (set test values to null)
        df_masked = df_sample.clone()
        mask = pl.Series([i in test_idx for i in range(len(df_masked))])
        df_masked = df_masked.with_columns(
            pl.when(mask).then(None).otherwise(pl.col(column)).alias(column)
        )

        # Impute
        df_imputed, _ = impute_knn(
            df_masked, column, n_neighbors, neighbor_weights, numeric_features, categorical_features
        )

        # Collect predictions for test set
        test_predictions = df_imputed[test_idx][column].to_numpy()
        test_true = true_values[test_idx]

        predictions.extend(test_predictions)
        true_vals.extend(test_true)

    predictions = np.array(predictions)
    true_vals = np.array(true_vals)

    # Calculate metrics based on data type
    metrics = {
        "column": column,
        "n_samples": n_sample,
        "n_folds": n_folds,
    }

    if is_categorical(df, column):
        metrics["type"] = "categorical"
        metrics["accuracy"] = accuracy_score(true_vals, predictions.round())
        metrics["precision"] = precision_score(
            true_vals, predictions.round(), average="weighted", zero_division=0
        )
        metrics["recall"] = recall_score(
            true_vals, predictions.round(), average="weighted", zero_division=0
        )
        metrics["f1"] = f1_score(
            true_vals, predictions.round(), average="weighted", zero_division=0
        )
    else:
        metrics["type"] = "continuous"
        metrics["rmse"] = np.sqrt(mean_squared_error(true_vals, predictions))
        metrics["mae"] = mean_absolute_error(true_vals, predictions)
        metrics["r2"] = r2_score(true_vals, predictions)

    return metrics


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
    """Validate MICE imputation quality using k-fold cross-validation.

    Args:
        df: DataFrame with complete (non-missing) values
        columns: Columns to validate
        n_folds: Number of cross-validation folds
        sample_pct: Percentage of values to mask and test (0-100)
        max_iter: Maximum iterations for MICE
        random_state: Random state for reproducibility
        numeric_features: List of numeric feature columns for imputation
        categorical_features: List of categorical feature columns for imputation

    Returns:
        Dictionary mapping column names to validation metrics
    """
    # Filter to rows with all target columns non-missing
    df_complete = df
    for col in columns:
        df_complete = df_complete.filter(pl.col(col).is_not_null())

    n_complete = len(df_complete)

    if n_complete == 0:
        return {col: {"error": "No complete values to validate"} for col in columns}

    # Sample rows to test
    n_sample = max(1, int(n_complete * sample_pct / 100))
    n_sample = min(n_sample, n_complete)

    rng = np.random.default_rng(random_state)
    sample_indices = rng.choice(n_complete, size=n_sample, replace=False)
    df_sample = df_complete[sample_indices]

    # Extract true values for each column
    true_values = {col: df_sample[col].to_numpy() for col in columns}

    # K-fold validation
    kf = KFold(n_splits=n_folds, shuffle=True, random_state=random_state)
    predictions = {col: [] for col in columns}
    true_vals = {col: [] for col in columns}

    for _, test_idx in kf.split(df_sample.to_numpy()):
        # Create masked version (set test values to null for all target columns)
        df_masked = df_sample.clone()
        mask = pl.Series([i in test_idx for i in range(len(df_masked))])

        for col in columns:
            df_masked = df_masked.with_columns(
                pl.when(mask).then(None).otherwise(pl.col(col)).alias(col)
            )

        # Impute
        df_imputed, _ = impute_mice(
            df_masked,
            columns,
            max_iter=max_iter,
            random_state=random_state,
            numeric_features=numeric_features,
            categorical_features=categorical_features,
            verbose=False,
        )

        # Collect predictions for test set
        for col in columns:
            test_predictions = df_imputed[test_idx][col].to_numpy()
            test_true = true_values[col][test_idx]

            predictions[col].extend(test_predictions)
            true_vals[col].extend(test_true)

    # Calculate metrics for each column
    results = {}
    for col in columns:
        preds = np.array(predictions[col])
        trues = np.array(true_vals[col])

        metrics = {
            "column": col,
            "n_samples": n_sample,
            "n_folds": n_folds,
        }

        if is_categorical(df, col):
            metrics["type"] = "categorical"
            metrics["accuracy"] = accuracy_score(trues, preds.round())
            metrics["precision"] = precision_score(
                trues, preds.round(), average="weighted", zero_division=0
            )
            metrics["recall"] = recall_score(
                trues, preds.round(), average="weighted", zero_division=0
            )
            metrics["f1"] = f1_score(trues, preds.round(), average="weighted", zero_division=0)
        else:
            metrics["type"] = "continuous"
            metrics["rmse"] = np.sqrt(mean_squared_error(trues, preds))
            metrics["mae"] = mean_absolute_error(trues, preds)
            metrics["r2"] = r2_score(trues, preds)

        results[col] = metrics

    return results


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
