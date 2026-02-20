"""Tests for imputation module."""

import polars as pl
import pytest

from processing.imputation.flags import create_flag_column, create_flag_columns
from processing.imputation.knn import impute_knn
from processing.imputation.mice import impute_mice
from processing.imputation.validation import (
    is_categorical,
    validate_knn_imputation,
    validate_mice_imputation,
)


class TestKNNImputation:
    """Tests for KNN imputation."""

    def test_basic_knn_imputation(self):
        """Should impute missing values using KNN."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "feature1": [1.0, 2.0, 3.0, 4.0, 5.0],
                "feature2": [10.0, 20.0, 30.0, 40.0, 50.0],
                "target": [100.0, None, 300.0, None, 500.0],
            }
        )

        result_df, stats = impute_knn(df, "target", n_neighbors=2, neighbor_weights="uniform")

        # Should have imputed 2 values
        assert stats["n_missing"] == 2
        assert stats["n_imputed"] == 2
        assert stats["pct_imputed"] == pytest.approx(40.0)

        # No nulls should remain
        assert result_df["target"].null_count() == 0

    def test_no_missing_values(self):
        """Should skip imputation when no missing values."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "value": [1.0, 2.0, 3.0],
            }
        )

        result_df, stats = impute_knn(df, "value", n_neighbors=2)

        assert stats["n_missing"] == 0
        assert stats["n_imputed"] == 0
        assert result_df.equals(df)

    def test_all_missing_values(self):
        """Should handle all missing values gracefully."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "feature": [1.0, 2.0, 3.0],
                "target": [None, None, None],
            }
        )

        _, stats = impute_knn(df, "target", n_neighbors=2)

        assert stats["n_missing"] == 3
        assert stats["n_imputed"] == 0
        assert stats["pct_imputed"] == 100.0

    def test_categorical_imputation(self):
        """Should impute categorical values (integer codes)."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "feature": [1.0, 2.0, 3.0, 4.0, 5.0],
                "mode": [1, None, 1, None, 2],
            }
        )

        result_df, stats = impute_knn(df, "mode", n_neighbors=2)

        assert stats["n_imputed"] == 2
        assert result_df["mode"].null_count() == 0
        # Values should be reasonable (between 1 and 2)
        assert result_df["mode"].min() >= 1  # pyright: ignore[reportOperatorIssue]
        assert result_df["mode"].max() <= 2  # pyright: ignore[reportOperatorIssue]


class TestMICEImputation:
    """Tests for MICE imputation."""

    def test_basic_mice_imputation(self):
        """Should impute correlated columns using MICE."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "col1": [1.0, None, 3.0, 4.0, 5.0],
                "col2": [10.0, 20.0, None, 40.0, 50.0],
                "col3": [100.0, 200.0, 300.0, 400.0, 500.0],
            }
        )

        result_df, stats = impute_mice(df, columns=["col1", "col2"], max_iter=5, random_state=42)

        # Should have imputed values
        assert stats["col1"]["n_imputed"] == 1
        assert stats["col2"]["n_imputed"] == 1
        assert result_df["col1"].null_count() == 0
        assert result_df["col2"].null_count() == 0

    def test_no_missing_in_any_column(self):
        """Should skip imputation when no missing values."""
        df = pl.DataFrame(
            {
                "col1": [1.0, 2.0, 3.0],
                "col2": [10.0, 20.0, 30.0],
            }
        )

        result_df, stats = impute_mice(df, columns=["col1", "col2"])

        assert stats["col1"]["n_imputed"] == 0
        assert stats["col2"]["n_imputed"] == 0
        assert result_df.equals(df)


class TestFlagColumns:
    """Tests for imputation flag columns."""

    def test_create_single_flag_column(self):
        """Should create flag column for imputed values."""
        original_df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "value": [1.0, None, 3.0],
            }
        )

        imputed_df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "value": [1.0, 2.0, 3.0],
            }
        )

        result_df = create_flag_column(imputed_df, original_df, "value")

        assert "value_imputed" in result_df.columns
        assert result_df["value_imputed"].to_list() == [False, True, False]

    def test_create_multiple_flag_columns(self):
        """Should create flag columns for multiple imputed columns."""
        original_df = pl.DataFrame(
            {
                "col1": [1.0, None, 3.0],
                "col2": [10.0, 20.0, None],
            }
        )

        imputed_df = pl.DataFrame(
            {
                "col1": [1.0, 2.0, 3.0],
                "col2": [10.0, 20.0, 30.0],
            }
        )

        result_df = create_flag_columns(imputed_df, original_df, ["col1", "col2"])

        assert "col1_imputed" in result_df.columns
        assert "col2_imputed" in result_df.columns
        assert result_df["col1_imputed"].to_list() == [False, True, False]
        assert result_df["col2_imputed"].to_list() == [False, False, True]


class TestValidation:
    """Tests for imputation validation."""

    def test_is_categorical(self):
        """Should correctly identify categorical columns."""
        df = pl.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.5, 2.5, 3.5],
                "str_col": ["a", "b", "c"],
            }
        )

        assert is_categorical(df, "int_col") is True
        assert is_categorical(df, "float_col") is False
        assert is_categorical(df, "str_col") is True

    def test_knn_validation_categorical(self):
        """Should validate KNN imputation on categorical data."""
        df = pl.DataFrame(
            {
                "feature": [1.0, 2.0, 3.0, 4.0, 5.0] * 20,  # 100 rows
                "mode": [1, 1, 2, 2, 1] * 20,
            }
        )

        metrics = validate_knn_imputation(
            df,
            column="mode",
            n_folds=3,
            sample_pct=10.0,
            n_neighbors=3,
            neighbor_weights="uniform",
            random_state=42,
        )

        assert metrics["type"] == "categorical"
        assert "accuracy" in metrics
        assert "precision" in metrics
        assert "recall" in metrics
        assert "f1" in metrics
        assert 0 <= metrics["accuracy"] <= 1

    def test_knn_validation_continuous(self):
        """Should validate KNN imputation on continuous data."""
        df = pl.DataFrame(
            {
                "feature": [1.0, 2.0, 3.0, 4.0, 5.0] * 20,
                "distance": [10.5, 20.3, 15.7, 25.1, 18.9] * 20,
            }
        )

        metrics = validate_knn_imputation(
            df,
            column="distance",
            n_folds=3,
            sample_pct=10.0,
            n_neighbors=3,
            neighbor_weights="distance",
            random_state=42,
        )

        assert metrics["type"] == "continuous"
        assert "rmse" in metrics
        assert "mae" in metrics
        assert "r2" in metrics
        assert metrics["rmse"] >= 0

    def test_mice_validation(self):
        """Should validate MICE imputation on multiple columns."""
        df = pl.DataFrame(
            {
                "col1": [1.0, 2.0, 3.0, 4.0, 5.0] * 20,
                "col2": [10.0, 20.0, 30.0, 40.0, 50.0] * 20,
            }
        )

        metrics = validate_mice_imputation(
            df, columns=["col1", "col2"], n_folds=3, sample_pct=10.0, max_iter=5, random_state=42
        )

        assert "col1" in metrics
        assert "col2" in metrics
        assert metrics["col1"]["type"] == "continuous"
        assert "rmse" in metrics["col1"]


class TestEdgeCases:
    """Tests for edge cases."""

    def test_knn_with_single_row(self):
        """Should handle single row gracefully."""
        df = pl.DataFrame(
            {
                "feature": [1.0],
                "target": [None],
            }
        )

        _, stats = impute_knn(df, "target", n_neighbors=1)
        # Cannot impute with only one row
        assert stats["n_imputed"] == 0

    def test_mice_with_insufficient_data(self):
        """Should handle insufficient data gracefully."""
        df = pl.DataFrame(
            {
                "col1": [1.0, None],
                "col2": [None, 2.0],
            }
        )

        result_df, _ = impute_mice(df, columns=["col1", "col2"])
        # Should attempt imputation but may not be accurate
        assert result_df is not None

    def test_knn_missing_column(self):
        """Should raise error for missing column."""
        df = pl.DataFrame({"a": [1, 2, 3]})

        with pytest.raises(ValueError, match="Column 'missing' not found"):
            impute_knn(df, "missing", n_neighbors=2)

    def test_mice_missing_columns(self):
        """Should raise error for missing columns."""
        df = pl.DataFrame({"a": [1, 2, 3]})

        with pytest.raises(ValueError, match="Columns not found"):
            impute_mice(df, columns=["missing1", "missing2"])
