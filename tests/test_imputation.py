"""Tests for imputation module."""

from collections.abc import Callable

import numpy as np
import polars as pl
import pytest

from data_canon.codebook.households import IncomeBroad
from processing.imputation.comparison import compare_imputation_methods
from processing.imputation.flags import stash_preimputed_column, stash_preimputed_columns
from processing.imputation.impute_utils import (
    build_feature_matrix,
    decode_dense_to_integer,
    encode_integer_categoricals,
    is_categorical,
    prepare_column_for_imputation,
)
from processing.imputation.knn import impute_knn
from processing.imputation.mice import impute_mice
from processing.imputation.random_forest import impute_random_forest
from processing.imputation.validation import (
    validate_knn_imputation,
    validate_mice_imputation,
    validate_rf_imputation,
)

# The three imputers take different arguments and MICE reports per column, so each
# is wrapped to the same shape -- (frame, flat stats dict) for one target column --
# and the rules that hold for all three are stated once, as a table.
Imputer = Callable[[pl.DataFrame, str, list[str]], tuple[pl.DataFrame, dict]]


def _knn(df: pl.DataFrame, column: str, features: list[str]) -> tuple[pl.DataFrame, dict]:
    return impute_knn(
        df, column, n_neighbors=3, neighbor_weights="uniform", numeric_features=features
    )


def _rf(df: pl.DataFrame, column: str, features: list[str]) -> tuple[pl.DataFrame, dict]:
    return impute_random_forest(
        df, column, n_estimators=50, random_state=42, numeric_features=features
    )


def _mice(df: pl.DataFrame, column: str, features: list[str]) -> tuple[pl.DataFrame, dict]:
    result, stats = impute_mice(
        df, columns=[column], max_iter=5, random_state=42, numeric_features=features
    )
    return result, stats[column]


ALL_IMPUTERS = [
    pytest.param(_knn, id="knn"),
    pytest.param(_rf, id="rf"),
    pytest.param(_mice, id="mice"),
]


def _non_contiguous_frame() -> pl.DataFrame:
    """60 rows whose category is determined by the feature, coded 10/20/30.

    Non-contiguous codes are the case that breaks a naive encoder: anything that
    treats the code as a dense index, or rounds a regression back to an integer,
    invents codes like 11 or 25 that never appeared in the data.
    """
    rng = np.random.default_rng(42)
    feature = rng.normal(size=60)
    codes = np.array([10, 20, 30])
    target = codes[np.digitize(feature, bins=[-0.5, 0.5]) % 3].tolist()
    for index in (0, 5, 10):
        target[index] = None

    return pl.DataFrame(
        {"feature": feature.tolist(), "cat": pl.Series("cat", target, dtype=pl.Int64)}
    )


class TestEveryImputer:
    """Rules that hold whichever method is chosen."""

    @pytest.mark.parametrize("impute", ALL_IMPUTERS)
    def test_no_missing_values_returns_the_frame_unchanged(self, impute: Imputer):
        """Should skip imputation when no missing values."""
        df = pl.DataFrame({"id": [1, 2, 3], "value": [1.0, 2.0, 3.0]})

        result_df, stats = impute(df, "value", ["value"])

        assert stats["n_missing"] == 0
        assert stats["n_imputed"] == 0
        assert result_df.equals(df)

    @pytest.mark.parametrize("impute", [pytest.param(_knn, id="knn"), pytest.param(_rf, id="rf")])
    def test_all_missing_values_imputes_nothing(self, impute: Imputer):
        """With no observed value to learn from, nothing is invented."""
        df = pl.DataFrame({"feature": [1.0, 2.0, 3.0], "target": [None, None, None]})

        result_df, stats = impute(df, "target", ["feature"])

        assert stats["n_missing"] == 3
        assert stats["n_imputed"] == 0
        assert stats["pct_imputed"] == 100.0
        assert result_df["target"].null_count() == 3

    @pytest.mark.parametrize("impute", ALL_IMPUTERS)
    def test_non_contiguous_integer_codes_stay_valid_codes(self, impute: Imputer):
        """Every imputed value is one of the codes that appeared in the data."""
        df = _non_contiguous_frame()

        result, stats = impute(df, "cat", ["feature"])

        assert stats["n_missing"] == 3
        assert stats["n_imputed"] == 3
        assert result["cat"].null_count() == 0
        assert set(result["cat"].to_list()).issubset({10, 20, 30})

    @pytest.mark.parametrize(
        ("call", "match"),
        [
            pytest.param(
                lambda df: impute_knn(df, "missing", n_neighbors=2),
                "Column 'missing' not found",
                id="knn",
            ),
            pytest.param(
                lambda df: impute_random_forest(df, "missing", numeric_features=["a"]),
                "Column 'missing' not found",
                id="rf",
            ),
            pytest.param(
                lambda df: impute_mice(df, columns=["missing1", "missing2"]),
                "Columns not found",
                id="mice",
            ),
        ],
    )
    def test_a_column_that_is_not_there_raises(self, call: Callable, match: str):
        """Should raise error for missing column."""
        df = pl.DataFrame({"a": [1, 2, 3]})

        with pytest.raises(ValueError, match=match):
            call(df)


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

        result_df, stats = impute_knn(
            df,
            "target",
            n_neighbors=2,
            neighbor_weights="uniform",
            numeric_features=["feature1", "feature2"],
        )

        # Should have imputed 2 values
        assert stats["n_missing"] == 2
        assert stats["n_imputed"] == 2
        assert stats["pct_imputed"] == pytest.approx(40.0)

        # No nulls should remain
        assert result_df["target"].null_count() == 0


class TestMICEImputation:
    """Tests for MICE imputation."""

    def test_basic_mice_imputation(self):
        """Should impute correlated columns using MICE, from numeric and categorical features."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "col1": [1.0, None, 3.0, 4.0, 5.0],
                "col2": [10.0, 20.0, None, 40.0, 50.0],
                "col3": [100.0, 200.0, 300.0, 400.0, 500.0],
                "col4": [2, 3, 2, 4, 3],
            }
        )

        result_df, stats = impute_mice(
            df,
            columns=["col1", "col2"],
            max_iter=5,
            random_state=42,
            numeric_features=["col1", "col2", "col3"],
            categorical_features=["col4"],
        )

        # Should have imputed values
        assert stats["col1"]["n_imputed"] == 1
        assert stats["col2"]["n_imputed"] == 1
        assert result_df["col1"].null_count() == 0
        assert result_df["col2"].null_count() == 0

    def test_mice_with_insufficient_data(self):
        """Two rows, one observation each: the fallback is that column's own value.

        There is nothing to regress on, so ``IterativeImputer`` stops at the
        initial fill rather than failing, and each column is completed with the
        mean of the single value it has.
        """
        df = pl.DataFrame({"col1": [1.0, None], "col2": [None, 2.0]})

        result_df, stats = impute_mice(
            df, columns=["col1", "col2"], numeric_features=["col1", "col2"]
        )

        assert result_df["col1"].to_list() == [1.0, 1.0]
        assert result_df["col2"].to_list() == [2.0, 2.0]
        assert stats["col1"]["n_imputed"] == 1
        assert stats["col2"]["n_imputed"] == 1


class TestRandomForestImputation:
    """Tests for Random Forest imputation."""

    def test_basic_rf_categorical_imputation(self):
        """Should impute categorical values using Random Forest classifier."""
        rng = np.random.default_rng(42)
        n = 50
        feature = rng.normal(size=n).tolist()
        # Deterministic categories based on feature
        target = [1 if f > 0 else 2 for f in feature]
        # Null out some
        target[0] = None
        target[5] = None
        target[10] = None

        df = pl.DataFrame(
            {
                "feature": feature,
                "mode": pl.Series("mode", target, dtype=pl.Int64),
            }
        )

        result_df, stats = impute_random_forest(
            df, "mode", n_estimators=50, random_state=42, numeric_features=["feature"]
        )

        assert stats["n_missing"] == 3
        assert stats["n_imputed"] == 3
        assert result_df["mode"].null_count() == 0
        # Values should be valid categories
        assert set(result_df["mode"].to_list()).issubset({1, 2})

    def test_basic_rf_continuous_imputation(self):
        """Should impute continuous values using Random Forest regressor."""
        df = pl.DataFrame(
            {
                "x": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
                "y": [2.0, 4.0, None, 8.0, 10.0, 12.0, None, 16.0, 18.0, 20.0],
            }
        )

        result_df, stats = impute_random_forest(
            df, "y", n_estimators=50, random_state=42, numeric_features=["x"]
        )

        assert stats["n_missing"] == 2
        assert stats["n_imputed"] == 2
        assert result_df["y"].null_count() == 0

    def test_rf_with_categorical_features_returns_feature_importance(self):
        """One-hot encoded categorical features are used, and reported by name."""
        rng = np.random.default_rng(42)
        n = 50
        df = pl.DataFrame(
            {
                "age": rng.normal(40, 10, size=n).tolist(),
                "gender": rng.choice([1, 2], size=n).tolist(),
                "income": pl.Series(
                    "income",
                    [*rng.choice([1, 2, 3], size=n - 3).tolist(), None, None, None],
                    dtype=pl.Int64,
                ),
            }
        )

        result_df, stats = impute_random_forest(
            df,
            "income",
            n_estimators=50,
            random_state=42,
            numeric_features=["age"],
            categorical_features=["gender"],
        )

        assert stats["n_imputed"] == 3
        assert result_df["income"].null_count() == 0
        assert set(result_df["income"].to_list()).issubset({1, 2, 3})

        fi = stats["feature_importance"]
        # age + gender, with the one-hot columns aggregated back to the source name.
        assert set(fi) == {"age", "gender"}
        assert pytest.approx(sum(fi.values()), abs=0.01) == 1.0
        # Sorted descending, so the reader can stop at the first few.
        values = list(fi.values())
        assert values == sorted(values, reverse=True)


class TestPreimputedStash:
    """Tests for pre-imputation value stashing."""

    @pytest.mark.parametrize(
        ("original", "imputed", "expected"),
        [
            pytest.param([1.0, None, 3.0], [1.0, 2.0, 3.0], [1.0, None, 3.0], id="null"),
            # 999 is PNTA and 995 is MISSING: both are answers, and have to stay
            # distinguishable from a genuine null after imputation overwrites them.
            pytest.param(
                [1, 999, None, 3, 995],
                [1, 2, 2, 3, 2],
                [1, 999, None, 3, 995],
                id="pnta-vs-null",
            ),
        ],
    )
    def test_stash_single_column(self, original: list, imputed: list, expected: list):
        """Should stash original values including nulls."""
        result_df = stash_preimputed_column(
            pl.DataFrame({"value": imputed}), pl.DataFrame({"value": original}), "value"
        )

        assert result_df["value_preimputed"].to_list() == expected
        assert result_df["value"].to_list() == imputed

    def test_stash_multiple_columns(self):
        """Should stash original values for multiple columns."""
        original_df = pl.DataFrame({"col1": [1.0, None, 3.0], "col2": [10.0, 20.0, None]})
        imputed_df = pl.DataFrame({"col1": [1.0, 2.0, 3.0], "col2": [10.0, 20.0, 30.0]})

        result_df = stash_preimputed_columns(imputed_df, original_df, ["col1", "col2"])

        assert result_df["col1_preimputed"].to_list() == [1.0, None, 3.0]
        assert result_df["col2_preimputed"].to_list() == [10.0, 20.0, None]


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

    @pytest.mark.parametrize(
        "validate",
        [
            pytest.param(
                lambda df, column: validate_knn_imputation(
                    df,
                    column=column,
                    n_folds=3,
                    sample_pct=10.0,
                    n_neighbors=3,
                    neighbor_weights="uniform",
                    random_state=42,
                    numeric_features=["feature"],
                ),
                id="knn",
            ),
            pytest.param(
                lambda df, column: validate_rf_imputation(
                    df,
                    column=column,
                    n_folds=3,
                    sample_pct=10.0,
                    n_estimators=50,
                    random_state=42,
                    numeric_features=["feature"],
                ),
                id="rf",
            ),
        ],
    )
    def test_categorical_validation_beats_the_majority_class(self, validate: Callable):
        """The feature determines the class, so a bare 0 <= accuracy <= 1 proves nothing."""
        df = pl.DataFrame({"feature": [1.0, 2.0, 3.0, 4.0, 5.0] * 20, "mode": [1, 1, 2, 2, 1] * 20})

        metrics = validate(df, "mode")

        assert metrics["type"] == "categorical"
        assert set(metrics) >= {"accuracy", "precision", "recall", "f1"}
        assert metrics["accuracy"] >= 0.5

    @pytest.mark.parametrize(
        "validate",
        [
            pytest.param(
                lambda df, column: validate_knn_imputation(
                    df,
                    column=column,
                    n_folds=3,
                    sample_pct=10.0,
                    n_neighbors=3,
                    neighbor_weights="distance",
                    random_state=42,
                    numeric_features=["feature"],
                ),
                id="knn",
            ),
            pytest.param(
                lambda df, column: validate_rf_imputation(
                    df,
                    column=column,
                    n_folds=3,
                    sample_pct=10.0,
                    n_estimators=50,
                    random_state=42,
                    numeric_features=["feature"],
                ),
                id="rf",
            ),
        ],
    )
    def test_continuous_validation_reports_error_metrics(self, validate: Callable):
        """Should validate imputation on continuous data."""
        df = pl.DataFrame(
            {
                "feature": [1.0, 2.0, 3.0, 4.0, 5.0] * 20,
                "distance": [10.5, 20.3, 15.7, 25.1, 18.9] * 20,
            }
        )

        metrics = validate(df, "distance")

        assert metrics["type"] == "continuous"
        assert set(metrics) >= {"rmse", "mae", "r2"}
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
            df,
            columns=["col1", "col2"],
            n_folds=3,
            sample_pct=10.0,
            max_iter=5,
            random_state=42,
            numeric_features=["col1", "col2"],
        )

        assert set(metrics) == {"col1", "col2"}
        assert metrics["col1"]["type"] == "continuous"
        assert "rmse" in metrics["col1"]


class TestPrepareColumnForImputation:
    """Tests for preparing columns for imputation."""

    def test_prepare_income_column(self):
        """Test preparing income_bin column replaces MISSING/PNTA with null."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 2, 3, 4, 5],
                "income_bin": [1, 2, 995, 999, 3],  # 995=MISSING, 999=PNTA
            }
        )

        df_prepared, resolved_values = prepare_column_for_imputation(
            df, "households", "income_bin", ["MISSING", "PNTA"]
        )

        assert resolved_values == [IncomeBroad.MISSING.value, IncomeBroad.PNTA.value]
        assert df_prepared["income_bin"].to_list() == [1, 2, None, None, 3]


class TestDenseIntegerEncoding:
    """Tests for integer categorical dense encoding / decoding."""

    def test_encodes_non_contiguous_codes(self):
        """Non-contiguous integer codes should be mapped to dense 0..N."""
        df = pl.DataFrame({"income_bin": [1, 2, 5, 6, None]})
        encoded, encodings = encode_integer_categoricals(df, ["income_bin"])

        assert "income_bin" in encodings
        # Dense codes should be 0..3 for the four unique values
        assert encodings["income_bin"] == {0: 1, 1: 2, 2: 5, 3: 6}
        vals = encoded["income_bin"].drop_nulls().to_list()
        assert sorted(vals) == [0.0, 1.0, 2.0, 3.0]
        # Null should be preserved
        assert encoded["income_bin"].null_count() == 1

    def test_skips_already_dense_columns(self):
        """Columns coded 0..N-1 should not be re-encoded."""
        df = pl.DataFrame({"status": [0, 1, 2, 3]})
        encoded, encodings = encode_integer_categoricals(df, ["status"])

        assert "status" not in encodings
        assert encoded["status"].to_list() == [0, 1, 2, 3]

    def test_skips_non_integer_columns(self):
        """Float/string columns should be ignored entirely."""
        df = pl.DataFrame({"val": [1.5, 2.5, 3.5]})
        _, encodings = encode_integer_categoricals(df, ["val"])

        assert encodings == {}

    def test_decode_dense_to_integer_basic(self):
        """Dense float predictions should round and decode to original codes."""
        mapping = {0: 1, 1: 2, 2: 5, 3: 6}
        values = np.array([0.3, 0.7, 2.1, 2.9])
        decoded = decode_dense_to_integer(values, mapping)

        assert decoded == [1, 2, 5, 6]

    def test_decode_dense_clamps_out_of_range(self):
        """Values outside [0, N-1] should be clamped to the nearest valid key."""
        mapping = {0: 10, 1: 20, 2: 30}
        values = np.array([-1.0, 5.0])
        decoded = decode_dense_to_integer(values, mapping)

        assert decoded == [10, 30]


class TestBuildFeatureMatrix:
    """Tests for build_feature_matrix feature name tracking."""

    def test_returns_feature_names(self):
        """Feature names should include continuous and one-hot encoded columns."""
        df = pl.DataFrame(
            {
                "target": [1.0, 2.0, 3.0, 4.0],
                "num_feat": [10.0, 20.0, 30.0, 40.0],
                "cat_feat": [1, 2, 1, 2],
            }
        )

        matrix, _indices, names = build_feature_matrix(
            df,
            target_columns=["target"],
            numeric_features=["num_feat"],
            categorical_features=["cat_feat"],
        )

        assert set(names) == {"num_feat", "target", "cat_feat=1", "cat_feat=2"}
        assert len(names) == matrix.shape[1]

    def test_feature_names_empty_categoricals(self):
        """With no categoricals the names are the numeric features then the targets.

        The order is what indexes the matrix columns, so it is asserted exactly.
        """
        df = pl.DataFrame({"target": [1.0, None, 3.0], "feat": [10.0, 20.0, 30.0]})

        matrix, _indices, names = build_feature_matrix(df, ["target"], ["feat"], [])

        assert names == ["feat", "target"]
        assert matrix.shape[1] == 2


class TestMethodComparison:
    """Tests for head-to-head method comparison."""

    def _make_comparison_data(self) -> tuple[dict[str, pl.DataFrame], dict]:
        """Build a small dataset plus config for comparison tests."""
        df = pl.DataFrame(
            {
                "feature": [1.0, 2.0, 3.0, 4.0, 5.0] * 20,
                "mode": [1, 1, 2, 2, 1] * 20,
            }
        )
        tables = {"persons": df}
        config: dict = {
            "persons": [
                {
                    "method": "knn",
                    "column": "mode",
                    "numeric_features": ["feature"],
                    "n_neighbors": 3,
                }
            ]
        }
        return tables, config

    def test_comparison_returns_all_methods(self, tmp_path):
        """Comparison produces one row per method, with the metric columns, and saves."""
        tables, config = self._make_comparison_data()
        csv_path = str(tmp_path / "comparison.csv")

        result = compare_imputation_methods(
            config,
            tables,
            n_folds=3,
            sample_pct=10.0,
            random_state=42,
            output_path=csv_path,
        )

        assert len(result) == 3
        assert set(result["method"].to_list()) == {"knn", "rf", "mice"}

        # All rows should reference the same column
        assert result["variable"].unique().to_list() == ["mode"]
        assert result["table"].unique().to_list() == ["persons"]

        assert {
            "table",
            "variable",
            "method",
            "type",
            "n_samples",
            "n_folds",
            "accuracy",
            "precision",
            "recall",
            "f1",
        }.issubset(result.columns)

        # The CSV written to output_path is the same table.
        saved = pl.read_csv(csv_path)
        assert saved.shape == result.shape
        assert set(saved["method"].to_list()) == {"knn", "rf", "mice"}

    def test_comparison_deduplicates_columns(self):
        """Same column configured twice should only produce one set of rows."""
        tables, config = self._make_comparison_data()
        # Add a duplicate config block with RF method
        config["persons"].append(
            {
                "method": "rf",
                "column": "mode",
                "numeric_features": ["feature"],
            }
        )

        result = compare_imputation_methods(
            config,
            tables,
            n_folds=3,
            sample_pct=10.0,
            random_state=42,
        )

        # Still 3 rows (one per method), not 6
        assert len(result) == 3

    def test_comparison_splits_mice_columns(self):
        """MICE columns:[a,b] should produce two sets of 3 method rows."""
        df = pl.DataFrame(
            {
                "feature": [1.0, 2.0, 3.0, 4.0, 5.0] * 20,
                "col_a": [1, 2, 1, 2, 1] * 20,
                "col_b": [10, 20, 30, 10, 20] * 20,
            }
        )
        config = {
            "persons": [
                {
                    "method": "mice",
                    "columns": ["col_a", "col_b"],
                    "numeric_features": ["feature"],
                }
            ]
        }
        result = compare_imputation_methods(
            config,
            {"persons": df},
            n_folds=3,
            sample_pct=10.0,
            random_state=42,
        )

        # 2 columns x 3 methods = 6 rows
        assert len(result) == 6
        assert set(result["variable"].to_list()) == {"col_a", "col_b"}
        for var in ("col_a", "col_b"):
            subset = result.filter(pl.col("variable") == var)
            assert set(subset["method"].to_list()) == {"knn", "rf", "mice"}
