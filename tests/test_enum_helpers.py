"""Test enum helpers utility."""

import polars as pl

from data_canon.codebook.households import IncomeBroad
from utils.enum_helpers import (
    get_enum_class_for_field,
    prepare_column_for_imputation,
    resolve_enum_labels,
)


def test_resolve_enum_labels():
    """Test resolving enum labels to values."""
    # Test with income_broad field
    values = resolve_enum_labels("households", "income_broad", ["MISSING", "PNTA"])
    assert values == [995, 999], f"Expected [995, 999], got {values}"


def test_get_enum_class():
    """Test getting enum class for a field."""
    enum_class = get_enum_class_for_field("households", "income_broad")
    assert enum_class is IncomeBroad, f"Expected IncomeBroad, got {enum_class}"


def test_prepare_column_for_imputation():
    """Test preparing a column for imputation."""
    # Create test data with MISSING and PNTA values
    df = pl.DataFrame(
        {
            "hh_id": [1, 2, 3, 4, 5],
            "income_broad": [1, 2, 995, 999, 3],  # 995=MISSING, 999=PNTA
        }
    )

    # Prepare column
    df_prepared, resolved_values = prepare_column_for_imputation(
        df, "households", "income_broad", ["MISSING", "PNTA"]
    )

    # Check that missing values were resolved
    assert resolved_values == [995, 999], f"Expected [995, 999], got {resolved_values}"

    # Check that values were replaced with null
    income_col = df_prepared["income_broad"]
    assert income_col[0] == 1
    assert income_col[1] == 2
    assert income_col[2] is None  # Was 995 (MISSING)
    assert income_col[3] is None  # Was 999 (PNTA)
    assert income_col[4] == 3


if __name__ == "__main__":
    test_get_enum_class()
    test_resolve_enum_labels()
    test_prepare_column_for_imputation()
