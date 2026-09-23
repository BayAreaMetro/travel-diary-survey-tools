"""Tests for cross-table join features in imputation."""

import polars as pl
import pytest

from processing.imputation.impute_utils import (
    add_household_agg_features,
    aggregate_from_children,
    join_parent_tables,
    strip_joined_columns,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_households():
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 3],
            "income_bin": [2, 5, 3],
            "residence_type": [1, 2, 1],
            "home_lat": [37.7, 37.8, 37.9],
        }
    )


def _make_persons():
    return pl.DataFrame(
        {
            "person_id": [10, 11, 20, 30],
            "hh_id": [1, 1, 2, 3],
            "age": [3, 4, 5, 2],
            "race": [1, 1, 2, None],
            "gender": [1, 2, 1, None],
            "employment": [1, 1, 2, 3],
            "student": [0, 1, 0, 1],
        }
    )


@pytest.fixture
def tables() -> dict:
    """Three households and the four persons hanging off them."""
    return {"households": _make_households(), "persons": _make_persons()}


# ---------------------------------------------------------------------------
# join_parent_tables
# ---------------------------------------------------------------------------


class TestJoinParentTables:
    """Tests for join_parent_tables."""

    def test_joins_household_columns_to_persons(self, tables):
        """Should add household columns to persons."""
        result, added = join_parent_tables(tables["persons"], "persons", tables, ["households"])

        # Should have added household columns (minus hh_id which already exists)
        assert "income_bin" in result.columns
        assert "residence_type" in result.columns
        assert "home_lat" in result.columns
        assert set(added) == {"income_bin", "residence_type", "home_lat"}

        # Values should match via hh_id
        row_p10 = result.filter(pl.col("person_id") == 10)
        assert row_p10["income_bin"][0] == 2
        assert row_p10["residence_type"][0] == 1

        row_p20 = result.filter(pl.col("person_id") == 20)
        assert row_p20["income_bin"][0] == 5

    def test_no_duplicate_columns(self, tables):
        """Should skip columns that already exist on child."""
        hh = pl.DataFrame({"hh_id": [1], "age": [99], "extra": [42]})
        persons = pl.DataFrame({"person_id": [10], "hh_id": [1], "age": [3]})
        tables = {"households": hh, "persons": persons}

        result, added = join_parent_tables(persons, "persons", tables, ["households"])

        # age already exists on persons, so only extra should be added
        assert "extra" in added
        assert "age" not in added
        # Persons' own age should be preserved
        assert result["age"][0] == 3

    def test_missing_parent_table_warns(self):
        """Should warn and skip when parent table is missing."""
        persons = _make_persons()
        tables = {"persons": persons}  # no households

        result, added = join_parent_tables(persons, "persons", tables, ["households"])

        assert added == []
        assert result.shape == persons.shape

    def test_unknown_relationship_raises(self):
        """Should raise ValueError for undefined FK relationship."""
        persons = _make_persons()
        tables = {"persons": persons, "tours": pl.DataFrame({"tour_id": [1]})}

        with pytest.raises(ValueError, match="No foreign key relationship"):
            join_parent_tables(persons, "persons", tables, ["tours"])

    def test_empty_join_tables(self):
        """Should return unchanged df when join_tables is empty."""
        persons = _make_persons()
        tables = {"persons": persons, "households": _make_households()}

        result, added = join_parent_tables(persons, "persons", tables, [])

        assert added == []
        assert result.equals(persons)


# ---------------------------------------------------------------------------
# add_household_agg_features
# ---------------------------------------------------------------------------


class TestAddHouseholdAggFeatures:
    """Tests for add_household_agg_features."""

    def test_mode_excludes_self(self):
        """Mode should be computed from other household members only."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 1, 1],
                "person_id": [10, 11, 12],
                "race": [1, 2, 2],
            }
        )

        result, added = add_household_agg_features(df, ["race"])

        assert "hh_mode_race" in added
        # Person 10: other members have [2, 2] => mode = 2
        assert result.filter(pl.col("person_id") == 10)["hh_mode_race"][0] == 2
        # Person 11: other members have [1, 2] => mode could be 1 or 2
        # Person 12: other members have [1, 2] => mode could be 1 or 2

    def test_single_person_household_null(self):
        """Single-person households should get null for hh_mode."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 2],
                "person_id": [10, 20],
                "race": [1, 2],
            }
        )

        result, added = add_household_agg_features(df, ["race"])

        assert "hh_mode_race" in added
        # Both are single-person households, so hh_mode should be null
        assert result["hh_mode_race"].null_count() == 2

    def test_all_null_target_column(self):
        """Should produce null agg when all target values are null."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 1],
                "person_id": [10, 11],
                "race": [None, None],
            }
        )

        result, added = add_household_agg_features(df, ["race"])

        assert "hh_mode_race" in added
        assert result["hh_mode_race"].null_count() == 2

    def test_missing_hh_or_person_id_skips(self):
        """Should skip gracefully if hh_id or person_id is missing."""
        df = pl.DataFrame({"some_col": [1, 2, 3]})

        result, added = add_household_agg_features(df, ["some_col"])

        assert added == []
        assert result.equals(df)

    @pytest.mark.parametrize(
        ("targets", "expected_added"),
        [
            pytest.param(["race"], ["hh_mode_race"], id="one-column"),
            pytest.param(
                ["race", "ethnicity"],
                ["hh_mode_race", "hh_mode_ethnicity"],
                id="two-columns",
            ),
            # A target that is not on the frame is skipped, not invented.
            pytest.param(["race", "nonexistent"], ["hh_mode_race"], id="one-column-absent"),
        ],
    )
    def test_one_agg_column_per_existing_target(
        self, targets: list[str], expected_added: list[str]
    ):
        """Should add agg features for each target column that exists."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 1],
                "person_id": [10, 11],
                "race": [1, 2],
                "ethnicity": [3, 4],
            }
        )

        result, added = add_household_agg_features(df, targets)

        assert added == expected_added
        assert set(expected_added).issubset(result.columns)


# ---------------------------------------------------------------------------
# strip_joined_columns
# ---------------------------------------------------------------------------


class TestStripJoinedColumns:
    """Tests for strip_joined_columns."""

    def test_removes_added_columns(self):
        """Should remove specified columns."""
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "keep": [10, 20],
                "temp_a": [100, 200],
                "temp_b": [300, 400],
            }
        )

        result = strip_joined_columns(df, ["temp_a", "temp_b"])

        assert "temp_a" not in result.columns
        assert "temp_b" not in result.columns
        assert "id" in result.columns
        assert "keep" in result.columns

    def test_handles_missing_column_gracefully(self):
        """Should not fail if a column to strip doesn't exist."""
        df = pl.DataFrame({"id": [1], "val": [2]})

        result = strip_joined_columns(df, ["nonexistent", "also_missing"])

        assert result.equals(df)


# ---------------------------------------------------------------------------
# End-to-end: join → impute → strip
# ---------------------------------------------------------------------------


class TestJoinImputeStripLifecycle:
    """Integration test for the full join → impute → strip lifecycle."""

    def test_full_lifecycle_preserves_original_schema(self, tables):
        """After join and strip, df should have same columns as original."""
        persons = tables["persons"]

        original_cols = set(persons.columns)

        # Join
        enriched, added = join_parent_tables(persons, "persons", tables, ["households"])
        assert len(enriched.columns) > len(original_cols)

        # Add agg features
        enriched, agg_added = add_household_agg_features(enriched, ["race", "gender"])
        added.extend(agg_added)

        # Strip
        result = strip_joined_columns(enriched, added)

        assert set(result.columns) == original_cols
        assert len(result) == len(persons)


# ---------------------------------------------------------------------------
# aggregate_from_children (child → parent pivot counts)
# ---------------------------------------------------------------------------


class TestAggregateFromChildren:
    """Tests for aggregate_from_children."""

    def test_basic_pivot_count(self, tables):
        """Should create one column per unique value in the child field."""
        hh = tables["households"]

        config = {"persons": {"pivot_count": ["employment"]}}
        result, added = aggregate_from_children(hh, "households", tables, config)

        # employment has values 1, 2, 3 → 3 columns
        assert "persons_count_employment=1" in added
        assert "persons_count_employment=2" in added
        assert "persons_count_employment=3" in added
        assert len(added) == 3

        # hh 1 has persons 10 (emp=1) and 11 (emp=1) → count_1=2, count_2=0, count_3=0
        row_hh1 = result.filter(pl.col("hh_id") == 1)
        assert row_hh1["persons_count_employment=1"][0] == 2
        assert row_hh1["persons_count_employment=2"][0] == 0
        assert row_hh1["persons_count_employment=3"][0] == 0

        # hh 2 has person 20 (emp=2) → count_1=0, count_2=1, count_3=0
        row_hh2 = result.filter(pl.col("hh_id") == 2)
        assert row_hh2["persons_count_employment=1"][0] == 0
        assert row_hh2["persons_count_employment=2"][0] == 1

        # hh 3 has person 30 (emp=3) → count_3=1
        row_hh3 = result.filter(pl.col("hh_id") == 3)
        assert row_hh3["persons_count_employment=3"][0] == 1

    def test_missing_child_table_warns(self, tables):
        """Should warn and skip when child table is not available."""
        hh = _make_households()
        tables = {"households": hh}  # no persons

        config = {"persons": {"pivot_count": ["employment"]}}
        result, added = aggregate_from_children(hh, "households", tables, config)

        assert added == []
        assert result.shape == hh.shape

    def test_missing_field_warns(self, tables):
        """Should warn and skip fields that don't exist on child table."""
        hh = tables["households"]

        config = {"persons": {"pivot_count": ["nonexistent"]}}
        _, added = aggregate_from_children(hh, "households", tables, config)

        assert added == []

    def test_unknown_relationship_raises(self, tables):
        """Should raise ValueError for undefined FK relationship."""
        hh = _make_households()
        tours = pl.DataFrame({"tour_id": [1]})
        tables = {"households": hh, "tours": tours}

        # households is asked for as a child of persons, and that direction has
        # no foreign key declared.
        config = {"households": {"pivot_count": ["hh_id"]}}
        with pytest.raises(ValueError, match="No foreign key relationship"):
            aggregate_from_children(hh, "persons", tables, config)

    def test_households_with_no_children_get_zeros(self):
        """Parent rows with no matching children should have 0 counts."""
        hh = pl.DataFrame({"hh_id": [1, 2, 99]})  # hh 99 has no persons
        persons = pl.DataFrame(
            {
                "person_id": [10, 20],
                "hh_id": [1, 2],
                "employment": [1, 2],
            }
        )
        tables = {"households": hh, "persons": persons}

        config = {"persons": {"pivot_count": ["employment"]}}
        result, added = aggregate_from_children(hh, "households", tables, config)

        row_99 = result.filter(pl.col("hh_id") == 99)
        for col in added:
            assert row_99[col][0] == 0

    def test_multiple_pivot_count_fields_are_added_then_strippable(self, tables):
        """Every field in pivot_count contributes its own columns, and all strip off."""
        hh = tables["households"]
        original_cols = set(hh.columns)

        config = {"persons": {"pivot_count": ["employment", "student"]}}
        enriched, added = aggregate_from_children(hh, "households", tables, config)

        assert [c for c in added if "employment" in c] == [
            "persons_count_employment=1",
            "persons_count_employment=2",
            "persons_count_employment=3",
        ]
        assert [c for c in added if "student" in c] == [
            "persons_count_student=0",
            "persons_count_student=1",
        ]

        result = strip_joined_columns(enriched, added)
        assert set(result.columns) == original_cols
