"""Tests for add_existing_weights function."""

import polars as pl
import pytest

from data_canon.codebook.persons import AgeCategory
from processing.weighting.existing_weights import add_existing_weights


def _weight_file(tmp_path, name: str, **columns) -> str:
    """Write a weight CSV and return its path."""
    path = tmp_path / f"{name}.csv"
    pl.DataFrame(columns).write_csv(path)
    return str(path)


class TestAddExistingWeights:
    """Test add_existing_weights function."""

    def test_load_single_weight_file(self, tmp_path):
        """Test loading weights from a single file."""
        # Create test household data
        households = pl.DataFrame(
            {
                "hh_id": [1, 2, 3],
                "hh_size": [2, 3, 1],
            }
        )

        # Create test weight file
        weight_file = tmp_path / "hh_weights.csv"
        weights_df = pl.DataFrame(
            {
                "hh_id": [1, 2, 3],
                "hh_weight": [1.5, 2.0, 1.0],
            }
        )
        weights_df.write_csv(weight_file)

        # Load weights
        weights_config = {
            "hh_weight": {
                "weight_path": str(weight_file),
            }
        }

        result = add_existing_weights(
            weights=weights_config, households=households, usability_profile="test"
        )

        assert "households" in result
        assert "hh_weight" in result["households"].columns
        assert result["households"]["hh_weight"].to_list() == [1.5, 2.0, 1.0]

    def test_a_weight_column_already_there_is_replaced_not_duplicated(self, tmp_path):
        """Re-running over a table that already carries the column overwrites it.

        Joining onto a frame that already has the target column would otherwise
        leave polars to suffix the incoming one, and the stale values would be
        the ones every downstream read picks up.
        """
        households = pl.DataFrame({"hh_id": [1, 2], "hh_weight": [99.0, 99.0]})
        weights_config = {
            "hh_weight": {
                "weight_path": _weight_file(tmp_path, "hh", hh_id=[1, 2], hh_weight=[1.5, 2.0])
            }
        }

        result = add_existing_weights(
            weights=weights_config, households=households, usability_profile="test"
        )

        assert result["households"]["hh_weight"].to_list() == [1.5, 2.0]
        assert [c for c in result["households"].columns if c.startswith("hh_weight")] == [
            "hh_weight"
        ]

    def test_a_record_no_supplied_total_covers_is_reported(self, tmp_path, caplog):
        """A household absent from the weight file joins to null, and that is said.

        Left silent it is a column of nulls with no explanation of which records
        the supplied totals did not reach.
        """
        households = pl.DataFrame({"hh_id": [1, 2, 3], "hh_size": [2, 3, 1]})
        weights_config = {
            "hh_weight": {
                "weight_path": _weight_file(tmp_path, "hh", hh_id=[1, 2], hh_weight=[1.5, 2.0])
            }
        }

        with caplog.at_level("WARNING"):
            result = add_existing_weights(
                weights=weights_config, households=households, usability_profile="test"
            )

        assert result["households"]["hh_weight"].to_list() == [1.5, 2.0, None]
        assert "1 / 3 NULL" in caplog.text

    def test_custom_id_column_names(self, tmp_path):
        """Test using custom ID column names in table and weight file."""
        # Create test data with custom ID column
        persons = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "age": [
                    AgeCategory.AGE_25_TO_34.value,
                    AgeCategory.AGE_35_TO_44.value,
                    AgeCategory.AGE_45_TO_54.value,
                ],
            }
        )

        # Weight file uses different ID column name
        weight_file = tmp_path / "person_weights.csv"
        weights_df = pl.DataFrame(
            {
                "pid": [1, 2, 3],  # Different column name
                "person_weight": [1.2, 1.5, 1.8],
            }
        )
        weights_df.write_csv(weight_file)

        weights_config = {
            "person_weight": {
                "weight_path": str(weight_file),
                "weight_id_col": "pid",
            }
        }

        result = add_existing_weights(
            weights=weights_config, persons=persons, usability_profile="test"
        )

        assert "persons" in result
        assert "person_weight" in result["persons"].columns
        assert result["persons"]["person_weight"].to_list() == [1.2, 1.5, 1.8]

    def test_custom_weight_column_name(self, tmp_path):
        """Test using custom weight column name."""
        trips = pl.DataFrame(
            {
                "unlinked_trip_id": [1, 2, 3],
                "mode": ["car", "walk", "transit"],
            }
        )

        weight_file = tmp_path / "trip_weights.csv"
        weights_df = pl.DataFrame(
            {
                "unlinked_trip_id": [1, 2, 3],
                "wt": [2.0, 1.5, 1.8],  # Custom weight column name
            }
        )
        weights_df.write_csv(weight_file)

        weights_config = {
            "unlinked_trip_weight": {
                "weight_path": str(weight_file),
                "weight_col": "wt",
                "keep_name": True,
            }
        }

        result = add_existing_weights(
            weights=weights_config, unlinked_trips=trips, usability_profile="test"
        )

        assert "unlinked_trips" in result
        assert "wt" in result["unlinked_trips"].columns
        assert result["unlinked_trips"]["wt"].to_list() == [2.0, 1.5, 1.8]

    def test_custom_weight_column_name_is_redistributed(self, tmp_path):
        """A kept vendor column is the one usability redistributes.

        Under ``keep_name`` the vendor's column is the only record of where the
        weight landed, so reaching for the canonical name instead reads a column
        that was never created. Needs the usability column present, since that
        is what makes the redistribution run at all.
        """
        trips = pl.DataFrame(
            {
                "unlinked_trip_id": [1, 2, 3],
                "day_id": [1, 1, 1],
                "usable_test": [True, True, False],
            }
        )

        weight_file = tmp_path / "trip_weights.csv"
        pl.DataFrame({"unlinked_trip_id": [1, 2, 3], "wt": [2.0, 1.5, 1.8]}).write_csv(weight_file)

        result = add_existing_weights(
            weights={
                "unlinked_trip_weight": {
                    "weight_path": str(weight_file),
                    "weight_col": "wt",
                    "keep_name": True,
                }
            },
            unlinked_trips=trips,
            usability_profile="test",
        )

        wt = result["unlinked_trips"]["wt"]
        assert wt[2] == 0.0
        assert wt.sum() == pytest.approx(5.3)

    def test_derive_person_weights_from_household(self, tmp_path):
        """Test deriving person weights from household weights."""
        households = pl.DataFrame(
            {
                "hh_id": [1, 2],
                "hh_size": [2, 1],
            }
        )

        persons = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "hh_id": [1, 1, 2],
                "age": [
                    AgeCategory.AGE_25_TO_34.value,
                    AgeCategory.AGE_25_TO_34.value,
                    AgeCategory.AGE_45_TO_54.value,
                ],
            }
        )

        # Only provide household weights
        hh_weight_file = tmp_path / "hh_weights.csv"
        hh_weights_df = pl.DataFrame(
            {
                "hh_id": [1, 2],
                "hh_weight": [1.5, 2.0],
            }
        )
        hh_weights_df.write_csv(hh_weight_file)

        weights_config = {
            "hh_weight": {
                "weight_path": str(hh_weight_file),
            }
        }

        result = add_existing_weights(
            weights=weights_config,
            households=households,
            persons=persons,
            derive_missing_weights=True,
            usability_profile="test",
        )

        # Check households have weights
        assert "hh_weight" in result["households"].columns
        assert result["households"]["hh_weight"].to_list() == [1.5, 2.0]

        # Check persons derived weights from households
        assert "person_weight" in result["persons"].columns
        assert result["persons"]["person_weight"].to_list() == [1.5, 1.5, 2.0]

    def test_error_on_invalid_config_key(self, tmp_path):
        """Test that invalid config keys raise an error."""
        households = pl.DataFrame(
            {
                "hh_id": [1, 2],
            }
        )

        weight_file = tmp_path / "weights.csv"
        pl.DataFrame({"hh_id": [1, 2], "weight": [1.0, 2.0]}).write_csv(weight_file)

        weights_config = {
            "invalid_key": {  # Invalid key
                "weight_path": str(weight_file),
            }
        }

        with pytest.raises(ValueError, match="Invalid weight config key"):
            add_existing_weights(
                weights=weights_config, households=households, usability_profile="test"
            )

    def test_error_on_nonexistent_file(self):
        """Test that nonexistent weight file raises FileNotFoundError."""
        households = pl.DataFrame(
            {
                "hh_id": [1, 2],
            }
        )

        weights_config = {
            "hh_weight": {
                "weight_path": "/nonexistent/path/to/weights.csv",
            }
        }

        with pytest.raises(FileNotFoundError, match="Weight file does not exist"):
            add_existing_weights(
                weights=weights_config, households=households, usability_profile="test"
            )

    @pytest.mark.parametrize(
        ("columns", "match"),
        [
            pytest.param(
                {"wrong_id": [1, 2], "hh_weight": [1.0, 2.0]},
                "missing required ID column",
                id="no_id_column",
            ),
            pytest.param(
                {"hh_id": [1, 2], "wrong_weight": [1.0, 2.0]},
                "missing required weight column",
                id="no_weight_column",
            ),
        ],
    )
    def test_error_on_missing_column_in_weight_file(self, tmp_path, columns, match):
        """A weight file has to carry both the key it joins on and the weight."""
        households = pl.DataFrame({"hh_id": [1, 2]})
        weights_config = {"hh_weight": {"weight_path": _weight_file(tmp_path, "w", **columns)}}

        with pytest.raises(ValueError, match=match):
            add_existing_weights(
                weights=weights_config, households=households, usability_profile="test"
            )

    def test_multiple_tables_with_weights(self, tmp_path):
        """Test loading weights for multiple tables."""
        households = pl.DataFrame(
            {
                "hh_id": [1, 2],
            }
        )

        persons = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "hh_id": [1, 1, 2],
            }
        )

        # Create weight files
        hh_weight_file = tmp_path / "hh_weights.csv"
        pl.DataFrame(
            {
                "hh_id": [1, 2],
                "hh_weight": [1.5, 2.0],
            }
        ).write_csv(hh_weight_file)

        person_weight_file = tmp_path / "person_weights.csv"
        pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "person_weight": [1.2, 1.3, 2.1],
            }
        ).write_csv(person_weight_file)

        weights_config = {
            "hh_weight": {
                "weight_path": str(hh_weight_file),
            },
            "person_weight": {
                "weight_path": str(person_weight_file),
            },
        }

        result = add_existing_weights(
            weights=weights_config,
            households=households,
            persons=persons,
            usability_profile="test",
        )

        assert "hh_weight" in result["households"].columns
        assert "person_weight" in result["persons"].columns
        assert result["households"]["hh_weight"].to_list() == [1.5, 2.0]
        assert result["persons"]["person_weight"].to_list() == [1.2, 1.3, 2.1]

    def test_table_not_found_is_skipped_not_an_error(self, tmp_path):
        """Test that a warning is logged when weight file provided but table doesn't exist."""
        # No households provided
        weight_file = tmp_path / "hh_weights.csv"
        pl.DataFrame(
            {
                "hh_id": [1, 2],
                "hh_weight": [1.0, 2.0],
            }
        ).write_csv(weight_file)

        weights_config = {
            "hh_weight": {
                "weight_path": str(weight_file),
            }
        }

        result = add_existing_weights(
            weights=weights_config,
            households=None,  # Table not provided
            usability_profile="test",
        )

        # Should not raise error, just skip the table
        assert "households" not in result


class TestSuppliedTotalPreserved:
    """Supplied weights are redistributed onto usable records, never shrunk.

    The vendor's anchor cannot be re-balanced from here -- their weights already
    sum to their population estimate -- so dropping records must leave each
    table's supplied total intact. These use ``survey_complete`` rather than the
    default ``usable``, since the fixtures carry no tour structure.
    """

    def _households(self) -> pl.DataFrame:
        """Four households, one incomplete (hh 3)."""
        return pl.DataFrame(
            {
                "hh_id": [1, 2, 3, 4],
                "hh_size": [2, 3, 1, 2],
                "survey_complete": [True, True, False, True],
            }
        )

    def _weights_config(self, tmp_path) -> dict:
        weight_file = tmp_path / "hh_weights.csv"
        pl.DataFrame({"hh_id": [1, 2, 3, 4], "hh_weight": [10.0, 20.0, 30.0, 40.0]}).write_csv(
            weight_file
        )
        return {"hh_weight": {"weight_path": str(weight_file)}}

    def test_supplied_household_total_is_preserved(self, tmp_path):
        """Households have no parent, so the supplied total is held by rescaling."""
        result = add_existing_weights(
            weights=self._weights_config(tmp_path),
            households=self._households(),
            usability_profile="survey_complete",
        )
        weights = result["households"].sort("hh_id")["hh_weight"].to_list()
        # hh 3 (incomplete) stays 0; the supplied total of 100 is retained
        assert weights[2] == 0.0
        assert sum(weights) == pytest.approx(100.0)
        # Survivors scaled by 100/70, keeping their relative proportions
        scale = 100.0 / 70.0
        assert weights[0] == pytest.approx(10.0 * scale)
        assert weights[1] == pytest.approx(20.0 * scale)
        assert weights[3] == pytest.approx(40.0 * scale)

    def test_supplied_day_weight_is_conserved_within_the_person(self, tmp_path):
        """A supplied day weight moves to the *same person's* usable days.

        Day weights are conserved within the person -- never pooled across a
        household. Person 1's unusable day moves onto their own remaining day;
        person 2's days are untouched even though they share the household.
        """
        days = pl.DataFrame(
            {
                "day_id": [10, 20, 30, 40],
                "person_id": [1, 1, 2, 2],
                "hh_id": [1, 1, 1, 1],
                "survey_complete": [True, False, True, True],
            }
        )
        weight_file = tmp_path / "day_weights.csv"
        pl.DataFrame({"day_id": [10, 20, 30, 40], "day_weight": [10.0, 10.0, 5.0, 5.0]}).write_csv(
            weight_file
        )

        result = add_existing_weights(
            weights={"day_weight": {"weight_path": str(weight_file)}},
            days=days,
            usability_profile="survey_complete",
        )
        weights = result["days"].sort("day_id")["day_weight"].to_list()
        # Person 1: 20 supplied over one usable day; person 2: unchanged.
        assert weights == pytest.approx([20.0, 0.0, 5.0, 5.0])
        assert sum(weights) == pytest.approx(30.0)

    def test_missing_scope_column_raises(self, tmp_path):
        """Days without person_id cannot be conserved as declared, so this fails loudly."""
        days = pl.DataFrame({"day_id": [10, 20], "hh_id": [1, 1], "survey_complete": [True, False]})
        weight_file = tmp_path / "day_weights.csv"
        pl.DataFrame({"day_id": [10, 20], "day_weight": [10.0, 10.0]}).write_csv(weight_file)

        with pytest.raises(ValueError, match="missing its scope column"):
            add_existing_weights(
                weights={"day_weight": {"weight_path": str(weight_file)}},
                days=days,
                usability_profile="survey_complete",
            )

    def test_no_usable_record_is_safe(self, tmp_path):
        """If nothing is usable there is nowhere to put the weight; no error."""
        households = pl.DataFrame(
            {"hh_id": [1, 2], "hh_size": [2, 3], "survey_complete": [False, False]}
        )
        weight_file = tmp_path / "hh_weights.csv"
        pl.DataFrame({"hh_id": [1, 2], "hh_weight": [10.0, 20.0]}).write_csv(weight_file)
        result = add_existing_weights(
            weights={"hh_weight": {"weight_path": str(weight_file)}},
            households=households,
            usability_profile="survey_complete",
        )
        assert result["households"]["hh_weight"].to_list() == [0.0, 0.0]
