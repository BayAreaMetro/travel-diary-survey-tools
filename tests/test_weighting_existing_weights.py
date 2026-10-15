"""Tests for add_existing_weights function."""

import polars as pl
import pytest

from data_canon.codebook.persons import AgeCategory
from processing.completeness.kept_records import select_profile_columns
from processing.weighting.existing_weights import add_existing_weights


def _weight_file(tmp_path, name: str, **columns) -> str:
    """Write a weight CSV and return its path."""
    path = tmp_path / f"{name}.csv"
    pl.DataFrame(columns).write_csv(path)
    return str(path)


def _weights_for(profile: str, **kwargs) -> dict[str, pl.DataFrame]:
    """Run ``add_existing_weights`` for one profile, read under the base names.

    The step writes ``hh_weight_<profile>`` and so on. A consumer resolves its
    own profile's columns to the base names with ``select_profile_columns``,
    which is how these tests read them too.
    """
    return select_profile_columns(
        add_existing_weights(weight_profiles=[profile], **kwargs), profile
    )


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

        result = _weights_for(
            preserve_travel_totals=True,
            household_size_top_group=6,
            weights=weights_config,
            households=households,
            profile="test",
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

        result = _weights_for(
            preserve_travel_totals=True,
            household_size_top_group=6,
            weights=weights_config,
            households=households,
            profile="test",
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
            result = _weights_for(
                preserve_travel_totals=True,
                household_size_top_group=6,
                weights=weights_config,
                households=households,
                profile="test",
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

        result = _weights_for(
            preserve_travel_totals=True,
            household_size_top_group=6,
            weights=weights_config,
            persons=persons,
            profile="test",
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
            }
        }

        result = _weights_for(
            preserve_travel_totals=True,
            household_size_top_group=6,
            weights=weights_config,
            unlinked_trips=trips,
            profile="test",
        )

        assert "unlinked_trips" in result
        assert "wt" not in result["unlinked_trips"].columns
        assert result["unlinked_trips"]["unlinked_trip_weight"].to_list() == [2.0, 1.5, 1.8]

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

        result = _weights_for(
            preserve_travel_totals=True,
            household_size_top_group=6,
            weights=weights_config,
            households=households,
            persons=persons,
            derive_missing_weights=True,
            profile="test",
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
            _weights_for(
                preserve_travel_totals=True,
                household_size_top_group=6,
                weights=weights_config,
                households=households,
                profile="test",
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
            _weights_for(
                preserve_travel_totals=True,
                household_size_top_group=6,
                weights=weights_config,
                households=households,
                profile="test",
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
            _weights_for(
                preserve_travel_totals=True,
                household_size_top_group=6,
                weights=weights_config,
                households=households,
                profile="test",
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

        result = _weights_for(
            preserve_travel_totals=True,
            household_size_top_group=6,
            weights=weights_config,
            households=households,
            persons=persons,
            profile="test",
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

        result = _weights_for(
            preserve_travel_totals=True,
            household_size_top_group=6,
            weights=weights_config,
            households=None,  # Table not provided
            profile="test",
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

    def _one_person_each(self, households: pl.DataFrame) -> pl.DataFrame:
        """One person per household, sharing its verdict: every household is size 1."""
        return households.select(pl.col("hh_id").alias("person_id"), "hh_id", "survey_complete")

    def test_supplied_household_total_is_preserved(self, tmp_path):
        """Households have nothing above them, so the supplied total is held by rescaling.

        All four households have one person, so they share one size group and
        one factor.
        """
        households = self._households()
        result = _weights_for(
            preserve_travel_totals=True,
            household_size_top_group=6,
            weights=self._weights_config(tmp_path),
            households=households,
            persons=self._one_person_each(households),
            profile="survey_complete",
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

        result = _weights_for(
            preserve_travel_totals=True,
            household_size_top_group=6,
            weights={"day_weight": {"weight_path": str(weight_file)}},
            days=days,
            profile="survey_complete",
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
            _weights_for(
                preserve_travel_totals=True,
                household_size_top_group=6,
                weights={"day_weight": {"weight_path": str(weight_file)}},
                days=days,
                profile="survey_complete",
            )

    def test_no_usable_record_loses_the_total_and_raises(self, tmp_path):
        """If nothing is usable there is nowhere to put the weight, and the run stops.

        The supplied 30 cannot land anywhere, so the table's total would be
        lost. The size group names where.
        """
        households = pl.DataFrame({"hh_id": [1, 2], "survey_complete": [False, False]})
        weight_file = tmp_path / "hh_weights.csv"
        pl.DataFrame({"hh_id": [1, 2], "hh_weight": [10.0, 20.0]}).write_csv(weight_file)
        with pytest.raises(ValueError, match=r"household size 1: .* no kept household"):
            _weights_for(
                preserve_travel_totals=True,
                household_size_top_group=6,
                weights={"hh_weight": {"weight_path": str(weight_file)}},
                households=households,
                persons=self._one_person_each(households),
                profile="survey_complete",
            )


class TestHouseholdsScaleBySize:
    """Each household size gets its own factor; persons copy their household.

    Four households. Households 2 and 4 are dropped, with their persons.

    | hh | size | supplied | kept |
    |----|------|----------|------|
    | 1  | 1    | 30       | yes  |
    | 2  | 1    | 10       | no   |
    | 3  | 2    | 20       | yes  |
    | 4  | 2    | 10       | no   |

    Persons are supplied their household's weight; each has one day, supplied
    the person's weight.

    * Size 1: 40 supplied, 30 kept, factor 4/3. Household 1 is 40.
    * Size 2: 30 supplied, 20 kept, factor 3/2. Household 3 is 30.

    Persons copy their household, so person 11 is 40 and persons 31 and 32 are
    30 each. Both totals hold: households 70, persons 100.

    One factor for all sizes would be 70/50 = 1.4: households 42 and 28,
    persons 42 + 28 + 28 = 98. The dropped household 4 is larger than the
    dropped household 2, which is what pulls the person total down.
    """

    def _run(
        self,
        tmp_path,
        *,
        top_group: int = 6,
        limits: dict[str, float] | None = None,
        dropped: tuple[int, ...] = (2, 4),
    ) -> dict[str, pl.DataFrame]:
        hh_ids = [1, 2, 3, 4]
        person_ids, person_hh = [11, 21, 31, 32, 41, 42], [1, 2, 3, 3, 4, 4]
        hh_weight = {1: 30.0, 2: 10.0, 3: 20.0, 4: 10.0}
        kept = [h not in dropped for h in person_hh]
        tables = {
            "households": pl.DataFrame(
                {"hh_id": hh_ids, "usable_p": [h not in dropped for h in hh_ids]}
            ),
            "persons": pl.DataFrame(
                {"person_id": person_ids, "hh_id": person_hh, "usable_p": kept}
            ),
            "days": pl.DataFrame(
                {
                    "day_id": [p * 10 for p in person_ids],
                    "person_id": person_ids,
                    "hh_id": person_hh,
                    "usable_p": kept,
                }
            ),
        }
        person_weight = [hh_weight[h] for h in person_hh]
        weights = {
            "hh_weight": {
                "weight_path": _weight_file(
                    tmp_path, "hh", hh_id=hh_ids, hh_weight=[hh_weight[h] for h in hh_ids]
                )
            },
            "person_weight": {
                "weight_path": _weight_file(
                    tmp_path, "person", person_id=person_ids, person_weight=person_weight
                )
            },
            "day_weight": {
                "weight_path": _weight_file(
                    tmp_path,
                    "day",
                    day_id=[p * 10 for p in person_ids],
                    day_weight=person_weight,
                )
            },
        }
        return _weights_for(
            weights=weights,
            profile="p",
            preserve_travel_totals=True,
            household_size_top_group=top_group,
            max_weight_factor=limits,
            **tables,
        )

    @staticmethod
    def _weights(result, table: str, key: str, col: str) -> list[float]:
        return result[table].sort(key)[col].to_list()

    def test_each_size_has_its_own_factor(self, tmp_path):
        """Household 1 is 30 x 4/3; household 3 is 20 x 3/2."""
        result = self._run(tmp_path)
        hh = self._weights(result, "households", "hh_id", "hh_weight")
        assert hh == pytest.approx([40.0, 0.0, 30.0, 0.0])

    def test_persons_copy_their_household(self, tmp_path):
        """Persons 11, 31 and 32 carry their household's weight."""
        result = self._run(tmp_path)
        persons = self._weights(result, "persons", "person_id", "person_weight")
        assert persons == pytest.approx([40.0, 0.0, 30.0, 30.0, 0.0, 0.0])

    def test_each_household_is_consistent(self, tmp_path):
        """Household weight x number of persons = the sum of its persons' weights."""
        result = self._run(tmp_path)
        per_hh = (
            result["persons"]
            .group_by("hh_id")
            .agg(pl.col("person_weight").sum(), pl.len().alias("n"))
            .join(result["households"].select("hh_id", "hh_weight"), on="hh_id")
            .sort("hh_id")
        )
        assert per_hh["person_weight"].to_list() == pytest.approx(
            (per_hh["hh_weight"] * per_hh["n"]).to_list()
        )

    def test_both_totals_are_held(self, tmp_path):
        """Households 70 and persons 100, as supplied; days carry the person total."""
        result = self._run(tmp_path)
        assert result["households"]["hh_weight"].sum() == pytest.approx(70.0)
        assert result["persons"]["person_weight"].sum() == pytest.approx(100.0)
        assert result["days"]["day_weight"].sum() == pytest.approx(100.0)

    def test_sizes_from_the_top_group_up_share_one_factor(self, tmp_path):
        """A top group of 1 puts every size in one group: one factor, 70/50.

        The household total still holds; the person total falls to 98.
        """
        result = self._run(tmp_path, top_group=1)
        hh = self._weights(result, "households", "hh_id", "hh_weight")
        assert hh == pytest.approx([42.0, 0.0, 28.0, 0.0])
        assert result["persons"]["person_weight"].sum() == pytest.approx(98.0)

    def test_a_size_with_weight_but_no_kept_household_raises(self, tmp_path):
        """Drop household 1 too: size 1's 40 has no kept household to go to."""
        with pytest.raises(ValueError, match=r"household size 1: .* no kept household"):
            self._run(tmp_path, dropped=(1, 2, 4))

    def test_household_sizes_need_the_persons_table(self, tmp_path):
        """Size is the number of persons, so without persons there is no size."""
        weight = _weight_file(tmp_path, "hh", hh_id=[1], hh_weight=[10.0])
        with pytest.raises(ValueError, match="persons table"):
            _weights_for(
                weights={"hh_weight": {"weight_path": weight}},
                profile="p",
                preserve_travel_totals=True,
                household_size_top_group=6,
                households=pl.DataFrame({"hh_id": [1], "usable_p": [True]}),
            )

    def test_the_top_group_is_required(self, tmp_path):
        """The config must state the grouping; the code does not assume one."""
        weight = _weight_file(tmp_path, "hh", hh_id=[1], hh_weight=[10.0])
        with pytest.raises(TypeError, match="household_size_top_group"):
            _weights_for(
                weights={"hh_weight": {"weight_path": weight}},
                profile="p",
                preserve_travel_totals=True,
                households=pl.DataFrame({"hh_id": [1], "usable_p": [True]}),
            )


class TestTheWeightFactorLimit:
    """An opt-in limit on how far a record's weight may grow past its supplied weight.

    Same households as ``TestHouseholdsScaleBySize``: household 1 grows 4/3 and
    household 3 grows 3/2, and their persons with them.
    """

    _run = TestHouseholdsScaleBySize._run

    def test_a_limit_above_every_factor_passes(self, tmp_path):
        """The largest factor is 1.5, under 2."""
        result = self._run(tmp_path, limits={"households": 2.0})
        assert result["households"]["hh_weight"].sum() == pytest.approx(70.0)

    def test_a_household_past_the_limit_is_named(self, tmp_path):
        """Household 3 grows 1.5x, past 1.4; household 1's 1.33x is within it."""
        with pytest.raises(ValueError, match=r"1 households record\(s\) .* hh_id: \[3\]"):
            self._run(tmp_path, limits={"households": 1.4})

    def test_persons_can_be_limited_too(self, tmp_path):
        """Persons 31 and 32 grow 1.5x with their household."""
        with pytest.raises(ValueError, match=r"2 persons record\(s\) .* person_id: \[31, 32\]"):
            self._run(tmp_path, limits={"persons": 1.4})

    def test_a_limit_on_a_table_without_a_supplied_weight_raises(self, tmp_path):
        """A factor needs a supplied weight to compare against."""
        with pytest.raises(ValueError, match="max_weight_factor names tours"):
            self._run(tmp_path, limits={"tours": 2.0})


class TestADroppedPersonStaysInTheirHousehold:
    """A dropped person's weight goes to the household's other persons, never further.

    Every supplied household and person weight is 10.

    | person | hh | size | kept | days, supplied |
    |--------|----|------|------|----------------|
    | 11     | 1  | 2    | yes  | 110: 5 kept, 111: 5 dropped |
    | 12     | 1  | 2    | no   | 120: 10 dropped |
    | 21     | 2  | 1    | yes  | 210: 10 kept |
    | 31     | 3  | 1    | no   | 310: 10 dropped; household 3 dropped |

    * Households, size 1: 20 supplied, 10 kept, factor 2. Household 2 is 20.
      Size 2: household 1 is kept whole, factor 1, so it stays 10.
    * Persons copy their household: household 1 is 10 x 2 = 20 on person 11
      alone, since person 12 is dropped. Person 21 is 20.
    * Days: day 110 is person 11's only kept day, so it carries 20. Day 210
      carries 20.
    """

    def _run(self, tmp_path) -> dict[str, pl.DataFrame]:
        tables = {
            "households": pl.DataFrame({"hh_id": [1, 2, 3], "usable_p": [True, True, False]}),
            "persons": pl.DataFrame(
                {
                    "person_id": [11, 12, 21, 31],
                    "hh_id": [1, 1, 2, 3],
                    "usable_p": [True, False, True, False],
                }
            ),
            "days": pl.DataFrame(
                {
                    "day_id": [110, 111, 120, 210, 310],
                    "person_id": [11, 11, 12, 21, 31],
                    "hh_id": [1, 1, 1, 2, 3],
                    "usable_p": [True, False, False, True, False],
                }
            ),
        }
        weights = {
            "hh_weight": {
                "weight_path": _weight_file(
                    tmp_path, "hh", hh_id=[1, 2, 3], hh_weight=[10.0, 10.0, 10.0]
                )
            },
            "person_weight": {
                "weight_path": _weight_file(
                    tmp_path,
                    "person",
                    person_id=[11, 12, 21, 31],
                    person_weight=[10.0, 10.0, 10.0, 10.0],
                )
            },
            "day_weight": {
                "weight_path": _weight_file(
                    tmp_path,
                    "day",
                    day_id=[110, 111, 120, 210, 310],
                    day_weight=[5.0, 5.0, 10.0, 10.0, 10.0],
                )
            },
        }
        return _weights_for(
            weights=weights,
            profile="p",
            preserve_travel_totals=True,
            household_size_top_group=6,
            **tables,
        )

    def test_the_persons_carry_their_household(self, tmp_path):
        """Person 11 takes person 12's share of household 1; person 21 copies household 2."""
        persons = self._run(tmp_path)["persons"].sort("person_id")["person_weight"].to_list()
        assert persons == pytest.approx([20.0, 0.0, 20.0, 0.0])

    def test_the_days_carry_their_persons_weight(self, tmp_path):
        """Day 110 is person 11's only kept day; day 210 is person 21's."""
        days = self._run(tmp_path)["days"].sort("day_id")["day_weight"].to_list()
        assert days == pytest.approx([20.0, 0.0, 0.0, 20.0, 0.0])

    def test_each_persons_days_sum_to_the_person(self, tmp_path):
        """The identity the step checks, person by person."""
        result = self._run(tmp_path)
        per_person = (
            result["days"]
            .group_by("person_id")
            .agg(pl.col("day_weight").sum())
            .join(result["persons"].select("person_id", "person_weight"), on="person_id")
        )
        assert per_person["day_weight"].to_list() == pytest.approx(
            per_person["person_weight"].to_list()
        )

    def test_the_totals_are_held(self, tmp_path):
        """Households 30 and persons 40, as supplied; days carry the person total."""
        result = self._run(tmp_path)
        assert result["households"]["hh_weight"].sum() == pytest.approx(30.0)
        assert result["persons"]["person_weight"].sum() == pytest.approx(40.0)
        assert result["days"]["day_weight"].sum() == pytest.approx(40.0)


class TestAStrandedPersonIsTreatedAsDropped:
    """A usable person with no kept day the supplier weighted cannot carry weight.

    Days take weight in proportion to the supplier's, so a day weighted 0 stays
    0. A person whose weighted days were all dropped has no day to put weight
    on, so they count as a dropped person. A household with no person able to
    carry weight counts as a dropped household. Both records stay usable, with
    weight 0.

    Every supplied household and person weight is 10.

    | person | hh | size | days, supplied | can carry |
    |--------|----|------|----------------|-----------|
    | 11     | 1  | 2    | 110: 10 dropped, 111: 0 kept | no |
    | 12     | 1  | 2    | 120: 10 kept | yes |
    | 21     | 2  | 1    | 210: 10 dropped, 211: 0 kept | no |
    | 31     | 3  | 1    | 310: 10 kept | yes |

    * Household 2 has no one who can carry weight, so it counts as dropped.
      Size 1: 20 supplied, 10 kept, factor 2, so household 3 is 20. Size 2:
      household 1 stays 10.
    * Persons copy their household: household 1 is 10 x 2 = 20 on person 12
      alone. Person 31 is 20.
    """

    def _run(self, tmp_path) -> dict[str, pl.DataFrame]:
        tables = {
            "households": pl.DataFrame({"hh_id": [1, 2, 3], "usable_p": [True, True, True]}),
            "persons": pl.DataFrame(
                {
                    "person_id": [11, 12, 21, 31],
                    "hh_id": [1, 1, 2, 3],
                    "usable_p": [True, True, True, True],
                }
            ),
            "days": pl.DataFrame(
                {
                    "day_id": [110, 111, 120, 210, 211, 310],
                    "person_id": [11, 11, 12, 21, 21, 31],
                    "hh_id": [1, 1, 1, 2, 2, 3],
                    "usable_p": [False, True, True, False, True, True],
                }
            ),
        }
        weights = {
            "hh_weight": {
                "weight_path": _weight_file(
                    tmp_path, "hh", hh_id=[1, 2, 3], hh_weight=[10.0, 10.0, 10.0]
                )
            },
            "person_weight": {
                "weight_path": _weight_file(
                    tmp_path,
                    "person",
                    person_id=[11, 12, 21, 31],
                    person_weight=[10.0, 10.0, 10.0, 10.0],
                )
            },
            "day_weight": {
                "weight_path": _weight_file(
                    tmp_path,
                    "day",
                    day_id=[110, 111, 120, 210, 211, 310],
                    day_weight=[10.0, 0.0, 10.0, 10.0, 0.0, 10.0],
                )
            },
        }
        return _weights_for(
            weights=weights,
            profile="p",
            preserve_travel_totals=True,
            household_size_top_group=6,
            **tables,
        )

    def test_the_stranded_persons_weight_stays_in_the_household(self, tmp_path):
        """Person 11's share of household 1 goes to person 12."""
        persons = self._run(tmp_path)["persons"].sort("person_id")
        assert persons["person_weight"].to_list() == pytest.approx([0.0, 20.0, 0.0, 20.0])

    def test_a_household_no_one_can_carry_is_dropped(self, tmp_path):
        """Household 2 goes to 0; household 3, the same size, takes its weight."""
        households = self._run(tmp_path)["households"].sort("hh_id")
        assert households["hh_weight"].to_list() == pytest.approx([10.0, 0.0, 20.0])

    def test_it_reaches_the_receivers_days(self, tmp_path):
        """Day 120 carries person 12's 20; day 310 carries person 31's 20."""
        days = self._run(tmp_path)["days"].sort("day_id")
        assert days["day_weight"].to_list() == pytest.approx([0.0, 0.0, 20.0, 0.0, 0.0, 20.0])

    def test_the_records_stay_usable(self, tmp_path):
        """Only the weight moves; the records and their verdicts are untouched."""
        result = self._run(tmp_path)
        assert result["persons"]["usable_p"].to_list() == [True, True, True, True]
        assert result["households"]["usable_p"].to_list() == [True, True, True]

    def test_usable_records_the_supplier_weighted_0_are_counted(self, tmp_path, caplog):
        """Days 111 and 211 are usable to us and were supplied 0; the log says so."""
        with caplog.at_level("INFO"):
            self._run(tmp_path)
        assert "days: 2 of 4 usable record(s) were supplied a day_weight_p of 0" in caplog.text


class TestTheStepChecksItsOwnHierarchy:
    """The identities are checked inside the step, on real runs, not only in tests."""

    def test_the_derived_levels_are_checked(self, tmp_path, monkeypatch):
        """Persons and days derived from a household file go through the hierarchy check."""
        seen: list[set[str]] = []

        def spy(*_args, only=None, **_kwargs):
            seen.append(set(only))

        monkeypatch.setattr("processing.weighting.existing_weights._check_hierarchy", spy)
        _weights_for(
            weights={
                "hh_weight": {
                    "weight_path": _weight_file(tmp_path, "hh", hh_id=[1], hh_weight=[10.0])
                }
            },
            profile="p",
            derive_missing_weights=True,
            preserve_travel_totals=True,
            household_size_top_group=6,
            households=pl.DataFrame({"hh_id": [1], "usable_p": [True]}),
            persons=pl.DataFrame({"person_id": [11], "hh_id": [1], "usable_p": [True]}),
            days=pl.DataFrame(
                {"day_id": [110], "person_id": [11], "hh_id": [1], "usable_p": [True]}
            ),
        )
        assert seen == [{"persons", "days"}]


class TestTripsFollowTheirDay:
    """A trip moves with its day, whatever ``preserve_travel_totals`` says.

    One household, one person (weight 10), two days supplied at 5 each. Day 111
    is unusable, so day 110 takes the person's whole 10: its factor is 2.

    Day 110 has trips 1001 (usable) and 1002 (unusable); day 111 has trip 1101.
    The vendor gives each trip its day's weight, 5.

    * ``false``: trip 1001 keeps its own claim times its day's factor, 5 x 2 =
      10. Trip 1002 is dropped, and its weight is gone.
    * ``true``: trip 1002's claim also moves onto 1001, within day 110, so 1001
      is 5 x 2 x (10 / 5) = 20.

    Trip 1101 is on a dropped day, so it is 0 either way: its day's weight went
    to day 110, and the trips on day 110 carry it.

    Each trip is its own linked trip and its own tour (tours 1, 3, 2), so tour
    1 must carry exactly what trip 1001 ends at.
    """

    def _run(
        self, tmp_path, preserve: bool, day_weights: tuple[float, ...] = (5.0, 5.0)
    ) -> dict[str, pl.DataFrame]:
        tables = {
            "households": pl.DataFrame({"hh_id": [1], "usable_p": [True]}),
            "persons": pl.DataFrame({"person_id": [11], "hh_id": [1], "usable_p": [True]}),
            "days": pl.DataFrame(
                {
                    "day_id": [110, 111],
                    "person_id": [11, 11],
                    "hh_id": [1, 1],
                    "usable_p": [True, False],
                }
            ),
            "unlinked_trips": pl.DataFrame(
                {
                    "unlinked_trip_id": [1001, 1002, 1101],
                    "linked_trip_id": [1, 2, 3],
                    "tour_id": [1, 3, 2],
                    "day_id": [110, 110, 111],
                    "person_id": [11, 11, 11],
                    "hh_id": [1, 1, 1],
                    "usable_p": [True, False, False],
                }
            ),
            "linked_trips": pl.DataFrame(
                {
                    "linked_trip_id": [1, 2, 3],
                    "tour_id": [1, 3, 2],
                    "day_id": [110, 110, 111],
                    "usable_p": [True, False, False],
                }
            ),
            "tours": pl.DataFrame(
                {"tour_id": [1, 2, 3], "day_id": [110, 111, 110], "usable_p": [True, False, False]}
            ),
        }
        weights = {
            "hh_weight": {"weight_path": _weight_file(tmp_path, "hh", hh_id=[1], hh_weight=[10.0])},
            "person_weight": {
                "weight_path": _weight_file(
                    tmp_path, "person", person_id=[11], person_weight=[10.0]
                )
            },
            "day_weight": {
                "weight_path": _weight_file(
                    tmp_path, "day", day_id=[110, 111], day_weight=list(day_weights)
                )
            },
            "unlinked_trip_weight": {
                "weight_path": _weight_file(
                    tmp_path,
                    "trip",
                    unlinked_trip_id=[1001, 1002, 1101],
                    unlinked_trip_weight=[5.0, 5.0, 5.0],
                )
            },
        }
        return _weights_for(
            weights=weights,
            profile="p",
            derive_missing_weights=True,
            preserve_travel_totals=preserve,
            household_size_top_group=6,
            **tables,
        )

    @pytest.mark.parametrize(("preserve", "expected"), [(False, 10.0), (True, 20.0)])
    def test_the_tour_carries_its_final_trips_weight(self, tmp_path, preserve, expected):
        """Tour 1 and linked trip 1 are trip 1001's weight, day factor included."""
        result = self._run(tmp_path, preserve=preserve)
        tours = result["tours"].sort("tour_id")["tour_weight"].to_list()
        linked = result["linked_trips"].sort("linked_trip_id")["linked_trip_weight"].to_list()
        assert tours == pytest.approx([expected, 0.0, 0.0])
        assert linked == pytest.approx([expected, 0.0, 0.0])

    def _trips(self, result) -> list[float]:
        return result["unlinked_trips"].sort("unlinked_trip_id")["unlinked_trip_weight"].to_list()

    def test_false_scales_each_kept_trip_by_its_days_factor(self, tmp_path):
        """5 x 2: the trip keeps its own claim, and its day doubled."""
        assert self._trips(self._run(tmp_path, preserve=False)) == pytest.approx([10.0, 0.0, 0.0])

    def test_true_also_moves_the_dropped_trip_onto_the_kept_one(self, tmp_path):
        """5 x 2 x 2: the day's factor, then trip 1002's claim within the day."""
        assert self._trips(self._run(tmp_path, preserve=True)) == pytest.approx([20.0, 0.0, 0.0])

    def test_a_weighted_trip_on_an_unweighted_day_raises(self, tmp_path):
        """The supplier weighted trip 1101 but not its day, 111: there is no factor.

        Day 110 keeps the person's weight, so the person can still carry it and
        the trip check is what fails.
        """
        with pytest.raises(ValueError, match=r"1 unlinked_trips record\(s\) .* day_weight of 0"):
            self._run(tmp_path, preserve=False, day_weights=[10.0, 0.0])


class TestPreserveTravelTotals:
    """Whether a dropped trip's or tour's weight moves to the kept ones, or is dropped.

    One person, one day. Tour 1 has trips 101 and 102 and is usable; tour 2 has
    trip 103 and is not. The vendor supplies trip weights 6, 12, 18 (total 36).

    A tour's weight is the mean of its final trips, so a tour and its own trips
    always agree. Tour totals are not a target.

    * ``true``: trip 103's 18 moves onto 101 and 102 in proportion, giving 12
      and 24, so trips on the day still total 36. Tour 1 is the mean of 12 and
      24, 18.
    * ``false``: every kept trip keeps exactly its own weight. Trips are 6, 12,
      0 (total 18) and tour 1 is the mean of 6 and 12, 9.
    """

    def _run(self, tmp_path, preserve: bool) -> dict[str, pl.DataFrame]:
        usable = {"usable_p": [True]}
        tables = {
            "households": pl.DataFrame({"hh_id": [1], **usable}),
            "persons": pl.DataFrame({"person_id": [11], "hh_id": [1], **usable}),
            "days": pl.DataFrame({"day_id": [110], "person_id": [11], "hh_id": [1], **usable}),
            "unlinked_trips": pl.DataFrame(
                {
                    "unlinked_trip_id": [101, 102, 103],
                    "linked_trip_id": [1, 2, 3],
                    "tour_id": [1, 1, 2],
                    "day_id": [110] * 3,
                    "usable_p": [True, True, False],
                }
            ),
            "linked_trips": pl.DataFrame(
                {
                    "linked_trip_id": [1, 2, 3],
                    "tour_id": [1, 1, 2],
                    "day_id": [110] * 3,
                    "usable_p": [True, True, False],
                }
            ),
            "tours": pl.DataFrame(
                {"tour_id": [1, 2], "day_id": [110, 110], "usable_p": [True, False]}
            ),
        }
        weights = {
            "hh_weight": {"weight_path": _weight_file(tmp_path, "hh", hh_id=[1], hh_weight=[10.0])},
            "person_weight": {
                "weight_path": _weight_file(
                    tmp_path, "person", person_id=[11], person_weight=[10.0]
                )
            },
            "day_weight": {
                "weight_path": _weight_file(tmp_path, "day", day_id=[110], day_weight=[10.0])
            },
            "unlinked_trip_weight": {
                "weight_path": _weight_file(
                    tmp_path,
                    "trip",
                    unlinked_trip_id=[101, 102, 103],
                    unlinked_trip_weight=[6.0, 12.0, 18.0],
                )
            },
        }
        return _weights_for(
            weights=weights,
            profile="p",
            derive_missing_weights=True,
            preserve_travel_totals=preserve,
            household_size_top_group=6,
            **tables,
        )

    def _weights(self, result, table: str, col: str, key: str) -> list[float]:
        return result[table].sort(key)[col].to_list()

    def test_true_holds_the_trip_totals(self, tmp_path):
        """Trip 103's weight moves to the kept trips; the trip total stays at 36."""
        result = self._run(tmp_path, preserve=True)
        unlinked = self._weights(
            result, "unlinked_trips", "unlinked_trip_weight", "unlinked_trip_id"
        )
        linked = self._weights(result, "linked_trips", "linked_trip_weight", "linked_trip_id")
        assert unlinked == pytest.approx([12.0, 24.0, 0.0])
        assert linked == pytest.approx([12.0, 24.0, 0.0])

    def test_true_makes_the_tour_the_mean_of_its_final_trips(self, tmp_path):
        """Tour 1's trips end at 12 and 24, so tour 1 is 18; tour 2 is dropped."""
        result = self._run(tmp_path, preserve=True)
        tours = self._weights(result, "tours", "tour_weight", "tour_id")
        assert tours == pytest.approx([18.0, 0.0])

    def test_false_keeps_every_kept_trip_at_its_own_weight(self, tmp_path):
        """Nothing moves: the dropped trip's 18 is gone, and the trip total is 18."""
        result = self._run(tmp_path, preserve=False)
        unlinked = self._weights(
            result, "unlinked_trips", "unlinked_trip_weight", "unlinked_trip_id"
        )
        linked = self._weights(result, "linked_trips", "linked_trip_weight", "linked_trip_id")
        assert unlinked == pytest.approx([6.0, 12.0, 0.0])
        assert linked == pytest.approx([6.0, 12.0, 0.0])

    def test_false_makes_the_tour_the_mean_of_its_final_trips(self, tmp_path):
        """Tour 1's trips end at 6 and 12, so tour 1 is 9; tour 2 is dropped."""
        result = self._run(tmp_path, preserve=False)
        tours = self._weights(result, "tours", "tour_weight", "tour_id")
        assert tours == pytest.approx([9.0, 0.0])

    @pytest.mark.parametrize("preserve", [True, False])
    def test_households_persons_and_days_are_held_either_way(self, tmp_path, preserve):
        """The option is about travel only; the population targets never move."""
        result = self._run(tmp_path, preserve=preserve)
        assert result["households"]["hh_weight"].sum() == pytest.approx(10.0)
        assert result["persons"]["person_weight"].sum() == pytest.approx(10.0)
        assert result["days"]["day_weight"].sum() == pytest.approx(10.0)

    def test_the_option_is_required(self, tmp_path):
        """The config must state the choice; the code does not assume one."""
        with pytest.raises(TypeError, match="preserve_travel_totals"):
            _weights_for(
                weights={
                    "hh_weight": {
                        "weight_path": _weight_file(tmp_path, "hh", hh_id=[1], hh_weight=[1.0])
                    }
                },
                profile="p",
                households=pl.DataFrame({"hh_id": [1], "usable_p": [True]}),
            )
