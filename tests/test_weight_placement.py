"""The placement check: every weight sits on a record its consumer will receive.

Three ways weight has gone missing, each pinned so it raises rather than
reaching a model short:

* weight left on a record the profile does not admit;
* a supplied total that shrank in the redistribution;
* weight on a record the formatter removes -- a usable person in an unusable
  household, which no per-table flag reveals.
"""

import polars as pl
import pytest

from processing.weighting.validation.weight_checks import check_weight_placement

FLAG = "usable_p"


def _tables(*, person_flags: list[bool], person_weights: list[float]) -> dict[str, pl.DataFrame]:
    """Two households; household 2 is unusable. Persons 1 and 2 are in 1, person 3 in 2."""
    return {
        "households": pl.DataFrame(
            {"hh_id": [1, 2], "hh_weight": [10.0, 0.0], FLAG: [True, False]}
        ),
        "persons": pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "hh_id": [1, 1, 2],
                "person_weight": person_weights,
                FLAG: person_flags,
            }
        ),
    }


class TestItPasses:
    """Clean tables go through without a sound."""

    def test_weight_only_on_kept_records(self):
        """Household 2 and its person carry nothing, so nothing is lost."""
        check_weight_placement(
            _tables(person_flags=[True, True, False], person_weights=[5.0, 5.0, 0.0]),
            FLAG,
            supplied_totals={"households": 10.0, "persons": 10.0},
        )


class TestItRaises:
    """Each failure names the table and the amount."""

    def test_weight_on_a_record_the_profile_rejects(self):
        """Person 3 is unusable yet carries 4."""
        with pytest.raises(ValueError, match=r"1 persons record.*not admitted"):
            check_weight_placement(
                _tables(person_flags=[True, True, False], person_weights=[5.0, 5.0, 4.0]),
                FLAG,
            )

    def test_a_supplied_total_that_shrank(self):
        """12 was supplied for persons; only 10 is there."""
        with pytest.raises(ValueError, match=r"persons: person_weight sums to 10.0000 but 12.0000"):
            check_weight_placement(
                _tables(person_flags=[True, True, False], person_weights=[5.0, 5.0, 0.0]),
                FLAG,
                supplied_totals={"persons": 12.0},
            )

    def test_a_usable_person_in_an_unusable_household(self):
        """Person 3 passes on their own, carries 4, and their household does not pass.

        Every per-table flag is satisfied. Only running the formatter's own
        record selection shows it: the person references a household the
        profile does not keep, which the cascade should have carried down.
        """
        with pytest.raises(ValueError, match=r"1 persons record\(s\) .* reference a households"):
            check_weight_placement(
                _tables(person_flags=[True, True, True], person_weights=[5.0, 5.0, 4.0]),
                FLAG,
            )


class TestPartialCalls:
    """A table present without a verdict is a partial call, not a failure."""

    def test_an_unflagged_table_skips_the_kept_records_check(self, caplog):
        """The redistribution already skipped it; the check says so rather than failing."""
        tables = _tables(person_flags=[True, True, True], person_weights=[5.0, 5.0, 4.0])
        tables["persons"] = tables["persons"].drop(FLAG)
        with caplog.at_level("DEBUG"):
            check_weight_placement(tables, FLAG)
        assert "Skipping the kept-records total check: persons" in caplog.text
