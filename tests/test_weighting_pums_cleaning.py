"""The PUMS rules: which records count, in which dollars, on which weights."""

import polars as pl
import pytest

from processing.weighting.core.specs import PumsWeights
from processing.weighting.data_prep.pums_cleaning import (
    adjust_income,
    apply_pums_weights,
    clean_pums,
    drop_group_quarters,
    drop_vacant_homes,
)


@pytest.fixture
def households() -> pl.DataFrame:
    """Two occupied homes, one vacant, one institutional and one noninstitutional GQ."""
    return pl.DataFrame(
        {
            "SERIALNO": ["H1", "H2", "VAC", "GQI", "GQN"],
            "NP": [2, 1, 0, 1, 1],
            "TYPEHUGQ": [1, 1, 1, 2, 3],
            "WGTP": [100.0, 50.0, 80.0, 0.0, 0.0],
            "WGTP1": [110.0, 40.0, 70.0, 0.0, 0.0],
            "HINCP": [50_000.0, 20_000.0, -60_000.0, -60_000.0, -60_000.0],
            "ADJINC": [1.02, 1.01, 1.02, 1.02, 1.02],
        }
    )


@pytest.fixture
def persons() -> pl.DataFrame:
    """H1 has two members, H2 one; each GQ record is one person."""
    return pl.DataFrame(
        {
            "SERIALNO": ["H1", "H1", "H2", "GQI", "GQN"],
            "SPORDER": [1, 2, 1, 1, 1],
            "PWGTP": [90.0, 130.0, 60.0, 40.0, 30.0],
            "PWGTP1": [80.0, 120.0, 70.0, 50.0, 20.0],
        }
    )


def test_vacant_homes_are_dropped(households, persons):
    """A vacant home is not a household; nobody lives there to drop."""
    hh, per = drop_vacant_homes(households, persons)
    assert "VAC" not in hh["SERIALNO"].to_list()
    assert per.equals(persons)


def test_group_quarters_are_dropped_with_their_residents(households, persons):
    """Institutional and noninstitutional alike, for now."""
    hh, per = drop_group_quarters(households, persons)
    assert sorted(hh["SERIALNO"].to_list()) == ["H1", "H2", "VAC"]
    assert sorted(per["SERIALNO"].unique().to_list()) == ["H1", "H2"]


def test_income_is_in_the_pums_years_dollars(households):
    """HINCP x ADJINC, the factor as the API sends it."""
    hh = adjust_income(households.head(2))
    assert hh["HINCP"].to_list() == pytest.approx([51_000.0, 20_200.0])


@pytest.mark.parametrize(
    "adjinc",
    [
        pytest.param([1.02, None], id="blank"),
        pytest.param([1_019_518.0, 1_019_518.0], id="a_downloaded_files_integer_form"),
    ],
)
def test_income_stops_on_a_factor_that_is_not_one(households, adjinc):
    """A blank, or a factor a million times too big, is never applied."""
    hh = households.head(2).with_columns(pl.Series("ADJINC", adjinc, dtype=pl.Float64))
    with pytest.raises(ValueError, match="ADJINC is not an income factor"):
        adjust_income(hh)


@pytest.fixture
def occupied(households, persons) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Only H1 and H2, as the weights rule sees them after the drops."""
    return households.filter(pl.col("SERIALNO").is_in(["H1", "H2"])), persons.filter(
        pl.col("SERIALNO").is_in(["H1", "H2"])
    )


def test_household_option_gives_persons_their_households_weights(occupied):
    """Replicates follow the weight."""
    hh, per = apply_pums_weights(*occupied, PumsWeights.HOUSEHOLD)
    per = per.sort("SERIALNO", "SPORDER")
    assert per["PWGTP"].to_list() == [100.0, 100.0, 50.0]
    assert per["PWGTP1"].to_list() == [110.0, 110.0, 40.0]
    assert hh.equals(occupied[0])


def test_person_option_gives_households_their_members_mean(occupied):
    """H1: (90 + 130) / 2 = 110, replicate (80 + 120) / 2 = 100; H2 its one member."""
    hh, per = apply_pums_weights(*occupied, PumsWeights.PERSON)
    hh = hh.sort("SERIALNO")
    assert hh["WGTP"].to_list() == [110.0, 60.0]
    assert hh["WGTP1"].to_list() == [100.0, 70.0]
    assert per.equals(occupied[1])


def test_independent_option_changes_nothing(occupied):
    """Each level keeps its own weight."""
    hh, per = apply_pums_weights(*occupied, PumsWeights.INDEPENDENT)
    assert hh.equals(occupied[0])
    assert per.equals(occupied[1])


def test_replicates_must_pair_up(occupied):
    """A replicate on one side only cannot be carried across."""
    hh, per = occupied
    with pytest.raises(ValueError, match="replicate weights differ"):
        apply_pums_weights(hh, per.drop("PWGTP1"), PumsWeights.HOUSEHOLD)


def test_clean_pums_runs_every_rule(households, persons):
    """Vacant and group quarters gone, income adjusted, weights set."""
    hh, per = clean_pums(households, persons, pums_weights=PumsWeights.HOUSEHOLD)
    assert sorted(hh["SERIALNO"].to_list()) == ["H1", "H2"]
    assert hh.sort("SERIALNO")["HINCP"].to_list() == pytest.approx([51_000.0, 20_200.0])
    assert per.sort("SERIALNO", "SPORDER")["PWGTP"].to_list() == [100.0, 100.0, 50.0]


def test_a_missing_column_is_named(households, persons):
    """No rule runs on a table that lacks what it reads."""
    with pytest.raises(KeyError, match="ADJINC"):
        clean_pums(households.drop("ADJINC"), persons, pums_weights=PumsWeights.INDEPENDENT)
