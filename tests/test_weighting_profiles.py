"""The per-profile column contract, from config to consumer.

Three things have to line up. A config names its universes, and an ambiguous
naming is refused rather than guessed at. Each universe's columns are its base
name with the profile appended verbatim, and the suffix never reaches the id
columns derived from the same stem. A consumer then reads the columns belonging
to the profile it gates on -- reading another profile's weights, or falling back
to a bare column, publishes expansion factors for a universe the output is not
describing.
"""

import polars as pl
import pytest

from processing.formatting.usable_records import select_profile_weights
from processing.weighting.core.hierarchy import (
    HIERARCHY,
    LEVELS,
    seed_col_for,
    weight_col_for,
    weight_columns_for,
)
from processing.weighting.core.specs import WeightingConfig, resolve_fitted_profiles

STRICT = "ctramp"
RELAXED = "analysis"
PROFILE = "analysis"


def _config(**overrides) -> WeightingConfig:
    """A WeightingConfig with only the universe settings that matter here."""
    return WeightingConfig(geography={}, state_fips="06", pums_year=2023, **overrides)


# ---------------------------------------------------------------------------
# Which universes a run is being asked to weight
#
# Both weighting entry points resolve this through one function, so they cannot
# disagree about what naming a flag, a profile list, or both is meant to mean.
# The cases that must be refused are all ones where the config is ambiguous
# rather than wrong in a way the run would notice.
# ---------------------------------------------------------------------------
class TestResolvingTheUniverse:
    """What the two spellings resolve to, and which are refused."""

    def test_a_single_flag_means_one_unsuffixed_set(self):
        """``(None,)`` is what makes every column keep its declared name."""
        assert resolve_fitted_profiles("ctramp", None) == (None,)

    def test_profiles_are_returned_in_the_order_given(self):
        """Fits run in this order, and their reports are read in it."""
        assert resolve_fitted_profiles(None, ["b", "a"]) == ("b", "a")

    @pytest.mark.parametrize(
        "profiles",
        [
            pytest.param([], id="an_empty_list"),
            pytest.param(None, id="nothing_at_all"),
        ],
    )
    def test_an_unstated_universe_is_refused(self, profiles):
        """Both spellings of "no opinion" reach the same refusal."""
        with pytest.raises(ValueError, match="needs a universe"):
            resolve_fitted_profiles(None, profiles)

    def test_naming_both_is_refused(self):
        """They say different things about what gets fitted and what gets written.

        The message names both, so the fix is visible without going back to
        the config.
        """
        with pytest.raises(ValueError, match="Name either usability_profile") as excinfo:
            resolve_fitted_profiles("x", ["y"])
        assert "'x'" in str(excinfo.value)
        assert "y" in str(excinfo.value)

    def test_a_repeated_profile_is_refused(self):
        """The second fit would overwrite the first under the same column names."""
        with pytest.raises(ValueError, match="same profile twice"):
            resolve_fitted_profiles(None, ["a", "b", "a"])


class TestTheConfigUsesIt:
    """WeightingConfig delegates, and adds only the rule that is its own."""

    def test_a_profile_config_exposes_one_fit_each(self):
        """One balancing run per profile, in the order configured."""
        assert _config(weight_profiles=("a", "b")).fitted_profiles == ("a", "b")

    def test_flag_for_resolves_the_profile_to_its_column(self):
        """A profile gates on its own verdict column, whichever way it was named."""
        assert _config(weight_profiles=("a",)).flag_for("a") == "usable_a"
        assert _config(usability_profile="x").flag_for(None) == "usable_x"

    def test_profiles_without_the_gate_are_refused(self):
        """Every fit would then see the same seed and differ in name only.

        That is the one outcome this whole change exists to prevent: weights
        indistinguishable from fitted ones that match no control total.
        """
        with pytest.raises(ValueError, match="requires exclude_incompletes"):
            _config(weight_profiles=("a",), exclude_incompletes=False)

    def test_a_single_flag_without_the_gate_is_still_allowed(self):
        """It weights every complete household, which is a coherent thing to ask."""
        assert _config(
            usability_profile="survey_complete", exclude_incompletes=False
        ).fitted_profiles == (None,)


# ---------------------------------------------------------------------------
# How a universe's weight columns are named
# ---------------------------------------------------------------------------
class TestColumnNaming:
    """The suffix is appended as given -- no stripping, no case change."""

    @pytest.mark.parametrize("level", HIERARCHY, ids=lambda level: level.table)
    def test_every_level_resolves(self, level):
        """Both spellings agree, with and without a profile, for every table."""
        assert weight_col_for(level.weight_col, None) == level.weight_col
        assert level.weight_col_for(None) == level.weight_col
        assert weight_columns_for(None)[level.table] == level.weight_col

        assert weight_col_for(level.weight_col, PROFILE) == f"{level.weight_col}_{PROFILE}"
        assert level.weight_col_for(PROFILE) == f"{level.weight_col}_{PROFILE}"
        assert weight_columns_for(PROFILE)[level.table] == f"{level.weight_col}_{PROFILE}"

    @pytest.mark.parametrize(
        ("resolve", "base", "profile", "expected"),
        [
            # The obvious "tidier" rule would drop the _usable that every
            # profile name in practice ends with.  It must not.
            pytest.param(weight_col_for, "day_weight", PROFILE, "day_weight_analysis"),
            pytest.param(weight_col_for, "hh_weight", "ctramp", "hh_weight_ctramp"),
            # A profile name is a label, not something to normalise.
            pytest.param(
                weight_col_for, "tour_weight", "Weird_Name_v2", "tour_weight_Weird_Name_v2"
            ),
            # base_weight belongs to a fit, so it is suffixed like the weights.
            pytest.param(seed_col_for, "base_weight", None, "base_weight"),
            pytest.param(seed_col_for, "base_weight", PROFILE, "base_weight_analysis"),
        ],
        ids=[
            "trailing_suffix_kept",
            "another_profile",
            "case_and_underscores_survive",
            "seed_column_unsuffixed",
            "seed_column_takes_the_same_suffix",
        ],
    )
    def test_the_profile_is_appended_verbatim(self, resolve, base, profile, expected):
        """The resolved name is the base plus the profile, spelled as given."""
        assert resolve(base, profile) == expected

    @pytest.mark.parametrize(
        ("resolve", "base", "match"),
        [
            pytest.param(weight_col_for, "hh_wt", "not a hierarchy weight column", id="a_typo"),
            pytest.param(
                weight_col_for,
                "base_weight",
                "not a hierarchy weight column",
                id="seed_column_is_not_a_weight_column",
            ),
            pytest.param(
                seed_col_for,
                "hh_weight",
                "not a seed column",
                id="weight_column_is_not_a_seed_column",
            ),
        ],
    )
    def test_an_undeclared_name_raises(self, resolve, base, match):
        """A stem the hierarchy does not declare is a caller's typo, not a new level."""
        with pytest.raises(ValueError, match=match):
            resolve(base, PROFILE)

    def test_ids_are_the_table_keys_we_expect(self):
        """``id_col`` is read off ``weight_col``, so the suffix must not reach it."""
        assert LEVELS["households"].id_col == "hh_id"
        assert LEVELS["unlinked_trips"].id_col == "unlinked_trip_id"
        assert LEVELS["joint_tours"].id_col == "joint_tour_id"

    def test_the_key_says_which_side_of_the_edge_it_is_read_from(self):
        """Households are the root, so there is no edge and no key to carry.

        A DOWN edge carries the parent's id, identifying the record the weight
        came from; an UP edge carries its own, identifying the grouping.
        """
        assert LEVELS["households"].key is None
        assert LEVELS["persons"].key == "hh_id"
        assert LEVELS["joint_tours"].key == "joint_tour_id"


def _households(**columns) -> dict[str, pl.DataFrame]:
    """A one-row households table carrying the given weight columns."""
    return {"households": pl.DataFrame({"hh_id": [1], **columns})}


class TestResolution:
    """The named profile's column becomes the base name; the others go."""

    def test_each_consumer_gets_its_own_numbers(self):
        """The same tables, read under two profiles, give two different weights.

        The suffix comes off the profile that was asked for, so every
        downstream read is unchanged, and the other profile's column goes --
        it is not this consumer's to deliver, and invites the wrong read.
        """
        tables = _households(**{f"hh_weight_{STRICT}": [7.0], f"hh_weight_{RELAXED}": [9.0]})

        strict = select_profile_weights(tables, STRICT)["households"]
        assert strict["hh_weight"].to_list() == [7.0]
        assert f"hh_weight_{STRICT}" not in strict.columns
        assert f"hh_weight_{RELAXED}" not in strict.columns

        relaxed = select_profile_weights(tables, RELAXED)["households"]
        assert relaxed["hh_weight"].to_list() == [9.0]
        assert f"hh_weight_{STRICT}" not in relaxed.columns
        assert f"hh_weight_{RELAXED}" not in relaxed.columns

    def test_every_level_is_resolved_not_just_households(self):
        """One rule over the whole hierarchy, not a per-table special case."""
        tables = {
            "days": pl.DataFrame({"day_id": [1], f"day_weight_{STRICT}": [3.0]}),
            "tours": pl.DataFrame({"tour_id": [1], f"tour_weight_{STRICT}": [4.0]}),
        }
        out = select_profile_weights(tables, STRICT)
        assert out["days"]["day_weight"].to_list() == [3.0]
        assert out["tours"]["tour_weight"].to_list() == [4.0]

    def test_the_input_is_not_mutated(self):
        """The caller's frames are left alone, as everywhere else in the gate."""
        tables = _households(**{f"hh_weight_{STRICT}": [7.0]})
        select_profile_weights(tables, STRICT)
        assert f"hh_weight_{STRICT}" in tables["households"].columns


class TestNoSuffixedColumns:
    """A run that weighted a single profile writes the bare names, and is untouched."""

    def test_a_bare_column_is_left_alone(self):
        """Nothing to resolve, and nothing that needs resolving."""
        tables = _households(hh_weight=[5.0])
        out = select_profile_weights(tables, STRICT)["households"]
        assert out["hh_weight"].to_list() == [5.0]

    def test_a_table_with_no_weight_at_all_is_left_alone(self):
        """A table can reach the gate before any weighting step ran."""
        tables = _households()
        assert select_profile_weights(tables, STRICT)["households"].columns == ["hh_id"]

    def test_a_non_weight_table_is_ignored(self):
        """Only the levels that carry weights are considered."""
        tables = {"zones": pl.DataFrame({"taz": [1], "hh_weight_x": [1.0]})}
        assert "hh_weight_x" in select_profile_weights(tables, STRICT)["zones"].columns


class TestTheUnweightedProfile:
    """The case this raises on, because falling back is worse than stopping."""

    def test_a_profile_that_was_never_weighted_raises(self):
        """Otherwise the bare column is read -- whatever it happens to hold.

        This is the failure the e2e caught: a formatter reading a profile the
        weighting was not told to fit, publishing a loader placeholder as an
        expansion factor.
        """
        tables = _households(**{f"hh_weight_{RELAXED}": [9.0]})
        with pytest.raises(ValueError, match="but not for 'ctramp'") as excinfo:
            select_profile_weights(tables, STRICT)
        # and the message names what was weighted, so the fix is obvious
        message = str(excinfo.value)
        assert RELAXED in message
        assert "weight_profiles" in message

    def test_a_stale_bare_column_does_not_rescue_it(self):
        """A bare column beside another profile's is still not this profile's."""
        tables = _households(hh_weight=[1.0], **{f"hh_weight_{RELAXED}": [9.0]})
        with pytest.raises(ValueError, match="but not for"):
            select_profile_weights(tables, STRICT)

    def test_both_present_prefers_the_profile_and_warns(self, caplog):
        """A bare column can be legitimate loader input, so this is not fatal."""
        tables = _households(hh_weight=[1.0], **{f"hh_weight_{STRICT}": [7.0]})
        with caplog.at_level("WARNING"):
            out = select_profile_weights(tables, STRICT)["households"]
        assert out["hh_weight"].to_list() == [7.0]
        assert any("carries both" in record.message for record in caplog.records)
