"""Degenerate and adversarial inputs to the usability cascade.

The cascade's happy path is covered in ``test_usability_profiles`` (what a
profile means, and that the naming is total). This file is the other half: the
shapes that arrive when something upstream is missing, empty, null, or
contradictory.

The single invariant tying them together is that **a verdict column is a
boolean, always**. Every consumer -- the weighting, both formatters, the writer
-- reads it as one, and a null would make each of them decide separately what
an unknown means. So an input the cascade cannot judge has to come out False,
never null and never quietly True. The tests below walk the ways an input can be
unjudgeable: a descriptor column that is not there, a descriptor that is there
but null, no rows at all, no surveyable members, a parent that does not exist.
"""

from datetime import datetime
from typing import ClassVar

import polars as pl
import pytest

from data_canon.codebook.days import TravelDow
from data_canon.codebook.tours import TourCategory, TourDataQuality
from processing.completeness import (
    NO_ZONE_COVERAGE,
    HouseholdDayNeeds,
    JointTours,
    TourClosesAt,
    UsabilityProfile,
    cascade_complete,
    parse_usability_profiles,
    stamp_usable,
    suggest_usability_columns,
)
from processing.completeness.usable_tours import _flag_tours
from tests.fixtures.schema_utils import add_household_days

STRICT = UsabilityProfile("test", TourClosesAt.PRIMARY_HOME, HouseholdDayNeeds.ALL_MEMBERS)
WIDEST = UsabilityProfile("widest", TourClosesAt.ANYWHERE, HouseholdDayNeeds.NOTHING)

DATE = datetime(2023, 5, 1)


def _stamp(tables: dict, profile: UsabilityProfile = STRICT) -> dict[str, pl.DataFrame]:
    working: dict[str, pl.DataFrame | None] = dict(tables)
    cascade_complete(add_household_days(working))
    stamp_usable(working, profile)
    return {name: df for name, df in working.items() if df is not None}


def _one_person(**tour_overrides) -> dict:
    """One person, one day, one tour -- the smallest frame the cascade accepts."""
    tour = {
        "tour_id": [10],
        "person_day_id": [1],
        "person_id": [1],
        "survey_complete": [True],
        "parent_tour_id": [10],
        "tour_data_quality": [TourDataQuality.VALID.value],
        "tour_category": [TourCategory.COMPLETE.value],
    }
    tour.update(tour_overrides)
    return {
        "households": pl.DataFrame({"hh_id": [1], "survey_complete": [True]}),
        "persons": pl.DataFrame(
            {"person_id": [1], "hh_id": [1], "survey_complete": [True], "surveyable": [True]}
        ),
        "person_days": pl.DataFrame(
            {
                "person_day_id": [1],
                "person_id": [1],
                "hh_id": [1],
                "travel_date": [DATE],
                "survey_complete": [True],
            }
        ),
        "tours": pl.DataFrame(tour),
    }


class TestAVerdictIsAlwaysABoolean:
    """No table may leave a null in the column consumers gate on.

    ``survey_complete`` is filled before it is used, but the tour descriptors were not:
    ``is_in`` propagates null, so a tour whose quality was never established came
    out null rather than rejected. That null then reached the delivered output,
    where the formatters happened to fill it and the weighting did not.
    """

    @pytest.mark.parametrize(
        "override",
        [
            # ``is_in`` returns null for a null input, so this term needs filling.
            pytest.param({"tour_data_quality": [None]}, id="quality"),
            # Same for the equality term, which is only live at ``primary_home``.
            pytest.param({"tour_category": [None]}, id="category"),
        ],
    )
    def test_a_null_descriptor_is_rejected_not_propagated(self, override: dict):
        """An input the cascade cannot judge comes out False, never null."""
        tables = _one_person(**override)
        assert tables["tours"][next(iter(override))].to_list() == [None]

        stamped = _stamp(tables)

        assert stamped["tours"]["usable_test"].to_list() == [False]

    def test_no_table_carries_a_null_verdict(self):
        """The property itself, over every table the cascade touches."""
        tables = _one_person(tour_data_quality=[None], tour_category=[None])
        tables["linked_trips"] = pl.DataFrame(
            {
                "linked_trip_id": [100],
                "tour_id": [10],
                "person_day_id": [1],
                "survey_complete": [None],
            }
        )
        tables["joint_tours"] = pl.DataFrame(
            {"joint_tour_id": [1], "person_day_id": [1], "survey_complete": [None]}
        )

        stamped = _stamp(tables)

        nulls = {
            name: df["usable_test"].null_count()
            for name, df in stamped.items()
            if "usable_test" in df.columns
        }
        assert nulls == dict.fromkeys(nulls, 0)


class TestDescriptorsThatAreNotThere:
    """A frame may predate tour extraction and carry neither descriptor.

    Each term is skipped rather than assumed, so the verdict falls back to
    ``survey_complete`` alone. That is the honest answer -- the cascade cannot judge a
    structure it was given no column for -- but it is *looser*, so the absence
    must never be silent at the point a consumer reads the result. That guard
    lives in the formatters; here we only pin what the cascade does.
    """

    def test_without_quality_the_quality_term_is_skipped(self):
        """No column means no judgement to make, not a judgement of False."""
        tables = _one_person()
        tables["tours"] = tables["tours"].drop("tour_data_quality")

        stamped = _stamp(tables)

        assert stamped["tours"]["usable_test"].to_list() == [True]

    def test_without_either_descriptor_the_verdict_is_complete_alone(self):
        """With both terms gone, the floor is all that is left -- and it still bites."""
        tables = _one_person(survey_complete=[False])
        tables["tours"] = tables["tours"].drop("tour_data_quality", "tour_category")

        stamped = _stamp(tables)

        assert stamped["tours"]["usable_test"].to_list() == [False]

    def test_a_bad_quality_still_bites_without_the_category_column(self):
        """The terms are independent: losing one does not disarm the other."""
        tables = _one_person(tour_data_quality=[TourDataQuality.SPATIAL_GAP.value])
        tables["tours"] = tables["tours"].drop("tour_category")

        stamped = _stamp(tables)

        assert stamped["tours"]["usable_test"].to_list() == [False]


class TestTheCategoryTermOnlyAppliesAtPrimaryHome:
    """Past ``primary_home`` the two columns would contradict each other.

    That the term is not dead at ``primary_home`` is pinned by
    ``test_completeness::test_only_admissible_tours_are_usable``, whose third
    tour has a valid quality and a partial category.

    ``tour_category`` and ``tour_data_quality`` state the same fact about where a
    tour ends. A profile admitting the partial quality codes has already decided
    that fact does not disqualify, so keeping a COMPLETE-category term beside it
    would re-impose exactly what the axis just relaxed -- and the profile would
    look inert.
    """

    @pytest.mark.parametrize("category", list(TourCategory))
    def test_anywhere_admits_a_partial_tour_whatever_its_category(self, category):
        """Every category, so the term is gone rather than merely usually agreeing."""
        stamped = _stamp(
            _one_person(
                tour_data_quality=[TourDataQuality.PARTIAL_DIARY_EDGE.value],
                tour_category=[category.value],
            ),
            WIDEST,
        )

        assert stamped["tours"]["usable_widest"].to_list() == [True]


class TestEmptyTables:
    """A run can legitimately have no rows; it must still have the columns.

    The writer delivers whatever the cascade registered, so a zero-row table that
    silently lost its verdict column would change the output *schema* -- and the
    formatters, which raise on a missing column, would fail on an empty run
    rather than produce an empty output.
    """

    EMPTY: ClassVar[dict[str, dict[str, pl.DataType]]] = {
        "households": {"hh_id": pl.Int64, "survey_complete": pl.Boolean},
        "persons": {
            "person_id": pl.Int64,
            "hh_id": pl.Int64,
            "survey_complete": pl.Boolean,
            "surveyable": pl.Boolean,
        },
        "person_days": {
            "person_day_id": pl.Int64,
            "person_id": pl.Int64,
            "hh_id": pl.Int64,
            "travel_date": pl.Datetime,
            "survey_complete": pl.Boolean,
        },
        "tours": {
            "tour_id": pl.Int64,
            "person_day_id": pl.Int64,
            "survey_complete": pl.Boolean,
            "parent_tour_id": pl.Int64,
            "tour_data_quality": pl.Int64,
            "tour_category": pl.Int64,
        },
    }

    @pytest.fixture
    def stamped(self) -> dict[str, pl.DataFrame]:
        """Every table zero-row but fully typed, as an empty run delivers them."""
        return _stamp({name: pl.DataFrame(schema=s) for name, s in self.EMPTY.items()})

    @pytest.mark.parametrize("table", list(EMPTY))
    def test_every_table_still_gains_the_column(self, stamped, table):
        """A vanished column would change the output schema on an empty run."""
        assert stamped[table].height == 0
        # Boolean, not Null-dtype, or the writer's schema check fails downstream.
        assert stamped[table].schema["usable_test"] == pl.Boolean

    def test_household_days_gain_both_columns_too(self, stamped):
        """The household-day's completeness and the profile's verdict, typed."""
        assert stamped["household_days"].schema["survey_complete"] == pl.Boolean
        assert stamped["household_days"].schema["usable_test"] == pl.Boolean


class TestSurveyableMembers:
    """The ALL reduction runs over surveyable member-days only.

    A date with no surveyable member's day on it still exists, but nothing the
    survey meant to observe was observed on it, so it is neither complete nor
    usable.
    """

    def _two_members(self, surveyable: list[bool], survey_complete: list[bool]) -> dict:
        return {
            "households": pl.DataFrame({"hh_id": [1], "survey_complete": [True]}),
            "persons": pl.DataFrame(
                {
                    "person_id": [1, 2],
                    "hh_id": [1, 1],
                    "survey_complete": [True, True],
                    "surveyable": surveyable,
                }
            ),
            "person_days": pl.DataFrame(
                {
                    "person_day_id": [1, 2],
                    "person_id": [1, 2],
                    "hh_id": [1, 1],
                    "travel_date": [DATE, DATE],
                    "survey_complete": survey_complete,
                }
            ),
            "tours": pl.DataFrame(
                {
                    "tour_id": [10, 20],
                    "person_day_id": [1, 2],
                    "person_id": [1, 2],
                    "survey_complete": [True, True],
                    "parent_tour_id": [10, 20],
                    "tour_data_quality": [TourDataQuality.VALID.value] * 2,
                    "tour_category": [TourCategory.COMPLETE.value] * 2,
                }
            ),
        }

    def test_an_unsurveyable_member_cannot_veto_the_date(self):
        """A roommate who files nothing must not cost the household its day."""
        stamped = _stamp(self._two_members([True, False], [True, False]))

        assert stamped["household_days"]["survey_complete"].to_list() == [True]
        assert stamped["household_days"]["usable_test"].to_list() == [True]
        assert stamped["households"]["usable_test"].to_list() == [True]
        # Their own row keeps its own verdict rather than borrowing the good one.
        assert stamped["person_days"]["usable_test"].to_list() == [True, False]

    def test_a_date_with_no_surveyable_member_carries_no_usable_day(self):
        """The household-day exists, but nobody it was meant to observe was observed."""
        stamped = _stamp(self._two_members([False, False], [False, False]))

        assert stamped["household_days"]["survey_complete"].to_list() == [False]
        assert stamped["person_days"]["usable_test"].to_list() == [False, False]
        assert stamped["households"]["usable_test"].to_list() == [False]

    def test_a_contradictory_unsurveyable_day_is_believed_not_overruled(self):
        """An unsurveyable person marked complete keeps that verdict.

        The survey could not collect their travel, so the row contradicts itself.
        The cascade does not invent a correction: the defect is upstream, and
        silently flipping it here would hide it from whoever has to fix it. It
        cannot rescue the household either: the one surveyable member's day is
        incomplete, so there is no usable household-day.
        """
        stamped = _stamp(self._two_members([True, False], [False, True]))

        assert stamped["person_days"]["survey_complete"].to_list() == [False, True]
        assert stamped["household_days"]["usable_test"].to_list() == [False]
        assert stamped["households"]["usable_test"].to_list() == [False]


class TestSubtoursWhoseParentIsMissing:
    """A parent that is not in the frame is a different defect, not this one."""

    def test_an_orphan_subtour_keeps_its_own_verdict(self):
        """Dropping it here would hide the missing parent behind a usability zero."""
        stamped = _stamp(_one_person(parent_tour_id=[99]))

        assert stamped["tours"]["usable_test"].to_list() == [True]

    def test_a_subtour_still_falls_when_its_parent_is_present_and_fails(self):
        """The parent rule is real; only its *absence* is treated as someone else's bug."""
        tables = _one_person()
        tables["tours"] = pl.DataFrame(
            {
                "tour_id": [10, 11],
                "person_day_id": [1, 1],
                "person_id": [1, 1],
                "survey_complete": [True, True],
                "parent_tour_id": [10, 10],
                "tour_data_quality": [
                    TourDataQuality.SPATIAL_GAP.value,
                    TourDataQuality.VALID.value,
                ],
                "tour_category": [TourCategory.COMPLETE.value] * 2,
            }
        )

        stamped = _stamp(tables)

        assert stamped["tours"].sort("tour_id")["usable_test"].to_list() == [False, False]


class TestTravelDays:
    """A day on a day of the week the profile does not take is unusable, with its travel."""

    TUE_THU = UsabilityProfile(
        "test",
        TourClosesAt.PRIMARY_HOME,
        HouseholdDayNeeds.ALL_MEMBERS,
        travel_days=(TravelDow.TUESDAY, TravelDow.WEDNESDAY, TravelDow.THURSDAY),
    )

    def _days_on(self, dows: list[int | None]) -> dict:
        """One person with one day per given weekday, each day carrying one tour and trip."""
        n = len(dows)
        ids = list(range(1, n + 1))
        tables = _one_person(
            tour_id=[10 * i for i in ids],
            person_day_id=ids,
            person_id=[1] * n,
            survey_complete=[True] * n,
            parent_tour_id=[10 * i for i in ids],
            tour_data_quality=[TourDataQuality.VALID.value] * n,
            tour_category=[TourCategory.COMPLETE.value] * n,
        )
        tables["person_days"] = pl.DataFrame(
            {
                "person_day_id": ids,
                "person_id": [1] * n,
                "hh_id": [1] * n,
                "day_num": ids,
                "travel_date": [datetime(2023, 5, i) for i in ids],
                "travel_dow": dows,
                "survey_complete": [True] * n,
            },
            schema_overrides={"travel_dow": pl.Int64},
        )
        tables["linked_trips"] = pl.DataFrame(
            {
                "linked_trip_id": [100 * i for i in ids],
                "tour_id": [10 * i for i in ids],
                "person_day_id": ids,
                "survey_complete": [True] * n,
            }
        )
        return tables

    def test_a_day_the_profile_does_not_take_is_unusable(self):
        """Tuesday is taken; Saturday is not, though it was reported in full."""
        stamped = _stamp(
            self._days_on([TravelDow.TUESDAY.value, TravelDow.SATURDAY.value]), self.TUE_THU
        )

        assert stamped["person_days"].sort("person_day_id")["usable_test"].to_list() == [
            True,
            False,
        ]

    def test_its_tours_and_trips_go_with_it(self):
        """Saturday's tour and trip inherit their day's verdict on the way down."""
        stamped = _stamp(
            self._days_on([TravelDow.TUESDAY.value, TravelDow.SATURDAY.value]), self.TUE_THU
        )

        assert stamped["tours"].sort("tour_id")["usable_test"].to_list() == [True, False]
        assert stamped["linked_trips"].sort("linked_trip_id")["usable_test"].to_list() == [
            True,
            False,
        ]

    def test_the_household_survives_on_a_day_it_takes(self):
        """Only Saturday's household-day fails; Tuesday's keeps the household and person."""
        stamped = _stamp(
            self._days_on([TravelDow.TUESDAY.value, TravelDow.SATURDAY.value]), self.TUE_THU
        )

        assert stamped["household_days"].sort("hh_day_id")["usable_test"].to_list() == [True, False]
        assert stamped["households"]["usable_test"].to_list() == [True]
        assert stamped["persons"]["usable_test"].to_list() == [True]

    def test_a_household_with_only_days_it_does_not_take_is_unusable(self):
        """Monday and Saturday are both outside Tue-Thu, so nothing is left to keep."""
        stamped = _stamp(
            self._days_on([TravelDow.MONDAY.value, TravelDow.SATURDAY.value]), self.TUE_THU
        )

        assert stamped["households"]["usable_test"].to_list() == [False]
        assert stamped["persons"]["usable_test"].to_list() == [False]

    def test_a_day_whose_weekday_is_unknown_is_unusable(self):
        """A day that cannot be placed in the week cannot be shown to be one it takes."""
        stamped = _stamp(self._days_on([TravelDow.TUESDAY.value, None]), self.TUE_THU)

        assert stamped["person_days"].sort("person_day_id")["usable_test"].to_list() == [
            True,
            False,
        ]

    def test_every_day_taken_reads_no_weekday(self):
        """A profile taking all seven days asks nothing of travel_dow, so it need not be there."""
        tables = self._days_on([TravelDow.SATURDAY.value])
        tables["person_days"] = tables["person_days"].drop("travel_dow")

        stamped = _stamp(tables, STRICT)

        assert stamped["person_days"]["usable_test"].to_list() == [True]

    def test_a_limit_without_travel_dow_raises(self):
        """A limit with nothing to read must stop, not pass every day."""
        tables = self._days_on([TravelDow.TUESDAY.value])
        tables["person_days"] = tables["person_days"].drop("travel_dow")

        with pytest.raises(ValueError, match="no travel_dow"):
            _stamp(tables, self.TUE_THU)


class TestExcludedPersons:
    """A person the cleaner excluded has no usable day, under any profile."""

    LOOSE = UsabilityProfile("test", TourClosesAt.PRIMARY_HOME, HouseholdDayNeeds.NOTHING)

    def _two_members(self, reasons: list[str | None]) -> dict:
        """Two members on one date, each with one complete tour and trip."""
        return {
            "households": pl.DataFrame({"hh_id": [1], "survey_complete": [True]}),
            "persons": pl.DataFrame(
                {
                    "person_id": [1, 2],
                    "hh_id": [1, 1],
                    "survey_complete": [True, True],
                    "surveyable": [True, True],
                    "exclusion_reason": reasons,
                },
                schema_overrides={"exclusion_reason": pl.String},
            ),
            "person_days": pl.DataFrame(
                {
                    "person_day_id": [1, 2],
                    "person_id": [1, 2],
                    "hh_id": [1, 1],
                    "travel_date": [DATE, DATE],
                    "survey_complete": [True, True],
                }
            ),
            "tours": pl.DataFrame(
                {
                    "tour_id": [10, 20],
                    "person_day_id": [1, 2],
                    "person_id": [1, 2],
                    "survey_complete": [True, True],
                    "parent_tour_id": [10, 20],
                    "tour_data_quality": [TourDataQuality.VALID.value] * 2,
                    "tour_category": [TourCategory.COMPLETE.value] * 2,
                }
            ),
            "linked_trips": pl.DataFrame(
                {
                    "linked_trip_id": [100, 200],
                    "tour_id": [10, 20],
                    "person_day_id": [1, 2],
                    "survey_complete": [True, True],
                }
            ),
        }

    def test_the_excluded_persons_day_tour_and_trip_are_unusable(self):
        """Everything of the excluded member goes; the other member keeps theirs."""
        stamped = _stamp(self._two_members([None, "survey_tech excluded: form"]), self.LOOSE)

        for table, key in (
            ("person_days", "person_day_id"),
            ("tours", "tour_id"),
            ("linked_trips", "linked_trip_id"),
        ):
            assert stamped[table].sort(key)["usable_test"].to_list() == [True, False], table
        assert stamped["persons"].sort("person_id")["usable_test"].to_list() == [True, False]

    def test_survey_complete_is_untouched(self):
        """The vendor's verdict stays the vendor's; only usability narrows."""
        stamped = _stamp(self._two_members([None, "survey_tech excluded: form"]), self.LOOSE)

        assert stamped["person_days"]["survey_complete"].to_list() == [True, True]

    def test_a_household_needing_every_member_loses_the_date(self):
        """An excluded member is an unusable member, so the whole date fails."""
        stamped = _stamp(self._two_members([None, "survey_tech excluded: form"]), STRICT)

        assert stamped["household_days"]["usable_test"].to_list() == [False]
        assert stamped["households"]["usable_test"].to_list() == [False]
        assert stamped["persons"]["usable_test"].to_list() == [False, False]

    def test_a_household_needing_no_whole_date_keeps_its_other_member(self):
        """With no whole-date rule, the household stands on the member it kept."""
        stamped = _stamp(self._two_members([None, "survey_tech excluded: form"]), self.LOOSE)

        assert stamped["households"]["usable_test"].to_list() == [True]

    def test_a_household_whose_members_are_all_excluded_is_unusable(self):
        """No household reason is needed: with no usable day, the household falls."""
        reason = "survey_tech excluded: form"
        stamped = _stamp(self._two_members([reason, reason]), self.LOOSE)

        assert stamped["households"]["usable_test"].to_list() == [False]

    def test_without_the_column_no_one_is_excluded(self):
        """The field defaults to null, so a table without it excludes no one."""
        tables = self._two_members([None, None])
        tables["persons"] = tables["persons"].drop("exclusion_reason")

        stamped = _stamp(tables, STRICT)

        assert stamped["households"]["usable_test"].to_list() == [True]


class TestATourNeedsEveryTrip:
    """One unusable trip fails its tour, which then takes the rest of its trips down."""

    def _two_tours(self, table: str, on_tour_10: list[bool]) -> dict:
        """Tour 10 carries the given trips; tour 11 carries one complete trip."""
        tables = _one_person(
            tour_id=[10, 11],
            person_day_id=[1, 1],
            person_id=[1, 1],
            survey_complete=[True, True],
            parent_tour_id=[10, 11],
            tour_data_quality=[TourDataQuality.VALID.value] * 2,
            tour_category=[TourCategory.COMPLETE.value] * 2,
        )
        tables[table] = pl.DataFrame(
            {
                "trip_id": list(range(len(on_tour_10) + 1)),
                "tour_id": [10] * len(on_tour_10) + [11],
                "person_day_id": [1] * (len(on_tour_10) + 1),
                "survey_complete": [*on_tour_10, True],
            }
        )
        return tables

    @pytest.mark.parametrize("table", ["linked_trips", "unlinked_trips"])
    def test_one_unusable_trip_fails_its_tour(self, table: str):
        """Tour 10 falls; tour 11 beside it, with all its trips usable, does not."""
        stamped = _stamp(self._two_tours(table, on_tour_10=[True, False]))

        assert stamped["tours"].sort("tour_id")["usable_test"].to_list() == [False, True]

    @pytest.mark.parametrize("table", ["linked_trips", "unlinked_trips"])
    def test_the_failed_tour_takes_its_other_trips_down(self, table: str):
        """The usable trip on tour 10 inherits its tour's verdict on the way down."""
        stamped = _stamp(self._two_tours(table, on_tour_10=[True, False]))

        assert stamped[table].sort("trip_id")["usable_test"].to_list() == [False, False, True]

    def test_the_day_survives_on_its_other_tour(self):
        """A day needs one usable tour, so one failed tour does not take it down."""
        stamped = _stamp(self._two_tours("linked_trips", on_tour_10=[True, False]))

        assert stamped["person_days"]["usable_test"].to_list() == [True]

    def test_every_trip_usable_leaves_the_tour_alone(self):
        """The rule changes nothing when every trip is usable."""
        stamped = _stamp(self._two_tours("linked_trips", on_tour_10=[True, True]))

        assert stamped["tours"].sort("tour_id")["usable_test"].to_list() == [True, True]

    def test_unflagged_trips_raise_rather_than_pass_every_tour(self):
        """Calling the fuse before the trips are flagged must not read as 'all trips fine'."""
        tables = self._two_tours("linked_trips", on_tour_10=[True, False])

        with pytest.raises(ValueError, match="Flag the trips first"):
            _flag_tours(tables, STRICT)


class TestJointQuorum:
    """A grouping is joint only while enough members survive."""

    def _joint_tour(self, member_qualities: list[TourDataQuality]) -> dict:
        n = len(member_qualities)
        return {
            "households": pl.DataFrame({"hh_id": [1], "survey_complete": [True]}),
            "persons": pl.DataFrame(
                {
                    "person_id": list(range(1, n + 1)),
                    "hh_id": [1] * n,
                    "survey_complete": [True] * n,
                    "surveyable": [True] * n,
                }
            ),
            "person_days": pl.DataFrame(
                {
                    "person_day_id": list(range(1, n + 1)),
                    "person_id": list(range(1, n + 1)),
                    "hh_id": [1] * n,
                    "travel_date": [DATE] * n,
                    "survey_complete": [True] * n,
                }
            ),
            "tours": pl.DataFrame(
                {
                    "tour_id": [10 + i for i in range(n)],
                    "person_day_id": list(range(1, n + 1)),
                    "person_id": list(range(1, n + 1)),
                    "joint_tour_id": [1] * n,
                    "survey_complete": [True] * n,
                    "parent_tour_id": [10 + i for i in range(n)],
                    "tour_data_quality": [q.value for q in member_qualities],
                    "tour_category": [TourCategory.COMPLETE.value] * n,
                }
            ),
            "joint_tours": pl.DataFrame(
                {"joint_tour_id": [1], "person_day_id": [1], "survey_complete": [True]}
            ),
        }

    def test_exactly_the_minimum_survives(self):
        """The boundary itself: two usable members is still joint."""
        qualities = [TourDataQuality.VALID] * 2 + [TourDataQuality.SPATIAL_GAP]

        stamped = _stamp(self._joint_tour(qualities), WIDEST)

        assert stamped["tours"]["usable_widest"].to_list()[:2] == [True, True]
        assert stamped["joint_tours"]["usable_widest"].to_list() == [True]

    def test_one_below_the_minimum_is_no_longer_joint(self):
        """A one-participant joint tour would violate CT-RAMP's num_participants."""
        qualities = [TourDataQuality.VALID] + [TourDataQuality.SPATIAL_GAP] * 2

        stamped = _stamp(self._joint_tour(qualities), WIDEST)

        assert stamped["joint_tours"]["usable_widest"].to_list() == [False]

    def test_a_grouping_with_no_members_at_all_falls_to_its_own_complete(self):
        """A member table that was never supplied is a legitimate partial call."""
        tables = _one_person()
        tables["joint_tours"] = pl.DataFrame(
            {"joint_tour_id": [1, 2], "person_day_id": [1, 1], "survey_complete": [True, False]}
        )

        stamped = _stamp(tables)

        assert stamped["joint_tours"]["usable_test"].to_list() == [True, False]


class TestStampingTwice:
    """The same profile run twice must not drift.

    Nothing forces a caller to stamp each profile once, and the step re-runs on
    resume. A pass that read its own previous output would compound.
    """

    def test_a_repeated_pass_is_idempotent(self):
        """Re-running the step on resume must not compound its own output."""
        tables = _stamp(self._mixed())
        first = {name: df["usable_test"].to_list() for name, df in tables.items()}
        columns_before = {name: df.columns for name, df in tables.items()}

        working: dict[str, pl.DataFrame | None] = dict(tables)
        stamp_usable(working, STRICT)

        assert {name: working[name]["usable_test"].to_list() for name in first} == first
        # The second pass writes the same two names, never a suffixed copy.
        assert {name: working[name].columns for name in columns_before} == columns_before

    def _mixed(self) -> dict:
        """One tour the profile admits and one it does not, so drift would show."""
        tables = _one_person()
        tables["tours"] = pl.DataFrame(
            {
                "tour_id": [10, 11],
                "person_day_id": [1, 1],
                "person_id": [1, 1],
                "survey_complete": [True, True],
                "parent_tour_id": [10, 11],
                "tour_data_quality": [
                    TourDataQuality.VALID.value,
                    TourDataQuality.SPATIAL_GAP.value,
                ],
                "tour_category": [TourCategory.COMPLETE.value] * 2,
            }
        )
        return tables


class TestTheFamilyPrefixesCannotCollide:
    """Every profile's verdict column is distinct, whatever the profile is called.

    This used to be a guarded failure. A profile derived its verdict column from
    its bare name, so one called ``hh_day_ctramp`` wrote into the column
    ``ctramp`` derived for its household-day reduction, and whichever ran second
    won silently. Prefixing the family removed the overlap, and the household-day
    reduction now lives on ``household_days`` under the same ``usable_`` name, so
    there is one family left -- this records why the guard is safe to be gone.
    """

    def _axes(
        self,
        closes_at: str = TourClosesAt.PRIMARY_HOME,
        household: str = HouseholdDayNeeds.ALL_MEMBERS,
    ) -> dict:
        return {
            "tour_closes_at": closes_at,
            "household_day_needs": household,
            "zone_coverage": NO_ZONE_COVERAGE,
            "joint_tours": JointTours.PARTIAL,
            "travel_days": [
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
            ],
        }

    # Names chosen to collide under the old scheme, plus the family prefixes
    # themselves, which are the other way anyone might trip it.
    ADVERSARIAL = (
        "a",
        "hh_day_a",
        "usable_a",
        "usable",
        "hh_day",
        "hh_day_usable_a",
        "usable_hh_day_a",
    )

    def test_no_two_adversarial_names_share_a_column(self):
        """Every name writes its own column, all distinct, all under the prefix."""
        profiles = parse_usability_profiles({n: self._axes() for n in self.ADVERSARIAL})

        written = [p.flag for p in profiles]
        assert len(set(written)) == len(written) == len(self.ADVERSARIAL)
        assert all(p.flag.startswith("usable_") for p in profiles)

        # `a` and `hh_day_a` wrote the same column before the prefix existed.
        by_name = {p.name: p for p in profiles}
        assert (by_name["a"].flag, by_name["hh_day_a"].flag) == ("usable_a", "usable_hh_day_a")


class TestTheDidYouMeanLine:
    """The candidates a consumer is offered when its column is not there.

    A profile's name comes from config, so there is nothing about a usability
    column that distinguishes it from any other boolean. The line says so by
    offering all of them rather than filtering to a shape it cannot know. That
    admits unrelated columns, which is the cheaper error: a reader discards one
    at a glance, where a confident empty answer sends them to the wrong step.
    """

    def test_every_boolean_is_offered_in_order(self):
        """Freely named profiles included, non-booleans excluded, sorted by name.

        Column order is an accident of the frame; the message should not be.
        """
        frame = pl.DataFrame(
            {
                "zeta": [True],
                "tour_id": [1],
                "keep_for_ctramp": [True],
                "tour_purpose": ["work"],
                "alpha": [True],
            }
        )

        line = suggest_usability_columns(frame)

        assert line.endswith("alpha, keep_for_ctramp, zeta.")
        assert "tour_purpose" not in line
        assert "tour_id" not in line

    def test_a_frame_with_no_booleans_says_so(self):
        """Better than an empty list, which reads as a truncated message."""
        frame = pl.DataFrame({"tour_id": [1], "tour_purpose": ["work"]})

        assert suggest_usability_columns(frame) == "It carries no boolean columns at all."
