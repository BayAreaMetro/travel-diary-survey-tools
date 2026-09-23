"""Unit tests for CT-RAMP formatter.

Tests formatting, field corrections, and end-to-end transformation from
canonical survey data to CT-RAMP model format.
"""

from datetime import datetime, time
from pathlib import Path

import polars as pl
import pytest

from data_canon.codebook.ctramp import (
    CTRAMPPersonType,
    CTRAMPTourCategory,
    JTFChoice,
    WFHChoice,
    build_alternatives,
    load_alternatives_from_csv,
)
from data_canon.codebook.households import IncomeBroad
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    JobType,
)
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import PurposeCategory
from data_canon.models.survey import JointTourModel
from processing.formatting.ctramp.ctramp_config import CTRAMPConfig
from processing.formatting.ctramp.format_ctramp import format_ctramp
from processing.formatting.ctramp.format_households import format_households
from processing.formatting.ctramp.format_persons import format_persons
from processing.formatting.ctramp.format_tours import (
    format_individual_tour,
)
from processing.formatting.ctramp.format_trips import (
    format_individual_trip,
)
from tests.fixtures import (
    create_household,
    create_linked_trip,
    create_person,
    create_tour,
    days_for_persons,
    empty_joint_trips,
    empty_unlinked_trips,
    get_tour_schema,
)
from tests.fixtures.schema_utils import model_to_polars_schema


def joint_tours_for(tours: pl.DataFrame) -> pl.DataFrame:
    """A joint_tours row per joint_tour_id the given tours name.

    hh_id and day_id come from the member tours rather than being invented, since
    joint_tours carries required references to households and days -- inventing
    them makes the joint tour itself dangle and get dropped.
    """
    schema = model_to_polars_schema(JointTourModel)
    schema["usable_test"] = pl.Boolean
    members = tours.filter(pl.col("joint_tour_id").is_not_null())
    if members.is_empty():
        return pl.DataFrame(schema=schema)
    return (
        members.group_by("joint_tour_id")
        .agg(
            pl.col("hh_id").first(),
            pl.col("day_id").first(),
            pl.len().cast(pl.Int64).alias("num_participants"),
        )
        .with_columns(pl.lit(value=True).alias("usable_test"))
        .sort("joint_tour_id")
    )


def format_tours_for(persons: pl.DataFrame, tours: pl.DataFrame, config: CTRAMPConfig):
    """Format one household's tours, with a round trip generated per tour.

    Returns the formatted tours, the formatted households and the canonical
    trips, since the trip-level tests need all three.
    """
    households = pl.DataFrame([create_household(hh_id=1, income_bin=IncomeBroad.INCOME_75TO100)])
    households_formatted = format_households(households, persons, tours, config)
    trips = pl.DataFrame(
        [
            create_linked_trip(
                trip_id=10000 + 2 * i + offset,
                tour_id=row["tour_id"],
                person_id=row["person_id"],
                hh_id=1,
                tour_direction=direction,
            )
            for i, row in enumerate(tours.iter_rows(named=True))
            for offset, direction in ((0, TourDirection.OUTBOUND), (1, TourDirection.INBOUND))
        ]
    )
    tours_formatted = format_individual_tour(
        tours_canonical=tours,
        linked_trips_canonical=trips,
        unlinked_trips_canonical=pl.DataFrame(),
        persons_canonical=persons,
        households_ctramp=households_formatted,
        config=config,
    )
    return tours_formatted, households_formatted, trips


class TestHouseholdFieldCorrections:
    """Tests for household field corrections."""

    def test_jtf_choice_computed_from_joint_tours(self, standard_config):
        """jtf_choice counts the household's joint tours by purpose.

        Two joint shopping tours have to reach TWO_SHOP rather than the old
        hardcoded -4, and the count has to survive the whole format_ctramp run.
        """
        households = pl.DataFrame([create_household(hh_id=1, home_taz=100)])
        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1),
                create_person(person_id=102, hh_id=1),
            ]
        )

        # Two joint shopping tours, each shared by both members.
        tours = pl.DataFrame(
            [
                create_tour(
                    tour_id=1000 + i,
                    hh_id=1,
                    person_id=person_id,
                    day_id=day_id,
                    joint_tour_id=joint_tour_id,
                    tour_purpose=PurposeCategory.SHOP,
                )
                for i, (person_id, day_id, joint_tour_id) in enumerate(
                    [
                        (101, 10101, 9001),
                        (102, 10201, 9001),
                        (101, 10101, 9002),
                        (102, 10201, 9002),
                    ],
                    start=1,
                )
            ],
            schema=get_tour_schema(),
        )

        # Both legs of each tour: a one-trip tour is structurally invalid and
        # would be dropped before it could count toward jtf_choice.
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    linked_trip_id=10000 + 4 * leg + i,
                    tour_id=1000 + i,
                    person_id=person_id,
                    day_id=day_id,
                    tour_direction=direction,
                    joint_tour_id=joint_tour_id,
                )
                for leg, direction in enumerate((TourDirection.OUTBOUND, TourDirection.INBOUND))
                for i, (person_id, day_id, joint_tour_id) in enumerate(
                    [
                        (101, 10101, 9001),
                        (102, 10201, 9001),
                        (101, 10101, 9002),
                        (102, 10201, 9002),
                    ],
                    start=1,
                )
            ]
        )

        result = format_ctramp(
            persons,
            households,
            linked_trips=trips,
            tours=tours,
            joint_trips=empty_joint_trips(),
            unlinked_trips=empty_unlinked_trips(),
            joint_tours=joint_tours_for(tours),
            days=days_for_persons(persons),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_survey_year_to_ctramp_year=standard_config.income_survey_year_to_ctramp_year,
            usability_profile="test",
        )

        households_ctramp = result["households_ctramp"]
        # With 2 joint shopping tours, should get TWO_SHOP (JTFChoice value 7)
        assert households_ctramp["jtf_choice"][0] == JTFChoice.TWO_SHOP.value, (
            "Should have TWO_SHOP jtf_choice"
        )


class TestPersonFieldCorrections:
    """Tests for person field corrections."""

    def test_inmf_matches_csv_fixture(self):
        """Validate that get_inmf_code_from_counts matches the CSV fixture row by row."""
        csv_path = (
            Path(__file__).parent
            / "fixtures"
            / "CTRAMP_IndividualNonMandatoryTourFrequencyAlternatives.csv"
        )

        # Example usage: print all alternatives
        csv_alternatives = load_alternatives_from_csv(csv_path)
        # Use `maxes` (inclusive max frequencies) with the new API
        py_alternatives = build_alternatives(
            maxes={
                "escort": 2,
                "shopping": 1,
                "othmaint": 1,
                "othdiscr": 1,
                "eatout": 1,
                "social": 1,
            }
        )

        # Compare
        for code in sorted(set(csv_alternatives.keys()).union(py_alternatives.keys())):
            alt_csv = csv_alternatives.get(code)
            alt_py = py_alternatives.get(code)
            assert alt_csv == alt_py, f"Mismatch for code {code}: CSV={alt_csv}, PY={alt_py}"

    def test_age_continuous_from_category_midpoint(self, standard_config):
        """Test that age is continuous value (midpoint), not category code."""
        persons = pl.DataFrame(
            [
                create_person(person_id=101, age=AgeCategory.AGE_UNDER_5),  # 2.5
                create_person(person_id=102, age=AgeCategory.AGE_5_TO_15),  # 10
                create_person(person_id=103, age=AgeCategory.AGE_35_TO_44),  # 39.5
                create_person(person_id=104, age=AgeCategory.AGE_85_AND_UP),  # 87.5
            ]
        )

        result = format_persons(persons, pl.DataFrame(), standard_config)

        # Age category midpoints, not the category codes 1-11.
        assert result["age"].to_list() == [2, 10, 39, 87]

    @pytest.mark.parametrize(
        ("purposes", "expected_code"),
        [
            # Codes are the alternative numbers in
            # CTRAMP_IndividualNonMandatoryTourFrequencyAlternatives.csv, keyed
            # on (escort, shopping, othmaint, othdiscr, eatout, social).
            pytest.param([PurposeCategory.WORK], 0, id="mandatory_tours_do_not_count"),
            pytest.param([PurposeCategory.OTHER], 2, id="one_othdiscr"),
            pytest.param([PurposeCategory.MEAL], 5, id="one_eatout"),
            pytest.param([PurposeCategory.ERRAND], 9, id="one_othmaint"),
            pytest.param([PurposeCategory.SHOP], 17, id="one_shopping"),
            pytest.param(
                [PurposeCategory.SHOP, PurposeCategory.MEAL, PurposeCategory.SOCIALREC],
                23,
                id="shop_eatout_social",
            ),
            pytest.param([PurposeCategory.ESCORT], 33, id="one_escort"),
            pytest.param([PurposeCategory.ESCORT] * 2, 65, id="two_escort"),
            # Counts above the codebook maximum are capped, not overflowed.
            pytest.param([PurposeCategory.ESCORT] * 3, 65, id="three_escort_caps_to_two"),
            pytest.param([PurposeCategory.SHOP] * 2, 17, id="two_shopping_caps_to_one"),
            pytest.param(
                [
                    PurposeCategory.ESCORT,
                    PurposeCategory.ESCORT,
                    PurposeCategory.SHOP,
                    PurposeCategory.ERRAND,
                    PurposeCategory.OTHER,
                    PurposeCategory.MEAL,
                    PurposeCategory.SOCIALREC,
                ],
                96,
                id="every_category_at_its_maximum",
            ),
        ],
    )
    def test_inmf_choice_binned_to_codebook(self, purposes, expected_code, standard_config):
        """inmf_choice bins a person's non-mandatory tours to a codebook alternative."""
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                create_tour(tour_id=1001 + i, person_id=101, tour_num=i + 1, tour_purpose=purpose)
                for i, purpose in enumerate(purposes)
            ],
            schema=get_tour_schema(),
        )

        tours_formatted, _, _ = format_tours_for(persons, tours, standard_config)
        result = format_persons(persons, tours_formatted, standard_config)

        assert result["inmf_choice"][0] == expected_code

    def test_wfh_choice_detects_work_from_home(self, standard_config):
        """Test that wfh_choice is derived from job_type and employment status.

        ``create_person`` emits no ``telecommute_time``, so only the ``job_type``
        fallback branch is exercised here; the production path that carries
        ``telecommute_time`` down from days is not reached.
        """
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    employment=Employment.EMPLOYED_FULLTIME,
                    job_type=JobType.FIXED.value,  # Not WFH
                ),
                create_person(
                    person_id=102,
                    hh_id=1,
                    employment=Employment.EMPLOYED_FULLTIME,
                    job_type=JobType.WFH.value,  # WFH
                ),
                create_person(
                    person_id=103,
                    hh_id=1,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    job_type=JobType.WFH.value,  # Non-worker, so not WFH
                ),
            ]
        )

        tours = pl.DataFrame(
            [
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    tour_purpose=PurposeCategory.WORK,
                )
            ],
            schema=get_tour_schema(),
        )

        tours_formatted, _, _ = format_tours_for(persons, tours, standard_config)
        result = format_persons(persons, tours_formatted, standard_config)

        assert result["wfh_choice"][0] == WFHChoice.NON_WORKER_OR_NO_WFH.value, (
            "Employed person with FIXED job_type should not be WFH"
        )
        assert result["wfh_choice"][1] == WFHChoice.WORKS_FROM_HOME.value, (
            "Employed person with WFH job_type should be WFH"
        )
        assert result["wfh_choice"][2] == WFHChoice.NON_WORKER_OR_NO_WFH.value, (
            "Non-worker should not be WFH even with WFH job_type"
        )

    def test_reported_telecommute_time_is_preferred_over_the_job_type(self, standard_config):
        """When days carry telecommute_time, that decides WFH, not job_type.

        This is the production path: ``format_persons`` reads the column when it
        is present and only falls back to ``job_type`` when it is not. The two
        disagree here on purpose, so a test that reached the fallback instead
        would fail rather than quietly agree.
        """
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    employment=Employment.EMPLOYED_FULLTIME,
                    job_type=JobType.FIXED.value,
                ),
                create_person(
                    person_id=102,
                    hh_id=1,
                    employment=Employment.EMPLOYED_FULLTIME,
                    job_type=JobType.FIXED.value,
                ),
            ]
        ).with_columns(pl.Series("telecommute_time", [0, 240]))

        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, tour_purpose=PurposeCategory.WORK)],
            schema=get_tour_schema(),
        )

        tours_formatted, _, _ = format_tours_for(persons, tours, standard_config)
        result = format_persons(persons, tours_formatted, standard_config)

        assert result["wfh_choice"].to_list() == [
            WFHChoice.NON_WORKER_OR_NO_WFH.value,
            WFHChoice.WORKS_FROM_HOME.value,
        ], "person 102 telecommutes and made no work tour, despite a FIXED job_type"


class TestIndividualTripFieldCorrections:
    """Tests for individual trip field corrections."""

    def test_trip_carries_the_hour_and_the_tour_purpose(self, standard_config):
        """The trip table derives depart_hour and joins tour_purpose through.

        ``depart_hour`` is the hour part of ``depart_time``; ``tour_purpose`` is
        a left join from the formatted tours, so a broken join shows up as a
        null rather than a wrong label.
        """
        households = pl.DataFrame([create_household(hh_id=1, income_bin=IncomeBroad.INCOME_50TO75)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1, tour_purpose=PurposeCategory.WORK)],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.OUTBOUND,
                    depart_time=datetime.combine(datetime(2024, 1, 1), time(8, 30)),
                )
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        tours_formatted = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )
        result = format_individual_trip(
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            tours_ctramp=tours_formatted,
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        assert result["depart_hour"][0] == 8, "depart_hour should be extracted from depart_time"
        assert result["tour_purpose"][0] == "work_med", "Should be income-segmented work"


class TestIndividualTourFieldCorrections:
    """Tests for individual tour field corrections."""

    def test_person_type_on_tours_is_the_integer_code(self, standard_config):
        """``person_type`` reaches the tour table as the CT-RAMP integer code.

        It is a left join from persons, where the string label lives in ``type``
        instead; a tour row carrying the label would mean the wrong column was
        joined. (This test was named and documented as asserting a string label,
        which is the opposite of what it checks.)
        """
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    age=AgeCategory.AGE_35_TO_44,
                    employment=Employment.EMPLOYED_FULLTIME,
                )
            ]
        )
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        assert isinstance(result["person_type"][0], int), "person_type should be integer enum"
        assert result["person_type"][0] == CTRAMPPersonType.FULL_TIME_WORKER.value, (
            "Should output person type code for Full-time worker"
        )

    def test_tour_category_string_not_int(self, standard_config):
        """Test that tour_category outputs string labels (MANDATORY, etc), not integers."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                # Mandatory tour
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    tour_purpose=PurposeCategory.WORK,
                ),
                # Non-mandatory tour
                create_tour(
                    tour_id=1002,
                    person_id=101,
                    tour_purpose=PurposeCategory.SHOP,
                ),
                # At-work subtour
                create_tour(
                    tour_id=1003,
                    person_id=101,
                    parent_tour_id=1001,
                    tour_purpose=PurposeCategory.MEAL,
                ),
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(trip_id=i, tour_id=tid, tour_direction=TourDirection.OUTBOUND)
                for i, tid in [(10001, 1001), (10002, 1002), (10003, 1003)]
            ]
            + [
                create_linked_trip(trip_id=i, tour_id=tid, tour_direction=TourDirection.INBOUND)
                for i, tid in [(10004, 1001), (10005, 1002), (10006, 1003)]
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        assert result["tour_category"][0] == CTRAMPTourCategory.MANDATORY.value, (
            "Work tour should be MANDATORY"
        )
        assert result["tour_category"][1] == CTRAMPTourCategory.INDIVIDUAL_NON_MANDATORY.value, (
            "Shopping should be INDIVIDUAL_NON_MANDATORY"
        )
        assert result["tour_category"][2] == CTRAMPTourCategory.AT_WORK.value, (
            "Subtour should be AT_WORK"
        )

    def test_tour_purpose_not_all_othdisc(self, standard_config):
        """Test that tour_purpose correctly maps various purposes, not all to 'othdisc'."""
        households = pl.DataFrame(
            [create_household(hh_id=1, income_bin=IncomeBroad.INCOME_75TO100)]
        )
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                create_tour(tour_id=1001, person_id=101, tour_purpose=PurposeCategory.WORK),
                create_tour(tour_id=1002, person_id=101, tour_purpose=PurposeCategory.SCHOOL),
                create_tour(tour_id=1003, person_id=101, tour_purpose=PurposeCategory.SHOP),
                create_tour(tour_id=1004, person_id=101, tour_purpose=PurposeCategory.MEAL),
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(trip_id=i, tour_id=tid, tour_direction=TourDirection.OUTBOUND)
                for i, tid in [(10001, 1001), (10002, 1002), (10003, 1003), (10004, 1004)]
            ]
            + [
                create_linked_trip(trip_id=i, tour_id=tid, tour_direction=TourDirection.INBOUND)
                for i, tid in [(10005, 1001), (10006, 1002), (10007, 1003), (10008, 1004)]
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        assert result["tour_purpose"].to_list() == [
            "work_med",  # income-segmented
            "school_grade",  # the default fixture person is not a college student
            "shopping",
            "eatout",
        ]
