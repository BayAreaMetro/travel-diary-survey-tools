"""Edge cases in how joint travel reaches the CT-RAMP output.

CT-RAMP represents joint travel only at the *tour* level, and only for groups it
admits as joint. A survey detects sharing at the *trip* level, for any group. The
gap between those two facts is where this module lives:

* trip-level sharing with no joint tour behind it (the common escort case),
* groups CT-RAMP rejects on purpose grounds and reclassifies as individual,
* the structural invariants those two rules have to leave intact -- above all
  that the individual and joint files *partition* travel rather than overlap it,
  which is what makes their differing weight conventions safe to add up.

Fixtures here give members *unequal* weights on purpose: with equal weights the
sum, the mean and twice-the-first all coincide, so a wrong weight convention
still passes.
"""

from collections.abc import Sequence

import polars as pl
import pytest

from data_canon.codebook.ctramp import CTRAMPModeType
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import PurposeCategory
from processing.formatting.ctramp.ctramp_config import CTRAMPConfig
from processing.formatting.ctramp.format_households import format_households
from processing.formatting.ctramp.format_joint_tours import (
    format_joint_tour,
    identify_misclassified_joint_tours,
)
from processing.formatting.ctramp.format_joint_trips import format_joint_trip
from processing.formatting.ctramp.format_tours import format_individual_tour
from processing.formatting.ctramp.format_trips import format_individual_trip
from tests.fixtures import (
    create_household,
    create_linked_trip,
    create_person,
    create_tour,
    empty_joint_tours,
    get_tour_schema,
)

MIN_JOINT = 2
BOTH_LEGS = (TourDirection.OUTBOUND, TourDirection.INBOUND)

# Unequal on purpose -- see the module docstring.
MEMBER_WEIGHTS = (10.0, 30.0)


@pytest.fixture
def standard_config():
    """CT-RAMP config with the usual thresholds."""
    return CTRAMPConfig(
        income_low_threshold=30000,
        income_med_threshold=60000,
        income_high_threshold=100000,
        income_survey_year_to_ctramp_year=0.5319148936,
    )


def _two_person_household():
    households = pl.DataFrame([create_household(hh_id=1)])
    persons = pl.DataFrame(
        [
            create_person(person_id=101, hh_id=1, person_num=1),
            create_person(person_id=102, hh_id=1, person_num=2),
        ]
    )
    return households, persons


def _tours_frame(specs: Sequence[dict]) -> pl.DataFrame:
    """Build a canonical tours frame from per-tour spec dicts."""
    rows = [
        create_tour(
            tour_id=spec["tour_id"],
            person_id=spec["person_id"],
            person_num=spec["person_num"],
            tour_num=spec.get("tour_num", 1),
            joint_tour_id=spec.get("joint_tour_id"),
            tour_purpose=spec["purpose"],
            num_travelers=spec.get("num_travelers", 2),
            tour_weight=spec["weight"],
        )
        for spec in specs
    ]
    return pl.DataFrame(rows, schema=get_tour_schema()).with_columns(
        pl.col("joint_tour_id").cast(pl.Int64)
    )


def _shared_tour(
    purpose: PurposeCategory,
    *,
    joint_tour_id: int | None = 5001,
    weights: tuple[float, float] = MEMBER_WEIGHTS,
    num_travelers: int = 2,
):
    """Two members on tours of the same purpose, optionally grouped as joint."""
    return _tours_frame(
        [
            {
                "tour_id": 1001,
                "person_id": 101,
                "person_num": 1,
                "joint_tour_id": joint_tour_id,
                "purpose": purpose,
                "weight": weights[0],
                "num_travelers": num_travelers,
            },
            {
                "tour_id": 1002,
                "person_id": 102,
                "person_num": 2,
                "joint_tour_id": joint_tour_id,
                "purpose": purpose,
                "weight": weights[1],
                "num_travelers": num_travelers,
            },
        ]
    )


def _trips_for(
    tours: pl.DataFrame,
    *,
    joint_trip_ids: tuple[int | None, int | None] | None = None,
    shared_directions: Sequence[TourDirection] = BOTH_LEGS,
    unshared_joint_tours: Sequence[int] = (),
):
    """One outbound and one inbound trip per tour, carrying the tour's weight.

    A leg becomes a joint trip when its tour is joint, its direction is in
    *shared_directions*, and its joint tour is not in *unshared_joint_tours*.
    Ids are derived from the joint tour so two joint tours never collide; pass
    *joint_trip_ids* to set them directly, which is the only way to model
    trip-level sharing with no joint tour behind it.
    """
    rows = []
    trip_id = 1
    for tour in tours.iter_rows(named=True):
        joint_tour_id = tour["joint_tour_id"]
        for leg, direction in enumerate(BOTH_LEGS):
            if joint_trip_ids is not None:
                joint_trip_id = joint_trip_ids[leg]
            elif (
                joint_tour_id is not None
                and direction in shared_directions
                and joint_tour_id not in unshared_joint_tours
            ):
                joint_trip_id = joint_tour_id * 10 + leg
            else:
                joint_trip_id = None
            rows.append(
                create_linked_trip(
                    linked_trip_id=trip_id,
                    person_id=tour["person_id"],
                    person_num=tour["person_num"],
                    tour_id=tour["tour_id"],
                    joint_tour_id=joint_tour_id,
                    joint_trip_id=joint_trip_id,
                    tour_direction=direction,
                    num_travelers=tour["num_travelers"],
                    linked_trip_weight=tour["tour_weight"],
                )
            )
            trip_id += 1
    return pl.DataFrame(rows).with_columns(
        pl.col("joint_tour_id").cast(pl.Int64),
        pl.col("joint_trip_id").cast(pl.Int64),
    )


def _joint_trips_table(trips: pl.DataFrame) -> pl.DataFrame:
    """Aggregate member trips into the canonical joint_trips shape."""
    return (
        trips.filter(pl.col("joint_trip_id").is_not_null())
        .group_by("joint_trip_id")
        .agg(
            pl.col("hh_id").first(),
            pl.col("tour_id").first(),
            pl.col("o_purpose_category").first(),
            pl.col("d_purpose_category").first(),
            pl.col("o_taz").first(),
            pl.col("d_taz").first(),
            pl.col("mode_type").first(),
            pl.col("depart_time").first(),
            pl.col("arrive_time").first(),
            pl.col("tour_direction").first(),
            pl.len().alias("num_joint_travelers"),
            # The canonical convention: person-trips, so the SUM of members.
            pl.col("linked_trip_weight").sum().alias("joint_trip_weight"),
        )
    )


def _joint_tours_table(tours: pl.DataFrame) -> pl.DataFrame:
    """Aggregate member tours into the canonical joint_tours shape."""
    return (
        tours.filter(pl.col("joint_tour_id").is_not_null())
        .group_by("joint_tour_id")
        .agg(
            pl.col("hh_id").first(),
            pl.col("tour_weight").sum().alias("joint_tour_weight"),
        )
    )


def _format_all(tours, trips, persons, households, config, *, joint_tours_canonical=None):
    """Run the four tour/trip formatters over one scenario."""
    hh_ctramp = format_households(households, persons, tours, config)
    indiv_tours = format_individual_tour(
        tours_canonical=tours,
        linked_trips_canonical=trips,
        unlinked_trips_canonical=pl.DataFrame(),
        persons_canonical=persons,
        households_ctramp=hh_ctramp,
        config=config,
    )
    indiv_trips = format_individual_trip(
        linked_trips_canonical=trips,
        unlinked_trips_canonical=pl.DataFrame(),
        tours_ctramp=indiv_tours,
        persons_canonical=persons,
        households_ctramp=hh_ctramp,
        config=config,
    )
    joint_tours = format_joint_tour(
        tours_canonical=tours,
        linked_trips_canonical=trips,
        unlinked_trips_canonical=pl.DataFrame(),
        joint_tours_canonical=(
            empty_joint_tours() if joint_tours_canonical is None else joint_tours_canonical
        ),
        persons_canonical=persons,
        households_ctramp=hh_ctramp,
        config=config,
    )
    joint_trips = format_joint_trip(
        joint_trips_canonical=_joint_trips_table(trips),
        linked_trips_canonical=trips,
        unlinked_trips_canonical=pl.DataFrame(),
        tours_canonical=tours,
        households_ctramp=hh_ctramp,
        config=config,
    )
    return indiv_tours, indiv_trips, joint_tours, joint_trips


class TestSharingWithoutAJointTour:
    """Trip-level sharing that never became a joint tour."""

    def test_shared_trips_on_individual_tours_stay_individual(self, standard_config):
        """The escort case: one shared leg, two different tours, no joint tour.

        CT-RAMP has no record for a joint trip outside a joint tour, so this
        travel belongs in the individual files with shared-ride modes -- not
        dropped, and not invented as a joint tour.
        """
        households, persons = _two_person_household()
        tours = _shared_tour(PurposeCategory.SHOP, joint_tour_id=None)
        trips = _trips_for(tours, joint_trip_ids=(801, 802))

        _, indiv_trips, joint_tours, joint_trips = _format_all(
            tours, trips, persons, households, standard_config
        )

        assert len(indiv_trips) == len(trips)  # every trip kept
        assert joint_tours.is_empty()
        assert joint_trips.is_empty()  # nowhere in CT-RAMP to put them


class TestInadmissibleJointPurposes:
    """Groups CT-RAMP refuses to treat as joint."""

    @pytest.mark.parametrize(
        "purpose",
        [PurposeCategory.WORK, PurposeCategory.SCHOOL, PurposeCategory.ESCORT],
    )
    def test_rejected_purposes_move_to_the_individual_tables(self, purpose, standard_config):
        """Work, school and escort groups are reclassified, and land exactly once."""
        households, persons = _two_person_household()
        tours = _shared_tour(purpose)
        trips = _trips_for(tours)

        indiv_tours, indiv_trips, joint_tours, joint_trips = _format_all(
            tours, trips, persons, households, standard_config
        )

        assert len(indiv_tours) == len(tours)
        assert len(indiv_trips) == len(trips)
        assert joint_tours.is_empty()
        assert joint_trips.is_empty()

    def test_mixed_purpose_groups_are_reclassified(self):
        """A group whose members disagree on purpose is not one joint tour."""
        tours = _shared_tour(PurposeCategory.SHOP).with_columns(
            pl.when(pl.col("tour_id") == 1002)
            .then(pl.lit(PurposeCategory.SOCIALREC.value))
            .otherwise(pl.col("tour_purpose"))
            .alias("tour_purpose")
        )

        reclassified = identify_misclassified_joint_tours(tours)
        assert reclassified["joint_tour_id"].null_count() == len(tours)

    def test_admissible_purpose_survives_as_joint(self, standard_config):
        """The control: a same-purpose non-mandatory group stays joint."""
        households, persons = _two_person_household()
        tours = _shared_tour(PurposeCategory.SOCIALREC)
        trips = _trips_for(tours)

        indiv_tours, _, joint_tours, joint_trips = _format_all(
            tours, trips, persons, households, standard_config
        )

        assert indiv_tours.is_empty()
        assert len(joint_tours) == 1
        assert not joint_trips.is_empty()


class TestPartitionInvariants:
    """The individual and joint files must partition travel, never overlap it.

    Their weight conventions differ -- individual rows are per person, joint rows
    carry the whole party -- so an overlap does not merely duplicate records, it
    silently inflates every total derived from them.
    """

    @pytest.mark.parametrize(
        "purpose",
        [
            PurposeCategory.SOCIALREC,  # admissible: joint file
            PurposeCategory.ESCORT,  # reclassified: individual file
            PurposeCategory.WORK,  # reclassified: individual file
        ],
    )
    def test_no_tour_appears_in_both_files(self, purpose, standard_config):
        """Whatever the ruling, a tour is written to exactly one of the two."""
        households, persons = _two_person_household()
        tours = _shared_tour(purpose)
        trips = _trips_for(tours)

        indiv_tours, _, joint_tours, _ = _format_all(
            tours, trips, persons, households, standard_config
        )

        assert indiv_tours.is_empty() or joint_tours.is_empty()

    def test_a_mixed_household_splits_its_tours_between_the_files(self, standard_config):
        """One joint tour and one individual tour, in the same household.

        The all-or-nothing fixtures let ``is_empty()`` stand in for
        disjointness. Here both files are non-empty, so the partition has to be
        checked on identities: every canonical tour is written exactly once.
        """
        households, persons = _two_person_household()
        tours = _tours_frame(
            [
                # A joint social/rec tour shared by both members.
                {
                    "tour_id": 1001,
                    "person_id": 101,
                    "person_num": 1,
                    "joint_tour_id": 5001,
                    "purpose": PurposeCategory.SOCIALREC,
                    "weight": MEMBER_WEIGHTS[0],
                },
                {
                    "tour_id": 1002,
                    "person_id": 102,
                    "person_num": 2,
                    "joint_tour_id": 5001,
                    "purpose": PurposeCategory.SOCIALREC,
                    "weight": MEMBER_WEIGHTS[1],
                },
                # ...and a genuinely individual work tour for person 101.
                {
                    "tour_id": 1003,
                    "person_id": 101,
                    "person_num": 1,
                    "tour_num": 2,
                    "joint_tour_id": None,
                    "purpose": PurposeCategory.WORK,
                    "weight": MEMBER_WEIGHTS[0],
                    "num_travelers": 1,
                },
            ]
        )
        trips = _trips_for(tours)

        indiv_tours, _, joint_tours, _ = _format_all(
            tours, trips, persons, households, standard_config
        )

        assert not indiv_tours.is_empty()
        assert not joint_tours.is_empty()

        admitted = identify_misclassified_joint_tours(tours)
        individual_ids = set(admitted.filter(pl.col("joint_tour_id").is_null())["tour_id"])
        joint_member_ids = set(admitted.filter(pl.col("joint_tour_id").is_not_null())["tour_id"])

        assert set(indiv_tours["_tour_id_canonical"]) == individual_ids
        assert not individual_ids & joint_member_ids
        assert individual_ids | joint_member_ids == set(tours["tour_id"])
        assert len(joint_tours) == admitted["joint_tour_id"].drop_nulls().n_unique()

    @pytest.mark.parametrize(
        "purpose",
        [PurposeCategory.SOCIALREC, PurposeCategory.ESCORT],
    )
    def test_every_joint_trip_resolves_to_a_written_joint_tour(self, purpose, standard_config):
        """No dangling tour_id: the reference must exist in the joint tour file.

        This is the invariant the reclassification bug broke -- joint trips
        survived pointing at a joint tour that had been demoted to individual.
        """
        households, persons = _two_person_household()
        tours = _shared_tour(purpose)
        trips = _trips_for(tours)

        _, _, joint_tours, joint_trips = _format_all(
            tours, trips, persons, households, standard_config
        )

        if joint_trips.is_empty():
            return
        written = joint_tours.select("hh_id", "tour_id").unique()
        orphans = (
            joint_trips.select("hh_id", "tour_id")
            .unique()
            .join(written, on=["hh_id", "tour_id"], how="anti")
        )
        assert orphans.is_empty()

    def test_joint_trip_participants_meet_the_minimum(self, standard_config):
        """num_participants >= 2 -- a group of one is not joint."""
        households, persons = _two_person_household()
        tours = _shared_tour(PurposeCategory.SOCIALREC)
        trips = _trips_for(tours)

        _, _, _, joint_trips = _format_all(tours, trips, persons, households, standard_config)

        assert joint_trips["num_participants"].min() >= MIN_JOINT


class TestJointTourNumbering:
    """CT-RAMP numbers joint tours 0-based per household, in two places.

    ``format_joint_tour`` and ``format_joint_trip`` each rank
    ``joint_tour_id`` over ``hh_id`` independently, so the two files agree only
    while they rank the same set of joint tours.
    """

    def test_a_reclassified_joint_tour_does_not_shift_the_survivor(self, standard_config):
        """Demoting one of two joint tours leaves the other numbered consistently."""
        households, persons = _two_person_household()
        tours = _two_joint_tours(
            first_purpose=PurposeCategory.WORK,  # inadmissible -> reclassified
            second_purpose=PurposeCategory.SOCIALREC,
        )
        trips = _trips_for(tours)

        _, _, joint_tours, joint_trips = _format_all(
            tours, trips, persons, households, standard_config
        )

        assert len(joint_tours) == 1
        assert joint_tours["tour_id"].to_list() == [0]
        assert joint_trips["tour_id"].unique().to_list() == [0]

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Known defect: format_joint_trip ranks joint_tour_id over only the joint "
            "tours that have joint trips, while format_joint_tour ranks over all of "
            "them. A joint tour whose legs were never matched as joint trips shifts "
            "the numbering, silently re-pointing the surviving joint trips at the "
            "wrong joint tour. The anti-join orphan check cannot see it because the "
            "id it lands on does exist."
        ),
    )
    def test_joint_trips_point_at_the_joint_tour_they_belong_to(self, standard_config):
        """A joint tour with no joint trips must not renumber the ones that have them.

        Both files carry ``tour_purpose``, so the purposes are made to differ
        and the reference is checked by content rather than by mere existence.
        """
        households, persons = _two_person_household()
        tours = _two_joint_tours(
            first_purpose=PurposeCategory.SOCIALREC,
            second_purpose=PurposeCategory.SHOP,
        )
        # The first joint tour is genuinely shared, but its legs never matched
        # as joint trips -- so it appears in the tour file and not the trip file.
        trips = _trips_for(tours, unshared_joint_tours=(5001,))

        _, _, joint_tours, joint_trips = _format_all(
            tours, trips, persons, households, standard_config
        )

        resolved = joint_trips.select("hh_id", "tour_id", "tour_purpose").unique().join(
            joint_tours.select("hh_id", "tour_id", "tour_purpose").rename(
                {"tour_purpose": "_tour_file_purpose"}
            ),
            on=["hh_id", "tour_id"],
            how="left",
        )
        assert resolved["_tour_file_purpose"].to_list() == resolved["tour_purpose"].to_list()


def _two_joint_tours(*, first_purpose: PurposeCategory, second_purpose: PurposeCategory):
    """Two joint tours in one household, each shared by both members."""
    return _tours_frame(
        [
            {
                "tour_id": 1001,
                "person_id": 101,
                "person_num": 1,
                "tour_num": 1,
                "joint_tour_id": 5001,
                "purpose": first_purpose,
                "weight": MEMBER_WEIGHTS[0],
            },
            {
                "tour_id": 1002,
                "person_id": 102,
                "person_num": 2,
                "tour_num": 1,
                "joint_tour_id": 5001,
                "purpose": first_purpose,
                "weight": MEMBER_WEIGHTS[1],
            },
            {
                "tour_id": 1003,
                "person_id": 101,
                "person_num": 1,
                "tour_num": 2,
                "joint_tour_id": 5002,
                "purpose": second_purpose,
                "weight": MEMBER_WEIGHTS[0],
            },
            {
                "tour_id": 1004,
                "person_id": 102,
                "person_num": 2,
                "tour_num": 2,
                "joint_tour_id": 5002,
                "purpose": second_purpose,
                "weight": MEMBER_WEIGHTS[1],
            },
        ]
    )


class TestPartySize:
    """``num_participants`` counts the members behind the weight, not the party."""

    def test_an_unsurveyed_companion_does_not_join_the_count(self, standard_config):
        """Three travellers, two of them surveyed household members.

        ``num_participants`` is what CT-RAMP multiplies the weight by, so it has
        to match the member trips the weight was summed over -- two. Occupancy
        is carried separately by the mode, which still sees a party of three.
        """
        households, persons = _two_person_household()
        tours = _shared_tour(PurposeCategory.SOCIALREC, num_travelers=3)
        trips = _trips_for(tours)

        _, _, _, joint_trips = _format_all(tours, trips, persons, households, standard_config)

        assert joint_trips["num_participants"].unique().to_list() == [MIN_JOINT]
        assert joint_trips["trip_mode"].unique().to_list() == [CTRAMPModeType.SR3.value]

    def test_a_two_person_party_is_shared_ride_2(self, standard_config):
        """The control for the mode half of the previous test."""
        households, persons = _two_person_household()
        tours = _shared_tour(PurposeCategory.SOCIALREC, num_travelers=2)
        trips = _trips_for(tours)

        _, _, _, joint_trips = _format_all(tours, trips, persons, households, standard_config)

        assert joint_trips["trip_mode"].unique().to_list() == [CTRAMPModeType.SR2.value]


class TestPartialLegSharing:
    """A joint tour whose members only travelled together on one leg."""

    def test_only_the_shared_leg_becomes_a_joint_trip(self, standard_config):
        """The unshared leg is not a joint trip, so the joint file omits it."""
        households, persons = _two_person_household()
        tours = _shared_tour(PurposeCategory.SOCIALREC)
        trips = _trips_for(tours, shared_directions=(TourDirection.OUTBOUND,))

        _, _, joint_tours, joint_trips = _format_all(
            tours, trips, persons, households, standard_config
        )

        assert len(joint_tours) == 1
        assert len(joint_trips) == 1
        assert joint_trips["inbound"].to_list() == [0]

    def test_the_unshared_leg_of_a_joint_tour_is_written_nowhere(self, standard_config):
        """Documents a real gap: that travel reaches neither output file.

        The individual trip file takes only trips whose tour is in the
        individual tour file, and a joint tour is not; the joint trip file takes
        only legs carrying a ``joint_trip_id``. An inbound leg that was never
        matched as a joint trip therefore falls between them.
        """
        households, persons = _two_person_household()
        tours = _shared_tour(PurposeCategory.SOCIALREC)
        trips = _trips_for(tours, shared_directions=(TourDirection.OUTBOUND,))

        _, indiv_trips, _, joint_trips = _format_all(
            tours, trips, persons, households, standard_config
        )

        inbound = trips.filter(pl.col("tour_direction") == TourDirection.INBOUND.value)
        assert len(inbound) == MIN_JOINT  # one per member
        assert indiv_trips.is_empty()
        assert len(joint_trips) == 1  # the outbound leg only


class TestJointWeightExpansion:
    """``num_participants / sampleRate`` has to return the person-trips."""

    def test_joint_trip_expansion_returns_the_person_trips(self, standard_config):
        """The joint file stands in for person-trips the individual file omits.

        With unequal member weights this separates the sum from the mean: the
        round trip lands on the summed member weight, not twice the average.
        """
        households, persons = _two_person_household()
        tours = _shared_tour(PurposeCategory.SOCIALREC)
        trips = _trips_for(tours)

        _, _, _, joint_trips = _format_all(tours, trips, persons, households, standard_config)

        expanded = joint_trips["num_participants"] / joint_trips["sampleRate"]
        assert expanded.sum() == pytest.approx(trips["linked_trip_weight"].sum())
        # Each leg carries both members: 10 + 30, which twice the mean of a
        # single member would not reproduce.
        assert expanded.to_list() == pytest.approx([sum(MEMBER_WEIGHTS)] * 2)

    def test_joint_tour_expansion_returns_the_person_tours(self, standard_config):
        """The same round trip on the tour side, which the trip test cannot reach."""
        households, persons = _two_person_household()
        tours = _shared_tour(PurposeCategory.SOCIALREC)
        trips = _trips_for(tours)

        _, _, joint_tours, _ = _format_all(
            tours,
            trips,
            persons,
            households,
            standard_config,
            joint_tours_canonical=_joint_tours_table(tours),
        )

        assert "sampleRate" in joint_tours.columns
        expanded = MIN_JOINT / joint_tours["sampleRate"]
        assert expanded.sum() == pytest.approx(tours["tour_weight"].sum())

    def test_a_zero_weight_joint_record_has_no_sample_rate(self, standard_config):
        """A rate of 1/0 is not a number; null says "no rate" without poisoning sums."""
        households, persons = _two_person_household()
        tours = _shared_tour(PurposeCategory.SOCIALREC, weights=(0.0, 0.0))
        trips = _trips_for(tours)

        _, _, _, joint_trips = _format_all(tours, trips, persons, households, standard_config)

        assert joint_trips["joint_trip_weight"].to_list() == [0.0, 0.0]
        assert joint_trips["sampleRate"].null_count() == len(joint_trips)
