"""Test formatting of WORK_RELATED tours.

A WORK_RELATED tour is not automatically a work tour. What it becomes depends on
the traveller: a non-worker's WORK_RELATED tour is discretionary travel, while a
worker's WORK_RELATED tour hanging off an existing WORK tour is an at-work
subtour. Both outcomes are pinned here.

``test_basic_work_tour`` used to live here as a byte-identical copy of the one in
``test_ctramp_formatting.py``; that copy asserted the ``atWork_freq`` magic number
1 instead of ``AtWorkFreq.NO_SUBTOUR``, so the stronger original was kept.
"""

from datetime import datetime, time

import polars as pl

from data_canon.codebook.ctramp import AtWorkFreq, CTRAMPPersonType
from data_canon.codebook.households import IncomeBroad
from data_canon.codebook.persons import Employment
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import PurposeCategory
from processing.formatting.ctramp.format_households import format_households
from processing.formatting.ctramp.format_tours import format_individual_tour
from tests.fixtures import (
    create_household,
    create_linked_trip,
    create_person,
    create_tour,
    get_tour_schema,
)


def _one_person_household(employment: Employment, person_type: CTRAMPPersonType) -> pl.DataFrame:
    """A single-person household in the $75-100k income bin."""
    return pl.DataFrame(
        [
            create_person(
                person_id=101,
                hh_id=1,
                employment=employment,
                person_type=person_type.value,
            )
        ]
    )


def _tour(tour_id: int, purpose: PurposeCategory, depart: int, arrive: int, **kwargs):
    """One tour for person 101 between TAZ 100 and 200."""
    return create_tour(
        tour_id=tour_id,
        person_id=101,
        hh_id=1,
        person_num=1,
        tour_purpose=purpose,
        o_taz=100,
        d_taz=200,
        origin_depart_time=datetime.combine(datetime(2024, 1, 1), time(depart, 0)),
        origin_arrive_time=datetime.combine(datetime(2024, 1, 1), time(arrive, 0)),
        student_category="Not student",
        **kwargs,
    )


def _round_trip(tour_id: int, first_trip_id: int) -> list:
    """One outbound and one inbound trip, so the tour has zero stops on each leg."""
    return [
        create_linked_trip(
            trip_id=trip_id,
            tour_id=tour_id,
            person_id=101,
            hh_id=1,
            tour_direction=direction,
        )
        for trip_id, direction in (
            (first_trip_id, TourDirection.OUTBOUND),
            (first_trip_id + 1, TourDirection.INBOUND),
        )
    ]


def _format_tours(persons_canonical, tours_canonical, trips_canonical, config):
    """Run the individual tour formatter over one household's canonical frames."""
    households_canonical = pl.DataFrame(
        [create_household(hh_id=1, income_bin=IncomeBroad.INCOME_75TO100)]
    )
    households_ctramp = format_households(
        households_canonical, persons_canonical, tours_canonical, config
    )
    return format_individual_tour(
        tours_canonical=tours_canonical,
        linked_trips_canonical=trips_canonical,
        unlinked_trips_canonical=pl.DataFrame(),
        persons_canonical=persons_canonical,
        households_ctramp=households_ctramp,
        config=config,
    )


class TestWorkRelatedMapping:
    """Tests for formatting of WORK_RELATED tours."""

    def test_at_work_tour(self, standard_config):
        """A non-worker's WORK_RELATED tour is discretionary, not work.

        With no WORK tour to hang off and no employment, the tour cannot be an
        at-work subtour, so its purpose falls through to ``othdiscr`` and
        ``atWork_freq`` is NONE_NOT_WORK rather than NO_SUBTOUR.
        """
        persons_canonical = _one_person_household(
            Employment.UNEMPLOYED_NOT_LOOKING, CTRAMPPersonType.NON_WORKER
        )
        tours_canonical = pl.DataFrame(
            [_tour(1001, PurposeCategory.WORK_RELATED, 8, 17)],
            schema=get_tour_schema(),
        )
        trips_canonical = pl.DataFrame(_round_trip(1001, 10001))

        result = _format_tours(persons_canonical, tours_canonical, trips_canonical, standard_config)

        assert len(result) == 1
        assert result["tour_id"][0] == 0  # CTRAMP tour_id is 0-based (0 for first tour)
        assert result["hh_id"][0] == 1
        assert result["person_id"][0] == 101
        assert result["orig_taz"][0] == 100
        assert result["dest_taz"][0] == 200
        assert result["start_hour"][0] == 8
        assert result["end_hour"][0] == 17
        assert result["num_ob_stops"][0] == 0  # 1 OB trip = 0 stops
        assert result["num_ib_stops"][0] == 0  # 1 IB trip = 0 stops
        assert result["atWork_freq"][0] == AtWorkFreq.NONE_NOT_WORK.value
        assert result["tour_purpose"][0] == "othdiscr"

    def test_at_work_tour_worker(self, standard_config):
        """A worker's tour parented by a WORK tour maps to atwork_business."""
        persons_canonical = _one_person_household(
            Employment.EMPLOYED_FULLTIME, CTRAMPPersonType.FULL_TIME_WORKER
        )
        tours_canonical = pl.DataFrame(
            [
                _tour(1000, PurposeCategory.WORK, 7, 16, tour_num=1),
                _tour(1001, PurposeCategory.WORK, 8, 17, tour_num=2, parent_tour_id=1000),
            ],
            schema=get_tour_schema(),
        )
        trips_canonical = pl.DataFrame(_round_trip(1000, 10000) + _round_trip(1001, 10002))

        result = _format_tours(persons_canonical, tours_canonical, trips_canonical, standard_config)

        # Filter to the at-work subtour (parent tour_id 0 -> subtour encoded as 11)
        atwork_tour = result.filter(pl.col("tour_id") == 11)
        assert len(atwork_tour) == 1
        assert atwork_tour["tour_purpose"][0] == "atwork_business"
