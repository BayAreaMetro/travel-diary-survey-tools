"""Tests for trip linking: which segments join, and what the joined trip says.

Two halves. ``link_trip_ids`` decides where one journey ends and the next
begins, from the stop purpose plus the time and distance between segments;
``aggregate_linked_trips`` then rolls the surviving segments into one row.

The change-mode code used throughout these fixtures is 10. It is passed to
``link_trip_ids`` as ``change_mode_enum`` and written into
``d_purpose_category``, so the two agree and the rule under test is exercised.
It is *not* ``PurposeCategory.CHANGE_MODE``, which is 11; in the real codebook
10 is ERRAND. The linking rule reads whatever code it is handed, so the value
is arbitrary here, but do not read these fixtures as codebook examples.
"""

import inspect
import logging
from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.trips import ModeType, Purpose, PurposeCategory
from processing.link_trips.link import (
    _warn_on_occupancy_change,
    aggregate_linked_trips,
    link_trip_ids,
    link_trips,
)
from utils.create_ids import create_linked_trip_id, create_tour_ids

CHANGE_MODE = 10
"""The mode-change stop code these fixtures use. See the module docstring."""

WORK = PurposeCategory.WORK.value
SHOP = PurposeCategory.SHOP.value

HOME_COORD = (37.7, -122.4)
STOP_COORD = (37.71, -122.41)
WORK_COORD = (37.75, -122.45)
FAR_COORD = (38.5, -122.4)


def _leg(
    depart: datetime,
    arrive: datetime,
    d_purpose_category: int,
    *,
    o: tuple[float, float] = HOME_COORD,
    d: tuple[float, float] = STOP_COORD,
    person_id: int = 100,
    day_id: int = 10001,
    num_travelers: int | None = 1,
) -> dict:
    """One unlinked trip, carrying only the columns ``link_trip_ids`` reads."""
    return {
        "day_id": day_id,
        "person_id": person_id,
        "depart_time": depart,
        "arrive_time": arrive,
        "d_purpose_category": d_purpose_category,
        "o_lat": o[0],
        "o_lon": o[1],
        "d_lat": d[0],
        "d_lon": d[1],
        "num_travelers": num_travelers,
    }


def _trips(legs: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(legs, schema_overrides={"num_travelers": pl.Int64})


def _at(hour: int, minute: int, day: int = 1) -> datetime:
    return datetime(2024, 1, day, hour, minute)


def _two_segments(num_travelers: list[int | None]) -> pl.DataFrame:
    """Two segments meeting at a change-mode stop, close in time and space.

    Everything except the party size satisfies the linking rules, so any break
    in the occupancy tests is attributable to occupancy alone.
    """
    return _trips(
        [
            _leg(
                _at(8, 0),
                _at(8, 10),
                CHANGE_MODE,
                o=HOME_COORD,
                d=STOP_COORD,
                num_travelers=num_travelers[0],
            ),
            _leg(
                _at(8, 15),
                _at(8, 45),
                WORK,
                o=STOP_COORD,
                d=WORK_COORD,
                num_travelers=num_travelers[1],
            ),
        ]
    )


def _link(trips: pl.DataFrame, *, split: bool, dwell_buffer_distance: float = 100) -> pl.DataFrame:
    return link_trip_ids(
        trips,
        change_mode_enum=CHANGE_MODE,
        max_dwell_time=120,
        dwell_buffer_distance=dwell_buffer_distance,
        split_on_occupancy=split,
    )


# Linking rules ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("legs", "dwell_buffer_distance", "expected_linked_trips"),
    [
        pytest.param(
            [
                _leg(_at(8, 0), _at(8, 10), CHANGE_MODE, o=HOME_COORD, d=STOP_COORD),
                _leg(_at(8, 15), _at(8, 45), WORK, o=STOP_COORD, d=WORK_COORD),
            ],
            100,
            1,
            id="change_mode_stop_joins_two_segments",
        ),
        pytest.param(
            [
                _leg(_at(8, 0), _at(8, 30), WORK, o=HOME_COORD, d=WORK_COORD),
                _leg(_at(9, 0), _at(9, 30), SHOP, o=WORK_COORD, d=STOP_COORD),
            ],
            100,
            2,
            id="an_ordinary_stop_ends_the_trip",
        ),
        pytest.param(
            [
                _leg(_at(8, 0), _at(8, 30), CHANGE_MODE, person_id=100, day_id=10001),
                _leg(_at(8, 5), _at(8, 35), CHANGE_MODE, person_id=200, day_id=20001),
            ],
            100,
            2,
            id="each_person_is_linked_on_their_own",
        ),
        pytest.param(
            [
                _leg(_at(8, 0), _at(8, 10), CHANGE_MODE, o=HOME_COORD, d=STOP_COORD),
                _leg(_at(10, 30), _at(11, 0), WORK, o=STOP_COORD, d=WORK_COORD),
            ],
            100,
            2,
            id="a_150_minute_wait_exceeds_the_120_minute_maximum",
        ),
        pytest.param(
            [
                _leg(_at(8, 0), _at(8, 10), CHANGE_MODE, o=HOME_COORD, d=STOP_COORD),
                _leg(_at(8, 15), _at(8, 45), WORK, o=FAR_COORD, d=WORK_COORD),
            ],
            10,
            2,
            id="resuming_88km_away_is_not_the_same_journey",
        ),
        pytest.param(
            [
                _leg(_at(8, 0), _at(8, 10), CHANGE_MODE, o=HOME_COORD, d=STOP_COORD),
                _leg(_at(8, 15), _at(8, 25), CHANGE_MODE, o=STOP_COORD, d=STOP_COORD),
                _leg(_at(8, 30), _at(9, 0), WORK, o=STOP_COORD, d=WORK_COORD),
            ],
            100,
            1,
            id="a_chain_of_three_is_still_one_trip",
        ),
        pytest.param(
            [
                _leg(_at(8, 0), _at(8, 30), WORK, day_id=10001, o=HOME_COORD, d=WORK_COORD),
                _leg(_at(9, 0), _at(9, 30), SHOP, day_id=10001, o=WORK_COORD, d=STOP_COORD),
                _leg(_at(8, 0, day=2), _at(8, 30, day=2), WORK, day_id=10002, d=WORK_COORD),
                _leg(_at(9, 0, day=2), _at(9, 30, day=2), SHOP, day_id=10002, o=WORK_COORD),
            ],
            100,
            4,
            id="ids_stay_distinct_across_days",
        ),
        pytest.param(
            [
                _leg(_at(8, 0), _at(8, 30), CHANGE_MODE, o=HOME_COORD, d=STOP_COORD),
                _leg(_at(8, 0), _at(8, 30), WORK, o=STOP_COORD, d=WORK_COORD),
            ],
            100,
            1,
            id="identical_timestamps_still_link",
        ),
    ],
)
def test_linking_rules(legs, dwell_buffer_distance, expected_linked_trips):
    """The purpose of the stop, the wait, and the distance decide the break."""
    result = _link(_trips(legs), split=False, dwell_buffer_distance=dwell_buffer_distance)

    assert len(result) == len(legs)
    assert result["linked_trip_id"].n_unique() == expected_linked_trips


def test_empty_dataframe():
    """Should handle empty DataFrame gracefully."""
    # Create empty dataframe with explicit dtypes to avoid null dtype issues
    trips = pl.DataFrame(
        schema={
            "day_id": pl.Int64,
            "person_id": pl.Int64,
            "depart_time": pl.Datetime,
            "arrive_time": pl.Datetime,
            "d_purpose_category": pl.Int64,
            "o_lat": pl.Float64,
            "o_lon": pl.Float64,
            "d_lat": pl.Float64,
            "d_lon": pl.Float64,
        }
    )

    result = _link(trips, split=False)

    assert len(result) == 0
    assert "linked_trip_id" in result.columns


# Occupancy ---------------------------------------------------------------------
#
# Two segments joined at a mode-change stop are treated as one journey. If the
# number of travellers differs across that stop, the more likely reading is that
# somebody was picked up or dropped off, which makes the stop an activity rather
# than a transfer. ``split_on_occupancy`` refuses the link in that case.


def test_party_change_breaks_the_link_when_enabled():
    """One traveller becoming three is a pick-up, not a transfer."""
    result = _link(_two_segments([1, 3]), split=True)

    assert result["linked_trip_id"].n_unique() == 2


def test_party_change_is_linked_when_disabled():
    """Default behaviour is unchanged, so existing projects are unaffected."""
    result = _link(_two_segments([1, 3]), split=False)

    assert result["linked_trip_id"].n_unique() == 1


def test_steady_party_still_links():
    """Splitting on occupancy must not break an ordinary transfer."""
    result = _link(_two_segments([2, 2]), split=True)

    assert result["linked_trip_id"].n_unique() == 1


@pytest.mark.parametrize("sizes", [[None, 3], [1, None], [None, None]])
def test_unreported_party_size_does_not_break_the_link(sizes):
    """A missing party size is missing data, not evidence of a change."""
    result = _link(_two_segments(sizes), split=True)

    assert result["linked_trip_id"].n_unique() == 1


def test_missing_column_is_refused_rather_than_ignored():
    """Asking to split with nothing to split on is an error, not a silent no-op."""
    trips = _two_segments([1, 3]).drop("num_travelers")

    with pytest.raises(ValueError, match="num_travelers"):
        _link(trips, split=True)


def test_aggregation_warns_when_a_party_changes_inside_a_link(caplog):
    """The max() roll-up stays, but it announces what it is papering over."""
    linked = _link(_two_segments([1, 3]), split=False)

    with caplog.at_level(logging.WARNING, logger="processing.link_trips.link"):
        _warn_on_occupancy_change(linked)

    assert "change party size between segments" in caplog.text
    assert "split_on_occupancy=True" in caplog.text


@pytest.mark.parametrize(
    ("sizes", "split"),
    [
        pytest.param([2, 2], False, id="a_steady_party_is_not_noise"),
        pytest.param([None, None], False, id="nulls_alone_are_not_a_change"),
        pytest.param([1, 3], True, id="splitting_is_the_remedy_and_silences_it"),
    ],
)
def test_no_warning_when_there_is_nothing_to_report(caplog, sizes, split):
    """Only a real change inside a surviving link may warn."""
    linked = _link(_two_segments(sizes), split=split)

    with caplog.at_level(logging.WARNING, logger="processing.link_trips.link"):
        _warn_on_occupancy_change(linked)

    assert "change party size" not in caplog.text


def test_the_choice_has_no_default():
    """No default, so a run cannot leave this decision unmade.

    Either answer changes what a linked trip *is*, and a default would quietly
    pick one on the configuration's behalf.
    """
    for func in (link_trips, link_trip_ids):
        parameter = inspect.signature(func).parameters["split_on_occupancy"]
        assert parameter.default is inspect.Parameter.empty, (
            f"{func.__name__} gives split_on_occupancy a default, which decides "
            f"trip linking semantics for every config that stays silent"
        )
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY


# Aggregation -------------------------------------------------------------------


def _segments(
    times: list[tuple[datetime, datetime]],
    durations: list[float],
    modes: list[int] | None = None,
    distances: list[float] | None = None,
) -> pl.DataFrame:
    """Build one linked trip's segments from (depart, arrive) pairs.

    ``durations`` is the vendor's reported minutes in motion per segment, kept
    separate from the timestamps so a test can round them against each other.
    The journey runs home -> change mode -> ... -> work, so the aggregate's
    origin is always ``HOME_COORD`` and its destination ``WORK_COORD``.
    """
    n = len(times)
    return pl.DataFrame(
        {
            "linked_trip_id": [1] * n,
            "linked_trip_num": [1] * n,
            "person_id": [100] * n,
            "hh_id": [10] * n,
            "day_id": [10001] * n,
            "depart_time": [depart for depart, _ in times],
            "arrive_time": [arrive for _, arrive in times],
            "travel_dow": [1] * n,
            "depart_date": [depart.date() for depart, _ in times],
            "arrive_date": [arrive.date() for _, arrive in times],
            "depart_hour": [depart.hour for depart, _ in times],
            "depart_minute": [depart.minute for depart, _ in times],
            "depart_seconds": [depart.second for depart, _ in times],
            "arrive_hour": [arrive.hour for _, arrive in times],
            "arrive_minute": [arrive.minute for _, arrive in times],
            "arrive_seconds": [arrive.second for _, arrive in times],
            "o_purpose_category": [PurposeCategory.HOME.value]
            + [PurposeCategory.CHANGE_MODE.value] * (n - 1),
            "d_purpose_category": [PurposeCategory.CHANGE_MODE.value] * (n - 1)
            + [PurposeCategory.WORK.value],
            "o_purpose": [Purpose.HOME.value] + [Purpose.MODE_CHANGE.value] * (n - 1),
            "d_purpose": [Purpose.MODE_CHANGE.value] * (n - 1) + [Purpose.GROCERY.value],
            "o_lat": [HOME_COORD[0]] * n,
            "o_lon": [HOME_COORD[1]] * n,
            "d_lat": [WORK_COORD[0]] * n,
            "d_lon": [WORK_COORD[1]] * n,
            "mode_type": modes or [ModeType.WALK.value] * n,
            "num_travelers": [1] * n,
            "driver": [0] * n,
            "distance_meters": distances or [804.67] * n,
            "duration_minutes": durations,
            "unlinked_trip_weight": [1.0] * n,
        }
    )


def _aggregate(trips: pl.DataFrame) -> pl.DataFrame:
    return aggregate_linked_trips(trips, transit_mode_enums=[ModeType.TRANSIT.value])


def test_basic_aggregation():
    """Two segments become one row keeping the first origin and the last stop."""
    trips = _segments(
        [
            (_at(8, 0), _at(8, 10)),
            (_at(8, 15), _at(8, 45)),
        ],
        durations=[10.0, 30.0],
        modes=[ModeType.WALK.value, ModeType.TRANSIT.value],
        distances=[804.67, 8046.7],
    )

    result = _aggregate(trips)

    assert len(result) == 1
    row = result.row(0, named=True)

    # Origin comes from the first segment, destination from the last
    assert row["depart_time"] == _at(8, 0)
    assert (row["o_lat"], row["o_lon"]) == HOME_COORD
    assert row["o_purpose_category"] == PurposeCategory.HOME.value
    assert row["arrive_time"] == _at(8, 45)
    assert (row["d_lat"], row["d_lon"]) == WORK_COORD
    assert row["d_purpose_category"] == PurposeCategory.WORK.value

    assert row["distance_meters"] == 804.67 + 8046.7
    assert row["num_segments"] == 2
    # A transit leg anywhere makes the whole journey a transit trip
    assert row["mode_type"] == ModeType.TRANSIT.value


def test_longest_duration_mode_without_transit():
    """With no transit leg the journey takes the mode of its longest segment."""
    trips = _segments(
        [
            (_at(8, 0), _at(8, 10)),
            (_at(8, 15), _at(8, 45)),
        ],
        durations=[10.0, 30.0],
        modes=[ModeType.WALK.value, ModeType.CAR.value],
        distances=[804.67, 8046.7],
    )

    result = _aggregate(trips)

    assert result["mode_type"][0] == ModeType.CAR.value


class TestDwellDuration:
    """``dwell_duration_minutes`` is measured from the timestamps.

    It is the time spent waiting *between* the merged segments, so it is read
    off the gaps themselves rather than derived by subtracting reported segment
    durations from elapsed time. Those two quantities round independently, and
    differencing them used to manufacture negative dwell on trips that had none.
    """

    def test_dwell_is_the_gap_between_segments(self):
        """Two segments five minutes apart dwell for five minutes."""
        trips = _segments(
            [
                (_at(8, 0), _at(8, 10)),
                (_at(8, 15), _at(8, 45)),
            ],
            durations=[10.0, 30.0],
        )

        result = _aggregate(trips)

        row = result.row(0, named=True)
        assert row["dwell_duration_minutes"] == 5
        # 8:00 to 8:45 elapsed, of which 10 + 30 was in motion
        assert row["duration_minutes"] == 45.0
        assert row["travel_duration_minutes"] == 40.0

    def test_dwell_sums_every_gap(self):
        """Three segments have two gaps, and both count."""
        trips = _segments(
            [
                (_at(8, 0), _at(8, 10)),
                (_at(8, 15), _at(8, 30)),
                (_at(8, 37), _at(9, 0)),
            ],
            durations=[10.0, 15.0, 23.0],
        )

        result = _aggregate(trips)

        assert result.row(0, named=True)["dwell_duration_minutes"] == 5 + 7

    def test_single_segment_trip_has_no_dwell(self):
        """One segment has no gap to wait in, so its dwell is exactly zero."""
        trips = _segments([(_at(8, 0), _at(8, 10))], durations=[10.0])

        result = _aggregate(trips)

        assert result.row(0, named=True)["dwell_duration_minutes"] == 0

    def test_dwell_survives_durations_that_exceed_elapsed(self):
        """A segment reported longer than it elapsed must not dwell negatively.

        Vendor durations are whole minutes and elapsed was truncated, so a
        10.5-minute segment reported as 11 used to yield 10 - 11 = -1 minutes
        of dwell on a trip with no gap at all. This was 43% of BATS 2023.
        """
        trips = _segments([(_at(8, 0), datetime(2024, 1, 1, 8, 10, 30))], durations=[11.0])

        result = _aggregate(trips)

        row = result.row(0, named=True)
        assert row["duration_minutes"] < row["travel_duration_minutes"]
        assert row["dwell_duration_minutes"] == 0


# End to end --------------------------------------------------------------------


def test_end_to_end_linking():
    """Three segments, one change-mode stop, become two linked trips."""
    trips = pl.DataFrame(
        {
            "unlinked_trip_id": [1, 2, 3],
            "day_id": [10001, 10001, 10001],
            "person_id": [100, 100, 100],
            "hh_id": [10, 10, 10],
            "depart_time": [_at(8, 0), _at(8, 15), _at(17, 0)],
            "arrive_time": [_at(8, 10), _at(8, 45), _at(17, 30)],
            "travel_dow": [1, 1, 1],
            "depart_date": [datetime(2024, 1, 1)] * 3,
            "arrive_date": [datetime(2024, 1, 1)] * 3,
            "depart_hour": [8, 8, 17],
            "depart_minute": [0, 15, 0],
            "depart_seconds": [0, 0, 0],
            "arrive_hour": [8, 8, 17],
            "arrive_minute": [10, 45, 30],
            "arrive_seconds": [0, 0, 0],
            "o_purpose_category": [
                PurposeCategory.HOME.value,
                PurposeCategory.CHANGE_MODE.value,
                PurposeCategory.WORK.value,
            ],
            "d_purpose_category": [
                PurposeCategory.CHANGE_MODE.value,
                PurposeCategory.WORK.value,
                PurposeCategory.HOME.value,
            ],
            "o_purpose": [
                Purpose.HOME.value,
                Purpose.MODE_CHANGE.value,
                Purpose.PRIMARY_WORKPLACE.value,
            ],
            "d_purpose": [
                Purpose.MODE_CHANGE.value,
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.HOME.value,
            ],
            "o_lat": [37.7, 37.71, 37.75],
            "o_lon": [-122.4, -122.41, -122.45],
            "d_lat": [37.71, 37.75, 37.7],
            "d_lon": [-122.41, -122.45, -122.4],
            "mode_type": [
                ModeType.WALK.value,
                ModeType.TRANSIT.value,
                ModeType.TRANSIT.value,
            ],
            "distance_meters": [804.67, 8046.7, 8046.7],
            "num_travelers": [1, 1, 1],
            "driver": [0, 0, 0],
            "duration_minutes": [10.0, 30.0, 30.0],
            "unlinked_trip_weight": [1.0, 1.0, 1.0],
        }
    )

    result = link_trips(
        trips,
        change_mode_enum=PurposeCategory.CHANGE_MODE.value,
        transit_mode_enums=[ModeType.TRANSIT.value],
        max_dwell_time=120,
        dwell_buffer_distance=100,
        split_on_occupancy=False,
    )

    # Every segment keeps its own row, tagged with the journey it belongs to
    unlinked = result["unlinked_trips"]
    assert len(unlinked) == 3
    assert unlinked["linked_trip_id"].n_unique() == 2

    # The journeys themselves are one row each, and the ids are unique there
    linked = result["linked_trips"]
    assert len(linked) == 2
    assert linked["linked_trip_id"].n_unique() == 2
    assert linked["num_segments"].to_list() == [2, 1]


# ID creation -------------------------------------------------------------------


def test_multiple_trips_same_day():
    """Should handle multiple trips with same day_id correctly."""
    # Multiple trips sharing the same day_id is normal in trip table
    trips = pl.DataFrame({"day_id": [10001, 10001, 10001], "linked_trip_num": [1, 2, 3]})

    result = create_linked_trip_id(trips)

    # Unique ids come from combining day_id with the sequence number
    assert result["linked_trip_id"].to_list() == [1000101, 1000102, 1000103]


def test_tour_id_with_duplicate_day_id():
    """A subtour shares its day and tour number, and differs in the suffix."""
    trips = pl.DataFrame(
        {
            "day_id": [10001, 10001, 10001],
            "linked_trip_id": [1, 2, 3],
            "tour_num": [1, 1, 1],
            "subtour_num": [0, 1, 0],
        }
    )

    result = create_tour_ids(trips)

    assert result["tour_id"].to_list() == [100011000, 100011010, 100011000]
    # The subtour points back at the tour it was taken from
    assert result["parent_tour_id"].to_list() == [100011000, 100011000, 100011000]
