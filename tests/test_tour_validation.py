"""Tests for tour validation helper functions."""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.tours import TourCategory, TourDataQuality
from data_canon.codebook.trips import PurposeCategory
from processing.tours.validation_helpers import validate_and_correct_tours

PRIMARY_HOME, SECOND_HOME = 1101, 1102


def with_homes(linked_trips, o_homes=None, d_homes=None):
    """Add the home each trip end matched, as ``match_trip_ends`` reports it.

    Ends default to no home. ``SECOND_HOME`` is the person's other home.
    """
    n = linked_trips.height
    o_homes = o_homes or [None] * n
    d_homes = d_homes or [None] * n
    return linked_trips.with_columns(
        pl.Series("_o_home_id", o_homes, dtype=pl.Int64),
        pl.Series("_d_home_id", d_homes, dtype=pl.Int64),
        pl.Series("_o_at_other_home", [h == SECOND_HOME for h in o_homes]),
        pl.Series("_d_at_other_home", [h == SECOND_HOME for h in d_homes]),
    )


def _frames(
    category,
    *,
    purpose=PurposeCategory.SHOP.value,
    trips=2,
    start_home=PRIMARY_HOME,
    end_home=PRIMARY_HOME,
    subtour_num=0,
    legs=None,
):
    """One tour and its trips, with no other travel around it.

    Its first origin is at *start_home* and its last destination at *end_home*
    (None for no home). By default the legs form a continuous chain, each trip
    starting where the last one ended, so the internal spatial-gap check stays
    quiet; pass *legs* as ``[(o, d), ...]`` coordinate pairs to break that.
    """
    if legs is None:
        pts = [(37.0 + 0.1 * (i % 2), -122.0 - 0.1 * (i % 2)) for i in range(trips + 1)]
        legs = [(pts[i], pts[i + 1]) for i in range(trips)]
    trips = len(legs)

    tours = pl.DataFrame(
        {
            "tour_id": ["tour_1"],
            "person_id": [1],
            "day_id": [1],
            "trip_count": [trips],
            "subtour_num": [subtour_num],
            "tour_num": [1],
            "tour_category": [category.value],
            "tour_purpose": [purpose],
        }
    )
    linked_trips = pl.DataFrame(
        {
            "tour_id": ["tour_1"] * trips,
            "person_id": [1] * trips,
            "hh_id": [1] * trips,
            "day_id": [1] * trips,
            "tour_num": [1] * trips,
            "o_lat": [o[0] for o, _ in legs],
            "o_lon": [o[1] for o, _ in legs],
            "d_lat": [d[0] for _, d in legs],
            "d_lon": [d[1] for _, d in legs],
            "depart_time": [datetime(2023, 5, 1, 8 + i) for i in range(trips)],
            "arrive_time": [datetime(2023, 5, 1, 8 + i, 30) for i in range(trips)],
        }
    )
    o_homes = [start_home] + [None] * (trips - 1)
    d_homes = [None] * (trips - 1) + [end_home]
    return tours, with_homes(linked_trips, o_homes, d_homes)


def _two_day_frames(second_day_origin):
    """A tour ending away on day 1, and day 2 starting from *second_day_origin*."""
    tours = pl.DataFrame(
        {
            "tour_id": ["tour_1", "tour_2"],
            "person_id": [1, 1],
            "day_id": [1, 2],
            "trip_count": [1, 1],
            "subtour_num": [0, 0],
            "tour_num": [1, 1],
            "tour_category": [
                TourCategory.PARTIAL_END.value,
                TourCategory.PARTIAL_START.value,
            ],
            "tour_purpose": [PurposeCategory.SHOP.value] * 2,
        }
    )
    linked_trips = pl.DataFrame(
        {
            "tour_id": ["tour_1", "tour_2"],
            "person_id": [1, 1],
            "hh_id": [1, 1],
            "day_id": [1, 2],
            "tour_num": [1, 1],
            "o_lat": [37.0, second_day_origin[0]],
            "o_lon": [-122.0, second_day_origin[1]],
            "d_lat": [37.5, 37.0],
            "d_lon": [-122.5, -122.0],
            "depart_time": [datetime(2023, 5, 1, 20), datetime(2023, 5, 2, 8)],
            "arrive_time": [datetime(2023, 5, 1, 21), datetime(2023, 5, 2, 9)],
        }
    )
    return tours, with_homes(linked_trips)


class TestValidateAndCorrectTours:
    """Test validate_and_correct_tours function."""

    def test_unassigned_trip_raises(self):
        """A trip that boundary detection never placed in a tour is fatal.

        Every first trip of a person-day starts a tour, so tour_num < 1 means
        detection itself broke and the whole table is suspect.
        """
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "day_id": [1],
                "trip_count": [1],
                "subtour_num": [0],
                "tour_num": [0],
                "tour_category": [TourCategory.COMPLETE.value],
                "tour_purpose": [PurposeCategory.WORK.value],
            }
        )
        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "hh_id": [1],
                "day_id": [1],
                "tour_num": [0],
            }
        )

        with pytest.raises(ValueError, match="never assigned to a tour"):
            validate_and_correct_tours(tours, linked_trips)

    @pytest.mark.parametrize(
        "category",
        [TourCategory.PARTIAL_BOTH, TourCategory.PARTIAL_START, TourCategory.PARTIAL_END],
    )
    def test_lone_partial_tour_is_a_diary_edge(self, category):
        """With no trip either side of it, an open end is where the diary stops."""
        tours, linked_trips = _frames(category)

        result = validate_and_correct_tours(tours, linked_trips)

        assert result["tour_data_quality"][0] == TourDataQuality.PARTIAL_DIARY_EDGE.value

    def test_complete_tour_is_valid(self):
        """Both ends anchored and a real purpose: nothing to report."""
        tours, linked_trips = _frames(TourCategory.COMPLETE)

        result = validate_and_correct_tours(tours, linked_trips)

        assert result["tour_data_quality"][0] == TourDataQuality.VALID.value

    def test_category_is_not_rewritten(self):
        """Quality is a verdict on the category; it never edits it."""
        tours, linked_trips = _frames(TourCategory.PARTIAL_END, trips=1)

        result = validate_and_correct_tours(tours, linked_trips)

        assert result["tour_category"][0] == TourCategory.PARTIAL_END.value

    def test_tour_without_a_purpose_has_no_destination(self):
        """A null purpose means nothing was found to anchor the tour on.

        Aggregation leaves it null when every candidate was the return leg or a
        mode change -- an anchor-to-anchor loop being the common case.
        """
        tours, linked_trips = _frames(TourCategory.COMPLETE, purpose=None, trips=1)

        result = validate_and_correct_tours(tours, linked_trips)

        assert result["tour_data_quality"][0] == TourDataQuality.NO_DESTINATION.value

    def test_tour_closed_at_a_second_home_is_other_home(self):
        """A round trip from the second home is complete, but not from the primary."""
        tours, linked_trips = _frames(
            TourCategory.COMPLETE, start_home=SECOND_HOME, end_home=SECOND_HOME
        )

        result = validate_and_correct_tours(tours, linked_trips)

        assert result["tour_data_quality"][0] == TourDataQuality.OTHER_HOME.value

    def test_direct_move_between_homes_is_other_home(self):
        """Primary home straight to the second home: no stop, but the move is why."""
        tours, linked_trips = _frames(
            TourCategory.COMPLETE, purpose=None, trips=1, end_home=SECOND_HOME
        )

        result = validate_and_correct_tours(tours, linked_trips)

        assert result["tour_data_quality"][0] == TourDataQuality.OTHER_HOME.value

    def test_open_tour_from_a_second_home_reports_its_open_end(self):
        """One code per tour: an open end outranks the home it left from."""
        tours, linked_trips = _frames(
            TourCategory.PARTIAL_END, trips=1, start_home=SECOND_HOME, end_home=None
        )

        result = validate_and_correct_tours(tours, linked_trips)

        assert result["tour_data_quality"][0] == TourDataQuality.PARTIAL_DIARY_EDGE.value

    def test_subtour_beside_a_second_home_is_graded_on_its_own_anchor(self):
        """Subtours start and end at work or school, whatever home is nearby."""
        tours, linked_trips = _frames(
            TourCategory.COMPLETE, start_home=SECOND_HOME, end_home=SECOND_HOME, subtour_num=1
        )

        result = validate_and_correct_tours(tours, linked_trips)

        assert result["tour_data_quality"][0] == TourDataQuality.VALID.value

    def test_chain_resuming_next_day_is_a_day_split(self):
        """The journey continues from where it stopped, so it was merely cut."""
        tours, linked_trips = _two_day_frames((37.5, -122.5))

        result = validate_and_correct_tours(tours, linked_trips).sort("tour_id")

        assert result["tour_data_quality"].to_list() == [
            TourDataQuality.PARTIAL_DAY_SPLIT.value,
            TourDataQuality.PARTIAL_DAY_SPLIT.value,
        ]

    def test_reappearing_elsewhere_is_a_spatial_gap(self):
        """A leg is missing when the next trip starts somewhere else entirely."""
        tours, linked_trips = _two_day_frames((38.9, -123.9))

        result = validate_and_correct_tours(tours, linked_trips).sort("tour_id")

        assert result["tour_data_quality"].to_list() == [
            TourDataQuality.SPATIAL_GAP.value,
            TourDataQuality.SPATIAL_GAP.value,
        ]


class TestSpatialGapDetection:
    """Test SPATIAL_GAP flagging for tours that teleport across a missing leg."""

    # Home, two stops on the way, and a point far from either: the third trip
    # begins at FAR rather than at B, so the leg connecting them is missing.
    HOME, A, B = (37.70, -122.40), (37.75, -122.42), (37.76, -122.43)
    FAR = (38.30, -123.00)

    def _tour_with_a_hole(self):
        return _frames(
            TourCategory.COMPLETE,
            purpose=PurposeCategory.WORK.value,
            start_home=None,
            end_home=None,
            legs=[(self.HOME, self.A), (self.A, self.B), (self.FAR, self.HOME)],
        )

    def test_internal_gap_flags_spatial_gap(self):
        """A tour whose trips jump across a hole is flagged SPATIAL_GAP."""
        tours, linked_trips = self._tour_with_a_hole()

        result = validate_and_correct_tours(tours, linked_trips)

        assert result["tour_data_quality"][0] == TourDataQuality.SPATIAL_GAP.value

    def test_threshold_is_configurable(self):
        """A jump below the configured threshold is not flagged."""
        tours, linked_trips = self._tour_with_a_hole()

        # A very large threshold tolerates the jump -> tour stays VALID.
        result = validate_and_correct_tours(
            tours, linked_trips, spatial_gap_threshold_meters=1_000_000.0
        )

        assert result["tour_data_quality"][0] == TourDataQuality.VALID.value


class TestTourValidationIntegration:
    """Integration tests for tour validation workflow."""

    def test_full_validation_workflow(self):
        """Each structural shape lands on its own quality code."""
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1", "tour_2", "tour_3", "tour_4"],
                "person_id": [1, 1, 2, 2],
                "day_id": [1, 1, 1, 1],
                "trip_count": [2, 1, 3, 2],
                "subtour_num": [0, 0, 0, 0],
                "tour_num": [1, 2, 1, 2],
                "tour_category": [
                    TourCategory.COMPLETE.value,
                    TourCategory.COMPLETE.value,
                    TourCategory.PARTIAL_BOTH.value,
                    TourCategory.PARTIAL_START.value,
                ],
                "tour_purpose": [
                    PurposeCategory.WORK.value,
                    # Nothing but the return leg, so aggregation found no purpose.
                    None,
                    PurposeCategory.SOCIALREC.value,
                    PurposeCategory.SHOP.value,
                ],
            }
        )

        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1"] * 2 + ["tour_2"] + ["tour_3"] * 3 + ["tour_4"] * 2,
                "person_id": [1, 1, 1, 2, 2, 2, 2, 2],
                "hh_id": [1, 1, 1, 2, 2, 2, 2, 2],
                "day_id": [1] * 8,
                "tour_num": [1, 1, 2, 1, 1, 1, 2, 2],
            }
        )

        result = validate_and_correct_tours(tours, linked_trips).sort("tour_id")

        assert result["tour_data_quality"].to_list() == [
            TourDataQuality.VALID.value,  # anchor to anchor, with a purpose
            TourDataQuality.NO_DESTINATION.value,  # nothing to anchor on
            # Without coordinates there is no travel visible either side, which
            # is the diary-edge reading of an open end.
            TourDataQuality.PARTIAL_DIARY_EDGE.value,
            TourDataQuality.PARTIAL_DIARY_EDGE.value,
        ]
