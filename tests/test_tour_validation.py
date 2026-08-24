"""Tests for tour validation helper functions."""

import polars as pl
import pytest

from data_canon.codebook.tours import TourCategory, TourDataQuality
from data_canon.codebook.trips import PurposeCategory
from processing.tours.validation_helpers import validate_and_correct_tours


class TestValidateAndCorrectTours:
    """Test validate_and_correct_tours function."""

    def test_valid_tour_processing(self):
        """Test processing of valid tours."""
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "day_id": [1],
                "trip_count": [2],
                "tour_num": [1],
                "tour_category": [TourCategory.COMPLETE.value],
                "tour_purpose": [PurposeCategory.WORK.value],
            }
        )

        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1", "tour_1"],
                "person_id": [1, 1],
                "hh_id": [1, 1],
                "day_id": [1, 1],
                "tour_num": [1, 1],
                "_o_is_home": [True, False],
                "_d_is_home": [False, True],
                # Home-based tours: the anchor is home, so these mirror the flags above
                "_o_at_anchor": [True, False],
                "_d_at_anchor": [False, True],
            }
        )

        result = validate_and_correct_tours(tours, linked_trips)

        assert "tour_data_quality" in result.columns
        assert len(result) == 1

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
        ("category", "expected"),
        [
            (TourCategory.PARTIAL_BOTH, TourDataQuality.PARTIAL_BOTH),
            (TourCategory.PARTIAL_START, TourDataQuality.PARTIAL_START),
            (TourCategory.PARTIAL_END, TourDataQuality.PARTIAL_END),
            (TourCategory.COMPLETE, TourDataQuality.VALID),
        ],
    )
    def test_partial_quality_mirrors_category(self, category, expected):
        """Each partial quality code carries its category's own integer value."""
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "day_id": [1],
                "trip_count": [2],
                "tour_num": [1],
                "tour_category": [category.value],
                "tour_purpose": [PurposeCategory.SHOP.value],
            }
        )
        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1", "tour_1"],
                "person_id": [1, 1],
                "hh_id": [1, 1],
                "day_id": [1, 1],
                "tour_num": [1, 1],
            }
        )

        result = validate_and_correct_tours(tours, linked_trips)

        assert result["tour_data_quality"][0] == expected.value
        if expected is not TourDataQuality.VALID:
            # The mirror: same name, same integer, in both columns.
            assert expected.value == category.value

    def test_category_is_not_rewritten(self):
        """Quality is a verdict on the category; it never edits it.

        A single-trip tour that left its anchor is PARTIAL_END -- it started at
        home and stopped away. Overwriting that to PARTIAL_BOTH would make the
        column disagree with where the trip actually ran.
        """
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "day_id": [1],
                "trip_count": [1],
                "tour_num": [1],
                "tour_category": [TourCategory.PARTIAL_END.value],
                "tour_purpose": [PurposeCategory.SHOP.value],
            }
        )
        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "hh_id": [1],
                "day_id": [1],
                "tour_num": [1],
            }
        )

        result = validate_and_correct_tours(tours, linked_trips)

        assert result["tour_data_quality"][0] == TourDataQuality.SINGLE_TRIP.value
        assert result["tour_category"][0] == TourCategory.PARTIAL_END.value

    def test_single_anchor_to_anchor_trip_is_a_loop(self):
        """One trip that departs and returns to the anchor is LOOP_TRIP."""
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "day_id": [1],
                "trip_count": [1],
                "tour_num": [1],
                "tour_category": [TourCategory.COMPLETE.value],
                "tour_purpose": [PurposeCategory.SHOP.value],
            }
        )
        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "hh_id": [1],
                "day_id": [1],
                "tour_num": [1],
            }
        )

        result = validate_and_correct_tours(tours, linked_trips)

        assert result["tour_data_quality"][0] == TourDataQuality.LOOP_TRIP.value


class TestSpatialGapDetection:
    """Test SPATIAL_GAP flagging for tours that teleport across a missing leg."""

    def _tour(self, points, *, purpose=PurposeCategory.WORK.value):
        """Build a single multi-trip tour + linked_trips from o/d coordinates.

        points: list of (depart, o_home, d_home, o(lat,lon), d(lat,lon)).
        """
        n = len(points)
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "day_id": [1],
                "trip_count": [n],
                "tour_num": [1],
                "tour_category": [TourCategory.COMPLETE.value],
                "tour_purpose": [purpose],
            }
        )
        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1"] * n,
                "person_id": [1] * n,
                "hh_id": [1] * n,
                "day_id": [1] * n,
                "tour_num": [1] * n,
                "depart_time": [p[0] for p in points],
                "_o_is_home": [p[1] for p in points],
                "_d_is_home": [p[2] for p in points],
                # Home-based tours: the anchor is home
                "_o_at_anchor": [p[1] for p in points],
                "_d_at_anchor": [p[2] for p in points],
                "o_lat": [p[3][0] for p in points],
                "o_lon": [p[3][1] for p in points],
                "d_lat": [p[4][0] for p in points],
                "d_lon": [p[4][1] for p in points],
            }
        )
        return tours, linked_trips

    def test_internal_gap_flags_spatial_gap(self):
        """A tour whose trips jump across a hole is flagged SPATIAL_GAP."""
        home, a, b = (37.70, -122.40), (37.75, -122.42), (37.76, -122.43)
        far = (38.30, -123.00)  # >1km from b -> the connecting leg is missing
        tours, linked_trips = self._tour(
            [
                (8.0, True, False, home, a),
                (10.0, False, False, a, b),  # continuous: resumes at a
                (17.0, False, True, far, home),  # jumps: origin far from b
            ]
        )
        result = validate_and_correct_tours(tours, linked_trips)
        assert result["tour_data_quality"][0] == TourDataQuality.SPATIAL_GAP.value

    def test_continuous_tour_stays_valid(self):
        """A spatially continuous multi-trip tour remains VALID."""
        home, a = (37.70, -122.40), (37.75, -122.42)
        tours, linked_trips = self._tour(
            [
                (8.0, True, False, home, a),
                (17.0, False, True, a, home),  # resumes at a -> continuous
            ]
        )
        result = validate_and_correct_tours(tours, linked_trips)
        assert result["tour_data_quality"][0] == TourDataQuality.VALID.value

    def test_threshold_is_configurable(self):
        """A jump below the configured threshold is not flagged."""
        home, a, b = (37.70, -122.40), (37.75, -122.42), (37.76, -122.43)
        far = (38.30, -123.00)
        tours, linked_trips = self._tour(
            [
                (8.0, True, False, home, a),
                (10.0, False, False, a, b),
                (17.0, False, True, far, home),
            ]
        )
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
                "tour_num": [1, 2, 1, 2],
                "tour_category": [
                    TourCategory.COMPLETE.value,
                    TourCategory.COMPLETE.value,
                    TourCategory.PARTIAL_BOTH.value,
                    TourCategory.PARTIAL_START.value,
                ],
                "tour_purpose": [
                    PurposeCategory.WORK.value,
                    PurposeCategory.SHOP.value,
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
            TourDataQuality.VALID.value,  # 2 trips, anchor to anchor
            TourDataQuality.LOOP_TRIP.value,  # 1 trip, anchor to anchor
            TourDataQuality.PARTIAL_BOTH.value,  # 3 trips, never reaches anchor
            TourDataQuality.PARTIAL_START.value,  # 2 trips, only returns
        ]

    def test_mixed_quality_tours(self):
        """A one-trip tour is SINGLE_TRIP even though its shape is partial."""
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_good", "tour_bad"],
                "person_id": [1, 1],
                "day_id": [1, 1],
                "trip_count": [3, 1],
                "tour_num": [1, 2],
                "tour_category": [TourCategory.COMPLETE.value, TourCategory.PARTIAL_BOTH.value],
                "tour_purpose": [PurposeCategory.WORK.value, PurposeCategory.WORK.value],
            }
        )

        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_good", "tour_good", "tour_good", "tour_bad"],
                "person_id": [1, 1, 1, 1],
                "hh_id": [1, 1, 1, 1],
                "day_id": [1, 1, 1, 1],
                "tour_num": [1, 1, 1, 2],
            }
        )

        result = validate_and_correct_tours(tours, linked_trips).sort("tour_id")

        assert result["tour_data_quality"].to_list() == [
            TourDataQuality.SINGLE_TRIP.value,
            TourDataQuality.VALID.value,
        ]
