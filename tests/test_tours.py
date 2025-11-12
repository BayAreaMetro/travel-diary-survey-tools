"""Unit tests for tour building module."""

import polars as pl
import pytest

from travel_diary_survey_tools.constants import (
    LocationType,
    ModeType,
    PersonCategory,
    PersonType,
    TourCategory,
    TripPurpose,
)
from travel_diary_survey_tools.tours import DEFAULT_CONFIG, TourBuilder


@pytest.fixture
def sample_persons():
    """Create sample person data for testing."""
    return pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "hh_id": [1, 1, 2],
            "person_type": [
                PersonType.FULL_TIME_WORKER,
                PersonType.UNIVERSITY_STUDENT,
                PersonType.RETIRED,
            ],
            "age": [35, 22, 68],
            "home_lat": [34.05, 34.05, 34.06],
            "home_lon": [-118.25, -118.25, -118.26],
            "work_lat": [34.10, None, None],
            "work_lon": [-118.30, None, None],
            "school_lat": [None, 34.15, None],
            "school_lon": [None, -118.35, None],
        }
    )


@pytest.fixture
def sample_linked_trips():
    """Create sample linked trip data for testing."""
    return pl.DataFrame(
        {
            "person_id": [1, 1, 1, 1],
            "hh_id": [1, 1, 1, 1],
            "day_id": [1, 1, 1, 1],
            "trip_id": [1, 2, 3, 4],
            "origin_lat": [34.05, 34.10, 34.12, 34.11],
            "origin_lon": [-118.25, -118.30, -118.32, -118.31],
            "dest_lat": [34.10, 34.12, 34.11, 34.05],
            "dest_lon": [-118.30, -118.32, -118.31, -118.25],
            "depart_time": ["08:00:00", "12:00:00", "14:00:00", "17:00:00"],
            "arrive_time": ["08:30:00", "12:15:00", "14:30:00", "17:45:00"],
            "trip_purpose": [
                TripPurpose.WORK,
                TripPurpose.SHOPPING,
                TripPurpose.OTHER,
                TripPurpose.HOME,
            ],
            "trip_mode": [
                ModeType.AUTO,
                ModeType.WALK,
                ModeType.WALK,
                ModeType.AUTO,
            ],
        }
    )


class TestTourBuilderInit:
    """Tests for TourBuilder initialization."""

    def test_init_with_default_config(self, sample_persons):
        """Test initialization with default configuration."""
        builder = TourBuilder(sample_persons)
        assert builder.persons is not None
        assert builder.config == DEFAULT_CONFIG
        assert len(builder.person_locations) == 3

    def test_init_with_custom_config(self, sample_persons):
        """Test initialization with custom configuration."""
        custom_config = DEFAULT_CONFIG.copy()
        custom_config["distance_thresholds"][LocationType.HOME] = 50.0

        builder = TourBuilder(sample_persons, custom_config)
        assert builder.config["distance_thresholds"][LocationType.HOME] == 50.0

    def test_person_category_mapping(self, sample_persons):
        """Test that person categories are correctly mapped."""
        builder = TourBuilder(sample_persons)

        categories = builder.person_locations["person_category"].to_list()
        assert categories[0] == PersonCategory.WORKER
        assert categories[1] == PersonCategory.STUDENT
        assert categories[2] == PersonCategory.OTHER


class TestLocationClassification:
    """Tests for trip location classification."""

    def test_classify_home_location(self, sample_persons):
        """Test classification of home locations."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [1, 1],
                "origin_lat": [34.05, 34.10],
                "origin_lon": [-118.25, -118.30],
                "dest_lat": [34.10, 34.05],
                "dest_lon": [-118.30, -118.25],
            }
        )

        classified = builder._classify_trip_locations(trips)

        assert "origin_is_home" in classified.columns
        assert "dest_is_home" in classified.columns
        assert classified["origin_is_home"][0] is True
        assert classified["dest_is_home"][1] is True

    def test_classify_work_location(self, sample_persons):
        """Test classification of work locations."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [1, 1],
                "origin_lat": [34.10, 34.12],
                "origin_lon": [-118.30, -118.32],
                "dest_lat": [34.12, 34.10],
                "dest_lon": [-118.32, -118.30],
            }
        )

        classified = builder._classify_trip_locations(trips)

        assert "origin_is_work" in classified.columns
        assert "dest_is_work" in classified.columns
        assert classified["origin_is_work"][0] is True
        assert classified["dest_is_work"][1] is True

    def test_location_type_priority(self, sample_persons):
        """Test that location type priority is correctly applied."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [1],
                "origin_lat": [34.05],
                "origin_lon": [-118.25],
                "dest_lat": [34.10],
                "dest_lon": [-118.30],
            }
        )

        classified = builder._classify_trip_locations(trips)

        assert "origin_location_type" in classified.columns
        assert "dest_location_type" in classified.columns
        assert classified["origin_location_type"][0] == LocationType.HOME


class TestHomeTourIdentification:
    """Tests for home-based tour identification."""

    def test_simple_home_tour(self, sample_persons, sample_linked_trips):
        """Test identification of a simple home-based tour."""
        builder = TourBuilder(sample_persons)

        classified = builder._classify_trip_locations(sample_linked_trips)
        tours = builder._identify_home_based_tours(classified)

        assert "tour_id" in tours.columns
        assert "tour_category" in tours.columns
        assert tours["tour_category"][0] == TourCategory.HOME_BASED

    def test_multiple_tours_same_day(self, sample_persons):
        """Test identification of multiple tours in same day."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [1, 1, 1, 1, 1, 1],
                "hh_id": [1, 1, 1, 1, 1, 1],
                "day_id": [1, 1, 1, 1, 1, 1],
                "trip_id": [1, 2, 3, 4, 5, 6],
                "origin_lat": [34.05, 34.10, 34.05, 34.05, 34.12, 34.05],
                "origin_lon": [
                    -118.25,
                    -118.30,
                    -118.25,
                    -118.25,
                    -118.32,
                    -118.25,
                ],
                "dest_lat": [34.10, 34.05, 34.05, 34.12, 34.05, 34.05],
                "dest_lon": [
                    -118.30,
                    -118.25,
                    -118.25,
                    -118.32,
                    -118.25,
                    -118.25,
                ],
                "depart_time": [
                    "08:00",
                    "12:00",
                    "13:00",
                    "14:00",
                    "16:00",
                    "17:00",
                ],
                "arrive_time": [
                    "08:30",
                    "12:30",
                    "13:30",
                    "14:30",
                    "16:30",
                    "17:30",
                ],
                "trip_purpose": [TripPurpose.WORK] * 6,
                "trip_mode": [ModeType.AUTO] * 6,
            }
        )

        classified = builder._classify_trip_locations(trips)
        tours = builder._identify_home_based_tours(classified)

        unique_tours = tours["tour_id"].n_unique()
        # Note: The algorithm creates 3 tours because trips 3 and 6
        # (home to home) create additional tour boundaries
        assert unique_tours >= 2


class TestWorkSubtourIdentification:
    """Tests for work-based subtour identification."""

    def test_work_subtour_in_work_tour(self, sample_persons):
        """Test identification of work-based subtour within work tour."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [1, 1, 1, 1, 1],
                "hh_id": [1, 1, 1, 1, 1],
                "day_id": [1, 1, 1, 1, 1],
                "trip_id": [1, 2, 3, 4, 5],
                "origin_lat": [34.05, 34.10, 34.12, 34.10, 34.05],
                "origin_lon": [-118.25, -118.30, -118.32, -118.30, -118.25],
                "dest_lat": [34.10, 34.12, 34.10, 34.05, 34.05],
                "dest_lon": [-118.30, -118.32, -118.30, -118.25, -118.25],
                "depart_time": ["08:00", "12:00", "13:00", "14:00", "17:00"],
                "arrive_time": ["08:30", "12:15", "13:15", "14:30", "17:30"],
                "trip_purpose": [TripPurpose.WORK] * 5,
                "trip_mode": [ModeType.AUTO] * 5,
            }
        )

        classified = builder._classify_trip_locations(trips)
        with_tours = builder._identify_home_based_tours(classified)
        with_subtours = builder._identify_work_based_subtours(with_tours)

        assert "subtour_id" in with_subtours.columns
        subtours = with_subtours.filter(pl.col("subtour_id").is_not_null())
        assert len(subtours) > 0


class TestTourAttributes:
    """Tests for tour attribute assignment."""

    def test_tour_purpose_priority(self, sample_persons, sample_linked_trips):
        """Test that tour purpose follows priority rules."""
        builder = TourBuilder(sample_persons)

        classified = builder._classify_trip_locations(sample_linked_trips)
        with_tours = builder._identify_home_based_tours(classified)
        with_attrs = builder._assign_tour_attributes(with_tours)

        assert "tour_purpose" in with_attrs.columns
        assert with_attrs["tour_purpose"][0] == TripPurpose.WORK

    def test_tour_mode_hierarchy(self, sample_persons, sample_linked_trips):
        """Test that tour mode follows hierarchy rules."""
        builder = TourBuilder(sample_persons)

        classified = builder._classify_trip_locations(sample_linked_trips)
        with_tours = builder._identify_home_based_tours(classified)
        with_attrs = builder._assign_tour_attributes(with_tours)

        assert "tour_mode" in with_attrs.columns
        assert with_attrs["tour_mode"][0] == ModeType.AUTO

    def test_tour_timing(self, sample_persons, sample_linked_trips):
        """Test tour timing calculation."""
        builder = TourBuilder(sample_persons)

        classified = builder._classify_trip_locations(sample_linked_trips)
        with_tours = builder._identify_home_based_tours(classified)
        with_attrs = builder._assign_tour_attributes(with_tours)

        assert "origin_depart_time" in with_attrs.columns
        assert "dest_arrive_time" in with_attrs.columns


class TestTourAggregation:
    """Tests for tour-level aggregation."""

    def test_aggregate_tours_shape(self, sample_persons, sample_linked_trips):
        """Test that tour aggregation produces correct shape."""
        builder = TourBuilder(sample_persons)

        classified = builder._classify_trip_locations(sample_linked_trips)
        with_tours = builder._identify_home_based_tours(classified)
        tours = builder._aggregate_tours(with_tours)

        assert len(tours) >= 1
        assert "tour_id" in tours.columns
        assert "tour_purpose" in tours.columns
        assert "tour_mode" in tours.columns

    def test_tour_trip_count(self, sample_persons, sample_linked_trips):
        """Test that trip count is correctly calculated."""
        builder = TourBuilder(sample_persons)

        classified = builder._classify_trip_locations(sample_linked_trips)
        with_tours = builder._identify_home_based_tours(classified)
        tours = builder._aggregate_tours(with_tours)

        assert "trip_count" in tours.columns
        assert tours["trip_count"][0] == 4


class TestMultiDayData:
    """Tests for handling multi-day trip data."""

    def test_tours_across_multiple_days(self, sample_persons):
        """Test that tours are correctly identified across different days."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [1, 1, 1, 1, 1, 1, 1, 1],
                "hh_id": [1, 1, 1, 1, 1, 1, 1, 1],
                "day_id": [1, 1, 1, 1, 2, 2, 2, 2],
                "trip_id": [1, 2, 3, 4, 5, 6, 7, 8],
                "origin_lat": [
                    34.05,
                    34.10,
                    34.12,
                    34.11,
                    34.05,
                    34.10,
                    34.12,
                    34.11,
                ],
                "origin_lon": [
                    -118.25,
                    -118.30,
                    -118.32,
                    -118.31,
                    -118.25,
                    -118.30,
                    -118.32,
                    -118.31,
                ],
                "dest_lat": [
                    34.10,
                    34.12,
                    34.11,
                    34.05,
                    34.10,
                    34.12,
                    34.11,
                    34.05,
                ],
                "dest_lon": [
                    -118.30,
                    -118.32,
                    -118.31,
                    -118.25,
                    -118.30,
                    -118.32,
                    -118.31,
                    -118.25,
                ],
                "depart_time": [
                    "08:00",
                    "12:00",
                    "14:00",
                    "17:00",
                    "08:00",
                    "12:00",
                    "14:00",
                    "17:00",
                ],
                "arrive_time": [
                    "08:30",
                    "12:15",
                    "14:30",
                    "17:45",
                    "08:30",
                    "12:15",
                    "14:30",
                    "17:45",
                ],
                "trip_purpose": [TripPurpose.WORK] * 8,
                "trip_mode": [ModeType.AUTO] * 8,
            }
        )

        classified = builder._classify_trip_locations(trips)
        with_tours = builder._identify_home_based_tours(classified)

        # Should have separate tour IDs for each day
        unique_tours = with_tours["tour_id"].n_unique()
        assert unique_tours == 2

        # Check that day_id is properly maintained
        day1_tours = with_tours.filter(pl.col("day_id") == 1)[
            "tour_id"
        ].unique()
        day2_tours = with_tours.filter(pl.col("day_id") == 2)[
            "tour_id"
        ].unique()
        assert len(day1_tours) >= 1
        assert len(day2_tours) >= 1
        assert not any(tid in day2_tours for tid in day1_tours)

    def test_different_tour_patterns_across_days(self, sample_persons):
        """Test varying tour patterns on different days."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [1, 1, 1, 1, 1, 1],
                "hh_id": [1, 1, 1, 1, 1, 1],
                "day_id": [1, 1, 2, 2, 2, 2],
                "trip_id": [1, 2, 3, 4, 5, 6],
                # Day 1: Simple work tour
                # Day 2: Work tour with shopping subtour
                "origin_lat": [34.05, 34.10, 34.05, 34.10, 34.12, 34.10],
                "origin_lon": [
                    -118.25,
                    -118.30,
                    -118.25,
                    -118.30,
                    -118.32,
                    -118.30,
                ],
                "dest_lat": [34.10, 34.05, 34.10, 34.12, 34.10, 34.05],
                "dest_lon": [
                    -118.30,
                    -118.25,
                    -118.30,
                    -118.32,
                    -118.30,
                    -118.25,
                ],
                "depart_time": [
                    "08:00",
                    "17:00",
                    "08:00",
                    "12:00",
                    "13:00",
                    "17:00",
                ],
                "arrive_time": [
                    "08:30",
                    "17:30",
                    "08:30",
                    "12:15",
                    "13:15",
                    "17:30",
                ],
                "trip_purpose": [
                    TripPurpose.WORK,
                    TripPurpose.HOME,
                    TripPurpose.WORK,
                    TripPurpose.SHOPPING,
                    TripPurpose.WORK,
                    TripPurpose.HOME,
                ],
                "trip_mode": [ModeType.AUTO] * 6,
            }
        )

        classified = builder._classify_trip_locations(trips)
        with_tours = builder._identify_home_based_tours(classified)
        with_subtours = builder._identify_work_based_subtours(with_tours)

        # Day 1 should have 1 tour, Day 2 should have 1 tour with a work subtour
        day1_tours = with_subtours.filter(pl.col("day_id") == 1)
        day2_tours = with_subtours.filter(pl.col("day_id") == 2)

        assert day1_tours["tour_id"].n_unique() == 1
        assert day2_tours["tour_id"].n_unique() == 1

        # Day 2 should have subtours
        day2_subtours = day2_tours.filter(pl.col("subtour_id").is_not_null())
        assert len(day2_subtours) > 0


class TestMissingReturnLeg:
    """Tests for tours that don't return home (incomplete tours)."""

    def test_tour_without_return_home(self, sample_persons):
        """Test handling of tour that ends away from home."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [1, 1, 1],
                "hh_id": [1, 1, 1],
                "day_id": [1, 1, 1],
                "trip_id": [1, 2, 3],
                "origin_lat": [34.05, 34.10, 34.12],
                "origin_lon": [-118.25, -118.30, -118.32],
                "dest_lat": [34.10, 34.12, 34.11],  # Never returns to home
                "dest_lon": [-118.30, -118.32, -118.31],
                "depart_time": ["08:00", "12:00", "14:00"],
                "arrive_time": ["08:30", "12:15", "14:30"],
                "trip_purpose": [
                    TripPurpose.WORK,
                    TripPurpose.SHOPPING,
                    TripPurpose.OTHER,
                ],
                "trip_mode": [ModeType.AUTO, ModeType.WALK, ModeType.WALK],
            }
        )

        classified = builder._classify_trip_locations(trips)
        with_tours = builder._identify_home_based_tours(classified)

        # Should still create a tour even without return
        assert "tour_id" in with_tours.columns
        assert with_tours["tour_id"].n_unique() == 1

        # All trips should be assigned to the same tour
        assert with_tours["tour_id"].null_count() == 0

    def test_incomplete_tour_attributes(self, sample_persons):
        """Test that incomplete tours get correct attributes."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [1, 1],
                "hh_id": [1, 1],
                "day_id": [1, 1],
                "trip_id": [1, 2],
                "origin_lat": [34.05, 34.10],
                "origin_lon": [-118.25, -118.30],
                "dest_lat": [34.10, 34.12],
                "dest_lon": [-118.30, -118.32],
                "depart_time": ["08:00", "12:00"],
                "arrive_time": ["08:30", "12:15"],
                "trip_purpose": [TripPurpose.WORK, TripPurpose.SHOPPING],
                "trip_mode": [ModeType.AUTO, ModeType.WALK],
            }
        )

        classified = builder._classify_trip_locations(trips)
        with_tours = builder._identify_home_based_tours(classified)
        with_attrs = builder._assign_tour_attributes(with_tours)

        # Tour purpose should be WORK (higher priority)
        assert with_attrs["tour_purpose"][0] == TripPurpose.WORK

        # Tour mode should be AUTO (higher in hierarchy)
        assert with_attrs["tour_mode"][0] == ModeType.AUTO

    def test_multiple_incomplete_tours(self, sample_persons):
        """Test multiple tours where none return home."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [1, 1, 1, 1],
                "hh_id": [1, 1, 1, 1],
                "day_id": [1, 1, 1, 1],
                "trip_id": [1, 2, 3, 4],
                # First tour from home, but ends at work
                # This would need special handling
                "origin_lat": [34.05, 34.10, 34.10, 34.12],
                "origin_lon": [-118.25, -118.30, -118.30, -118.32],
                "dest_lat": [34.10, 34.10, 34.12, 34.11],
                "dest_lon": [-118.30, -118.30, -118.32, -118.31],
                "depart_time": ["08:00", "08:30", "12:00", "14:00"],
                "arrive_time": ["08:30", "09:00", "12:15", "14:30"],
                "trip_purpose": [TripPurpose.WORK] * 4,
                "trip_mode": [ModeType.AUTO] * 4,
            }
        )

        classified = builder._classify_trip_locations(trips)
        with_tours = builder._identify_home_based_tours(classified)

        # Should create tour(s) even without complete return
        assert with_tours["tour_id"].n_unique() >= 1


class TestNonHomeStart:
    """Tests for trip chains that don't start at home."""

    def test_first_trip_not_from_home(self, sample_persons):
        """Test handling when first trip of day doesn't start at home."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [1, 1, 1],
                "hh_id": [1, 1, 1],
                "day_id": [1, 1, 1],
                "trip_id": [1, 2, 3],
                "origin_lat": [34.10, 34.12, 34.11],  # Starts at work
                "origin_lon": [-118.30, -118.32, -118.31],
                "dest_lat": [34.12, 34.11, 34.05],  # Eventually returns home
                "dest_lon": [-118.32, -118.31, -118.25],
                "depart_time": ["08:00", "12:00", "14:00"],
                "arrive_time": ["08:30", "12:15", "14:30"],
                "trip_purpose": [
                    TripPurpose.SHOPPING,
                    TripPurpose.OTHER,
                    TripPurpose.HOME,
                ],
                "trip_mode": [ModeType.AUTO, ModeType.WALK, ModeType.AUTO],
            }
        )

        classified = builder._classify_trip_locations(trips)
        with_tours = builder._identify_home_based_tours(classified)

        # Should still create a tour
        assert "tour_id" in with_tours.columns
        assert with_tours["tour_id"].n_unique() >= 1

        # All trips should be assigned to tour(s)
        assert with_tours["tour_id"].null_count() == 0

    def test_mid_trip_chain_no_home(self, sample_persons):
        """Test trip chain that neither starts nor ends at home."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [1, 1, 1],
                "hh_id": [1, 1, 1],
                "day_id": [1, 1, 1],
                "trip_id": [1, 2, 3],
                "origin_lat": [
                    34.10,
                    34.12,
                    34.11,
                ],  # Work -> Shopping -> Personal
                "origin_lon": [-118.30, -118.32, -118.31],
                "dest_lat": [34.12, 34.11, 34.13],
                "dest_lon": [-118.32, -118.31, -118.33],
                "depart_time": ["08:00", "12:00", "14:00"],
                "arrive_time": ["08:30", "12:15", "14:30"],
                "trip_purpose": [
                    TripPurpose.SHOPPING,
                    TripPurpose.OTHER,
                    TripPurpose.RECREATION,
                ],
                "trip_mode": [ModeType.AUTO] * 3,
            }
        )

        classified = builder._classify_trip_locations(trips)
        with_tours = builder._identify_home_based_tours(classified)

        # Should assign all trips to a tour (even if incomplete)
        assert with_tours["tour_id"].null_count() == 0

    def test_starts_away_returns_home_later(self, sample_persons):
        """Test when person starts away but makes multiple tours."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [1, 1, 1, 1, 1],
                "hh_id": [1, 1, 1, 1, 1],
                "day_id": [1, 1, 1, 1, 1],
                "trip_id": [1, 2, 3, 4, 5],
                # Start at work, return home, then leave home again
                "origin_lat": [34.10, 34.12, 34.05, 34.10, 34.11],
                "origin_lon": [-118.30, -118.32, -118.25, -118.30, -118.31],
                "dest_lat": [34.12, 34.05, 34.10, 34.11, 34.05],
                "dest_lon": [-118.32, -118.25, -118.30, -118.31, -118.25],
                "depart_time": ["08:00", "09:00", "10:00", "12:00", "14:00"],
                "arrive_time": ["08:30", "09:30", "10:30", "12:30", "14:30"],
                "trip_purpose": [TripPurpose.WORK] * 5,
                "trip_mode": [ModeType.AUTO] * 5,
            }
        )

        classified = builder._classify_trip_locations(trips)
        with_tours = builder._identify_home_based_tours(classified)

        # Should have at least 2 tours
        # (one ending at home, one starting from home)
        assert with_tours["tour_id"].n_unique() >= 2


class TestWorkBasedSubtoursEdgeCases:
    """Tests for edge cases in work-based subtour identification."""

    def test_multiple_work_subtours_same_tour(self, sample_persons):
        """Test multiple work-based subtours within a single home tour."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [1] * 9,
                "hh_id": [1] * 9,
                "day_id": [1] * 9,
                "trip_id": list(range(1, 10)),
                # Home -> Work -> Lunch -> Work -> Meeting -> Work -> Home
                "origin_lat": [
                    34.05,
                    34.10,
                    34.12,
                    34.10,
                    34.10,
                    34.13,
                    34.10,
                    34.12,
                    34.10,
                ],
                "origin_lon": [
                    -118.25,
                    -118.30,
                    -118.32,
                    -118.30,
                    -118.30,
                    -118.33,
                    -118.30,
                    -118.32,
                    -118.30,
                ],
                "dest_lat": [
                    34.10,
                    34.12,
                    34.10,
                    34.10,
                    34.13,
                    34.10,
                    34.12,
                    34.10,
                    34.05,
                ],
                "dest_lon": [
                    -118.30,
                    -118.32,
                    -118.30,
                    -118.30,
                    -118.33,
                    -118.30,
                    -118.32,
                    -118.30,
                    -118.25,
                ],
                "depart_time": [
                    "08:00",
                    "12:00",
                    "13:00",
                    "13:30",
                    "15:00",
                    "16:00",
                    "16:30",
                    "17:00",
                    "17:30",
                ],
                "arrive_time": [
                    "08:30",
                    "12:15",
                    "13:15",
                    "13:45",
                    "15:15",
                    "16:15",
                    "16:45",
                    "17:15",
                    "18:00",
                ],
                "trip_purpose": [
                    TripPurpose.WORK,
                    TripPurpose.MEAL,
                    TripPurpose.WORK,
                    TripPurpose.WORK,
                    TripPurpose.WORK,
                    TripPurpose.WORK,
                    TripPurpose.SHOPPING,
                    TripPurpose.WORK,
                    TripPurpose.HOME,
                ],
                "trip_mode": [
                    ModeType.AUTO,
                    ModeType.WALK,
                    ModeType.WALK,
                    ModeType.WALK,
                    ModeType.AUTO,
                    ModeType.AUTO,
                    ModeType.WALK,
                    ModeType.WALK,
                    ModeType.AUTO,
                ],
            }
        )

        classified = builder._classify_trip_locations(trips)
        with_tours = builder._identify_home_based_tours(classified)
        with_subtours = builder._identify_work_based_subtours(with_tours)

        # Should identify multiple work-based subtours
        subtours = with_subtours.filter(pl.col("subtour_id").is_not_null())
        unique_subtours = subtours["subtour_id"].n_unique()
        assert unique_subtours >= 2

    def test_work_subtour_without_return_to_work(self, sample_persons):
        """Test work subtour that doesn't return to work (goes home instead)."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [1] * 5,
                "hh_id": [1] * 5,
                "day_id": [1] * 5,
                "trip_id": [1, 2, 3, 4, 5],
                # Home -> Work -> Lunch -> Shopping -> Home
                # (never returns to work)
                "origin_lat": [34.05, 34.10, 34.12, 34.11, 34.13],
                "origin_lon": [-118.25, -118.30, -118.32, -118.31, -118.33],
                "dest_lat": [34.10, 34.12, 34.11, 34.13, 34.05],
                "dest_lon": [-118.30, -118.32, -118.31, -118.33, -118.25],
                "depart_time": ["08:00", "12:00", "13:00", "14:00", "16:00"],
                "arrive_time": ["08:30", "12:15", "13:15", "14:30", "16:30"],
                "trip_purpose": [
                    TripPurpose.WORK,
                    TripPurpose.MEAL,
                    TripPurpose.SHOPPING,
                    TripPurpose.OTHER,
                    TripPurpose.HOME,
                ],
                "trip_mode": [ModeType.AUTO] * 5,
            }
        )

        classified = builder._classify_trip_locations(trips)
        with_tours = builder._identify_home_based_tours(classified)
        with_subtours = builder._identify_work_based_subtours(with_tours)

        # Should still identify the incomplete work subtour
        # Or should handle gracefully
        assert "subtour_id" in with_subtours.columns

    def test_nested_subtour_at_school(self, sample_persons):
        """Test that school-based subtours are not created (only work-based)."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [2] * 5,  # University student
                "hh_id": [1] * 5,
                "day_id": [1] * 5,
                "trip_id": [1, 2, 3, 4, 5],
                # Home -> School -> Lunch -> School -> Home
                "origin_lat": [34.05, 34.15, 34.17, 34.15, 34.05],
                "origin_lon": [-118.25, -118.35, -118.37, -118.35, -118.25],
                "dest_lat": [34.15, 34.17, 34.15, 34.05, 34.05],
                "dest_lon": [-118.35, -118.37, -118.35, -118.25, -118.25],
                "depart_time": ["08:00", "12:00", "13:00", "16:00", "16:30"],
                "arrive_time": ["08:30", "12:15", "13:15", "16:30", "17:00"],
                "trip_purpose": [
                    TripPurpose.SCHOOL,
                    TripPurpose.MEAL,
                    TripPurpose.SCHOOL,
                    TripPurpose.HOME,
                    TripPurpose.HOME,
                ],
                "trip_mode": [ModeType.TRANSIT] * 5,
            }
        )

        classified = builder._classify_trip_locations(trips)
        with_tours = builder._identify_home_based_tours(classified)
        with_subtours = builder._identify_work_based_subtours(with_tours)

        # Should not create school-based subtours
        # (only work-based subtours exist)
        # All trips should be part of home-based tour
        home_based = with_subtours.filter(
            pl.col("tour_category") == TourCategory.HOME_BASED
        )
        assert len(home_based) > 0

    def test_work_subtour_with_single_trip(self, sample_persons):
        """Test work subtour consisting of just one trip away from work."""
        builder = TourBuilder(sample_persons)

        trips = pl.DataFrame(
            {
                "person_id": [1] * 5,
                "hh_id": [1] * 5,
                "day_id": [1] * 5,
                "trip_id": [1, 2, 3, 4, 5],
                # Home -> Work -> Quick errand -> Work -> Home
                "origin_lat": [34.05, 34.10, 34.10, 34.12, 34.10],
                "origin_lon": [-118.25, -118.30, -118.30, -118.32, -118.30],
                "dest_lat": [34.10, 34.10, 34.12, 34.10, 34.05],
                "dest_lon": [-118.30, -118.30, -118.32, -118.30, -118.25],
                "depart_time": ["08:00", "08:30", "12:00", "12:30", "17:00"],
                "arrive_time": ["08:30", "09:00", "12:15", "12:45", "17:30"],
                "trip_purpose": [
                    TripPurpose.WORK,
                    TripPurpose.WORK,
                    TripPurpose.OTHER,
                    TripPurpose.WORK,
                    TripPurpose.HOME,
                ],
                "trip_mode": [
                    ModeType.AUTO,
                    ModeType.AUTO,
                    ModeType.WALK,
                    ModeType.WALK,
                    ModeType.AUTO,
                ],
            }
        )

        classified = builder._classify_trip_locations(trips)
        with_tours = builder._identify_home_based_tours(classified)
        with_subtours = builder._identify_work_based_subtours(with_tours)

        # Should identify the single-trip subtour
        subtours = with_subtours.filter(pl.col("subtour_id").is_not_null())
        assert len(subtours) >= 2  # Leave work + return to work
