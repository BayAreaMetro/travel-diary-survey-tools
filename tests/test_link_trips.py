"""Tests for trip linking functionality."""

from datetime import datetime

import polars as pl

from processing.link import (
    aggregate_linked_trips,
    link_trip_ids,
    link_trips,
)


class TestLinkTripIds:
    """Tests for link_trip_ids function."""

    def test_simple_two_trip_link(self):
        """Should link two trips with change_mode destination."""
        trips = pl.DataFrame(
            {
                "day_id": [1, 1],
                "person_id": [100, 100],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 8, 15),
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 10),
                    datetime(2024, 1, 1, 8, 45),
                ],
                "d_purpose_category": [10, 1],  # change_mode, then work
                "o_lat": [37.7, 37.71],
                "o_lon": [-122.4, -122.41],
                "d_lat": [37.71, 37.75],
                "d_lon": [-122.41, -122.45],
            }
        )

        result = link_trip_ids(
            trips,
            change_mode_code=10,
            max_dwell_time=120,
            dwell_buffer_distance=100,
        )

        # Both trips should have same linked_trip_id
        assert result["linked_trip_id"].n_unique() == 1
        assert len(result) == 2

    def test_no_linking_without_change_mode(self):
        """Should not link trips without change_mode destination."""
        trips = pl.DataFrame(
            {
                "day_id": [1, 1],
                "person_id": [100, 100],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 9, 0),
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 30),
                    datetime(2024, 1, 1, 9, 30),
                ],
                "d_purpose_category": [1, 2],  # work, then shop
                "o_lat": [37.7, 37.75],
                "o_lon": [-122.4, -122.45],
                "d_lat": [37.75, 37.8],
                "d_lon": [-122.45, -122.5],
            }
        )

        result = link_trip_ids(
            trips,
            change_mode_code=10,
            max_dwell_time=120,
            dwell_buffer_distance=100,
        )

        # Each trip should have different linked_trip_id
        assert result["linked_trip_id"].n_unique() == 2

    def test_multiple_person_isolation(self):
        """Should keep different persons' trips separate."""
        trips = pl.DataFrame(
            {
                "day_id": [1, 1],
                "person_id": [100, 200],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 8, 5),
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 30),
                    datetime(2024, 1, 1, 8, 35),
                ],
                "d_purpose_category": [10, 10],
                "o_lat": [37.7, 37.7],
                "o_lon": [-122.4, -122.4],
                "d_lat": [37.71, 37.71],
                "d_lon": [-122.41, -122.41],
            }
        )

        result = link_trip_ids(
            trips,
            change_mode_code=10,
            max_dwell_time=120,
            dwell_buffer_distance=100,
        )

        # Each person should have unique linked_trip_id
        assert result["linked_trip_id"].n_unique() == 2

    def test_max_dwell_time_threshold(self):
        """Should not link trips exceeding max dwell time."""
        trips = pl.DataFrame(
            {
                "day_id": [1, 1],
                "person_id": [100, 100],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 10, 30),  # 150 min gap
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 10),
                    datetime(2024, 1, 1, 11, 0),
                ],
                "d_purpose_category": [10, 1],
                "o_lat": [37.7, 37.71],
                "o_lon": [-122.4, -122.41],
                "d_lat": [37.71, 37.75],
                "d_lon": [-122.41, -122.45],
            }
        )

        result = link_trip_ids(
            trips,
            change_mode_code=10,
            max_dwell_time=120,  # 120 minutes
            dwell_buffer_distance=100,
        )

        # Should not link due to time gap
        assert result["linked_trip_id"].n_unique() == 2

    def test_dwell_buffer_distance_threshold(self):
        """Should not link trips exceeding buffer distance."""
        trips = pl.DataFrame(
            {
                "day_id": [1, 1],
                "person_id": [100, 100],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 8, 15),
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 10),
                    datetime(2024, 1, 1, 8, 45),
                ],
                "d_purpose_category": [10, 1],
                "o_lat": [37.7, 38.5],  # Far apart (~55 miles)
                "o_lon": [-122.4, -122.4],
                "d_lat": [37.71, 38.51],
                "d_lon": [-122.41, -122.41],
            }
        )

        result = link_trip_ids(
            trips,
            change_mode_code=10,
            max_dwell_time=120,
            dwell_buffer_distance=10,  # 10 miles
        )

        # Should not link due to distance
        assert result["linked_trip_id"].n_unique() == 2

    def test_chain_of_three_trips(self):
        """Should link chain of three trips with change_mode."""
        trips = pl.DataFrame(
            {
                "day_id": [1, 1, 1],
                "person_id": [100, 100, 100],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 8, 15),
                    datetime(2024, 1, 1, 8, 30),
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 10),
                    datetime(2024, 1, 1, 8, 25),
                    datetime(2024, 1, 1, 9, 0),
                ],
                "d_purpose_category": [
                    10,
                    10,
                    1,
                ],  # change_mode, change_mode, work
                "o_lat": [37.7, 37.71, 37.72],
                "o_lon": [-122.4, -122.41, -122.42],
                "d_lat": [37.71, 37.72, 37.75],
                "d_lon": [-122.41, -122.42, -122.45],
            }
        )

        result = link_trip_ids(
            trips,
            change_mode_code=10,
            max_dwell_time=120,
            dwell_buffer_distance=100,
        )

        # All three trips should have same linked_trip_id
        assert result["linked_trip_id"].n_unique() == 1
        assert len(result) == 3

    def test_global_unique_linked_trip_ids(self):
        """Should create globally unique linked_trip_ids across days."""
        trips = pl.DataFrame(
            {
                "day_id": [1, 1, 2, 2],
                "person_id": [100, 100, 100, 100],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 9, 0),
                    datetime(2024, 1, 2, 8, 0),
                    datetime(2024, 1, 2, 9, 0),
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 30),
                    datetime(2024, 1, 1, 9, 30),
                    datetime(2024, 1, 2, 8, 30),
                    datetime(2024, 1, 2, 9, 30),
                ],
                "d_purpose_category": [1, 2, 1, 2],
                "o_lat": [37.7, 37.75, 37.7, 37.75],
                "o_lon": [-122.4, -122.45, -122.4, -122.45],
                "d_lat": [37.75, 37.8, 37.75, 37.8],
                "d_lon": [-122.45, -122.5, -122.45, -122.5],
            }
        )

        result = link_trip_ids(
            trips,
            change_mode_code=10,
            max_dwell_time=120,
            dwell_buffer_distance=100,
        )

        # All linked_trip_ids should be unique globally
        assert result["linked_trip_id"].n_unique() == 4

    def test_identical_timestamps(self):
        """Should handle trips with identical timestamps."""
        trips = pl.DataFrame(
            {
                "day_id": [1, 1],
                "person_id": [100, 100],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 8, 0),  # Same time
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 30),
                    datetime(2024, 1, 1, 8, 30),  # Same time
                ],
                "d_purpose_category": [10, 1],
                "o_lat": [37.7, 37.71],
                "o_lon": [-122.4, -122.41],
                "d_lat": [37.71, 37.75],
                "d_lon": [-122.41, -122.45],
            }
        )

        result = link_trip_ids(
            trips,
            change_mode_code=10,
            max_dwell_time=120,
            dwell_buffer_distance=100,
        )

        # Should still process without error
        assert len(result) == 2
        assert result["linked_trip_id"].n_unique() == 1

    def test_empty_dataframe(self):
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

        result = link_trip_ids(
            trips,
            change_mode_code=10,
            max_dwell_time=120,
            dwell_buffer_distance=100,
        )

        assert len(result) == 0
        assert "linked_trip_id" in result.columns


class TestAggregateLinkedTrips:
    """Tests for aggregate_linked_trips function."""

    def test_basic_aggregation(self):
        """Should aggregate two linked trips correctly."""
        trips = pl.DataFrame(
            {
                "linked_trip_id": [1, 1],
                "person_id": [100, 100],
                "hh_id": [10, 10],
                "day_id": [1, 1],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 8, 15),
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 10),
                    datetime(2024, 1, 1, 8, 45),
                ],
                "depart_date": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
                "arrive_date": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
                "depart_hour": [8, 8],
                "depart_minute": [0, 15],
                "depart_seconds": [0, 0],
                "arrive_hour": [8, 9],
                "arrive_minute": [10, 30],
                "arrive_seconds": [0, 0],
                "o_purpose_category": [1, 11],
                "d_purpose_category": [11, 2],
                "o_lat": [37.7, 37.71],
                "o_lon": [-122.4, -122.41],
                "d_lat": [37.71, 37.75],
                "d_lon": [-122.41, -122.45],
                "mode_type": [1, 6],  # walk, transit
                "distance_miles": [0.5, 5.0],
                "duration_minutes": [10.0, 30.0],
                "trip_weight": [1.0, 1.0],
            }
        )

        result = aggregate_linked_trips(trips, transit_mode_codes=[6, 7])

        # Should have one aggregated trip
        assert len(result) == 1

        # Check origin is from first trip
        row = result.row(0, named=True)
        assert row["depart_time"] == datetime(2024, 1, 1, 8, 0)
        assert row["o_lat"] == 37.7
        assert row["o_lon"] == -122.4
        assert row["o_purpose_category"] == 1

        # Check destination is from last trip
        assert row["arrive_time"] == datetime(2024, 1, 1, 8, 45)
        assert row["d_lat"] == 37.75
        assert row["d_lon"] == -122.45
        assert row["d_purpose_category"] == 2

        # Check aggregated fields
        assert row["distance_miles"] == 5.5
        assert row["num_segments"] == 2
        assert row["mode_type"] == 6  # Transit takes precedence

    def test_transit_mode_precedence(self):
        """Should select transit mode when present in any segment."""
        trips = pl.DataFrame(
            {
                "linked_trip_id": [1, 1, 1],
                "person_id": [100, 100, 100],
                "hh_id": [10, 10, 10],
                "day_id": [1, 1, 1],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 8, 15),
                    datetime(2024, 1, 1, 8, 45),
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 10),
                    datetime(2024, 1, 1, 8, 40),
                    datetime(2024, 1, 1, 9, 0),
                ],
                "depart_date": [datetime(2024, 1, 1)] * 3,
                "arrive_date": [datetime(2024, 1, 1)] * 3,
                "depart_hour": [8, 8, 8],
                "depart_minute": [0, 15, 45],
                "depart_seconds": [0, 0, 0],
                "arrive_hour": [8, 8, 9],
                "arrive_minute": [10, 40, 0],
                "arrive_seconds": [0, 0, 0],
                "o_purpose_category": [0, 10, 10],
                "d_purpose_category": [10, 10, 1],
                "o_lat": [37.7, 37.71, 37.75],
                "o_lon": [-122.4, -122.41, -122.45],
                "d_lat": [37.71, 37.75, 37.8],
                "d_lon": [-122.41, -122.45, -122.5],
                "mode_type": [1, 6, 1],  # walk, transit, walk
                "distance_miles": [0.5, 10.0, 0.3],
                "duration_minutes": [10.0, 25.0, 15.0],
                "trip_weight": [1.0, 1.0, 1.0],
            }
        )

        result = aggregate_linked_trips(trips, transit_mode_codes=[6, 7])

        # Should select transit mode
        assert result["mode_type"][0] == 6

    def test_longest_duration_mode_without_transit(self):
        """Should select longest duration mode when no transit."""
        trips = pl.DataFrame(
            {
                "linked_trip_id": [1, 1],
                "person_id": [100, 100],
                "hh_id": [10, 10],
                "day_id": [1, 1],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 8, 15),
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 10),
                    datetime(2024, 1, 1, 8, 45),
                ],
                "depart_date": [datetime(2024, 1, 1)] * 2,
                "arrive_date": [datetime(2024, 1, 1)] * 2,
                "depart_hour": [8, 8],
                "depart_minute": [0, 15],
                "depart_seconds": [0, 0],
                "arrive_hour": [8, 8],
                "arrive_minute": [10, 45],
                "arrive_seconds": [0, 0],
                "o_purpose_category": [0, 10],
                "d_purpose_category": [10, 1],
                "o_lat": [37.7, 37.71],
                "o_lon": [-122.4, -122.41],
                "d_lat": [37.71, 37.75],
                "d_lon": [-122.41, -122.45],
                "mode_type": [1, 3],  # walk 10 min, drive 30 min
                "distance_miles": [0.5, 5.0],
                "duration_minutes": [10.0, 30.0],
                "trip_weight": [1.0, 1.0],
            }
        )

        result = aggregate_linked_trips(trips, transit_mode_codes=[6, 7])

        # Should select drive (mode 3) as longest duration
        assert result["mode_type"][0] == 3

    def test_dwell_duration_calculation(self):
        """Should calculate dwell time correctly."""
        trips = pl.DataFrame(
            {
                "linked_trip_id": [1, 1],
                "person_id": [100, 100],
                "hh_id": [10, 10],
                "day_id": [1, 1],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 8, 20),  # 10 min gap
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 10),
                    datetime(2024, 1, 1, 8, 45),
                ],
                "depart_date": [datetime(2024, 1, 1)] * 2,
                "arrive_date": [datetime(2024, 1, 1)] * 2,
                "depart_hour": [8, 8],
                "depart_minute": [0, 20],
                "depart_seconds": [0, 0],
                "arrive_hour": [8, 8],
                "arrive_minute": [10, 45],
                "arrive_seconds": [0, 0],
                "o_purpose_category": [0, 10],
                "d_purpose_category": [10, 1],
                "o_lat": [37.7, 37.71],
                "o_lon": [-122.4, -122.41],
                "d_lat": [37.71, 37.75],
                "d_lon": [-122.41, -122.45],
                "mode_type": [1, 1],
                "distance_miles": [0.5, 5.0],
                "duration_minutes": [10.0, 25.0],  # Travel time
                "trip_weight": [1.0, 1.0],
            }
        )

        result = aggregate_linked_trips(trips, transit_mode_codes=[6, 7])

        row = result.row(0, named=True)
        # Total duration: 8:00 to 8:45 = 45 min
        assert row["duration_minutes"] == 45.0
        # Travel duration: 10 + 25 = 35 min
        assert row["travel_duration_minutes"] == 35.0
        # Dwell duration: 45 - 35 = 10 min
        assert row["dwell_duration_minutes"] == 10.0

    def test_multiple_linked_trips(self):
        """Should aggregate multiple separate linked trips."""
        trips = pl.DataFrame(
            {
                "linked_trip_id": [1, 1, 2, 2],
                "person_id": [100, 100, 100, 100],
                "hh_id": [10, 10, 10, 10],
                "day_id": [1, 1, 1, 1],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 8, 15),
                    datetime(2024, 1, 1, 17, 0),
                    datetime(2024, 1, 1, 17, 15),
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 10),
                    datetime(2024, 1, 1, 8, 45),
                    datetime(2024, 1, 1, 17, 10),
                    datetime(2024, 1, 1, 17, 45),
                ],
                "depart_date": [datetime(2024, 1, 1)] * 4,
                "arrive_date": [datetime(2024, 1, 1)] * 4,
                "depart_hour": [8, 8, 17, 17],
                "depart_minute": [0, 15, 0, 15],
                "depart_seconds": [0, 0, 0, 0],
                "arrive_hour": [8, 8, 17, 17],
                "arrive_minute": [10, 45, 10, 45],
                "arrive_seconds": [0, 0, 0, 0],
                "o_purpose_category": [0, 10, 1, 10],
                "d_purpose_category": [10, 1, 10, 0],
                "o_lat": [37.7, 37.71, 37.75, 37.71],
                "o_lon": [-122.4, -122.41, -122.45, -122.41],
                "d_lat": [37.71, 37.75, 37.71, 37.7],
                "d_lon": [-122.41, -122.45, -122.41, -122.4],
                "mode_type": [1, 6, 6, 1],
                "distance_miles": [0.5, 5.0, 5.0, 0.5],
                "duration_minutes": [10.0, 30.0, 10.0, 30.0],
                "trip_weight": [1.0, 1.0, 1.0, 1.0],
            }
        )

        result = aggregate_linked_trips(trips, transit_mode_codes=[6, 7])

        # Should have two aggregated trips
        assert len(result) == 2
        # Sort by linked_trip_id to ensure consistent ordering
        assert sorted(result["linked_trip_id"].to_list()) == [1, 2]


class TestLinkTripsIntegration:
    """Integration tests for the complete link_trips function."""

    def test_end_to_end_linking(self):
        """Should link and aggregate trips end-to-end."""
        trips = pl.DataFrame(
            {
                "trip_id": [1, 2, 3],
                "day_id": [1, 1, 1],
                "person_id": [100, 100, 100],
                "hh_id": [10, 10, 10],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 8, 15),
                    datetime(2024, 1, 1, 17, 0),
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 10),
                    datetime(2024, 1, 1, 8, 45),
                    datetime(2024, 1, 1, 17, 30),
                ],
                "depart_date": [datetime(2024, 1, 1)] * 3,
                "arrive_date": [datetime(2024, 1, 1)] * 3,
                "depart_hour": [8, 8, 17],
                "depart_minute": [0, 15, 0],
                "depart_seconds": [0, 0, 0],
                "arrive_hour": [8, 8, 17],
                "arrive_minute": [10, 45, 30],
                "arrive_seconds": [0, 0, 0],
                "o_purpose_category": [1, 11, 2],
                "d_purpose_category": [11, 2, 1],
                "o_lat": [37.7, 37.71, 37.75],
                "o_lon": [-122.4, -122.41, -122.45],
                "d_lat": [37.71, 37.75, 37.7],
                "d_lon": [-122.41, -122.45, -122.4],
                "mode_type": [1, 6, 6],
                "distance_miles": [0.5, 5.0, 5.0],
                "duration_minutes": [10.0, 30.0, 30.0],
                "trip_weight": [1.0, 1.0, 1.0],
            }
        )

        result = link_trips(
            trips,
            change_mode_code=11,
            transit_mode_codes=[6, 7],
            max_dwell_time=120,
            dwell_buffer_distance=100,
        )

        # Should return dict with two DataFrames
        assert "trips_with_ids" in result
        assert "linked_trips" in result

        # Original trips with linked_trip_id
        trips_with_ids = result["trips_with_ids"]
        assert len(trips_with_ids) == 3
        assert "linked_trip_id" in trips_with_ids.columns

        # Aggregated linked trips
        linked_trips = result["linked_trips"]
        assert len(linked_trips) == 2  # Two linked trips

    def test_preserves_all_required_columns(self):
        """Should preserve all required columns in output."""
        trips = pl.DataFrame(
            {
                "trip_id": [1],
                "day_id": [1],
                "person_id": [100],
                "hh_id": [10],
                "depart_time": [datetime(2024, 1, 1, 8, 0)],
                "arrive_time": [datetime(2024, 1, 1, 8, 30)],
                "depart_date": [datetime(2024, 1, 1)],
                "arrive_date": [datetime(2024, 1, 1)],
                "depart_hour": [8],
                "depart_minute": [0],
                "depart_seconds": [0],
                "arrive_hour": [8],
                "arrive_minute": [30],
                "arrive_seconds": [0],
                "o_purpose_category": [1],
                "d_purpose_category": [2],
                "o_lat": [37.7],
                "o_lon": [-122.4],
                "d_lat": [37.75],
                "d_lon": [-122.45],
                "mode_type": [1],
                "distance_miles": [3.0],
                "duration_minutes": [30.0],
                "trip_weight": [1.0],
            }
        )

        result = link_trips(
            trips,
            change_mode_code=10,
            transit_mode_codes=[6, 7],
        )

        linked_trips = result["linked_trips"]

        # Check all expected columns are present
        expected_columns = [
            "linked_trip_id",
            "person_id",
            "hh_id",
            "day_id",
            "depart_time",
            "arrive_time",
            "o_purpose_category",
            "d_purpose_category",
            "mode_type",
            "distance_miles",
            "duration_minutes",
            "num_segments",
        ]

        for col in expected_columns:
            assert col in linked_trips.columns
