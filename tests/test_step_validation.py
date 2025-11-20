"""Tests for step-aware validation of Pydantic models.

This module tests the selective skip behavior of the pipeline, ensuring that
fields are only required in their designated pipeline steps.
"""

from datetime import datetime

import pytest

from data_canon.core.validators import (
    get_required_fields_for_step,
    validate_row_for_step,
)
from data_canon.models import UnlinkedTripModel


class TestSelectiveFieldRequirements:
    """Test that fields are only required in specific steps."""

    def test_always_required_fields(self):
        """Core ID fields should be required in all steps."""
        required = get_required_fields_for_step(UnlinkedTripModel, "any_step")

        assert "trip_id" in required
        assert "person_id" in required
        assert "hh_id" in required
        assert "day_id" in required

    def test_step_specific_fields_required_only_in_that_step(self):
        """Fields should only be required in their designated step."""
        # linked_trip_id required in extract_tours
        required_tours = get_required_fields_for_step(
            UnlinkedTripModel,
            "extract_tours"
        )
        assert "linked_trip_id" in required_tours
        assert "tour_id" in required_tours

        # But not required in other steps
        required_other = get_required_fields_for_step(
            UnlinkedTripModel,
            "preprocessing"
        )
        assert "linked_trip_id" not in required_other
        assert "tour_id" not in required_other

    def test_datetime_fields_required_only_in_link_trip(self):
        """Datetime fields should only be required in link_trip step."""
        required_link = get_required_fields_for_step(
            UnlinkedTripModel,
            "link_trip"
        )
        assert "depart_time" in required_link
        assert "arrive_time" in required_link

        # Not required in other steps
        required_other = get_required_fields_for_step(
            UnlinkedTripModel,
            "extract_tours"
        )
        assert "depart_time" not in required_other
        assert "arrive_time" not in required_other


class TestStepValidationBehavior:
    """Test the actual validation behavior across steps."""

    def test_validation_passes_without_step_specific_fields_in_wrong_step(self):
        """Should allow missing step-specific fields in other steps."""
        row = {
            "trip_id": 1,
            "person_id": 101,
            "hh_id": 1,
            "day_id": 1001,
            # No linked_trip_id or tour_id - OK for preprocessing
            "depart_date": "2024-01-15",
            "depart_hour": 10,
            "depart_minute": 0,
            "depart_seconds": 0,
            "arrive_date": "2024-01-15",
            "arrive_hour": 11,
            "arrive_minute": 30,
            "arrive_seconds": 0,
            "o_purpose_category": 1,
            "d_purpose_category": 2,
            "mode_type": 1,
            "duration_minutes": 90.0,
            "distance_miles": 10.5,
        }

        # Should pass - we're not in extract_tours step
        validate_row_for_step(row, UnlinkedTripModel, "preprocessing")

    def test_validation_fails_without_step_specific_fields_in_right_step(self):
        """Should require step-specific fields in their designated step."""
        row = {
            "trip_id": 1,
            "person_id": 101,
            "hh_id": 1,
            "day_id": 1001,
            # Missing linked_trip_id and tour_id
            "depart_date": "2024-01-15",
            "depart_hour": 10,
            "depart_minute": 0,
            "depart_seconds": 0,
            "arrive_date": "2024-01-15",
            "arrive_hour": 11,
            "arrive_minute": 30,
            "arrive_seconds": 0,
            "o_purpose_category": 1,
            "d_purpose_category": 2,
            "mode_type": 1,
            "duration_minutes": 90.0,
            "distance_miles": 10.5,
        }

        # Should fail - we're in extract_tours step and need these fields
        with pytest.raises(ValueError, match="linked_trip_id"):
            validate_row_for_step(row, UnlinkedTripModel, "extract_tours")

    def test_validation_passes_with_all_required_fields_for_step(self):
        """Should pass when all step-required fields are present."""
        row = {
            "trip_id": 1,
            "person_id": 101,
            "hh_id": 1,
            "day_id": 1001,
            "linked_trip_id": 1,
            "tour_id": 1,
            "depart_date": "2024-01-15",
            "depart_hour": 10,
            "depart_minute": 0,
            "depart_seconds": 0,
            "arrive_date": "2024-01-15",
            "arrive_hour": 11,
            "arrive_minute": 30,
            "arrive_seconds": 0,
            "o_purpose_category": 1,
            "d_purpose_category": 2,
            "mode_type": 1,
            "duration_minutes": 90.0,
            "distance_miles": 10.5,
        }

        # Should pass - all extract_tours fields present
        validate_row_for_step(row, UnlinkedTripModel, "extract_tours")

    def test_datetime_validation_selective_behavior(self):
        """Datetime fields should follow same selective pattern."""
        # Without datetime - OK for preprocessing
        row_no_dt = {
            "trip_id": 1,
            "person_id": 101,
            "hh_id": 1,
            "day_id": 1001,
            "depart_date": "2024-01-15",
            "depart_hour": 10,
            "depart_minute": 0,
            "depart_seconds": 0,
            "arrive_date": "2024-01-15",
            "arrive_hour": 11,
            "arrive_minute": 30,
            "arrive_seconds": 0,
            "o_purpose_category": 1,
            "d_purpose_category": 2,
            "mode_type": 1,
            "duration_minutes": 90.0,
            "distance_miles": 10.5,
        }
        validate_row_for_step(row_no_dt, UnlinkedTripModel, "preprocessing")

        # Without datetime - Fails for link_trip
        with pytest.raises(ValueError, match=r"depart_time|arrive_time"):
            validate_row_for_step(row_no_dt, UnlinkedTripModel, "link_trip")

        # With datetime - OK for link_trip
        row_with_dt = row_no_dt.copy()
        row_with_dt["depart_time"] = datetime(2024, 1, 15, 10, 0, 0)
        row_with_dt["arrive_time"] = datetime(2024, 1, 15, 11, 30, 0)
        validate_row_for_step(row_with_dt, UnlinkedTripModel, "link_trip")
