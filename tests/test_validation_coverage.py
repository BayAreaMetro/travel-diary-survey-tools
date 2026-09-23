"""Ensure test fixtures meet validation requirements for all pipeline steps.

These are health checks on the fixtures themselves: if a required field is added
to a model, the fixtures must grow it too or every test built on them goes stale.
An exception from ``validate_row_for_step`` fails the test on its own.
"""

from collections.abc import Callable

import pytest

import processing  # noqa: F401  # triggers step registration
from data_canon.models.survey import (
    HouseholdModel,
    LinkedTripModel,
    PersonModel,
    UnlinkedTripModel,
)
from data_canon.validation.row import validate_row_for_step
from tests.fixtures import create_household, create_linked_trip, create_person
from tests.fixtures.scenario_builders import (
    multi_stop_tour,
    multi_tour_day,
    simple_work_tour,
    work_tour_no_usual_location,
)


@pytest.mark.parametrize(
    ("factory", "model", "table", "step"),
    [
        pytest.param(create_person, PersonModel, "persons", "extract_tours", id="person"),
        pytest.param(create_household, HouseholdModel, "households", "extract_tours", id="hh"),
        pytest.param(
            lambda: create_linked_trip(unlinked_trip_id=1),
            LinkedTripModel,
            "linked_trips",
            "extract_tours",
            id="linked-trip",
        ),
    ],
)
def test_row_fixture_has_all_required_fields(
    factory: Callable[[], dict], model: type, table: str, step: str
) -> None:
    """Each single-row fixture carries every field its step requires."""
    validate_row_for_step(factory(), model, table, step_name=step)


@pytest.mark.parametrize(
    "scenario_fn",
    [simple_work_tour, multi_stop_tour, multi_tour_day, work_tour_no_usual_location],
    ids=["simple_work_tour", "multi_stop_tour", "multi_tour_day", "no_usual_location"],
)
def test_all_scenarios_validate(scenario_fn: Callable) -> None:
    """Ensure all pre-built scenarios pass validation."""
    hh, persons, _, trips = scenario_fn()

    for person_row in persons.to_dicts():
        validate_row_for_step(person_row, PersonModel, "persons", "extract_tours")

    for hh_row in hh.to_dicts():
        validate_row_for_step(hh_row, HouseholdModel, "households", "extract_tours")

    for trip_row in trips.to_dicts():
        validate_row_for_step(trip_row, UnlinkedTripModel, "unlinked_trips", "link_trips")
