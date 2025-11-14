"""Step-aware validation for Pydantic models.

This module provides utilities to validate data based on which pipeline step
is being executed, allowing fields to be required only in certain steps.
"""

import logging
from typing import Any

from pydantic import BaseModel, ValidationError as PydanticValidationError

logger = logging.getLogger(__name__)


def get_required_fields_for_step(
    model: type[BaseModel],
    step_name: str,
) -> set[str]:
    """Get field names that are required for a specific step.

    Args:
        model: Pydantic model class
        step_name: Name of the pipeline step

    Returns:
        Set of field names that are required in this step
    """
    required_fields = set()

    for field_name, field_info in model.model_fields.items():
        # Get step metadata from json_schema_extra
        extra = field_info.json_schema_extra or {}

        # Check if required in all steps
        if extra.get("required_in_all_steps", False):
            required_fields.add(field_name)
            continue

        # Check if required in this specific step
        required_in_steps = extra.get("required_in_steps", [])
        if step_name in required_in_steps:
            required_fields.add(field_name)

    return required_fields


def validate_row_for_step(
    row_dict: dict[str, Any],
    model: type[BaseModel],
    step_name: str | None = None,
) -> None:
    """Validate a single row for a specific pipeline step.

    This function validates that all fields required for the given step
    are present and valid. Fields not required for this step are still
    type-checked if present, but are not required.

    Args:
        row_dict: Dictionary representing a single row
        model: Pydantic model class to validate against
        step_name: Name of the pipeline step. If None, validates all fields.

    Raises:
        PydanticValidationError: If validation fails
    """
    if step_name is None:
        # No step specified - validate all fields strictly
        model(**row_dict)
        return

    # Get fields required for this step
    required_fields = get_required_fields_for_step(model, step_name)

    # Check for missing required fields
    missing_fields = []
    for field_name in required_fields:
        value = row_dict.get(field_name)
        if value is None:
            missing_fields.append(field_name)

    if missing_fields:
        msg = (
            f"Missing required fields for step '{step_name}': "
            f"{', '.join(missing_fields)}"
        )
        raise ValueError(msg)

    # Validate the row with the model
    # This will check types and constraints for all present fields
    try:
        model(**row_dict)
    except PydanticValidationError as e:
        # Filter errors to only show issues with step-required fields
        # or type errors in present fields
        relevant_errors = []
        for error in e.errors():
            field = error.get("loc", [None])[0]
            # Include error if it's for a required field or a present field
            if field in required_fields or row_dict.get(field) is not None:
                relevant_errors.append(error)

        if relevant_errors:
            # Re-raise with filtered errors
            raise PydanticValidationError.from_exception_data(
                model.__name__,
                relevant_errors,
            ) from e


def get_step_validation_summary(model: type[BaseModel]) -> dict[str, list[str]]:
    """Get a summary of which fields are required in which steps.

    Args:
        model: Pydantic model class

    Returns:
        Dictionary mapping step names to lists of required field names
    """
    step_fields: dict[str, list[str]] = {}
    all_steps_fields: list[str] = []

    for field_name, field_info in model.model_fields.items():
        extra = field_info.json_schema_extra or {}

        if extra.get("required_in_all_steps", False):
            all_steps_fields.append(field_name)
            continue

        required_in_steps = extra.get("required_in_steps", [])
        for step in required_in_steps:
            if step not in step_fields:
                step_fields[step] = []
            step_fields[step].append(field_name)

    # Add "ALL" entry for fields required in all steps
    if all_steps_fields:
        step_fields["ALL"] = all_steps_fields

    return step_fields


__all__ = [
    "get_required_fields_for_step",
    "get_step_validation_summary",
    "validate_row_for_step",
]
