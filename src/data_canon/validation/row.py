"""Row-level validation framework for canonical survey data.

This module provides step-aware row validation using Pydantic models.
Step-aware validation allows fields to be required only in specific pipeline
steps, enabling progressive data refinement throughout the pipeline.

Note: ValidationError class is defined in data_canon.core.dataclass to avoid
circular imports.
"""

import logging
import time
from typing import Any

import polars as pl
from pydantic import BaseModel
from pydantic import ValidationError as PydanticValidationError

from data_canon.core.exceptions import DataValidationError

logger = logging.getLogger(__name__)


# Step-Aware Row Validators -----------------------------------------------


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
        ValueError: If required fields are missing
    """
    if step_name is None:
        # No step specified - validate all fields strictly
        model.model_validate(row_dict, strict=False, from_attributes=False)
        return

    # Get fields required for this step
    required_fields = get_required_fields_for_step(model, step_name)

    # Check for missing required fields
    # Note: A field is only "missing" if it's not in the dict at all
    # Fields that are present but have None values are allowed if the
    # field type is Optional (e.g., float | None)
    missing_fields = [
        field_name
        for field_name in required_fields
        if field_name not in row_dict
    ]

    if missing_fields:
        msg = (
            f"Missing required fields for step '{step_name}': "
            f"{', '.join(missing_fields)}"
        )
        raise ValueError(msg)

    # Validate all fields present in row_dict (comprehensive validation)
    # This ensures type checking and constraints are enforced on all present
    # fields, regardless of whether they're required for this specific step
    try:
        # Create a model instance with only the present fields to avoid
        # Pydantic's required field enforcement for non-required fields
        model.model_validate(row_dict, strict=False, from_attributes=False)
    except PydanticValidationError as e:
        # Filter errors to only include fields that are present in the data
        # This prevents errors about missing optional fields
        present_fields = set(row_dict.keys())
        relevant_errors = [
            err
            for err in e.errors()
            if err.get("loc", [None])[0] in present_fields
        ]
        if relevant_errors:
            raise PydanticValidationError.from_exception_data(
                model.__name__,
                relevant_errors,
            ) from e


def validate_dataframe_rows(
    table_name: str,
    df: pl.DataFrame,
    model: type[BaseModel],
    step: str | None = None,
) -> None:
    """Validate all rows in a DataFrame using step-aware validation.

    Args:
        table_name: Name of the table being validated (for error messages)
        df: DataFrame to validate
        model: Pydantic model class for row validation
        step: Pipeline step name for step-aware validation.
             If None, validates all fields strictly.

    Raises:
        DataValidationError: If any row fails validation
    """
    if len(df) == 0:
        return

    total_rows = len(df)
    start_time = time.time()
    update_interval = 2  # seconds

    # Convert entire DataFrame to list of dicts once (faster than iter_rows)
    rows = df.to_dicts()

    # Batch validate with progress reporting
    batch_size = 100_000
    errors = []
    max_errors_to_collect = 10

    for batch_start in range(0, total_rows, batch_size):
        batch_end = min(batch_start + batch_size, total_rows)
        batch = rows[batch_start:batch_end]
        current_time = time.time()

        # Validate each row in batch
        for i, row in enumerate(batch):
            row_idx = batch_start + i
            try:
                validate_row_for_step(row, model, step)
            except (PydanticValidationError, ValueError) as e:
                # Collect errors instead of raising immediately
                errors.append((row_idx, str(e)))
                if len(errors) >= max_errors_to_collect:
                    break

        # Progress updates for large datasets (time-based)
        if (total_rows > batch_size) and (
            current_time - start_time >= update_interval
        ):
            percent_done = (batch_end / total_rows) * 100
            logger.info(
                "Row validation progress for '%s': %.1f%% (%s/%s rows)",
                table_name,
                percent_done,
                batch_end,
                total_rows,
            )

        # Stop early if we've collected enough errors
        if errors and len(errors) >= max_errors_to_collect:
            break

    # Raise with error details
    if errors:
        if len(errors) == 1:
            row_idx, msg = errors[0]
            raise DataValidationError(
                table=table_name,
                rule="row_validation",
                row_id=row_idx,
                message=msg,
            )
        # Multiple errors - provide summary
        error_summary = "\n".join(f"  Row {idx}: {msg}" for idx, msg in errors)
        raise DataValidationError(
            table=table_name,
            rule="row_validation",
            message=(
                f"Found {len(errors)} validation errors:\n{error_summary}"
            ),
        )


def get_step_validation_summary(
    model: type[BaseModel],
) -> dict[str, list[str]]:
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


# Public API ---------------------------------------------------------------

__all__ = [
    "get_required_fields_for_step",
    "get_step_validation_summary",
    "validate_dataframe_rows",
    "validate_row_for_step",
]
