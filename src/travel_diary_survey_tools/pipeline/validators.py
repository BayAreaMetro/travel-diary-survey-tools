"""Validation framework for canonical survey data.

This module provides a comprehensive validation system with multiple layers:
1. Column constraints (uniqueness)
2. Foreign key constraints
3. Row-level validation (Pydantic models with step-awareness)
4. Custom validators (user-defined business logic)
5. Required children (bidirectional FK)

Step-aware validation allows fields to be required only in specific pipeline
steps, enabling progressive data refinement throughout the pipeline.
"""

import logging
import time
from dataclasses import dataclass
from typing import Any

import polars as pl
from pydantic import BaseModel
from pydantic import ValidationError as PydanticValidationError

logger = logging.getLogger(__name__)


# Base Error Class ---------------------------------------------------------

@dataclass
class ValidationError(Exception):
    """Structured validation error with context.

    Attributes:
        table: Name of the table being validated
        rule: Name of the validation rule that failed
        message: Human-readable error description
        row_id: Optional row identifier for row-level errors
        column: Optional column name for column-level errors
    """

    table: str
    rule: str
    message: str
    row_id: int | None = None
    column: str | None = None

    def __str__(self) -> str:
        """Format error message."""
        parts = [f"[{self.table}]"]
        if self.row_id is not None:
            parts.append(f"row {self.row_id}")
        if self.column:
            parts.append(f"column '{self.column}'")
        parts.append(f"({self.rule})")
        parts.append(self.message)
        return " ".join(parts)


# Column Validators --------------------------------------------------------

def check_unique_constraints(
    table_name: str,
    df: pl.DataFrame,
    unique_columns: list[str],
) -> None:
    """Check uniqueness constraints on specified columns.

    Args:
        table_name: Name of the table being validated
        df: DataFrame to validate
        unique_columns: List of column names that must be unique

    Raises:
        ValidationError: If uniqueness constraint is violated
    """
    for col in unique_columns:
        if col not in df.columns:
            raise ValidationError(
                table=table_name,
                rule="unique_constraint",
                column=col,
                message=f"Column '{col}' not found in table",
            )

        # Get non-null values
        non_null = df.filter(pl.col(col).is_not_null())
        if len(non_null) == 0:
            continue

        # Check for duplicates using Polars
        duplicates = (
            non_null.group_by(col)
            .agg(pl.len().alias("count"))
            .filter(pl.col("count") > 1)
        )

        if len(duplicates) > 0:
            dup_values = duplicates[col].to_list()
            raise ValidationError(
                table=table_name,
                rule="unique_constraint",
                column=col,
                message=(
                    f"Duplicate values found: {dup_values[:10]}"
                    f"{' ...' if len(dup_values) > 10 else ''}"  # noqa: PLR2004
                ),
            )


# Foreign Key Validators ---------------------------------------------------

# Map parent table names to their FK column names
_TABLE_TO_FK_COLUMN = {
    "households": "hh_id",
    "persons": "person_id",
    "days": "day_id",
    "unlinked_trips": "trip_id",
    "linked_trips": "linked_trip_id",
    "tours": "tour_id",
}


def check_foreign_keys(
    table_name: str,
    df: pl.DataFrame,
    parent_tables: list[str],
    get_table_func: callable,
) -> None:
    """Check foreign key constraints.

    Args:
        table_name: Name of the table being validated
        df: DataFrame to validate (child table)
        parent_tables: List of parent table names
        get_table_func: Function to retrieve other tables by name

    Raises:
        ValidationError: If foreign key constraint is violated
    """
    for parent_table in parent_tables:
        # Get FK column names from mapping
        if parent_table not in _TABLE_TO_FK_COLUMN:
            msg = f"Unknown parent table: {parent_table}"
            raise ValueError(msg)

        child_col = _TABLE_TO_FK_COLUMN[parent_table]
        parent_col = child_col  # Same column name in both tables

        # Skip if child column doesn't exist yet (will be added later)
        if child_col not in df.columns:
            continue

        # Get parent table
        parent_df = get_table_func(parent_table)
        if parent_df is None:
            logger.warning(
                "Skipping FK check: parent table '%s' is None",
                parent_table,
            )
            continue

        # Check parent column exists
        if parent_col not in parent_df.columns:
            raise ValidationError(
                table=table_name,
                rule="foreign_key",
                column=child_col,
                message=(
                    f"Referenced column '{parent_col}' not found "
                    f"in table '{parent_table}'"
                ),
            )

        # Get non-null child values
        child_values = df.filter(pl.col(child_col).is_not_null())
        if len(child_values) == 0:
            continue

        # Get parent values as set
        parent_values = set(parent_df[parent_col].to_list())

        # Find orphaned child values
        child_set = set(child_values[child_col].to_list())
        orphaned = child_set - parent_values

        if orphaned:
            orphaned_list = sorted(orphaned)
            max_display = 10
            raise ValidationError(
                table=table_name,
                rule="foreign_key",
                column=child_col,
                message=(
                    f"FK violation: {len(orphaned)} values in '{child_col}' "
                    f"not found in '{parent_table}.{parent_col}': "
                    f"{orphaned_list[:max_display]}"
                    f"{' ...' if len(orphaned) > max_display else ''}"
                ),
            )


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
        model(**row_dict)
        return

    # Get fields required for this step
    required_fields = get_required_fields_for_step(model, step_name)

    # Check for missing required fields
    missing_fields = [
        field_name for field_name in required_fields
        if row_dict.get(field_name) is None
    ]

    if missing_fields:
        msg = (
            f"Missing required fields for step '{step_name}': "
            f"{', '.join(missing_fields)}"
        )
        raise ValueError(msg)

    # Build dict with only non-None values to avoid Pydantic's
    # required field enforcement for step-conditional fields
    filtered_row = {k: v for k, v in row_dict.items() if v is not None}

    # Validate all present fields in a single pass using model_validate
    # This is much faster than validating each field individually
    try:
        model.model_validate(filtered_row, strict=False)
    except PydanticValidationError as e:
        # Only re-raise errors for fields that are actually present
        # or required for this step
        relevant_errors = [
            err for err in e.errors()
            if (
                err.get("loc", [None])[0] in filtered_row
                or err.get("loc", [None])[0] in required_fields
            )
        ]
        if relevant_errors:
            raise PydanticValidationError.from_exception_data(
                model.__name__,
                relevant_errors,
            ) from e


def validate_dataframe_rows(  # noqa: C901
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
        ValidationError: If any row fails validation
    """
    if len(df) == 0:
        return

    total_rows = len(df)
    start_time = time.time()
    last_update_time = start_time
    progress_threshold = 100_000
    update_interval = 5  # seconds

    # Convert entire DataFrame to list of dicts once (faster than iter_rows)
    rows = df.to_dicts()

    # Batch validate with progress reporting
    batch_size = 10_000
    errors = []
    max_errors_to_collect = 10

    for batch_start in range(0, total_rows, batch_size):
        batch_end = min(batch_start + batch_size, total_rows)
        batch = rows[batch_start:batch_end]

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
        if total_rows > progress_threshold:
            current_time = time.time()
            if current_time - last_update_time >= update_interval:
                percent_done = (batch_end / total_rows) * 100
                logger.info(
                    "Row validation progress for '%s': %.1f%% (%s/%s rows)",
                    table_name,
                    percent_done,
                    batch_end,
                    total_rows,
                )
                last_update_time = current_time

        # Stop early if we've collected enough errors
        if errors and len(errors) >= max_errors_to_collect:
            break

    # Raise with error details
    if errors:
        if len(errors) == 1:
            row_idx, msg = errors[0]
            raise ValidationError(
                table=table_name,
                rule="row_validation",
                row_id=row_idx,
                message=msg,
            )
        # Multiple errors - provide summary
        error_summary = "\n".join(
            f"  Row {idx}: {msg}" for idx, msg in errors
        )
        raise ValidationError(
            table=table_name,
            rule="row_validation",
            message=(
                f"Found {len(errors)} validation errors:\n"
                f"{error_summary}"
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
    "ValidationError",
    "check_foreign_keys",
    "check_unique_constraints",
    "get_required_fields_for_step",
    "get_step_validation_summary",
    "validate_dataframe_rows",
    "validate_row_for_step",
]
