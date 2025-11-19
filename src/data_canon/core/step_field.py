"""Module for creating fields with step metadata."""
from typing import Any

from pydantic import Field


def step_field(
    *,
    required_in_steps: list[str] | str | None = None,
    unique: bool = False,
    **field_kwargs: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Create a Field with step metadata.

    This is a wrapper function used to annotate fields in data models
    with metadata indicating in which processing steps the field is required
    or created.

    Args:
        required_in_steps: List of step names where this field is required,
                          or the string "all" to require in all steps.
                          If None/empty, field is NOT required in any step.
        unique: Whether this field should be unique across records.
                Used to validate uniqueness constraints at the table level.
        **field_kwargs: All other Field parameters (ge, le, default, etc.)

    Returns:
        Field instance with step metadata attached

    Example:
        >>> # Required in all steps with uniqueness constraint
        >>> person_id: int = step_field(
        ...     ge=1, unique=True, required_in_steps="all"
        ... )

        >>> # Required only in specific steps
        >>> age: int | None = step_field(
        ...     required_in_steps=["imputation", "tour_building"],
        ...     ge=0,
        ...     default=None
        ... )

        >>> # Not required in any step (optional everywhere)
        >>> notes: str | None = step_field(default=None)
    """
    if "json_schema_extra" not in field_kwargs:
        field_kwargs["json_schema_extra"] = {}

    # Make a list if not already
    if isinstance(required_in_steps, str):
        required_in_steps = [required_in_steps]

    if required_in_steps is None:
        required_in_steps = []

    if unique:
        field_kwargs["json_schema_extra"]["unique"] = True

    # Handle "all" string for required in all steps
    if required_in_steps == ["all"]:
        field_kwargs["json_schema_extra"]["required_in_all_steps"] = True
        required_in_steps = []

    # Else pass specific steps list
    elif required_in_steps and len(required_in_steps) > 0:
        field_kwargs["json_schema_extra"]["required_in_steps"] = \
            required_in_steps

    return Field(**field_kwargs)
