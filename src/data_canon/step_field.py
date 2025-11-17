"""Module for creating fields with step metadata."""
from typing import Any

from pydantic import Field


def step_field(
    required_in_steps: list[str] | str | None = None,
    created_in_step: str | None = None,
    *,
    skip_final_check: bool = False,
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
        created_in_step: Step name where this field is created/output.
                        Used to validate that fields exist in step outputs.
                        If None, no creation validation is performed.
        skip_final_check: If True, this field will be skipped during
                          the final full validation step.
        **field_kwargs: All other Field parameters (ge, le, default, etc.)

    Returns:
        Field instance with step metadata attached

    Example:
        >>> # Required in all steps
        >>> person_id: int = step_field(ge=1)

        >>> # Required only in specific steps
        >>> age: int | None = step_field(
        ...     required_in_steps=["imputation", "tour_building"],
        ...     ge=0,
        ...     default=None
        ... )

        >>> # Created in a specific step
        >>> tour_id: int | None = step_field(
        ...     created_in_step="tour_building",
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

    # Handle "all" string for required in all steps
    if required_in_steps == ["all"]:
        field_kwargs["json_schema_extra"]["required_in_all_steps"] = True
        required_in_steps = []

    # Add final_check step unless skipped
    if not skip_final_check:
        required_in_steps.append("final_check")

    # Handle created_in_step metadata
    if created_in_step:
        field_kwargs["json_schema_extra"]["created_in_step"] = created_in_step

    # Else pass specific steps list
    elif required_in_steps and len(required_in_steps) > 0:
        field_kwargs["json_schema_extra"]["required_in_steps"] = \
            required_in_steps

    return Field(**field_kwargs)
