"""Generate column requirement matrix from data models.

This script reads the step metadata from Pydantic models and generates
a documentation matrix showing which columns are required in which
pipeline steps.
"""

import sys
import types
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pydantic import BaseModel

from data_canon.models import (
    HouseholdModel,
    LinkedTripModel,
    PersonDayModel,
    PersonModel,
    TourModel,
    UnlinkedTripModel,
)
from data_canon.validators import (
    get_step_validation_summary,
)


def get_field_type_description(field_info: object) -> str:
    """Extract human-readable type description from field.

    Args:
        field_info: Pydantic FieldInfo object

    Returns:
        String describing the field type
    """
    annotation = field_info.annotation

    # Handle Optional types (Union with None)
    if hasattr(annotation, "__origin__"):
        origin = annotation.__origin__
        # Check for Union type (includes | syntax)
        if origin is types.UnionType or str(origin) == "typing.Union":
            args = annotation.__args__
            non_none = [arg for arg in args if arg is not type(None)]
            if non_none and len(non_none) == 1:
                return non_none[0].__name__
            # Multiple non-None types
            type_names = [arg.__name__ for arg in non_none]
            return " or ".join(type_names)

    # Simple type
    if hasattr(annotation, "__name__"):
        return annotation.__name__

    # Convert to string and replace pipe with "or" for markdown compatibility
    # Replace " | " with " or " to avoid breaking markdown table delimiters
    return str(annotation).replace(" | ", " or ")


def get_field_constraints(field_info: object) -> str:
    """Extract validation constraints from field.

    Args:
        field_info: Pydantic FieldInfo object

    Returns:
        String describing constraints
    """
    constraints = []

    # Get constraints from metadata
    if hasattr(field_info, "metadata"):
        for item in field_info.metadata:
            if hasattr(item, "ge") and item.ge is not None:
                constraints.append(f"≥ {item.ge}")
            if hasattr(item, "le") and item.le is not None:
                constraints.append(f"≤ {item.le}")
            if hasattr(item, "gt") and item.gt is not None:
                constraints.append(f"> {item.gt}")
            if hasattr(item, "lt") and item.lt is not None:
                constraints.append(f"< {item.lt}")

    return ", ".join(constraints) if constraints else ""


def get_field_creation_info(model: type[BaseModel]) -> dict[str, str]:
    """Get information about which step creates each field.

    Args:
        model: Pydantic model class

    Returns:
        Dictionary mapping field names to the step that creates them
    """
    creation_info = {}
    for field_name, field_info in model.model_fields.items():
        extra = field_info.json_schema_extra or {}
        created_in_step = extra.get("created_in_step")
        if created_in_step:
            creation_info[field_name] = created_in_step
    return creation_info


def generate_matrix_markdown(models: dict[str, type]) -> str:  # noqa: C901, PLR0912, PLR0915
    """Generate markdown table showing column requirements per step.

    Args:
        models: Dictionary mapping table names to model classes

    Returns:
        Markdown formatted table string
    """
    # Collect all unique steps across all models
    all_steps = set()
    model_summaries = {}
    creation_info = {}

    for table_name, model in models.items():
        summary = get_step_validation_summary(model)
        model_summaries[table_name] = summary
        creation_info[table_name] = get_field_creation_info(model)
        for step in summary:
            if step != "ALL":
                all_steps.add(step)

    # Sort steps for consistent ordering
    sorted_steps = sorted(all_steps)

    # Build markdown table
    lines = []
    lines.append("# Column Requirement Matrix")
    lines.append("")
    lines.append(
        "This matrix shows which columns are required in which pipeline steps."
    )
    lines.append(
        "Generated automatically from Pydantic model field metadata."
    )
    lines.append("")
    lines.append("")
    lines.append("- ✓ = required in step")
    lines.append("- \\+ = created in step")
    lines.append("")

    # Create table header
    header = ["Table", "Field", "Type", "Constraints", *sorted_steps]
    lines.append("| " + " | ".join(header) + " |")
    lines.append("| " + " | ".join(["---"] * len(header)) + " |")

    for table_name, model in models.items():
        summary = model_summaries[table_name]
        all_steps_fields = set(summary.get("ALL", []))
        field_creation = creation_info[table_name]

        # Get all fields from model
        all_fields = list(model.model_fields.keys())

        # Create rows for each field
        for i, field_name in enumerate(all_fields):
            field_info = model.model_fields[field_name]
            field_type = get_field_type_description(field_info)
            constraints = get_field_constraints(field_info)

            # Add table name only in first row for this table
            if i == 0:
                row = [
                    f"**{table_name}**",
                    f"`{field_name}`",
                    field_type,
                    constraints,
                ]
            else:
                # Other rows - leave table name blank
                row = ["", f"`{field_name}`", field_type, constraints]

            # Check if field is required in ALL steps
            if field_name in all_steps_fields:
                # Check if created in any step
                for step in sorted_steps:
                    created_in = field_creation.get(field_name)
                    if created_in == step:
                        row.append("+")
                    else:
                        row.append("✓")
            else:
                # Check each step
                for step in sorted_steps:
                    step_fields = summary.get(step, [])
                    created_in = field_creation.get(field_name)

                    if created_in == step:
                        # Field is created in this step
                        row.append("+")
                    elif field_name in step_fields:
                        # Field is required in this step
                        row.append("✓")
                    else:
                        row.append("")

            lines.append("| " + " | ".join(row) + " |")

    lines.append("")

    return "\n".join(lines)


def generate_matrix_csv(models: dict[str, type]) -> str:  # noqa: C901, PLR0912
    """Generate CSV showing column requirements per step.

    Args:
        models: Dictionary mapping table names to model classes

    Returns:
        CSV formatted string
    """
    # Collect all unique steps across all models
    all_steps = set()
    model_summaries = {}
    creation_info = {}

    for table_name, model in models.items():
        summary = get_step_validation_summary(model)
        model_summaries[table_name] = summary
        creation_info[table_name] = get_field_creation_info(model)
        for step in summary:
            if step != "ALL":
                all_steps.add(step)

    # Sort steps for consistent ordering
    sorted_steps = sorted(all_steps)

    # Build CSV
    lines = []
    header = ["Table", "Field", "Type", "Constraints", *sorted_steps]
    lines.append(",".join(header))

    for table_name, model in models.items():
        summary = model_summaries[table_name]
        all_steps_fields = set(summary.get("ALL", []))
        field_creation = creation_info[table_name]

        # Get all fields from model
        all_fields = list(model.model_fields.keys())

        for field_name in all_fields:
            field_info = model.model_fields[field_name]
            field_type = get_field_type_description(field_info)
            constraints = get_field_constraints(field_info)
            
            row = [table_name, field_name, field_type, constraints]

            # Check if field is required in ALL steps
            if field_name in all_steps_fields:
                # Check if any are creation steps
                for step in sorted_steps:
                    created_in = field_creation.get(field_name)
                    if created_in == step:
                        row.append("+")
                    else:
                        row.append("x")
            else:
                # Check each step
                for step in sorted_steps:
                    step_fields = summary.get(step, [])
                    created_in = field_creation.get(field_name)

                    if created_in == step:
                        # Field is created in this step
                        row.append("+")
                    elif field_name in step_fields:
                        # Field is required in this step
                        row.append("x")
                    else:
                        row.append("")

            lines.append(",".join(row))

    return "\n".join(lines)


def main() -> None:
    """Generate and save column requirement matrices."""
    models = {
        "households": HouseholdModel,
        "persons": PersonModel,
        "days": PersonDayModel,
        "unlinked_trips": UnlinkedTripModel,
        "linked_trips": LinkedTripModel,
        "tours": TourModel,
    }

    # Generate markdown in repo root
    markdown = generate_matrix_markdown(models)
    output_path = Path(__file__).parent.parent / "COLUMN_REQUIREMENTS.md"
    output_path.write_text(markdown, encoding="utf-8")
    print(f"Generated: {output_path}")  # noqa: T201

    # Generate CSV in scripts folder
    csv = generate_matrix_csv(models)
    csv_path = Path(__file__).parent.parent / "column_requirements.csv"
    csv_path.write_text(csv, encoding="utf-8")
    print(f"Generated: {csv_path}")  # noqa: T201


if __name__ == "__main__":
    main()
