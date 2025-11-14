"""Generate column requirement matrix from data models.

This script reads the step metadata from Pydantic models and generates
a documentation matrix showing which columns are required in which
pipeline steps.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from travel_diary_survey_tools.data.models import (
    HouseholdModel,
    LinkedTripModel,
    PersonDayModel,
    PersonModel,
    TourModel,
    UnlinkedTripModel,
)
from travel_diary_survey_tools.data.step_validation import (
    get_step_validation_summary,
)


def generate_matrix_markdown(models: dict[str, type]) -> str:
    """Generate markdown table showing column requirements per step.

    Args:
        models: Dictionary mapping table names to model classes

    Returns:
        Markdown formatted table string
    """
    # Collect all unique steps across all models
    all_steps = set()
    model_summaries = {}

    for table_name, model in models.items():
        summary = get_step_validation_summary(model)
        model_summaries[table_name] = summary
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

    for table_name, model in models.items():
        lines.append(f"## {table_name}")
        lines.append("")

        summary = model_summaries[table_name]
        all_steps_fields = set(summary.get("ALL", []))

        # Get all fields from model
        all_fields = list(model.model_fields.keys())

        # Create table header
        header = ["Field", *sorted_steps]
        lines.append("| " + " | ".join(header) + " |")
        lines.append("| " + " | ".join(["---"] * len(header)) + " |")

        # Create rows for each field
        for field_name in all_fields:
            row = [f"`{field_name}`"]

            # Check if field is required in ALL steps
            if field_name in all_steps_fields:
                row.extend(["✓"] * len(sorted_steps))
            else:
                # Check each step
                for step in sorted_steps:
                    step_fields = summary.get(step, [])
                    if field_name in step_fields:
                        row.append("✓")
                    else:
                        row.append("")

            lines.append("| " + " | ".join(row) + " |")

        lines.append("")

    return "\n".join(lines)


def generate_matrix_csv(models: dict[str, type]) -> str:
    """Generate CSV showing column requirements per step.

    Args:
        models: Dictionary mapping table names to model classes

    Returns:
        CSV formatted string
    """
    # Collect all unique steps across all models
    all_steps = set()
    model_summaries = {}

    for table_name, model in models.items():
        summary = get_step_validation_summary(model)
        model_summaries[table_name] = summary
        for step in summary:
            if step != "ALL":
                all_steps.add(step)

    # Sort steps for consistent ordering
    sorted_steps = sorted(all_steps)

    # Build CSV
    lines = []
    header = ["Table", "Field", *sorted_steps]
    lines.append(",".join(header))

    for table_name, model in models.items():
        summary = model_summaries[table_name]
        all_steps_fields = set(summary.get("ALL", []))

        # Get all fields from model
        all_fields = list(model.model_fields.keys())

        for field_name in all_fields:
            row = [table_name, field_name]

            # Check if field is required in ALL steps
            if field_name in all_steps_fields:
                row.extend(["x"] * len(sorted_steps))
            else:
                # Check each step
                for step in sorted_steps:
                    step_fields = summary.get(step, [])
                    if field_name in step_fields:
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
    csv_path = Path(__file__).parent / "column_requirements.csv"
    csv_path.write_text(csv, encoding="utf-8")
    print(f"Generated: {csv_path}")  # noqa: T201


if __name__ == "__main__":
    main()
