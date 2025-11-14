"""Validation framework for canonical survey data.

This module provides a comprehensive validation system with multiple layers:
1. Column constraints (uniqueness)
2. Foreign key constraints
3. Row-level validation (Pydantic models)
4. Custom validators (user-defined business logic)
5. Required children (bidirectional FK)
"""

import logging
import time
from dataclasses import dataclass

import polars as pl
from pydantic import BaseModel

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
                    f"{' ...' if len(dup_values) > 10 else ''}"
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


# Row Validators -----------------------------------------------------------

def validate_rows_with_model(
    table_name: str,
    df: pl.DataFrame,
    model: type[BaseModel],
) -> None:
    """Validate DataFrame rows using a Pydantic model.

    Args:
        table_name: Name of the table being validated
        df: DataFrame to validate
        model: Pydantic model class for row validation

    Raises:
        ValidationError: If any row fails validation
    """
    total_rows = len(df)
    start_time = time.time()
    last_update_time = start_time

    for i, row in enumerate(df.iter_rows(named=True)):
        try:
            model(**row)
        except Exception as e:
            raise ValidationError(
                table=table_name,
                rule="row_validation",
                row_id=i,
                message=str(e),
            ) from e

        # Progress updates for large datasets
        progress_threshold = 100_000
        update_interval = 5
        if total_rows > progress_threshold:
            current_time = time.time()
            if (
                current_time - last_update_time >= update_interval
                or (i + 1) % (total_rows // 4) == 0
            ):
                percent_done = (i + 1) / total_rows * 100
                logger.info(
                    "Row validation progress for '%s': %.1f%% "
                    "(%s/%s rows)",
                    table_name,
                    percent_done,
                    i + 1,
                    total_rows,
                )
                last_update_time = current_time


# Public API ---------------------------------------------------------------

__all__ = [
    "ValidationError",
    "check_unique_constraints",
    "check_foreign_keys",
    "validate_rows_with_model",
]
