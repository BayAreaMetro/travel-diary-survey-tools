"""Data validation functions for travel survey data using Pydantic models."""
import inspect
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import polars as pl
from pydantic import BaseModel

from . import checks
from .models import (
    DayModel,
    HouseholdModel,
    LinkedTripModel,
    PersonModel,
    TourModel,
    UnlinkedTripModel,
)
from .step_validation import validate_row_for_step
from .validators import (
    ValidationError,
    check_foreign_keys,
    check_unique_constraints,
)

logger = logging.getLogger(__name__)

@dataclass
class CanonicalData:
    """Canonical data structure for travel survey data with validation tracking.

    When tables are set or modified, their validation status is automatically
    reset to False. Use the validate() method to validate specific tables.
    """

    households: pl.DataFrame | None = None
    persons: pl.DataFrame | None = None
    days: pl.DataFrame | None = None
    unlinked_trips: pl.DataFrame | None = None
    linked_trips: pl.DataFrame | None = None
    tours: pl.DataFrame | None = None

    # Track validation status - use field() to ensure proper initialization
    # Key format: (table_name, step_name) or table_name for non-step validation
    _validation_status: dict[str | tuple[str, str], bool] = field(
        default_factory=dict
    )

    # Model mapping for validation
    _models: dict[str, type[BaseModel]] = field(default_factory=lambda: {
        "households": HouseholdModel,
        "persons": PersonModel,
        "days": DayModel,
        "unlinked_trips": UnlinkedTripModel,
        "linked_trips": LinkedTripModel,
        "tours": TourModel,
    })

    # Uniqueness constraints
    _unique_constraints: dict[str, list[str]] = field(default_factory=lambda: {
        "households": ["hh_id"],
        "persons": ["person_id"],
        "days": ["day_id"],
        "unlinked_trips": ["trip_id"],
        "linked_trips": ["linked_trip_id"],
        "tours": ["tour_id"],
    })

    # Foreign key constraints: list of parent table names
    # FK column looked up from validators.foreign_key._TABLE_TO_FK_COLUMN
    _foreign_keys: dict[str, list[str]] = field(
        default_factory=lambda: {
            "persons": ["households"],
            "days": ["persons", "households"],
            "unlinked_trips": [
                "persons",
                "households",
                "days",
                "linked_trips",
                "tours",
            ],
            "linked_trips": ["persons", "households", "days", "tours"],
            "tours": ["persons", "days"],
        }
    )

    # Required children: child tables that MUST have records for each parent
    # Format: parent_table -> list of (child_table, fk_column)
    _required_children: dict[str, list[tuple[str, str]]] = field(
        default_factory=lambda: {
            "households": [("persons", "hh_id")],
            "persons": [("days", "person_id")],
        }
    )

    # Custom validators: table_name -> list of validator functions
    # Populated from checks.CUSTOM_VALIDATORS
    _custom_validators: dict[str, list[Callable]] = field(
        default_factory=lambda: {
            table: list(validators)
            for table, validators in checks.CUSTOM_VALIDATORS.items()
        }
    )

    def __setattr__(self, name: str, value: any) -> None:
        """Override setattr to track when tables are modified."""
        # Set the attribute normally first
        object.__setattr__(self, name, value)

        # If it's a table being set/modified, reset validation status
        # Check hasattr for _models to avoid issues during initialization
        if (
            hasattr(self, "_models")
            and name in self._models
            and hasattr(self, "_validation_status")
        ):
            # Clear all validation status for this table (all steps)
            keys_to_remove = [
                k for k in self._validation_status
                if (isinstance(k, tuple) and k[0] == name) or k == name
            ]
            for k in keys_to_remove:
                del self._validation_status[k]
            logger.debug(
                "Table '%s' modified - validation status reset",
                name
            )

    def validate(self, table_name: str, step: str | None = None) -> None:
        """Validate a table through all validation layers.

        Runs validation in this order:
        1. Column constraints (uniqueness)
        2. Foreign key constraints
        3. Row-level Pydantic validation (step-aware if step provided)
        4. Custom user-registered validators

        Args:
            table_name: Name of the table to validate
            step: Pipeline step name for step-aware validation.
                 If None, validates all fields strictly.

        Raises:
            ValidationError: If any validation check fails
        """
        if table_name not in self._models:
            valid_tables = ", ".join(self._models.keys())
            msg = (
                f"Invalid table name: {table_name}. "
                f"Valid tables: {valid_tables}"
            )
            raise ValueError(msg)

        df = getattr(self, table_name)
        if df is None:
            logger.warning(
                "Table '%s' is None - skipping validation",
                table_name
            )
            return

        start_time = time.time()
        step_info = f" for step '{step}'" if step else ""
        logger.info(
            "Validating table '%s'%s (%s rows)",
            table_name,
            step_info,
            f"{len(df):,}"
        )

        # 1. Column constraints
        if table_name in self._unique_constraints:
            check_unique_constraints(
                table_name,
                df,
                self._unique_constraints[table_name],
            )

        # 2. Foreign key constraints
        if table_name in self._foreign_keys:
            check_foreign_keys(
                table_name,
                df,
                self._foreign_keys[table_name],
                lambda t: getattr(self, t),
            )

        # 3. Row validation (step-aware)
        self._validate_rows_for_step(
            table_name,
            df,
            self._models[table_name],
            step,
        )

        # 4. Custom validators
        self._run_custom_validators(table_name, df)

        # 5. Required children (bidirectional FK check)
        self._check_required_children(table_name, df)

        # Mark as validated
        status_key = (table_name, step) if step else table_name
        self._validation_status[status_key] = True
        elapsed = time.time() - start_time
        logger.info(
            "✓ Table '%s'%s validated successfully in %.2fs",
            table_name,
            step_info,
            elapsed
        )

    def _validate_rows_for_step(
        self,
        table_name: str,
        df: pl.DataFrame,
        model: type[BaseModel],
        step: str | None = None,
    ) -> None:
        """Validate DataFrame rows using step-aware validation.

        Args:
            table_name: Name of the table being validated
            df: DataFrame to validate
            model: Pydantic model class for row validation
            step: Pipeline step name for step-aware validation

        Raises:
            ValidationError: If any row fails validation
        """
        total_rows = len(df)
        start_time = time.time()
        last_update_time = start_time

        for i, row in enumerate(df.iter_rows(named=True)):
            try:
                validate_row_for_step(row, model, step)
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

    def _run_custom_validators(
        self,
        table_name: str,
        _df: pl.DataFrame,
    ) -> None:
        """Run user-registered custom validators for a table.

        Args:
            table_name: Name of the table being validated
            df: DataFrame being validated
        """
        if table_name not in self._custom_validators:
            return

        for validator_func in self._custom_validators[table_name]:
            # Inspect function signature to build arguments
            sig = inspect.signature(validator_func)
            kwargs = {}

            for param_name in sig.parameters:
                if hasattr(self, param_name):
                    table_df = getattr(self, param_name)
                    # Skip validator if required table is None
                    if table_df is None:
                        logger.warning(
                            "Skipping validator %s: required table '%s' "
                            "is None",
                            validator_func.__name__,
                            param_name,
                        )
                        return
                    kwargs[param_name] = table_df
                else:
                    msg = (
                        f"Validator {validator_func.__name__} requires "
                        f"unknown table: {param_name}"
                    )
                    raise ValueError(msg)

            # Call validator
            errors = validator_func(**kwargs)
            if errors:
                # Convert string errors to structured errors
                if isinstance(errors, list):
                    error_msg = "; ".join(errors)
                else:
                    error_msg = str(errors)
                raise ValidationError(
                    table=table_name,
                    rule=validator_func.__name__,
                    message=error_msg,
                )

    def _check_required_children(
        self,
        table_name: str,
        df: pl.DataFrame,
    ) -> None:
        """Check that all records have required children (bidirectional FK).

        Args:
            table_name: Name of the table being validated
            df: DataFrame being validated
        """
        if table_name not in self._required_children:
            return

        parent_col = self._unique_constraints[table_name][0]
        parent_ids = set(df[parent_col].to_list())

        for child_table, child_fk_col in self._required_children[table_name]:
            child_df = getattr(self, child_table)

            if child_df is None:
                logger.warning(
                    "Skipping required children check: child table '%s' "
                    "is None",
                    child_table,
                )
                continue

            if child_fk_col not in child_df.columns:
                logger.warning(
                    "Skipping required children check: FK column '%s' "
                    "not in '%s'",
                    child_fk_col,
                    child_table,
                )
                continue

            child_parent_ids = set(
                child_df[child_fk_col].drop_nulls().unique().to_list()
            )
            parents_without_children = parent_ids - child_parent_ids

            if parents_without_children:
                missing_list = sorted(parents_without_children)
                max_display = 10
                sample = missing_list[:max_display]
                sample_str = ", ".join(str(v) for v in sample)
                has_more = len(parents_without_children) > max_display
                ellipsis = " ..." if has_more else ""
                msg = (
                    f"Found {len(parents_without_children)} '{table_name}' "
                    f"records with no '{child_table}' children. "
                    f"Sample: {sample_str}{ellipsis}"
                )
                raise ValidationError(
                    table=table_name,
                    rule="required_children",
                    column=parent_col,
                    message=msg,
                )

    def register_validator(self, *table_names: str) -> Callable:
        """Register a custom validator on one or more tables.

        Args:
            *table_names: One or more table names to register validator on

        Returns:
            Decorator function

        Example:
            >>> @data.register_validator("tours")
            >>> def check_tours(tours: pl.DataFrame) -> list[str]:
            >>>     errors = []
            >>>     # Check logic
            >>>     return errors

            >>> @data.register_validator("tours", "linked_trips")
            >>> def check_consistency(
            >>>     tours: pl.DataFrame,
            >>>     linked_trips: pl.DataFrame
            >>> ) -> list[str]:
            >>>     # Multi-table check
            >>>     return []
        """
        if not table_names:
            msg = "Must specify at least one table name"
            raise ValueError(msg)

        def decorator(func: Callable) -> Callable:
            for table_name in table_names:
                if table_name not in self._models:
                    msg = f"Unknown table: {table_name}"
                    raise ValueError(msg)
                if table_name not in self._custom_validators:
                    self._custom_validators[table_name] = []
                self._custom_validators[table_name].append(func)
            return func

        return decorator

    def is_validated(self, table_name: str, step: str | None = None) -> bool:
        """Check if a table has been validated.

        Args:
            table_name: Name of the table to check
            step: Optional step name to check step-specific validation

        Returns:
            True if table has been validated and not modified since
        """
        status_key = (table_name, step) if step else table_name
        return self._validation_status.get(status_key, False)

    def get_validation_status(
        self,
    ) -> dict[str | tuple[str, str], bool]:
        """Get validation status for all tables and steps.

        Returns:
            Dictionary mapping table names (or table,step tuples)
            to validation status
        """
        return self._validation_status.copy()
