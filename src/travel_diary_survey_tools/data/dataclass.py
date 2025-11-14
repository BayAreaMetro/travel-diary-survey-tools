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
from .validators import (
    ValidationError,
    check_foreign_keys,
    check_unique_constraints,
    validate_rows_with_model,
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
    _validation_status: dict[str, bool] = field(default_factory=lambda: {
        "households": False,
        "persons": False,
        "days": False,
        "unlinked_trips": False,
        "linked_trips": False,
        "tours": False,
    })

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
            self._validation_status[name] = False
            logger.debug(
                "Table '%s' modified - validation status reset to False",
                name
            )

    def validate(self, table_name: str) -> None:
        """Validate a table through all validation layers.

        Runs validation in this order:
        1. Column constraints (uniqueness)
        2. Foreign key constraints
        3. Row-level Pydantic validation
        4. Custom user-registered validators

        Args:
            table_name: Name of the table to validate

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
        logger.info(
            "Validating table '%s' (%s rows)",
            table_name,
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

        # 3. Row validation
        validate_rows_with_model(
            table_name,
            df,
            self._models[table_name],
        )

        # 4. Custom validators
        self._run_custom_validators(table_name, df)

        # 5. Required children (bidirectional FK check)
        self._check_required_children(table_name, df)

        # Mark as validated
        self._validation_status[table_name] = True
        elapsed = time.time() - start_time
        logger.info(
            "✓ Table '%s' validated successfully in %.2fs",
            table_name,
            elapsed
        )

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

    def is_validated(self, table_name: str) -> bool:
        """Check if a table has been validated.

        Args:
            table_name: Name of the table to check

        Returns:
            True if table has been validated and not modified since
        """
        if table_name not in self._validation_status:
            return False
        return self._validation_status[table_name]

    def get_validation_status(self) -> dict[str, bool]:
        """Get validation status for all tables.

        Returns:
            Dictionary mapping table names to validation status
        """
        return self._validation_status.copy()
