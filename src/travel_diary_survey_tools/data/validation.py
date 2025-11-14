"""Data validation functions for travel survey data using Pydantic models."""
import logging
import time
from dataclasses import dataclass, field

import polars as pl
from pydantic import BaseModel

from .models import (
    DayModel,
    HouseholdModel,
    LinkedTripModel,
    PersonModel,
    TourModel,
    UnlinkedTripModel,
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
        """Validate a specific table using its Pydantic model."""
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

        model = self._models[table_name]
        total_rows = len(df)
        start_time = time.time()
        last_update_time = start_time

        logger.info(
            "Validating table '%s' (%s rows)",
            table_name,
            f"{total_rows:,}"
        )

        for i, row in enumerate(df.iter_rows(named=True)):
            try:
                model(**row)
            except Exception as e:
                msg = (
                    f"Validation failed for '{table_name}' "
                    f"at row {i}: {e}"
                )
                raise ValueError(msg) from e

            # Progress updates for large datasets
            progress_threshold = 100_000
            update_interval = 5
            if total_rows > progress_threshold:
                current_time = time.time()
                if (
                    current_time - last_update_time >= update_interval or
                    (i + 1) % (total_rows // 4) == 0
                ):
                    percent_done = (i + 1) / total_rows * 100
                    logger.info(
                        "Validation progress for '%s': %.1f%% "
                        "(%s/%s rows)",
                        table_name,
                        percent_done,
                        i + 1,
                        total_rows,
                    )
                    last_update_time = current_time

        # Mark as validated
        self._validation_status[table_name] = True
        elapsed = time.time() - start_time
        logger.info(
            "✓ Table '%s' validated successfully in %.2fs",
            table_name,
            elapsed
        )

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
