"""Purpose priority calculation for tour attributes.

This module provides functionality to calculate priority values for trip
purposes based on person categories and configuration hierarchies.
"""

import polars as pl

from data_canon.codebook.persons import PersonType

from .configs import TourConfig


class PurposePrioritizer:
    """Calculates priority values for tour purpose assignment."""

    def __init__(self, config: TourConfig) -> None:
        """Initialize prioritizer with tour configuration.

        Args:
            config: TourConfig with purpose priority mappings
        """
        self.config = config
        self._priority_map = self._build_priority_mappings()

    def _build_priority_mappings(self) -> dict:
        """Build cached priority mappings from config."""
        return {
            "purpose_by_category": (
                self.config.purpose_priority_by_person_category
            ),
            "default_purpose": self.config.default_purpose_priority,
        }

    def add_priority_column(
        self, df: pl.DataFrame, alias: str = "purpose_priority"
    ) -> pl.DataFrame:
        """Add purpose priority column to dataframe.

        Args:
            df: DataFrame with d_purpose_category and person_category columns
            alias: Column name for the priority values

        Returns:
            DataFrame with added purpose_priority column
        """

        def get_purpose_priority(purpose: int, category: str) -> int:
            """Get purpose priority based on person category."""
            cat_map = self._priority_map["purpose_by_category"].get(
                category,
                self._priority_map["purpose_by_category"][
                    PersonType.OTHER
                ],
            )
            return cat_map.get(
                purpose, self._priority_map["default_purpose"]
            )

        return df.with_columns([
            pl.struct(["d_purpose_category", "person_category"])
            .map_elements(
                lambda x: get_purpose_priority(
                    x["d_purpose_category"], x["person_category"]
                ),
                return_dtype=pl.Int32,
            )
            .alias(alias)
        ])

    @property
    def priority_mappings(self) -> dict:
        """Get the cached priority mappings."""
        return self._priority_map
