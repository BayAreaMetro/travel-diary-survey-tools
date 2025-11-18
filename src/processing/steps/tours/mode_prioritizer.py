"""Mode priority calculation for tour attributes.

This module provides functionality to calculate priority values for trip
modes based on the configured mode hierarchy.
"""

import polars as pl

from .configs import TourConfig


class ModePrioritizer:
    """Calculates priority values for tour mode assignment."""

    def __init__(self, config: TourConfig) -> None:
        """Initialize prioritizer with tour configuration.

        Args:
            config: TourConfig with mode hierarchy
        """
        self.config = config
        self._mode_hierarchy = self._build_mode_hierarchy()

    def _build_mode_hierarchy(self) -> dict:
        """Build mode hierarchy mapping from config.

        Converts the ordered list of modes into a dictionary where
        the index represents priority (higher index = higher priority).

        Returns:
            Dictionary mapping mode types to priority integers
        """
        return {
            mode: idx for idx, mode in enumerate(self.config.mode_hierarchy)
        }

    def add_priority_column(
        self, df: pl.DataFrame, alias: str = "mode_priority"
    ) -> pl.DataFrame:
        """Add mode priority column to dataframe.

        Args:
            df: DataFrame with mode_type column
            alias: Column name for the priority values

        Returns:
            DataFrame with added mode_priority column
        """
        mode_hierarchy = self._mode_hierarchy

        return df.with_columns([
            pl.col("mode_type")
            .map_elements(
                lambda x: mode_hierarchy.get(x, 0), return_dtype=pl.Int32
            )
            .alias(alias)
        ])

    @property
    def mode_hierarchy(self) -> dict:
        """Get the mode hierarchy mapping."""
        return self._mode_hierarchy
