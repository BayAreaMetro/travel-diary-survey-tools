"""Location classification for tour building.

This module provides functionality to classify trip origins and destinations
by their location type (home, work, school, other) based on distance thresholds
and person-specific location data.
"""

import logging

import polars as pl

from data_canon.codebook.generic import LocationType
from processing.utils import expr_haversine

from .configs import TourConfig

logger = logging.getLogger(__name__)


class LocationClassifier:
    """Classifies trip locations based on person location data."""

    def __init__(
        self,
        config: TourConfig,
        person_locations: pl.DataFrame
        ) -> None:
        """Initialize classifier with config and cached person locations.

        Args:
            config: TourConfig with distance thresholds
            person_locations: DataFrame with person location data
                             (person_id, home_lat/lon, work_lat/lon,
                              school_lat/lon, person_category)
        """
        self.config = config
        self.person_locations = person_locations

    def classify_trip_locations(
        self, linked_trips: pl.DataFrame
    ) -> pl.DataFrame:
        """Classify trip origins and destinations by location type.

        Args:
            linked_trips: Trip data with o_lat, o_lon, d_lat, d_lon

        Returns:
            Trips with added columns:
            - o_is_home, o_is_work, o_is_school (bool flags)
            - d_is_home, d_is_work, d_is_school (bool flags)
            - o_location_type, d_location_type (LocationType enum)
        """
        logger.info("Classifying trip locations...")

        # Join person locations
        linked_trips = linked_trips.join(
            self.person_locations, on="person_id", how="left"
        )

        # Calculate distances to all known locations
        linked_trips = self._add_distance_columns(linked_trips)

        # Create boolean flags for location matches
        linked_trips = self._add_location_flags(linked_trips)

        # Determine primary location type for each trip end
        linked_trips = self._add_location_types(linked_trips)

        # Clean up temporary columns
        linked_trips = self._drop_temp_columns(linked_trips)

        logger.info("Location classification complete")
        return linked_trips

    def _add_distance_columns(
        self, df: pl.DataFrame
    ) -> pl.DataFrame:
        """Calculate haversine distances to known locations."""
        distance_cols = [
            expr_haversine(
                pl.col(f"{end}_lat"),
                pl.col(f"{end}_lon"),
                pl.col(f"{loc}_lat"),
                pl.col(f"{loc}_lon"),
            ).alias(f"{end}_dist_to_{loc}_meters")
            for loc in ["home", "work", "school"]
            for end in ["o", "d"]
        ]
        return df.with_columns(distance_cols)

    def _add_location_flags(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create boolean flags for location matches."""
        location_configs = {
            "home": (LocationType.HOME, None),
            "work": (LocationType.WORK, "work_lat"),
            "school": (LocationType.SCHOOL, "school_lat"),
        }

        flag_cols = []
        for loc, (loc_type, null_check) in location_configs.items():
            for end in ["o", "d"]:
                check = self._is_within_threshold(
                    f"{end}_dist_to_{loc}_meters", loc_type
                )
                if null_check:
                    check = check & pl.col(null_check).is_not_null()
                flag_cols.append(check.alias(f"{end}_is_{loc}"))

        return df.with_columns(flag_cols)

    def _add_location_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """Determine primary location type based on priority."""

        def build_location_expr(prefix: str) -> pl.Expr:
            """Build expression for location type with priority order."""
            expr = pl.lit(LocationType.OTHER)
            # Reverse priority order: HOME > WORK > SCHOOL > OTHER
            for loc_type in [
                LocationType.SCHOOL,
                LocationType.WORK,
                LocationType.HOME,
            ]:
                col_name = f"{prefix}_is_{loc_type.name.lower()}"
                expr = pl.when(pl.col(col_name)).then(
                    pl.lit(loc_type)
                ).otherwise(expr)
            return expr

        return df.with_columns([
            build_location_expr("o").alias("o_location_type"),
            build_location_expr("d").alias("d_location_type"),
        ])

    def _is_within_threshold(
        self, distance_col: str, location_type: LocationType
    ) -> pl.Expr:
        """Check if distance is within threshold for location type."""
        threshold = self.config.distance_thresholds[location_type]
        return pl.col(distance_col) <= threshold

    def _drop_temp_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Drop temporary location and distance columns."""
        temp_cols = [
            "home_lat", "home_lon", "work_lat", "work_lon",
            "school_lat", "school_lon", "person_type",
        ]
        drop_cols = [
            c for c in df.columns
            if "dist_to" in c or c in temp_cols
        ]
        return df.drop(drop_cols)
