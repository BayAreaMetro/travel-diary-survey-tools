"""Runner script for the BATS 2023 DaySim processing pipeline."""

import logging
import os
from pathlib import Path

import geopandas as gpd
import polars as pl

from pipeline.decoration import step
from pipeline.pipeline import Pipeline
from processing import (
    extract_tours,
    format_daysim,
    link_trips,
    load_data,
    write_data,
)
from processing.cleaning.clean_bats_2023 import clean_2023_bats

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

logger = logging.getLogger(__name__)

# For MTC network drives that seem to keep unmapping within python VM sessions
# Check if network drives are mapped; if not, map them
drives = {
    "M:": r"\\models.ad.mtc.ca.gov\data\models",
    "X:": r"\\model3-a\Model3A-Share",
}

for drive, path in drives.items():
    if not Path(drive).exists():
        logger.info("Mapping network drive %s to %s", drive, path)
        os.system(f"net use {drive} {path}")  # noqa: S605

# Path to the YAML config file you provided
CONFIG_PATH = Path(__file__).parent / "config_daysim.yaml"

# ---------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# Optional: project-specific custom step functions
# You can define or import them here if needed
@step()
def custom_add_taz_ids(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    linked_trips: pl.DataFrame,
    taz_shapefile: gpd.GeoDataFrame,
) -> dict:
    """Custom step to add TAZ IDs based on locations."""
    # Rename TAZ1454 to TAZ_ID for clarity
    taz_shapefile = taz_shapefile.rename(columns={"TAZ1454": "TAZ_ID"})

    # Helper function to add TAZ ID based on lon/lat columns
    def add_taz_to_dataframe(
        df: pl.DataFrame,
        shp: gpd.GeoDataFrame,
        lon_col: str,
        lat_col: str,
        taz_col_name: str,
    ) -> pl.DataFrame:
        """Add TAZ ID to dataframe based on lon/lat coordinates."""
        gdf = gpd.GeoDataFrame(
            df.to_pandas(),
            geometry=gpd.points_from_xy(
                df[lon_col].to_list(), df[lat_col].to_list()
            ),
            crs="EPSG:4326",
        )

        # Set index TAZ_ID and geometry only for spatial join
        shp = shp.loc[:, ["TAZ_ID", "geometry"]].set_index("TAZ_ID")

        # Spatial join to find TAZ containing each point
        gdf_joined = gpd.sjoin(gdf, shp, how="left", predicate="within")
        gdf_joined = gdf_joined.rename(columns={"TAZ_ID": taz_col_name})
        gdf_joined = gdf_joined.drop(columns="geometry")

        return pl.from_pandas(gdf_joined)

    # Get home_taz for households
    households = add_taz_to_dataframe(
        households,
        taz_shapefile,
        lon_col="home_lon",
        lat_col="home_lat",
        taz_col_name="home_taz",
    )

    # Get work_taz and school_taz for persons
    persons = add_taz_to_dataframe(
        persons,
        taz_shapefile,
        lon_col="work_lon",
        lat_col="work_lat",
        taz_col_name="work_taz",
    )

    persons = add_taz_to_dataframe(
        persons,
        taz_shapefile,
        lon_col="school_lon",
        lat_col="school_lat",
        taz_col_name="school_taz",
    )

    # Get o_taz and d_taz for linked trips
    linked_trips = add_taz_to_dataframe(
        linked_trips,
        taz_shapefile,
        lon_col="o_lon",
        lat_col="o_lat",
        taz_col_name="o_taz",
    )

    linked_trips = add_taz_to_dataframe(
        linked_trips,
        taz_shapefile,
        lon_col="d_lon",
        lat_col="d_lat",
        taz_col_name="d_taz",
    )

    # SF MTC Has only TAZ, so we spoof MAZ from TAZ
    households = households.with_columns(pl.col("home_taz").alias("home_maz"))
    persons = persons.with_columns(
        pl.col("work_taz").alias("work_maz"),
        pl.col("school_taz").alias("school_maz"),
    )
    linked_trips = linked_trips.with_columns(
        pl.col("o_taz").alias("o_maz"),
        pl.col("d_taz").alias("d_maz"),
    )

    return {
        "households": households,
        "persons": persons,
        "linked_trips": linked_trips,
    }


# Set up custom steps dictionary ----------------------------------
processing_steps = [
    load_data,
    clean_2023_bats,
    custom_add_taz_ids,
    link_trips,
    extract_tours,
    format_daysim,
    write_data,
]


# ---------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("Starting BATS 2023 DaySim Processing Pipeline")

    pipeline = Pipeline(
        config_path=CONFIG_PATH,
        steps=processing_steps,
        caching=True,
    )
    result = pipeline.run()

    logger.info("Pipeline finished successfully.")
