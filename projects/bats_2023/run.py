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
CONFIG_PATH = Path(__file__).parent / "config.yaml"

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

    # Add TAZ IDs to all dataframes
    dataframe_configs = [
        ("households", "home_lon", "home_lat", "home_taz"),
        ("persons", "work_lon", "work_lat", "work_taz"),
        ("persons", "school_lon", "school_lat", "school_taz"),
        ("linked_trips", "o_lon", "o_lat", "o_taz"),
        ("linked_trips", "d_lon", "d_lat", "d_taz"),
    ]
    results = {
        "households": households,
        "persons": persons,
        "linked_trips": linked_trips,
    }

    for df_name, lon_col, lat_col, taz_col_name in dataframe_configs:
        results[df_name] = add_taz_to_dataframe(
            results[df_name],
            taz_shapefile,
            lon_col=lon_col,
            lat_col=lat_col,
            taz_col_name=taz_col_name,
        )

    # SF MTC Has only TAZ, so we spoof MAZ from TAZ
    results["households"] = results["households"].with_columns(
        pl.col("home_taz").alias("home_maz")
    )
    results["persons"] = results["persons"].with_columns(
        pl.col("work_taz").alias("work_maz"),
        pl.col("school_taz").alias("school_maz"),
    )
    results["linked_trips"] = results["linked_trips"].with_columns(
        pl.col("o_taz").alias("o_maz"),
        pl.col("d_taz").alias("d_maz"),
    )

    return results


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
