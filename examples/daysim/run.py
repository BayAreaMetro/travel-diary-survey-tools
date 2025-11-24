"""Runner script for the BATS 2023 DaySim processing pipeline."""

import logging
import os
from pathlib import Path

import geopandas as gpd
import polars as pl

from data_canon.models.survey import PersonDayModel
from processing.decoration import step
from processing.pipeline import Pipeline
from processing.utils.helpers import add_time_columns, expr_haversine

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

os.system(r"net use M: \\models.ad.mtc.ca.gov\data\models")  # noqa: S605, S607
os.system(r"net use X: \\model3-a\Model3A-Share")  # noqa: S605, S607

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
def custom_cleaning(
    # households: pl.DataFrame,
    persons: pl.DataFrame,
    days: pl.DataFrame,
    unlinked_trips: pl.DataFrame
    ) -> dict[str, pl.DataFrame]:
    """Custom cleaning steps go here, not in the main pipeline."""
    # CLEANUP UNLINKED TRIPS =================================
    # Much wow...
    unlinked_trips = unlinked_trips.rename({"arrive_second": "arrive_seconds"})

    # Add time columns if missing
    unlinked_trips = add_time_columns(unlinked_trips)

    # "Correct" trips when depart_time > arrive_time, flip them
    # including the separate hours, minutes, seconds columns
    # Create a swap condition to reuse
    swap_condition = pl.col("depart_time") > pl.col("arrive_time")
    # Swap depart/arrive columns when depart_time > arrive_time
    swap_cols = [
        ("depart_time", "arrive_time"),
        ("depart_hour", "arrive_hour"),
        ("depart_minute", "arrive_minute"),
        ("depart_seconds", "arrive_seconds"),
    ]

    unlinked_trips = unlinked_trips.with_columns(
        [
            pl.when(swap_condition).then(pl.col(b)).otherwise(pl.col(a)).alias(a)
            for a, b in swap_cols
        ] +
        [
            pl.when(swap_condition).then(pl.col(a)).otherwise(pl.col(b)).alias(b)
            for a, b in swap_cols
        ]
    )

    # Replace any -1 value in *_purpose columns with missing code
    unlinked_trips = unlinked_trips.with_columns(
        [
            pl.when(pl.col(col_name) == -1)
            .then(996)
            .otherwise(pl.col(col_name))
            .alias(col_name)
            for col_name in [
                "o_purpose",
                "d_purpose",
                "o_purpose_category",
                "d_purpose_category",
            ]
        ]
    )

    # If distance is null, recalculate it from lat/lon
    unlinked_trips = unlinked_trips.with_columns(
        pl.when(pl.col("distance_meters").is_null())
            .then(
                expr_haversine(
                    pl.col("o_lon"),
                    pl.col("o_lat"),
                    pl.col("d_lon"),
                    pl.col("d_lat"),
                )
            )
            .otherwise(pl.col("distance_meters"))
            .alias("distance_meters")
    )

    # If duration_minutes is null, recalculate it from depart/arrive times
    unlinked_trips = unlinked_trips.with_columns(
        pl.when(pl.col("duration_minutes").is_null())
            .then(
                (pl.col("arrive_time") - pl.col("depart_time"))
                .dt.total_minutes()
            )
            .otherwise(pl.col("duration_minutes"))
            .alias("duration_minutes")
    )

    # ADD DAYS FOR PERSONS WITHOUT DAYS =================================
    # Find persons without days
    persons_without_days = persons.filter(
        ~pl.col("person_id").is_in(days["person_id"].unique().implode())
    )

    # Get travel_dow from other household members' days
    days_for_dow = (
        days
        .select(["hh_id", "travel_dow"])
        .filter(pl.col("hh_id").is_in(persons_without_days["hh_id"].unique().implode()))
        .unique()
    )

    # Create a default day for each person without days
    dummy_days = (
        persons_without_days
        .join(days_for_dow, on="hh_id", how="left")
        .with_columns(
            # Construct default day_id (person_id * 100 + travel_dow)
            (pl.col("person_id") * 100 + pl.col("travel_dow")).alias("day_id")
        )
        .select(PersonDayModel.model_json_schema().get("properties").keys())
    )
    # Add dummy days to days dataframe
    days = pl.concat([days, dummy_days], how="diagonal")

    return {"unlinked_trips": unlinked_trips, "days": days}

@step()
def custom_add_taz_ids(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    linked_trips: pl.DataFrame,
    taz_shapefile: gpd.GeoDataFrame,
) -> dict:
    """Custom step to add TAZ IDs based on locations."""\
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
                df[lon_col].to_list(),
                df[lat_col].to_list()
            ),
            crs="EPSG:4326"
        )

        # Set index TAZ_ID and geometry only for spatial join
        shp = shp.loc[:, ["TAZ_ID", "geometry"]].set_index("TAZ_ID")

        # Spatial join to find TAZ containing each point
        gdf_joined = gpd.sjoin(
            gdf,
            shp,
            how="left",
            predicate="within"
        )
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
    households = households.with_columns(
        pl.col("home_taz").alias("home_maz")
    )
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
        "linked_trips": linked_trips
    }



def custom_postprocessing(
    households_daysim: pl.DataFrame,
    persons_daysim: pl.DataFrame,
    days_daysim: pl.DataFrame,
    linked_trips_daysim: pl.DataFrame,
    tours_daysim: pl.DataFrame,
    ) -> dict[str, pl.DataFrame]:
    """Custom post-processing steps go here, not in the main pipeline."""
    return {
        "households_daysim": households_daysim,
        "persons_daysim": persons_daysim,
        "days_daysim": days_daysim,
        "linked_trips_daysim": linked_trips_daysim,
        "tours_daysim": tours_daysim,
    }


# Set up custom steps dictionary ----------------------------------
custom_steps = {
    "custom_cleaning": custom_cleaning,
    "custom_add_taz_ids": custom_add_taz_ids,
    "custom_postprocessing": custom_postprocessing,
}


# ---------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("Starting BATS 2023 DaySim Processing Pipeline")

    pipeline = Pipeline(config_path=CONFIG_PATH, custom_steps=custom_steps)
    result = pipeline.run()

    logger.info("Pipeline finished successfully.")
