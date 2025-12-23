"""A custom helper step to add TAZ/MAZ IDs based on locations for BATS 2023."""

import geopandas as gpd
import polars as pl

from pipeline.decoration import step


# Helper function to add zone ID based on lon/lat columns
def join_zone_from_latlon(
    df: pl.DataFrame,
    shp: gpd.GeoDataFrame,
    lon_col: str,
    lat_col: str,
    zone_col_name: str,
    zone_id_field: str = "TAZ_ID",
) -> pl.DataFrame:
    """Add TAZ/MAZ zone ID to dataframe based on lon/lat coordinates."""
    # Drop the zone_col_name if it already exists
    if zone_col_name in df.columns:
        df = df.drop(zone_col_name)

    # Extract just the coordinates for spatial join to avoid type corruption
    coords_df = df.select([lon_col, lat_col])

    # Convert only coordinates to GeoDataFrame
    gdf = gpd.GeoDataFrame(
        coords_df.to_pandas(),
        geometry=gpd.points_from_xy(
            coords_df[lon_col].to_list(), coords_df[lat_col].to_list()
        ),
        crs="EPSG:4326",
    )

    # Set index zone_id and geometry only for spatial join
    shp = shp.loc[:, [zone_id_field, "geometry"]].set_index(zone_id_field)

    # Spatial join to find zone containing each point
    gdf_joined = gpd.sjoin(gdf, shp, how="left", predicate="within")

    # Extract just the zone ID column
    zone_series = pl.from_pandas(
        gdf_joined[[zone_id_field]].reset_index(drop=True)
    )

    # Add the zone column to the original dataframe
    result = df.with_columns(zone_series[zone_id_field].alias(zone_col_name))

    return result


# Optional: project-specific custom step functions
# You can define or import them here if needed
@step()
def custom_add_taz_ids(  # noqa: PLR0913
    households: pl.DataFrame,
    persons: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
    taz_shapefile: gpd.GeoDataFrame,
    maz_shapefile: gpd.GeoDataFrame | None = None,
    taz_field_name: str = "TAZ1454",
    maz_field_name: str = "MAZ_ID",
) -> dict:
    """Custom step to add TAZ and MAZ IDs based on locations."""
    # Rename source field to TAZ_ID for consistency
    taz_shapefile = taz_shapefile.rename(columns={taz_field_name: "TAZ_ID"})

    # Rename MAZ field if MAZ shapefile provided
    if maz_shapefile is not None:
        maz_shapefile = maz_shapefile.rename(columns={maz_field_name: "MAZ_ID"})

    # Add TAZ IDs to all dataframes
    taz_configs = [
        ("households", "home_lon", "home_lat", "home_taz"),
        ("persons", "work_lon", "work_lat", "work_taz"),
        ("persons", "school_lon", "school_lat", "school_taz"),
        ("unlinked_trips", "o_lon", "o_lat", "o_taz"),
        ("unlinked_trips", "d_lon", "d_lat", "d_taz"),
        ("linked_trips", "o_lon", "o_lat", "o_taz"),
        ("linked_trips", "d_lon", "d_lat", "d_taz"),
        ("tours", "o_lon", "o_lat", "o_taz"),
        ("tours", "d_lon", "d_lat", "d_taz"),
    ]
    results = {
        "households": households,
        "persons": persons,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "tours": tours,
    }

    for df_name, lon_col, lat_col, taz_col_name in taz_configs:
        results[df_name] = join_zone_from_latlon(
            results[df_name],
            taz_shapefile,
            lon_col=lon_col,
            lat_col=lat_col,
            zone_col_name=taz_col_name,
            zone_id_field="TAZ_ID",
        )

    # Add MAZ IDs if MAZ shapefile provided, otherwise spoof from TAZ
    if maz_shapefile is not None:
        maz_configs = [
            ("households", "home_lon", "home_lat", "home_maz"),
            ("persons", "work_lon", "work_lat", "work_maz"),
            ("persons", "school_lon", "school_lat", "school_maz"),
            ("linked_trips", "o_lon", "o_lat", "o_maz"),
            ("linked_trips", "d_lon", "d_lat", "d_maz"),
            ("tours", "o_lon", "o_lat", "o_maz"),
            ("tours", "d_lon", "d_lat", "d_maz"),
        ]

        for df_name, lon_col, lat_col, maz_col_name in maz_configs:
            results[df_name] = join_zone_from_latlon(
                results[df_name],
                maz_shapefile,
                lon_col=lon_col,
                lat_col=lat_col,
                zone_col_name=maz_col_name,
                zone_id_field="MAZ_ID",
            )
    else:
        # Spoof MAZ from TAZ if no MAZ shapefile provided
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
        results["tours"] = results["tours"].with_columns(
            pl.col("o_taz").alias("o_maz"),
            pl.col("d_taz").alias("d_maz"),
        )
    return results
