"""PUMA-specific crosswalk wrapper.

``PumaCrosswalk`` fetches Census PUMA and block geographies and
delegates the heavy lifting to :func:`utils.crosswalk.build_crosswalk`.

See ``src/utils/CROSSWALK.md`` for the mathematical formulation.

Usage::

    xw = PumaCrosswalk(geo_cfg, state_fips="06", pums_year=2023)
    households = xw.assign_households(households)
    pums_hh, pums_per = xw.allocate_pums_weights(pums_hh, pums_per)
"""

import logging
from pathlib import Path

import geopandas as gpd
import polars as pl
from pydantic import BaseModel, field_validator
from shapely.ops import unary_union

from processing.weighting.core.census_geo import get_block_gdf, get_puma_gdf
from utils.crosswalk import _ALBERS_CRS, build_crosswalk

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config models (parsed from YAML)
# ---------------------------------------------------------------------------
class TargetZoneConfig(BaseModel):
    """Target zone polygon source."""

    file: str
    id_field: str | None = None


class GeographyConfig(BaseModel):
    """Geography block of the weighting YAML config.

    Attributes:
    ----------
    target_zones : TargetZoneConfig
        Polygon file and optional zone-ID column.
    resolution : int
        Raster cell size in metres (default 250).
    """

    target_zones: TargetZoneConfig
    resolution: int = 250

    @field_validator("resolution")
    @classmethod
    def _positive_resolution(cls, v: int) -> int:
        if v <= 0:
            msg = f"Resolution must be positive, got {v}"
            raise ValueError(msg)
        return v


# ---------------------------------------------------------------------------
# PumaCrosswalk
# ---------------------------------------------------------------------------
class PumaCrosswalk:
    """Population-weighted PUMA-to-target-zone crosswalk.

    Loads target zones and Census geographies on construction, rasterizes
    block population, and cross-tabulates to produce allocation weights.
    Exposes ``crosswalk_df``, ``puma_ids``, and ``target_gdf`` for
    downstream use.
    """

    def __init__(
        self,
        config: GeographyConfig,
        state_fips: str,
        pums_year: int,
    ) -> None:
        """Build crosswalk from *config* and Census geographies."""
        logger.info("Initializing PumaCrosswalk with config: %s", config)
        # -- target zones -------------------------------------------------
        self.target_gdf = _load_target_zones(
            config.target_zones.file,
            config.target_zones.id_field,
        )

        # -- Census geographies (pygris handles download & caching) -------
        puma_gdf = get_puma_gdf(state_fips, pums_year)
        block_gdf = get_block_gdf(state_fips, pums_year)

        # -- clip to study area -------------------------------------------
        logger.info("Clipping Census geographies to target zone extent")
        study_crs = puma_gdf.crs or "EPSG:4326"
        study_area = unary_union(self.target_gdf.to_crs(study_crs).geometry)
        puma_gdf = puma_gdf[puma_gdf.intersects(study_area)].copy()
        block_gdf = block_gdf[block_gdf.intersects(study_area)].copy()
        self.puma_ids: list[str] = sorted(puma_gdf["puma_id"].unique().tolist())
        if not self.puma_ids:
            msg = "No PUMAs overlap the target zone geometry."
            raise ValueError(msg)
        logger.info(
            "Overlapping PUMAs: %d (number may be high due to some may just be touching)",
            len(self.puma_ids),
        )

        # -- rasterize & cross-tabulate -----------------------------------
        self.crosswalk_df = self._build_crosswalk(
            puma_gdf,
            block_gdf,
            config.resolution,
        )

    # -- public methods ---------------------------------------------------

    def assign_households(
        self,
        households: pl.DataFrame,
        lon_col: str = "home_lon",
        lat_col: str = "home_lat",
    ) -> pl.DataFrame:
        """Point-in-polygon assignment of households to target zones.

        Returns *households* with a ``target_id`` column added (null for
        points outside all zones).
        """
        points = gpd.GeoDataFrame(
            {"hh_id": households["hh_id"].to_list()},
            geometry=gpd.points_from_xy(
                households[lon_col].to_list(),
                households[lat_col].to_list(),
            ),
            crs="EPSG:4326",
        )
        target = self.target_gdf.to_crs("EPSG:4326")[["target_id", "geometry"]]
        joined = gpd.sjoin(points, target, how="left", predicate="within")
        joined = joined.drop(columns=["geometry", "index_right"])

        result_pl = pl.from_pandas(joined[["hh_id", "target_id"]]).unique(
            subset=["hh_id"], keep="first"
        )
        return households.join(result_pl, on="hh_id", how="left")

    def allocate_pums_weights(
        self,
        hh_df: pl.DataFrame,
        person_df: pl.DataFrame,
        geo_col: str = "PUMA",
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Join crosswalk to PUMS and scale weights by allocation factor.

        Produces ``_xw_WGTP`` / ``_xw_PWGTP`` columns (original weight
        times ``allocation_weight``) and adds ``target_id`` to both frames.
        """
        xw = self.crosswalk_df.select("puma_id", "target_id", "allocation_weight")

        hh_xw = hh_df.join(
            xw, left_on=pl.col(geo_col).cast(pl.Utf8), right_on="puma_id", how="inner"
        ).with_columns(
            (pl.col("WGTP").cast(pl.Float64) * pl.col("allocation_weight")).alias("_xw_WGTP"),
        )
        person_xw = (
            person_df.join(hh_df.select("SERIALNO", geo_col), on="SERIALNO", how="left")
            .join(xw, left_on=pl.col(geo_col).cast(pl.Utf8), right_on="puma_id", how="inner")
            .with_columns(
                (pl.col("PWGTP").cast(pl.Float64) * pl.col("allocation_weight")).alias("_xw_PWGTP"),
            )
        )
        logger.info(
            "Crosswalk join: %d HH -> %d, %d persons -> %d",
            len(hh_df),
            len(hh_xw),
            len(person_df),
            len(person_xw),
        )
        return hh_xw, person_xw

    # -- private -----------------------------------------------------------

    def _build_crosswalk(
        self,
        puma_gdf: gpd.GeoDataFrame,
        block_gdf: gpd.GeoDataFrame,
        resolution: int,
    ) -> pl.DataFrame:
        """Clip Census data and delegate to :func:`build_crosswalk`."""
        puma_albers = puma_gdf.to_crs(_ALBERS_CRS)
        block_albers = block_gdf.to_crs(_ALBERS_CRS)

        # clip blocks to PUMA extent
        puma_bounds = unary_union(puma_albers.geometry).bounds
        block_albers = block_albers.cx[
            puma_bounds[0] : puma_bounds[2],
            puma_bounds[1] : puma_bounds[3],
        ]
        block_albers = block_albers[block_albers["pop20"] > 0].copy()
        logger.info("Blocks (pop > 0): %d", len(block_albers))

        df = build_crosswalk(
            source_gdf=puma_albers,
            target_gdf=self.target_gdf,
            weight_gdf=block_albers,
            source_id_col="puma_id",
            target_id_col="target_id",
            weight_col="pop20",
            resolution=resolution,
        )
        # Rename generic source_id back to puma_id for PUMS consumers
        return df.rename({"source_id": "puma_id"})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _load_target_zones(
    source: str | Path | gpd.GeoDataFrame,
    id_field: str | None,
) -> gpd.GeoDataFrame:
    """Load and normalise target zone polygons."""
    gdf = source.copy() if isinstance(source, gpd.GeoDataFrame) else gpd.read_file(source)
    if id_field is None:
        gdf = gdf.dissolve()
        gdf["target_id"] = "1"
    else:
        if id_field not in gdf.columns:
            msg = f"id_field {id_field!r} not found. Available: {list(gdf.columns)}"
            raise ValueError(msg)
        gdf = gdf.rename(columns={id_field: "target_id"})
        gdf["target_id"] = gdf["target_id"].astype(str)
    return gdf[["target_id", "geometry"]].reset_index(drop=True)
