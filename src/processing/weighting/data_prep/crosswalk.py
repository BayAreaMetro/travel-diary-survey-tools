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
import plotly.graph_objects as go
import polars as pl
from pydantic import BaseModel, field_validator
from shapely.ops import unary_union

from processing.weighting.data_prep.census_geo import get_block_gdf, get_puma_gdf
from utils.crosswalk import build_crosswalk

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
    resolution: int = 100
    min_allocation: float = 0.0

    @field_validator("resolution")
    @classmethod
    def _positive_resolution(cls, v: int) -> int:
        if v <= 0:
            msg = f"Resolution must be positive, got {v}"
            raise ValueError(msg)
        return v

    @field_validator("min_allocation")
    @classmethod
    def _valid_min_allocation(cls, v: float) -> float:
        if not 0 <= v < 1:
            msg = f"min_allocation must be in [0, 1), got {v}"
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
    downstream use.  The crosswalk uses ``ctrl_geoid`` as the target
    zone column name.
    """

    def __init__(
        self,
        config: GeographyConfig,
        state_fips: str,
        pums_year: int,
        cache_dir: Path | None = None,
    ) -> None:
        """Build crosswalk from *config* and Census geographies."""
        logger.info("Initializing PumaCrosswalk with config: %s", config)
        # -- target zones -------------------------------------------------
        self.target_gdf = _load_target_zones(
            config.target_zones.file,
            config.target_zones.id_field,
        )

        # -- Census geographies (pygris handles download & caching) -------
        puma_gdf = get_puma_gdf(state_fips, pums_year, cache_dir=cache_dir)
        block_gdf = get_block_gdf(state_fips, pums_year, cache_dir=cache_dir)

        # -- clip to study area -------------------------------------------
        logger.info("Clipping Census geographies to target zone extent")
        study_crs = puma_gdf.crs or "EPSG:4326"
        study_area = unary_union(self.target_gdf.to_crs(study_crs).geometry)

        self.block_gdf = block_gdf[block_gdf.intersects(study_area)].copy()
        self.puma_gdf = puma_gdf[puma_gdf.intersects(study_area)].copy()

        self.puma_ids: list[str] = sorted(self.puma_gdf["puma_id"].unique().tolist())

        if not self.puma_ids:
            msg = "No PUMAs overlap the target zone geometry."
            raise ValueError(msg)
        logger.info(
            "Overlapping PUMAs: %d (number may be high due to some may just be touching)",
            len(self.puma_ids),
        )

        # -- rasterize & cross-tabulate -----------------------------------
        self.crosswalk_df = build_crosswalk(
            source_gdf=self.puma_gdf,
            target_gdf=self.target_gdf,
            weight_gdf=self.block_gdf,
            source_id_col="puma_id",
            target_id_col="ctrl_geoid",
            weight_col="pop20",
            resolution=config.resolution,
            min_allocation=config.min_allocation,
        ).rename({"source_id": "puma_id", "target_id": "ctrl_geoid"})

    # -- public methods ---------------------------------------------------

    def assign_households(
        self,
        households: pl.DataFrame,
        lon_col: str = "home_lon",
        lat_col: str = "home_lat",
    ) -> pl.DataFrame:
        """Point-in-polygon assignment of households to target zones.

        Returns *households* with a ``ctrl_geoid`` column added (null
        for points outside all zones).
        """
        points = gpd.GeoDataFrame(
            {"hh_id": households["hh_id"].to_list()},
            geometry=gpd.points_from_xy(
                households[lon_col].to_list(),
                households[lat_col].to_list(),
            ),
            crs="EPSG:4326",
        )
        target = self.target_gdf.to_crs("EPSG:4326")[["ctrl_geoid", "geometry"]]
        joined = gpd.sjoin(points, target, how="left", predicate="within")
        joined = joined.drop(columns=["geometry", "index_right"])

        result_pl = pl.from_pandas(joined[["hh_id", "ctrl_geoid"]]).unique(
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
        times ``allocation_weight``) and adds ``ctrl_geoid`` to both
        frames.
        """
        xw = self.crosswalk_df.select("puma_id", "ctrl_geoid", "allocation_weight")

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

    def plot_crosswalk(self, output_dir: Path) -> None:
        """Write an interactive HTML map of the crosswalk to *output_dir*/.

        Layers:
        - PUMA boundaries (dashed grey) — full extent
        - Study area outline (bold black)
        - Target zones (solid border, transparent fill) with tooltip
          showing PUMA allocation weights from the crosswalk.
        """
        puma_4326 = self.puma_gdf.to_crs("EPSG:4326")
        target_4326 = self.target_gdf.to_crs("EPSG:4326")
        study_boundary = target_4326.dissolve()

        # Build tooltip per target zone showing allocation weights
        xw = self.crosswalk_df.select(
            "puma_id",
            "ctrl_geoid",
            "population",
            "allocation_weight",
        )
        tooltips: dict[str, str] = {}
        for geo_id in target_4326["ctrl_geoid"]:
            rows = xw.filter(pl.col("ctrl_geoid") == geo_id).sort(
                "allocation_weight",
                descending=True,
            )
            if rows.is_empty():
                tooltips[geo_id] = f"Zone {geo_id}: no PUMA overlap"
                continue
            zone_pop = rows["population"].sum()
            lines = [f"Zone {geo_id} (BG 2020 person pop {zone_pop:,.0f})"]
            for r in rows.iter_rows(named=True):
                aw = r["allocation_weight"] * 100
                # Drop slivers below 0.01% allocation to reduce tooltip noise (outer joins)
                if aw < 0.01:  # noqa: PLR2004
                    continue
                lines.append(f"  PUMA {r['puma_id']}: {aw:.2f}%")
            tooltips[geo_id] = "<br>".join(lines)
        target_4326 = target_4326.copy()
        target_4326["tooltip"] = target_4326["ctrl_geoid"].map(tooltips)

        fig = go.Figure()

        # PUMAs -- full boundaries, outline only
        fig.add_trace(
            go.Choroplethmap(
                geojson=puma_4326.__geo_interface__,
                locations=puma_4326.index,
                z=[0] * len(puma_4326),
                colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(0,0,0,0)"]],
                marker_line_color="rgba(100,100,100,0.7)",
                marker_line_width=1,
                showscale=False,
                hoverinfo="skip",
            )
        )

        # Study area -- bold black outline
        fig.add_trace(
            go.Choroplethmap(
                geojson=study_boundary.__geo_interface__,
                locations=study_boundary.index,
                z=[0] * len(study_boundary),
                colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(0,0,0,0)"]],
                marker_line_color="black",
                marker_line_width=3,
                showscale=False,
                hoverinfo="skip",
            )
        )

        # Target zones -- solid border, transparent blue fill, tooltip
        fig.add_trace(
            go.Choroplethmap(
                geojson=target_4326.__geo_interface__,
                locations=target_4326.index,
                z=[0] * len(target_4326),
                colorscale=[[0, "rgba(70,130,180,0.15)"], [1, "rgba(70,130,180,0.15)"]],
                marker_line_color="steelblue",
                marker_line_width=2,
                showscale=False,
                text=target_4326["tooltip"],
                hoverinfo="text",
            )
        )

        # PUMA labels at centroids (grey)
        puma_centroids = puma_4326.to_crs("EPSG:5070").centroid.to_crs("EPSG:4326")
        fig.add_trace(
            go.Scattermap(
                lon=[p.x for p in puma_centroids],
                lat=[p.y for p in puma_centroids],
                mode="text",
                text=[f"PUMA {pid}" for pid in puma_4326["puma_id"]],
                textfont={"size": 10, "color": "gray"},
                hoverinfo="skip",
                showlegend=False,
            )
        )

        # Zone labels at centroids (blue)
        zone_centroids = target_4326.to_crs("EPSG:5070").centroid.to_crs("EPSG:4326")
        fig.add_trace(
            go.Scattermap(
                lon=[p.x for p in zone_centroids],
                lat=[p.y for p in zone_centroids],
                mode="text",
                text=[f"Zone {gid}" for gid in target_4326["ctrl_geoid"]],
                textfont={"size": 12, "color": "steelblue"},
                hoverinfo="skip",
                showlegend=False,
            )
        )

        # Layout -- use PUMA bounds so full boundaries are visible
        bounds = puma_4326.total_bounds
        fig.update_layout(
            map={
                "style": "carto-positron",
                "center": {"lat": (bounds[1] + bounds[3]) / 2, "lon": (bounds[0] + bounds[2]) / 2},
                "zoom": 8,
            },
            margin={"l": 0, "r": 0, "t": 30, "b": 0},
            title="Crosswalk: PUMA to Target Zones",
        )

        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        html_path = out / "crosswalk_map.html"
        fig.write_html(str(html_path))
        logger.info("Crosswalk map written to %s", html_path)


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
        gdf["ctrl_geoid"] = "1"
    else:
        if id_field not in gdf.columns:
            msg = f"id_field {id_field!r} not found. Available: {list(gdf.columns)}"
            raise ValueError(msg)
        gdf = gdf.rename(columns={id_field: "ctrl_geoid"})
        gdf["ctrl_geoid"] = gdf["ctrl_geoid"].astype(str)
    return gdf[["ctrl_geoid", "geometry"]].reset_index(drop=True)
