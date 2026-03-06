"""Generic population-weighted geography crosswalk utilities.

Provides :func:`build_crosswalk`, a generic function that maps *source*
polygons to *target* polygons via a *weight* polygon layer (e.g. Census
blocks with population counts).  The three layers are rasterized onto a
common grid and ``exactextract`` performs the zonal cross-tabulation with
sub-pixel coverage fractions.

See ``src/utils/CROSSWALK.md`` for the mathematical formulation.

Usage::

    from utils.crosswalk import build_crosswalk

    xw = build_crosswalk(
        source_gdf=pumas, target_gdf=counties, weight_gdf=blocks,
        source_id_col="PUMACE20", target_id_col="COUNTYFP",
        weight_col="pop20", resolution=250,
    )
"""

import contextlib
import logging
import os
import tempfile
from collections.abc import Generator
from pathlib import Path

import geopandas as gpd
import numpy as np
import polars as pl
import rasterio
import rasterio.features
import rasterio.transform
from exactextract import exact_extract
from rasterio.crs import CRS
from rasterio.transform import from_bounds
from shapely.ops import unary_union

logger = logging.getLogger(__name__)

_ALBERS_CRS = CRS.from_epsg(5070)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def build_crosswalk(
    source_gdf: gpd.GeoDataFrame,
    target_gdf: gpd.GeoDataFrame,
    weight_gdf: gpd.GeoDataFrame,
    *,
    source_id_col: str = "source_id",
    target_id_col: str = "target_id",
    weight_col: str = "pop20",
    resolution: int = 250,
) -> pl.DataFrame:
    """Build a population-weighted crosswalk between two polygon layers.

    Parameters
    ----------
    source_gdf:
        Source zone polygons (e.g. PUMAs).  Must contain *source_id_col*.
    target_gdf:
        Target zone polygons (e.g. counties, TAZs).  Must contain
        *target_id_col*.
    weight_gdf:
        Weight polygons carrying a numeric attribute (e.g. Census blocks
        with ``pop20``).  Must contain *weight_col* and geometry.
    source_id_col:
        Column in *source_gdf* identifying source zones.
    target_id_col:
        Column in *target_gdf* identifying target zones.
    weight_col:
        Numeric column in *weight_gdf* used as the allocation weight
        (typically population).
    resolution:
        Raster cell size in metres (EPSG:5070).

    Returns:
    -------
    pl.DataFrame
        Columns ``[source_id, target_id, population, allocation_weight]``.
        ``allocation_weight`` sums to 1.0 for each ``source_id``.
    """
    # Validate required columns
    for gdf, col, label in [
        (source_gdf, source_id_col, "source_gdf"),
        (target_gdf, target_id_col, "target_gdf"),
        (weight_gdf, weight_col, "weight_gdf"),
    ]:
        if col not in gdf.columns:
            msg = f"{label} missing column {col!r}. Available: {list(gdf.columns)}"
            raise ValueError(msg)

    # Project everything to equal-area CRS
    src_albers = source_gdf.to_crs(_ALBERS_CRS)
    tgt_albers = target_gdf.to_crs(_ALBERS_CRS)
    wgt_albers = weight_gdf.to_crs(_ALBERS_CRS)

    # Normalise ID columns to canonical names
    src_albers = src_albers.rename(columns={source_id_col: "source_id"})
    tgt_albers = tgt_albers.rename(columns={target_id_col: "target_id"})

    # Compute raster grid from union of source extent
    total_weight = float(wgt_albers[weight_col].sum())
    src_bounds = unary_union(src_albers.geometry).bounds
    minx, miny, maxx, maxy = src_bounds

    # Resolution heuristic warning
    cell_area = resolution * resolution
    min_zone_area = tgt_albers.geometry.area.min()
    if min_zone_area / cell_area < 50:  # noqa: PLR2004
        logger.warning(
            "Smallest zone (%.0f m²) < 50x cell area (%d m²). Consider a finer resolution.",
            min_zone_area,
            cell_area,
        )

    w = int(np.ceil((maxx - minx) / resolution))
    h = int(np.ceil((maxy - miny) / resolution))
    transform = from_bounds(
        minx,
        miny,
        minx + w * resolution,
        miny + h * resolution,
        w,
        h,
    )
    shape = (h, w)

    # Rasterize
    pop_arr = _rasterize_weights(wgt_albers, weight_col, transform, shape)
    src_arr, int_to_src = _rasterize_categorical(
        src_albers,
        "source_id",
        transform,
        shape,
    )

    # Cross-tabulate via exactextract
    df = _cross_tabulate(pop_arr, src_arr, int_to_src, transform, tgt_albers)

    # Conservation check
    raster_pop = df["population"].sum()
    pct = abs(raster_pop - total_weight) / max(total_weight, 1) * 100
    if pct > 2:  # noqa: PLR2004
        logger.warning(
            "Weight conservation: %.0f vs %.0f (%.1f%% diff)",
            raster_pop,
            total_weight,
            pct,
        )

    # Normalise to allocation weights per source zone
    df = df.with_columns(
        (pl.col("population") / pl.col("population").sum().over("source_id")).alias(
            "allocation_weight"
        ),
    )
    logger.info(
        "Crosswalk: %d rows, %d source zones -> %d target zones",
        len(df),
        df["source_id"].n_unique(),
        df["target_id"].n_unique(),
    )
    return df


# ---------------------------------------------------------------------------
# Rasterization helpers
# ---------------------------------------------------------------------------
def _rasterize_weights(
    gdf: gpd.GeoDataFrame,
    weight_col: str,
    transform: rasterio.transform.Affine,
    shape: tuple[int, int],
) -> np.ndarray:
    """Burn polygon weights into a float32 array (per-cell density).

    Each polygon's total weight is distributed evenly across the raster
    cells it covers:  ``cell_val = weight / n_cells_in_polygon``.
    """
    gdf = gdf.copy()
    gdf["_bid"] = np.arange(1, len(gdf) + 1, dtype=np.int32)

    bid_raster = np.asarray(
        rasterio.features.rasterize(
            list(zip(gdf.geometry, gdf["_bid"], strict=True)),
            out_shape=shape,
            transform=transform,
            fill=0,
            dtype="int32",
        ),
        dtype=np.int32,
    )

    # Build a density lookup: weight_i / n_cells_i for each polygon
    n = int(gdf["_bid"].max()) + 1
    raw = np.zeros(n, dtype=np.float32)
    raw[gdf["_bid"].values] = gdf[weight_col].values.astype(np.float32)
    counts = np.bincount(bid_raster.ravel(), minlength=n).astype(np.float32)
    counts[counts == 0] = 1  # avoid div-by-zero for unused IDs

    density = raw / counts
    weight_raster = np.where(bid_raster > 0, density[bid_raster], 0.0).astype(np.float32)
    logger.info(
        "Weight raster (%s): %dx%d, total=%.0f",
        weight_col,
        shape[1],
        shape[0],
        weight_raster.sum(),
    )
    return weight_raster


def _rasterize_categorical(
    gdf: gpd.GeoDataFrame,
    id_col: str,
    transform: rasterio.transform.Affine,
    shape: tuple[int, int],
) -> tuple[np.ndarray, dict[int, str]]:
    """Burn polygon zones into an int32 array with an ID lookup dict."""
    gdf = gdf.copy()
    unique_ids = sorted(gdf[id_col].unique())
    id_to_int = {v: i + 1 for i, v in enumerate(unique_ids)}
    gdf["_zint"] = gdf[id_col].map(id_to_int).astype(np.int32)

    raster = np.asarray(
        rasterio.features.rasterize(
            list(zip(gdf.geometry, gdf["_zint"], strict=True)),
            out_shape=shape,
            transform=transform,
            fill=0,
            dtype="int32",
        ),
        dtype=np.int32,
    )

    int_to_id = {v: k for k, v in id_to_int.items()}
    logger.info("%s raster: %d zones, %d cells", id_col, len(unique_ids), int((raster > 0).sum()))
    return raster, int_to_id


def _cross_tabulate(
    weight_arr: np.ndarray,
    source_arr: np.ndarray,
    int_to_source: dict[int, str],
    transform: rasterio.transform.Affine,
    target_gdf: gpd.GeoDataFrame,
) -> pl.DataFrame:
    """Cross-tabulate weights by source zone x target zone via exactextract.

    Both arrays are written as bands in a single GeoTIFF so that
    ``exactextract`` returns aligned per-cell arrays.  Per-feature
    accumulation uses ``np.bincount`` for a single vectorized pass.

    Returns columns ``[source_id, target_id, population]``.
    """
    with _temp_tif(weight_arr, source_arr, transform=transform, dtypes=["float32", "int32"]) as tif:
        ee_features: list[dict] = exact_extract(
            tif,
            target_gdf,
            ["values", "coverage"],
            include_cols=["target_id"],
        )  # pyright: ignore[reportAssignmentType]

    # band_1 = weight values / coverage, band_2 = source-zone integer labels
    n_src = max(int_to_source) + 1
    rows: list[dict[str, object]] = []
    for feat in ee_features:
        props = feat["properties"]
        wt_vals = np.asarray(props["band_1_values"], dtype=np.float64)
        src_vals = np.asarray(props["band_2_values"], dtype=np.int32)
        coverage = np.asarray(props["band_1_coverage"], dtype=np.float64)

        mask = src_vals > 0
        if not mask.any():
            continue
        totals = np.bincount(
            src_vals[mask],
            weights=(wt_vals * coverage)[mask],
            minlength=n_src,
        )

        tid = str(props["target_id"])
        for src_int, src_id in int_to_source.items():
            if totals[src_int] > 0:
                rows.append(
                    {
                        "source_id": src_id,
                        "target_id": tid,
                        "population": float(totals[src_int]),
                    }
                )

    if not rows:
        msg = "Cross-tabulation produced no results. Check source/target/weight overlap."
        raise ValueError(msg)

    return pl.DataFrame(rows).with_columns(
        pl.col("source_id").cast(pl.Utf8),
        pl.col("target_id").cast(pl.Utf8),
        pl.col("population").cast(pl.Float64),
    )


@contextlib.contextmanager
def _temp_tif(
    *arrays: np.ndarray,
    transform: rasterio.transform.Affine,
    dtypes: list[str] | None = None,
) -> Generator[str, None, None]:
    """Context manager: write bands to a temp GeoTIFF, delete on exit.

    A single multi-band file ensures ``exactextract`` returns aligned
    per-cell arrays for every band.
    """
    count = len(arrays)
    if dtypes is None:
        dtypes = ["float32"] * count
    fd, path = tempfile.mkstemp(suffix=".tif", prefix="crosswalk_")
    os.close(fd)
    try:
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            height=arrays[0].shape[0],
            width=arrays[0].shape[1],
            count=count,
            dtype=dtypes[0],
            crs=_ALBERS_CRS,
            transform=transform,
        ) as dst:
            for i, (arr, dt) in enumerate(zip(arrays, dtypes, strict=True), start=1):
                dst.write(arr.astype(dt), i)
        yield path
    finally:
        with contextlib.suppress(OSError, PermissionError):
            Path(path).unlink(missing_ok=True)
