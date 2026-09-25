"""Putting the survey and the PUMS on the same map.

The crosswalk redistributes PUMA-level control totals onto the target zones, by
rasterizing the block populations and cross-tabulating them with sub-pixel
coverage. Synthetic geometries throughout -- no Census API or TIGER downloads.
Beside it sit the two things that go wrong at the edges: households the control
geography cannot place at all, and the Census API failing mid-run.
"""

import geopandas as gpd
import numpy as np
import polars as pl
import pytest
import requests
from rasterio.transform import from_bounds
from shapely.geometry import box

from processing.weighting.core.specs import ControlSpec, ControlTotals
from processing.weighting.data_prep import census_geo
from processing.weighting.data_prep.census_geo import puma_vintage_for_pums_year
from processing.weighting.data_prep.control_data import (
    build_control_totals,
    recode_pums_households,
    recode_pums_persons,
)
from processing.weighting.data_prep.crosswalk import (
    GeographyConfig,
    PumaCrosswalk,
    TargetZoneConfig,
    _load_target_zones,
)
from processing.weighting.diagnostics.charts import crosswalk_figure
from processing.weighting.validation.coverage import check_control_geography_coverage
from utils.crosswalk import (
    _cross_tabulate,
    _rasterize_categorical,
    _rasterize_weights,
    build_crosswalk,
)


# ---------------------------------------------------------------------------
# Fixtures: synthetic geographies
# ---------------------------------------------------------------------------
@pytest.fixture
def two_pumas() -> gpd.GeoDataFrame:
    """Two adjacent rectangular PUMAs covering a 2000m x 1000m region.

    PUMA "A" covers x=[0, 1000], y=[0, 1000]
    PUMA "B" covers x=[1000, 2000], y=[0, 1000]
    """
    return gpd.GeoDataFrame(
        {"puma_id": ["A", "B"]},
        geometry=[box(0, 0, 1000, 1000), box(1000, 0, 2000, 1000)],
        crs="EPSG:5070",
    )


@pytest.fixture
def three_target_zones() -> gpd.GeoDataFrame:
    """Three target zones that cross PUMA boundaries.

    Zone "1": x=[0, 700], y=[0, 1000]     — fully inside PUMA A
    Zone "2": x=[700, 1300], y=[0, 1000]   — straddles PUMAs A and B
    Zone "3": x=[1300, 2000], y=[0, 1000]  — fully inside PUMA B
    """
    return gpd.GeoDataFrame(
        {"target_id": ["1", "2", "3"]},
        geometry=[box(0, 0, 700, 1000), box(700, 0, 1300, 1000), box(1300, 0, 2000, 1000)],
        crs="EPSG:5070",
    )


@pytest.fixture
def uniform_blocks() -> gpd.GeoDataFrame:
    """10 uniform blocks tiling the 2000x1000 region, each 200x1000m with pop=100.

    Total population: 1000.
    """
    blocks = []
    for i in range(10):
        x0 = i * 200
        blocks.append(
            {
                "block_id": f"B{i:02d}",
                "pop20": 100,
                "geometry": box(x0, 0, x0 + 200, 1000),
            }
        )
    return gpd.GeoDataFrame(blocks, crs="EPSG:5070")


@pytest.fixture
def target_zone_file(three_target_zones, tmp_path) -> str:
    """Write target zones to a temporary shapefile."""
    path = tmp_path / "targets.shp"
    three_target_zones.to_file(path)
    return str(path)


# ---------------------------------------------------------------------------
# Tests: census_geo helpers
# ---------------------------------------------------------------------------
class TestPumaVintage:
    """Verify correct PUMA vintage is returned for a given PUMS year."""

    @pytest.mark.parametrize(
        ("pums_year", "vintage"),
        [(2023, 2020), (2022, 2020), (2021, 2010), (2012, 2010)],
    )
    def test_vintage_for_year(self, pums_year, vintage):
        """2022 is the first PUMS year on the 2020 PUMA boundaries."""
        assert puma_vintage_for_pums_year(pums_year) == vintage

    def test_a_year_before_the_2010_pumas_raises(self):
        """There is no boundary set to place a 2011 extract on."""
        with pytest.raises(ValueError, match="before 2012"):
            puma_vintage_for_pums_year(2011)


# ---------------------------------------------------------------------------
# Tests: target zone loading
# ---------------------------------------------------------------------------
class TestLoadTargetZones:
    """Tests for _load_target_zones helper function."""

    def test_with_id_field(self, three_target_zones, target_zone_file):
        """Zones load the same from a GeoDataFrame and from a file path."""
        for source in (three_target_zones, target_zone_file):
            gdf = _load_target_zones(source, "target_id")
            assert "study_geoid" in gdf.columns
            assert len(gdf) == 3

    def test_single_boundary_mode(self, three_target_zones):
        """When id_field is None, should dissolve to a single geometry with study_geoid=1."""
        gdf = _load_target_zones(three_target_zones, None)
        assert len(gdf) == 1
        assert gdf["study_geoid"].iloc[0] == "1"

    def test_missing_id_field_raises(self, three_target_zones):
        """Test that a missing ID field raises a ValueError."""
        with pytest.raises(ValueError, match="not found"):
            _load_target_zones(three_target_zones, "nonexistent_col")


# ---------------------------------------------------------------------------
# Tests: rasterization
# ---------------------------------------------------------------------------
def _grid(bounds, resolution):
    """Inline helper replacing the removed _compute_grid."""
    minx, miny, maxx, maxy = bounds
    w = int(np.ceil((maxx - minx) / resolution))
    h = int(np.ceil((maxy - miny) / resolution))
    transform = from_bounds(minx, miny, minx + w * resolution, miny + h * resolution, w, h)
    return transform, (h, w)


class TestRasterization:
    """Tests for rasterization helpers: _rasterize_weights and _rasterize_categorical."""

    def test_population_raster_conserves_total(self, uniform_blocks):
        """Rasterized population should approximately conserve total population."""
        transform, shape = _grid((0, 0, 2000, 1000), resolution=100)
        arr = _rasterize_weights(uniform_blocks, "pop20", transform, shape)

        total = float(arr.sum())
        expected = uniform_blocks["pop20"].sum()
        assert abs(total - expected) / expected < 0.05, f"Got {total}, expected {expected}"

    def test_categorical_raster_labels_all_zones(self, two_pumas):
        """Verify that categorical raster produces integer labels that map back to original IDs."""
        transform, shape = _grid((0, 0, 2000, 1000), resolution=100)
        arr, int_to_id = _rasterize_categorical(two_pumas, "puma_id", transform, shape)

        # Should have 2 unique non-zero values
        unique = set(arr[arr > 0].ravel())
        assert len(unique) == 2

        # Lookup should map back to original IDs
        assert set(int_to_id.values()) == {"A", "B"}


# ---------------------------------------------------------------------------
# Tests: Pydantic config models
# ---------------------------------------------------------------------------
class TestGeographyConfig:
    """Tests for GeographyConfig validation and defaults."""

    def test_negative_resolution_raises(self):
        """Resolution must be positive."""
        with pytest.raises(ValueError, match="positive"):
            GeographyConfig(
                target_zones=TargetZoneConfig(file="zones.shp"),
                resolution=-100,
            )

    def test_a_positive_resolution_is_kept_as_given(self):
        """The validator returns the value; it does not round or clamp it."""
        config = GeographyConfig(
            target_zones=TargetZoneConfig(file="zones.shp"),
            resolution=250,
        )

        assert config.resolution == 250


# ---------------------------------------------------------------------------
# Tests: household target zone assignment
# ---------------------------------------------------------------------------
class TestAssignHouseholds:
    """Test PumaCrosswalk.assign_households via a minimal instance."""

    @staticmethod
    def _make_xw(target_gdf: gpd.GeoDataFrame) -> PumaCrosswalk:
        """Build a bare PumaCrosswalk with only target_gdf set."""
        obj = object.__new__(PumaCrosswalk)
        obj.target_gdf = _load_target_zones(target_gdf, "target_id")
        obj.zone_groups = {}
        obj._zone_remap = {}
        return obj

    def test_assigns_correct_zones(self, three_target_zones):
        """Households should be assigned to the correct target zones based on their home coord."""
        hh = pl.DataFrame(
            {
                "hh_id": [1, 2, 3, 4],
                "home_lon": [350.0, 1000.0, 1650.0, -999.0],
                "home_lat": [500.0, 500.0, 500.0, 500.0],
                "extra_col": ["keep_me"] * 4,
            }
        )
        target = three_target_zones.copy().set_crs("EPSG:4326", allow_override=True)

        xw = self._make_xw(target)
        result = xw.assign_households(hh)
        assert "ctrl_geoid" in result.columns
        assert result.height == 4

        assigned = result.select("hh_id", "ctrl_geoid").sort("hh_id")
        assert assigned[0, "ctrl_geoid"] == "1"
        assert assigned[1, "ctrl_geoid"] == "2"
        assert assigned[2, "ctrl_geoid"] == "3"
        assert assigned[3, "ctrl_geoid"] is None
        # the households' own columns come back untouched
        assert result.sort("hh_id")["extra_col"].to_list() == ["keep_me"] * 4


class TestAssignBlockGroups:
    """PumaCrosswalk.assign_block_groups matches homes to blocks' block groups."""

    @staticmethod
    def _make_xw() -> PumaCrosswalk:
        """Three unit blocks in a row: two in block group ...0001, one in ...0002."""
        obj = object.__new__(PumaCrosswalk)
        obj.block_gdf = gpd.GeoDataFrame(
            {"block_id": ["060010001001000", "060010001001001", "060010001002000"]},
            geometry=[box(0, 0, 1, 1), box(1, 0, 2, 1), box(2, 0, 3, 1)],
            crs="EPSG:4326",
        )
        return obj

    def test_block_groups(self):
        """Inside a block, on an edge within one block group, on an edge between two, outside."""
        hh = pl.DataFrame(
            {
                "hh_id": [1, 2, 3, 4, 5],
                "home_lon": [0.5, 1.0, 2.0, 2.5, 9.0],
                "home_lat": [0.5, 0.5, 0.5, 0.5, 0.5],
            }
        )
        result = self._make_xw().assign_block_groups(hh).sort("hh_id")
        assert result["bg_geo_id"].to_list() == [
            "060010001001",
            "060010001001",  # between blocks of the same block group: lies within it
            None,  # between block groups: within neither
            "060010001002",
            None,
        ]


# ---------------------------------------------------------------------------
# Tests: full crosswalk (integration with exactextract)
# ---------------------------------------------------------------------------
class TestCrossTabulation:
    """Integration tests: exactextract cross-tabulation with coverage fractions."""

    def test_cross_tabulate_runs_on_real_geometry(
        self, two_pumas, three_target_zones, uniform_blocks
    ):
        """The exactextract path runs end to end and conserves the population.

        Sub-block boundaries make the per-cell split approximate here, so the
        exact arithmetic is asserted in ``TestCrossTabMath`` below on a grid
        built for it.
        """
        transform, shape = _grid((0, 0, 2000, 1000), resolution=50)
        pop_arr = _rasterize_weights(uniform_blocks, "pop20", transform, shape)
        puma_arr, int_to_puma = _rasterize_categorical(two_pumas, "puma_id", transform, shape)

        result = _cross_tabulate(pop_arr, puma_arr, int_to_puma, transform, three_target_zones)

        assert set(result.columns) == {"source_id", "target_id", "population"}
        assert result["population"].sum() == pytest.approx(1000.0, abs=50)


# ---------------------------------------------------------------------------
# Tests: strict cross-tabulation math (sub-pixel coverage)
# ---------------------------------------------------------------------------
class TestCrossTabMath:
    """Exact arithmetic verification of the PUMA x target-zone cross-tab.

    Uses a 4x4 grid (100m cells, 400x400m region) with two PUMAs split
    at x=200 and three target zones with a *middle zone that straddles
    the PUMA boundary at non-cell-aligned x=150 and x=250*.

    This forces exactextract to produce fractional coverage (0.5) on the
    boundary cells, so we can verify the ``pop * coverage`` math exactly.

    Grid layout (100m cells, each cell labelled ``pop / puma_int``)::

        ┌───────┬───────┬───────┬───────┐
     y3 │ 10/A  │ 10/A  │ 20/B  │ 20/B  │
        ├───────┼───────┼───────┼───────┤
     y2 │ 10/A  │ 10/A  │ 20/B  │ 20/B  │
        ├───────┼───────┼───────┼───────┤
     y1 │ 30/A  │ 30/A  │ 40/B  │ 40/B  │
        ├───────┼───────┼───────┼───────┤
     y0 │ 30/A  │ 30/A  │ 40/B  │ 40/B  │
        └───────┴───────┴───────┴───────┘
         x0      x1      x2      x3
              |   |=========|   |
             150  zone M   250

    Zone L:  x=[0, 150]   -> full col0 + half col1 (coverage=0.5)
    Zone M:  x=[150, 250] -> half col1 + half col2  (coverage=0.5 each)
    Zone R:  x=[250, 400] -> half col2 + full col3

    Expected cross-tab (sum of pop * coverage by PUMA, target), column by
    column::

    col0: x=[0,100],  cells: (y0,x0)=30, (y1,x0)=30, (y2,x0)=10, (y3,x0)=10.  PUMA=A
    col1: x=[100,200], cells: 30, 30, 10, 10.  PUMA=A
    col2: x=[200,300], cells: 40, 40, 20, 20.  PUMA=B
    col3: x=[300,400], cells: 40, 40, 20, 20.  PUMA=B

    Zone L (x=0..150): full coverage on col0, 50% coverage on col1
      PUMA A only: (30+30+10+10)*1.0 + (30+30+10+10)*0.5 = 80 + 40 = 120

    Zone M (x=150..250): 50% coverage on col1 (PUMA A), 50% on col2 (PUMA B)
      PUMA A: (30+30+10+10)*0.5 = 40
      PUMA B: (40+40+20+20)*0.5 = 60

    Zone R (x=250..400): 50% on col2, full on col3
      PUMA B: (40+40+20+20)*0.5 + (40+40+20+20)*1.0 = 60 + 120 = 180

    Total: 120 + 40 + 60 + 180 = 400  ✓
    (sum of all cell values = 4*(30+30+10+10) + ... = 80+80+120+120=400 ✓)

    Allocation weights:
      PUMA A total = 120 + 40 = 160
        w(A->L) = 120/160 = 0.75,  w(A->M) = 40/160 = 0.25
      PUMA B total = 60 + 180 = 240
        w(B->M) = 60/240 = 0.25,   w(B->R) = 180/240 = 0.75
    """

    @pytest.fixture
    def grid_4x4(self):
        """Build the 4x4 test grid and run _cross_tabulate once.

        Returns the raw inputs *and* the cross-tab result so every test
        in this class can validate different properties without re-running
        the expensive exactextract call.
        """
        transform = from_bounds(0, 0, 400, 400, 4, 4)
        shape = (4, 4)

        # Population: top half 10/20, bottom half 30/40
        pop = np.array(
            [[10, 10, 20, 20], [10, 10, 20, 20], [30, 30, 40, 40], [30, 30, 40, 40]],
            dtype=np.float32,
        )
        # PUMA labels: left=1(A), right=2(B) at x=200
        puma = np.array(
            [[1, 1, 2, 2], [1, 1, 2, 2], [1, 1, 2, 2], [1, 1, 2, 2]],
            dtype=np.int32,
        )
        int_to_puma = {1: "A", 2: "B"}

        # Target zones straddle PUMA boundary at non-cell-aligned positions
        zones = gpd.GeoDataFrame(
            {"target_id": ["L", "M", "R"]},
            geometry=[
                box(0, 0, 150, 400),  # half-cell overlap with col 1
                box(150, 0, 250, 400),  # half-cell from PUMA A + half from B
                box(250, 0, 400, 400),  # half-cell overlap with col 2
            ],
            crs="EPSG:5070",
        )
        result = _cross_tabulate(pop, puma, int_to_puma, transform, zones)
        return {
            "pop": pop,
            "puma": puma,
            "int_to_puma": int_to_puma,
            "transform": transform,
            "shape": shape,
            "zones": zones,
            "result": result,
        }

    def test_population_by_puma_and_zone(self, grid_4x4):
        """Verify exact population totals with sub-pixel coverage."""
        result = grid_4x4["result"]

        def _pop(source_id: str, target_id: str) -> float:
            return result.filter(
                (pl.col("source_id") == source_id) & (pl.col("target_id") == target_id)
            )["population"].sum()

        # Zone L (x=0..150): PUMA A only
        assert _pop("A", "L") == pytest.approx(120.0, abs=1)
        # Zone M (x=150..250): straddles boundary
        assert _pop("A", "M") == pytest.approx(40.0, abs=1)
        assert _pop("B", "M") == pytest.approx(60.0, abs=1)
        # Zone R (x=250..400): PUMA B only
        assert _pop("B", "R") == pytest.approx(180.0, abs=1)

    def test_total_population_conserved(self, grid_4x4):
        """Cross-tab total must equal sum of all cell values."""
        result = grid_4x4["result"]
        pop = grid_4x4["pop"]
        assert result["population"].sum() == pytest.approx(float(pop.sum()), abs=1)

    def test_allocation_weights(self, grid_4x4):
        """Normalised weights per PUMA must match analytic values."""
        result = grid_4x4["result"]

        result = result.with_columns(
            (pl.col("population") / pl.col("population").sum().over("source_id")).alias("w"),
        )

        def _w(source_id: str, target_id: str) -> float:
            return result.filter(
                (pl.col("source_id") == source_id) & (pl.col("target_id") == target_id)
            )["w"].sum()

        # PUMA A: 120/(120+40) = 0.75 to L, 0.25 to M
        assert _w("A", "L") == pytest.approx(0.75, abs=0.02)
        assert _w("A", "M") == pytest.approx(0.25, abs=0.02)
        # PUMA B: 60/(60+180) = 0.25 to M, 0.75 to R
        assert _w("B", "M") == pytest.approx(0.25, abs=0.02)
        assert _w("B", "R") == pytest.approx(0.75, abs=0.02)


# ---------------------------------------------------------------------------
# Tests: control_data crosswalk integration
# ---------------------------------------------------------------------------
class TestControlDataCrosswalk:
    """Verify PumaCrosswalk.allocate_pums_weights + build_control_totals."""

    def test_crosswalk_redistributes_controls(self):
        """Expanded PUMS totals should be split across target zones."""
        # Synthetic PUMS data — 2 PUMAs, 3 households
        hh = pl.DataFrame(
            {
                "SERIALNO": ["H1", "H2", "H3"],
                "PUMA": ["A", "A", "B"],
                "ST": ["06", "06", "06"],
                "WGTP": [100, 200, 150],
                "NP": [1, 3, 2],
                "HINCP": [50000, 100000, 75000],
                "VEH": [1, 2, 0],
                "NOC": [0, 1, 0],
                "TYPEHUGQ": [1, 1, 1],
            }
        )
        per = pl.DataFrame(
            {
                "SERIALNO": ["H1", "H2", "H2", "H2", "H3", "H3"],
                "SPORDER": [1, 1, 2, 3, 1, 2],
                "PUMA": ["A", "A", "A", "A", "B", "B"],
                "ST": ["06", "06", "06", "06", "06", "06"],
                "PWGTP": [100, 200, 200, 200, 150, 150],
                "AGEP": [35, 40, 10, 5, 55, 25],
                "SEX": [1, 2, 1, 2, 1, 2],
                "ESR": [1, 1, 0, 0, 1, 3],
                "JWTRNS": [1, 1, None, None, 6, None],
                "SCHG": [None, None, 2, 1, None, None],
                "SCHL": [21, 22, None, None, 20, 19],
                "RAC1P": [1, 1, 1, 1, 2, 2],
                "HISP": [1, 1, 1, 1, 1, 1],
            }
        )

        hh_recoded = recode_pums_households(hh, per, ["h_size"])
        per_recoded = recode_pums_persons(per, ["h_size"])

        # Crosswalk: PUMA A splits 70/30 between zones T1/T2; PUMA B is 100% T2
        crosswalk_df = pl.DataFrame(
            {
                "puma_id": ["A", "A", "B"],
                "study_geoid": ["T1", "T2", "T2"],
                "ctrl_geoid": ["T1", "T2", "T2"],
                "allocation_weight": [0.7, 0.3, 1.0],
            }
        )

        # Use PumaCrosswalk.allocate_pums_weights via a minimal instance
        xw = object.__new__(PumaCrosswalk)
        xw.crosswalk_df = crosswalk_df
        hh_xw, per_xw = xw.allocate_pums_weights(hh_recoded, per_recoded, geo_col="PUMA")

        result = build_control_totals(
            hh_xw,
            per_xw,
            [ControlSpec(name="h_size")],
            geo_col="ctrl_geoid",
        )

        assert isinstance(result, ControlTotals)
        assert set(result.geo_ids) == {"T1", "T2"}

        # T1 should have 70% of PUMA A's totals
        t1_total = result.totals.filter(pl.col("geo_id") == "T1")["target_total"].sum()
        a_total = 100 + 200  # WGTP for HH1 + HH2 in PUMA A
        expected_t1 = a_total * 0.7
        assert abs(t1_total - expected_t1) < 1, f"T1 total {t1_total}, expected {expected_t1}"


# ---------------------------------------------------------------------------
# Tests: plot_crosswalk
# ---------------------------------------------------------------------------
class TestPlotCrosswalk:
    """Test crosswalk_figure produces valid Plotly HTML."""

    @pytest.fixture
    def crosswalk_data(self, two_pumas, three_target_zones, uniform_blocks):
        """Build crosswalk data without hitting Census APIs."""
        target_gdf = _load_target_zones(three_target_zones, "target_id")
        xw_df = build_crosswalk(
            source_gdf=two_pumas,
            target_gdf=target_gdf,
            weight_gdf=uniform_blocks,
            source_id_col="puma_id",
            target_id_col="study_geoid",
            weight_col="pop20",
            resolution=50,
        ).rename({"source_id": "puma_id", "target_id": "study_geoid"})
        return two_pumas, target_gdf, xw_df

    def test_produces_html(self, crosswalk_data):
        """The figure renders, with per-profile seeds and named zone groups on it."""
        puma_gdf, target_gdf, xw_df = crosswalk_data
        hh = pl.DataFrame({"hh_id": [1, 2, 3], "ctrl_geoid": ["1", "2", "3"]})
        fig = crosswalk_figure(
            puma_gdf=puma_gdf,
            target_gdf=target_gdf,
            crosswalk_df=xw_df,
            seeds={"ctramp": hh},
            zone_groups={"north": ["1", "2"]},
        )
        html = fig.to_html()
        assert "plotly" in html.lower()
        assert "north" in html.lower()

    def test_produces_html_without_zone_groups(self, crosswalk_data):
        """Grouping is optional: ungrouped zones each get their own colour.

        The grouped and ungrouped paths are separate branches in both the colour
        index and the label builder, so rendering with groups does not exercise
        rendering without them.
        """
        puma_gdf, target_gdf, xw_df = crosswalk_data
        fig = crosswalk_figure(
            puma_gdf=puma_gdf,
            target_gdf=target_gdf,
            crosswalk_df=xw_df,
        )

        # Asserted on the traces, not the HTML: plotly's own bundled script
        # contains the word "north" as a compass direction.
        assert len(fig.data) > 0
        assert "north" not in {trace.name for trace in fig.data if trace.name}


# ===========================================================================
# The bound on what the weighting can answer: households it cannot place
#
# The bound on what the weighting can answer: households it cannot place.
#
# Two region tests exist and are allowed to disagree. The model zones a usability
# profile reads are assigned with a snap tolerance; the control geography is a
# strict point-in-polygon. A household that passes the first and fails the second is
# admitted by the profile and belongs to no balancing zone, so no fit can weight it.
#
# Left alone that produces a column of nulls and no explanation. These tests pin
# that it is counted, and that a share large enough to mean a misconfigured
# geography stops the run rather than being absorbed.
# ===========================================================================


def _seed(placed: int, unplaceable: int) -> pl.DataFrame:
    """A seed with the given split of placed and unplaceable households."""
    n = placed + unplaceable
    return pl.DataFrame(
        {
            "hh_id": list(range(1, n + 1)),
            "ctrl_geoid": ["06001"] * placed + [None] * unplaceable,
        }
    )


class TestCounting:
    """The counts are what turn a column of nulls into a statement."""

    @pytest.mark.parametrize(
        ("placed", "unplaceable", "share"),
        [
            pytest.param(10, 0, 0.0, id="none_unplaceable"),
            pytest.param(9, 1, 0.1, id="one_in_ten"),
            pytest.param(1, 3, 0.75, id="most_of_them"),
        ],
    )
    def test_unplaceable_households_are_counted_not_dropped_silently(
        self, placed, unplaceable, share
    ):
        """The count is the report; the caller is what removes them from the seed."""
        coverage = check_control_geography_coverage(
            _seed(placed, unplaceable), profile="analysis", max_unplaceable_share=1.0
        )
        assert coverage.n_universe == placed + unplaceable
        assert coverage.n_placed == placed
        assert coverage.n_unplaceable == unplaceable
        assert coverage.unplaceable_share == pytest.approx(share)
        # a report covering several fits has to say which one each row is
        assert coverage.profile == "analysis"

    def test_an_empty_seed_has_no_share_rather_than_dividing_by_zero(self):
        """A profile that admits nothing is a different complaint, made elsewhere."""
        coverage = check_control_geography_coverage(
            _seed(0, 0), profile="p", max_unplaceable_share=0.0
        )
        assert coverage.unplaceable_share == 0.0

    def test_a_seed_with_no_geography_column_places_nothing(self):
        """Rather than reading as fully placed, which would hide the whole problem."""
        seed = pl.DataFrame({"hh_id": [1, 2]})
        coverage = check_control_geography_coverage(seed, profile="p", max_unplaceable_share=1.0)
        assert coverage.n_placed == 0
        assert coverage.n_unplaceable == 2


class TestTheTolerance:
    """A boundary effect is expected; a geography that does not cover the survey is not."""

    def test_a_share_above_the_tolerance_raises(self):
        """Half the survey outside the region is a geography, not a boundary.

        The message names both layers, so the reader knows which to go and look
        at, and names the setting that tolerates it.
        """
        with pytest.raises(ValueError, match="have no control geography") as excinfo:
            check_control_geography_coverage(_seed(5, 5), profile="p", max_unplaceable_share=0.01)
        message = str(excinfo.value)
        assert "point-in-polygon" in message
        assert "snap tolerance" in message
        assert "max_unplaceable_share" in message

    def test_a_share_at_the_tolerance_is_allowed(self):
        """The tolerance is what is tolerated, not the first value rejected."""
        coverage = check_control_geography_coverage(
            _seed(99, 1), profile="p", max_unplaceable_share=0.01
        )
        assert coverage.n_unplaceable == 1


# ===========================================================================
# The Census API fails; asking it for less, and asking again, must not fail the run
#
# The Census API fails; asking it for less, and asking again, must not fail the run.
#
# The 2020 block-population request asks for every block in a state at once --
# around 520,000 rows for California -- and the API returns 500 for it often
# enough that it cannot be the only route. It killed a run that had already read
# the survey, linked trips, built tours and weighted them.
#
# Two responses. A statewide failure falls through to the county-by-county walk
# that the 2010 vintage requires anyway, which asks for far less at a time. And a
# county request retries a server-side failure, since a run makes 58 of them and
# one 500 should not end it.
#
# Retries are for server faults only. A 4xx is our request being wrong and will
# fail identically however many times it is sent.
# ===========================================================================


def _http_error(status: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(f"{status} simulated", response=response)


class TestOnlyServerFaultsAreRetried:
    """Retrying a request the server has correctly rejected just wastes time."""

    @pytest.mark.parametrize(
        ("status", "transient"),
        [
            # 5xx means the server faltered; the same request may yet succeed
            *((s, True) for s in (500, 502, 503, 504)),
            # 4xx means the request is wrong, and it will stay wrong
            *((s, False) for s in (400, 401, 403, 404)),
        ],
    )
    def test_server_errors_are_transient(self, status, transient):
        """Only the server's own failures are worth asking again about."""
        assert census_geo._is_transient(_http_error(status)) is transient

    def test_connection_and_timeout_failures_are_transient(self):
        """The request never got a verdict, so it is worth asking again."""
        assert census_geo._is_transient(requests.ConnectionError("dropped"))
        assert census_geo._is_transient(requests.Timeout("slow"))

    def test_an_unrelated_error_is_not_retried(self):
        """A bug in our own parsing must surface, not be retried four times."""
        assert not census_geo._is_transient(ValueError("bad payload"))


class TestACountyRequestRetries:
    """One 500 among 58 counties must not end the run."""

    def _serve(self, monkeypatch, outcomes):
        """Each call raises or returns the next outcome; sleeping is skipped."""
        served = iter(outcomes)
        calls = {"n": 0}

        def fake_get(*_args, **_kwargs):
            calls["n"] += 1
            outcome = next(served)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        monkeypatch.setattr(census_geo.requests, "get", fake_get)
        monkeypatch.setattr(census_geo, "_census_json", lambda resp: resp)
        monkeypatch.setattr(census_geo, "_parse_block_response", lambda payload, _var: payload)
        monkeypatch.setattr(census_geo.time, "sleep", lambda _s: None)
        return calls

    def test_it_recovers_after_a_server_error(self, monkeypatch):
        """The case that ended a real run at step 8 of 12."""
        calls = self._serve(monkeypatch, [_http_error(500), {"060010001": 42}])

        result = census_geo._fetch_county_blocks("u", "P1_001N", "06", "001", "k")

        assert result == {"060010001": 42}
        assert calls["n"] == 2

    def test_a_sound_response_is_not_retried(self, monkeypatch):
        """Retrying success would double 58 requests for nothing."""
        calls = self._serve(monkeypatch, [{"060010001": 42}])

        census_geo._fetch_county_blocks("u", "P1_001N", "06", "001", "k")

        assert calls["n"] == 1

    def test_it_gives_up_rather_than_looping(self, monkeypatch):
        """Persistent failure is systematic; looping only delays reporting it."""
        attempts = census_geo._MAX_CENSUS_ATTEMPTS
        calls = self._serve(monkeypatch, [_http_error(503)] * attempts)

        with pytest.raises(requests.HTTPError):
            census_geo._fetch_county_blocks("u", "P1_001N", "06", "001", "k")

        assert calls["n"] == attempts

    def test_a_client_error_fails_immediately(self, monkeypatch):
        """No point asking three more times for something we asked for wrongly."""
        calls = self._serve(monkeypatch, [_http_error(404)])

        with pytest.raises(requests.HTTPError):
            census_geo._fetch_county_blocks("u", "P1_001N", "06", "001", "k")

        assert calls["n"] == 1
