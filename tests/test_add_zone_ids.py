"""Tests for add_zone_ids module."""

import geopandas as gpd
import polars as pl
import pytest
from shapely.geometry import Polygon

from processing.add_zone_ids.add_zone_ids import add_zone_ids, add_zone_to_dataframe


class TestAddZoneToDataframe:
    """Test add_zone_to_dataframe helper function."""

    def test_add_zone_to_points_within_polygon(self):
        """Test adding zone IDs to points within polygons."""
        # Create test dataframe with points
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "lon": [0.5, 1.5, 2.5],
                "lat": [0.5, 1.5, 2.5],
            }
        )

        # Create zones (square polygons)
        zones_gdf = gpd.GeoDataFrame(
            {"zone_id": ["Z1", "Z2", "Z3"]},
            geometry=[
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),  # Zone 1: 0-1
                Polygon([(1, 1), (2, 1), (2, 2), (1, 2)]),  # Zone 2: 1-2
                Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),  # Zone 3: 2-3
            ],
            crs="EPSG:4326",
        )

        result = add_zone_to_dataframe(
            df=df,
            df_index="id",
            shp=zones_gdf,
            lon_col="lon",
            lat_col="lat",
            zone_col_name="zone",
            zone_id_field="zone_id",
        )

        assert "zone" in result.columns
        assert result["zone"][0] == "Z1"
        assert result["zone"][1] == "Z2"
        assert result["zone"][2] == "Z3"

    def test_add_zone_to_points_outside_polygons(self):
        """Test handling points outside all zones."""
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "lon": [0.5, 10.0],  # Second point way outside
                "lat": [0.5, 10.0],
            }
        )

        zones_gdf = gpd.GeoDataFrame(
            {"zone_id": ["Z1"]},
            geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
            crs="EPSG:4326",
        )

        result = add_zone_to_dataframe(
            df=df,
            df_index="id",
            shp=zones_gdf,
            lon_col="lon",
            lat_col="lat",
            zone_col_name="zone",
            zone_id_field="zone_id",
        )

        assert result["zone"][0] == "Z1"
        assert result["zone"][1] is None  # Outside zone

    def test_all_numeric_zone_ids_become_int64(self):
        """Zone ids that are all digits are cast to Int64, which admits nulls.

        The pair to ``test_string_zone_ids``: a zone id that is not numeric
        stays text, because there is nothing to cast it to.
        """
        df = pl.DataFrame(
            {
                "id": [1],
                "lon": [0.5],
                "lat": [0.5],
            }
        )

        # Create zones with integer IDs
        zones_gdf = gpd.GeoDataFrame(
            {"zone_id": [100]},  # Integer zone ID
            geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
            crs="EPSG:4326",
        )

        result = add_zone_to_dataframe(
            df=df,
            df_index="id",
            shp=zones_gdf,
            lon_col="lon",
            lat_col="lat",
            zone_col_name="zone",
            zone_id_field="zone_id",
        )

        assert result["zone"].dtype == pl.Int64
        assert result["zone"][0] == 100


class TestAddZoneIds:
    """Test add_zone_ids function."""

    @pytest.fixture
    def sample_households(self):
        """Sample households data."""
        return pl.DataFrame(
            {
                "hh_id": [1, 2],
                "home_lon": [0.5, 1.5],
                "home_lat": [0.5, 1.5],
            }
        )

    @pytest.fixture
    def sample_persons(self):
        """Sample persons data."""
        return pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "hh_id": [1, 1, 2],
                "work_lon": [0.5, 1.5, 2.5],
                "work_lat": [0.5, 1.5, 2.5],
                "school_lon": [0.5, 1.5, 2.5],
                "school_lat": [0.5, 1.5, 2.5],
            }
        )

    @pytest.fixture
    def sample_trips(self):
        """Sample linked trips data."""
        return pl.DataFrame(
            {
                "unlinked_trip_id": [1, 2],
                "o_lon": [0.5, 1.5],
                "o_lat": [0.5, 1.5],
                "d_lon": [1.5, 2.5],
                "d_lat": [1.5, 2.5],
            }
        )

    @pytest.fixture(scope="class")
    def zone_shapefile(self, tmp_path_factory):
        """Create a test zone shapefile, written once for the whole class."""
        zones_gdf = gpd.GeoDataFrame(
            {"taz_id": [1, 2, 3]},
            geometry=[
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Polygon([(1, 1), (2, 1), (2, 2), (1, 2)]),
                Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
            ],
            crs="EPSG:4326",
        )

        shp_path = tmp_path_factory.mktemp("zones") / "zones.shp"
        zones_gdf.to_file(shp_path)
        return str(shp_path)

    def test_add_single_zone_geography(
        self, sample_households, sample_persons, sample_trips, zone_shapefile
    ):
        """Test adding a single zone geography."""
        zone_geographies = [
            {
                "shapefile": zone_shapefile,
                "zone_id_field": "taz_id",
                "zone_name": "taz",
            }
        ]

        result = add_zone_ids(
            households=sample_households,
            persons=sample_persons,
            unlinked_trips=sample_trips,
            zone_geographies=zone_geographies,
        )

        # Check households has home_taz
        assert "home_taz" in result["households"].columns
        assert result["households"]["home_taz"][0] == 1
        assert result["households"]["home_taz"][1] == 2

        # Check persons has work_taz and school_taz
        assert "work_taz" in result["persons"].columns
        assert "school_taz" in result["persons"].columns
        assert result["persons"]["work_taz"][0] == 1
        assert result["persons"]["school_taz"][2] == 3

        # Check trips has o_taz and d_taz
        assert "o_taz" in result["unlinked_trips"].columns
        assert "d_taz" in result["unlinked_trips"].columns
        assert result["unlinked_trips"]["o_taz"][0] == 1
        assert result["unlinked_trips"]["d_taz"][1] == 3

    def test_add_multiple_zone_geographies(
        self, sample_households, sample_persons, sample_trips, tmp_path
    ):
        """Test adding multiple zone geographies."""
        # Create two different zone shapefiles
        taz_gdf = gpd.GeoDataFrame(
            {"taz_id": ["T1", "T2"]},
            geometry=[
                Polygon([(0, 0), (1, 0), (1, 2), (0, 2)]),
                Polygon([(1, 0), (3, 0), (3, 2), (1, 2)]),
            ],
            crs="EPSG:4326",
        )
        taz_path = tmp_path / "taz.shp"
        taz_gdf.to_file(taz_path)

        county_gdf = gpd.GeoDataFrame(
            {"county_id": ["C1"]},
            geometry=[Polygon([(0, 0), (3, 0), (3, 3), (0, 3)])],
            crs="EPSG:4326",
        )
        county_path = tmp_path / "county.shp"
        county_gdf.to_file(county_path)

        zone_geographies = [
            {"shapefile": str(taz_path), "zone_id_field": "taz_id", "zone_name": "taz"},
            {"shapefile": str(county_path), "zone_id_field": "county_id", "zone_name": "county"},
        ]

        result = add_zone_ids(
            households=sample_households,
            persons=sample_persons,
            unlinked_trips=sample_trips,
            zone_geographies=zone_geographies,
        )

        # The TAZ split runs down x=1, so the two homes fall either side of it
        assert result["households"]["home_taz"].to_list() == ["T1", "T2"]
        # The third workplace is north of the TAZ layer entirely, so it has none
        assert result["persons"]["work_taz"].to_list() == ["T1", "T2", None]

        # The single county covers all of it
        assert result["households"]["home_county"].to_list() == ["C1", "C1"]
        assert result["persons"]["work_county"].to_list() == ["C1", "C1", "C1"]

    def test_string_zone_ids(self, sample_households, sample_persons, sample_trips, tmp_path):
        """Test handling of non-numeric (string) zone IDs."""
        # Create zones with string IDs (not convertible to integers)
        zones_gdf = gpd.GeoDataFrame(
            {"zone_id": ["TAZ_A", "TAZ_B", "TAZ_C"]},
            geometry=[
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Polygon([(1, 1), (2, 1), (2, 2), (1, 2)]),
                Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
            ],
            crs="EPSG:4326",
        )
        shp_path = tmp_path / "zones.shp"
        zones_gdf.to_file(shp_path)

        zone_geographies = [
            {
                "shapefile": str(shp_path),
                "zone_id_field": "zone_id",
                "zone_name": "taz",
            }
        ]

        result = add_zone_ids(
            households=sample_households,
            persons=sample_persons,
            unlinked_trips=sample_trips,
            zone_geographies=zone_geographies,
        )

        # Check that string zone IDs are preserved
        assert result["households"]["home_taz"][0] == "TAZ_A"
        assert result["households"]["home_taz"][1] == "TAZ_B"
        assert result["persons"]["work_taz"][2] == "TAZ_C"

    def test_replaces_existing_zone_column(
        self, sample_households, sample_persons, sample_trips, zone_shapefile
    ):
        """Test that existing zone columns are replaced with warning."""
        # Add existing zone column
        sample_households = sample_households.with_columns(pl.lit(999).alias("home_taz"))

        zone_geographies = [
            {
                "shapefile": zone_shapefile,
                "zone_id_field": "taz_id",
                "zone_name": "taz",
            }
        ]

        result = add_zone_ids(
            households=sample_households,
            persons=sample_persons,
            unlinked_trips=sample_trips,
            zone_geographies=zone_geographies,
        )

        # Should have replaced the old value
        assert result["households"]["home_taz"][0] == 1
        assert result["households"]["home_taz"][0] != 999
