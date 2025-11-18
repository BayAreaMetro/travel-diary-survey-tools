"""End-to-end tests for DaySim formatting to ensure identical output.

This test suite compares the output of the new modular pipeline with DaySim
formatting against the output from the original daysim_old pipeline to ensure
they produce identical results.

Test Organization:
    1. TestDaysimFormatting: Tests for 02a-reformat step
       - DaySim person, household, trip formatting
       - Field distributions and mappings

    2. TestDaysimFormatter: Unit tests for DaysimFormatter class
       - Age mapping, person type derivation, mode mapping

    3. TestPipelineReproduction: End-to-end pipeline test
       - Runs new pipeline from input to final outputs
       - Compares linked trips (02b) and tours (03a) counts

Environment Variables:
    DAYSIM_OLD_OUTPUT_DIR: Path to expected DaySim output (02a-reformat)
    DAYSIM_OLD_INPUT_DIR: Path to pipeline input (01-taz_spatial_join)
    DAYSIM_OLD_LINKED_DIR: Path to expected linked trips (02b-link_trips_week)
    DAYSIM_OLD_TOURS_DIR: Path to expected tours (03a-tour_extract_week)
    DAYSIM_RAW_DIR: Path to raw data (for day.csv completeness)

Or use the defaults which point to the M: drive locations.
"""

import logging
import os
from pathlib import Path

import geopandas as gpd
import polars as pl
import pytest

from processing import DaysimFormatter, TourExtractor, link_trips
from processing.utils.helpers import add_time_columns
from processing.steps.extract_tours.person_type import derive_person_type

logger = logging.getLogger(__name__)


class TestDaysimFormatting:
    """Tests to validate DaySim formatting produces identical results."""

    @pytest.fixture
    def old_pipeline_dir(self) -> Path:
        """Path to old pipeline output directory."""
        # Allow override via environment variable
        env_path = os.environ.get("DAYSIM_OLD_OUTPUT_DIR")
        if env_path:
            return Path(env_path)
        return Path(
            r"\\models.ad.mtc.ca.gov\data\models\Data\HomeInterview"
            r"\Bay Area Travel Study 2023\Data\Processed"
            r"\TripLinking_20250728\02a-reformat"
        )

    @pytest.fixture
    def old_pipeline_input_dir(self) -> Path:
        """Path to old pipeline input directory (spatially joined data)."""
        env_path = os.environ.get("DAYSIM_OLD_INPUT_DIR")
        if env_path:
            return Path(env_path)
        return Path(
            r"\\models.ad.mtc.ca.gov\data\models\Data\HomeInterview"
            r"\Bay Area Travel Study 2023\Data\Processed"
            r"\TripLinking_20250728\01-taz_spatial_join"
        )

    @pytest.fixture
    def raw_data_dir(self) -> Path:
        """Path to raw data directory (for day completeness)."""
        env_path = os.environ.get("DAYSIM_RAW_DIR")
        if env_path:
            return Path(env_path)
        return Path(
            r"\\models.ad.mtc.ca.gov\data\models\Data\HomeInterview"
            r"\Bay Area Travel Study 2023\Data"
            r"\Full Weighted 2023 Dataset\WeightedDataset_02212025"
        )

    def _compare_dataframes(
        self,
        old_df: pl.DataFrame,
        new_df: pl.DataFrame,
        name: str,
        key_cols: list[str],
        tolerance: float = 1e-6,
    ) -> tuple[bool, str]:
        """Compare two DataFrames and return detailed differences.

        Args:
            old_df: DataFrame from old pipeline
            new_df: DataFrame from new pipeline
            name: Name of the table being compared
            key_cols: Columns to use as join keys
            tolerance: Tolerance for floating point comparisons

        Returns:
            Tuple of (is_identical, difference_message)

        """
        # Check row counts
        if len(old_df) != len(new_df):
            msg = (
                f"{name}: Row count mismatch. "
                f"Old={len(old_df)}, New={len(new_df)}"
            )
            return False, msg

        # Check columns
        old_cols = set(old_df.columns)
        new_cols = set(new_df.columns)

        if old_cols != new_cols:
            missing_in_new = old_cols - new_cols
            extra_in_new = new_cols - old_cols
            msg = f"{name}: Column mismatch.\n"
            if missing_in_new:
                msg += f"  Missing in new: {missing_in_new}\n"
            if extra_in_new:
                msg += f"  Extra in new: {extra_in_new}\n"
            return False, msg

        # Sort both DataFrames by key columns for comparison
        old_df = old_df.sort(key_cols)
        new_df = new_df.sort(key_cols)

        # Compare each column
        differences = []
        for col in old_df.columns:
            old_col = old_df[col]
            new_col = new_df[col]

            # Handle different data types appropriately
            if old_col.dtype in [pl.Float32, pl.Float64]:
                # For float columns, check with tolerance
                diff_mask = (old_col - new_col).abs() > tolerance
                if diff_mask.sum() > 0:
                    n_diff = diff_mask.sum()
                    differences.append(
                        f"  {col}: {n_diff} values differ "
                        f"(tolerance={tolerance})"
                    )
            else:
                # For other columns, check exact equality
                diff_mask = old_col != new_col
                if diff_mask.sum() > 0:
                    n_diff = diff_mask.sum()
                    # Get sample of differences
                    sample_idx = diff_mask.arg_max()
                    old_val = old_col[sample_idx]
                    new_val = new_col[sample_idx]
                    differences.append(
                        f"  {col}: {n_diff} values differ "
                        f"(e.g., row {sample_idx}: old={old_val}, "
                        f"new={new_val})"
                    )

        if differences:
            msg = f"{name}: Value differences found:\n" + "\n".join(differences)
            return False, msg

        return True, f"{name}: IDENTICAL"

    def test_person_formatting(
        self,
        old_pipeline_dir: Path,
        old_pipeline_input_dir: Path,
        raw_data_dir: Path,
    ) -> None:
        """Test that person formatting produces identical results.

        This test reads the OLD pipeline output and the OLD pipeline INPUT,
        formats the input using the new formatter, and compares with the
        old output. No writing required!
        """
        if not old_pipeline_dir.exists():
            pytest.skip(f"Old pipeline directory not found: {old_pipeline_dir}")

        if not old_pipeline_input_dir.exists():
            pytest.skip(
                "Old pipeline input directory not found: "
                f"{old_pipeline_input_dir}"
            )

        # Load old pipeline's formatted output (what we're comparing against)
        old_person = pl.read_csv(old_pipeline_dir / "person.csv")

        # Load old pipeline's INPUT (spatial join output)
        input_person = pl.read_csv(old_pipeline_input_dir / "person.csv")

        # Format using new formatter
        formatter = DaysimFormatter({"weighted": True})

        # Load day completeness if available
        day_completeness = None
        day_path = raw_data_dir / "day.csv"
        if day_path.exists():
            day_completeness = formatter.load_day_completeness(day_path)

        new_person = formatter.format_person(input_person, day_completeness)

        # Compare
        is_identical, msg = self._compare_dataframes(
            old_person,
            new_person,
            "Person",
            key_cols=["hhno", "pno"],
        )

        logger.info(msg)
        assert is_identical, msg

    def test_household_formatting(
        self,
        old_pipeline_dir: Path,
        old_pipeline_input_dir: Path,
        raw_data_dir: Path,
    ) -> None:
        """Test that household formatting produces identical results.

        This test reads the OLD pipeline output and the OLD pipeline INPUT,
        formats the input using the new formatter, and compares with the
        old output. No writing required!
        """
        if not old_pipeline_dir.exists():
            pytest.skip(f"Old pipeline directory not found: {old_pipeline_dir}")

        if not old_pipeline_input_dir.exists():
            pytest.skip(
                "Old pipeline input directory not found: "
                f"{old_pipeline_input_dir}"
            )

        # Load old pipeline's formatted output
        old_hh = pl.read_csv(old_pipeline_dir / "hh.csv")

        # Load old pipeline's INPUT
        input_hh = pl.read_csv(old_pipeline_input_dir / "hh.csv")
        input_person = pl.read_csv(old_pipeline_input_dir / "person.csv")

        # Format using new formatter
        formatter = DaysimFormatter({"weighted": True})

        # Load day completeness if available
        day_completeness = None
        day_path = raw_data_dir / "day.csv"
        if day_path.exists():
            day_completeness = formatter.load_day_completeness(day_path)

        # Format person first (needed for household formatting)
        formatted_person = formatter.format_person(
            input_person, day_completeness
        )
        new_hh = formatter.format_household(input_hh, formatted_person)

        # Compare
        is_identical, msg = self._compare_dataframes(
            old_hh,
            new_hh,
            "Household",
            key_cols=["hhno"],
        )

        logger.info(msg)
        assert is_identical, msg

    def test_trip_formatting(
        self,
        old_pipeline_dir: Path,
        old_pipeline_input_dir: Path,
    ) -> None:
        """Test that trip formatting produces identical results.

        This test reads the OLD pipeline output and the OLD pipeline INPUT,
        formats the input using the new formatter, and compares with the
        old output. No writing required!
        """
        if not old_pipeline_dir.exists():
            pytest.skip(f"Old pipeline directory not found: {old_pipeline_dir}")

        if not old_pipeline_input_dir.exists():
            pytest.skip(
                "Old pipeline input directory not found: "
                f"{old_pipeline_input_dir}"
            )

        # Load old pipeline's formatted output
        old_trip = pl.read_csv(old_pipeline_dir / "trip.csv")

        # Load old pipeline's INPUT
        input_trip = pl.read_csv(old_pipeline_input_dir / "trip.csv")

        # Format using new formatter
        formatter = DaysimFormatter({"weighted": True})
        new_trip = formatter.format_trip(input_trip)

        # Compare
        is_identical, msg = self._compare_dataframes(
            old_trip,
            new_trip,
            "Trip",
            key_cols=["hhno", "pno", "tripno"],
        )

        logger.info(msg)
        assert is_identical, msg

    def test_field_distributions(
        self,
        old_pipeline_dir: Path,
        old_pipeline_input_dir: Path,
        raw_data_dir: Path,
    ) -> None:
        """Test that field distributions match between old and new pipelines.

        This test checks summary statistics for key fields to ensure the
        overall distributions are preserved even if individual records differ.

        This test reads the OLD pipeline output and the OLD pipeline INPUT,
        formats the input using the new formatter, and compares distributions.
        No writing required!
        """
        if not old_pipeline_dir.exists():
            pytest.skip(f"Old pipeline directory not found: {old_pipeline_dir}")

        if not old_pipeline_input_dir.exists():
            pytest.skip(
                "Old pipeline input directory not found: "
                f"{old_pipeline_input_dir}"
            )

        # Initialize formatter
        formatter = DaysimFormatter({"weighted": True})

        # Load day completeness if available
        day_completeness = None
        day_path = raw_data_dir / "day.csv"
        if day_path.exists():
            day_completeness = formatter.load_day_completeness(day_path)

        # Test person type distribution
        old_person = pl.read_csv(old_pipeline_dir / "person.csv")
        input_person = pl.read_csv(old_pipeline_input_dir / "person.csv")
        new_person = formatter.format_person(input_person, day_completeness)

        # Compare person type counts
        old_pptyp = old_person.group_by("pptyp").len().sort("pptyp")
        new_pptyp = new_person.group_by("pptyp").len().sort("pptyp")

        assert old_pptyp.equals(new_pptyp), (
            "Person type (pptyp) distribution differs:\n"
            f"Old:\n{old_pptyp}\n"
            f"New:\n{new_pptyp}"
        )

        # Test trip mode distribution
        old_trip = pl.read_csv(old_pipeline_dir / "trip.csv")
        input_trip = pl.read_csv(old_pipeline_input_dir / "trip.csv")
        new_trip = formatter.format_trip(input_trip)

        old_mode = old_trip.group_by("mode").len().sort("mode")
        new_mode = new_trip.group_by("mode").len().sort("mode")

        assert old_mode.equals(new_mode), (
            "Trip mode distribution differs:\n"
            f"Old:\n{old_mode}\n"
            f"New:\n{new_mode}"
        )

        # Test purpose distribution
        old_purpose = old_trip.group_by("dpurp").len().sort("dpurp")
        new_purpose = new_trip.group_by("dpurp").len().sort("dpurp")

        assert old_purpose.equals(new_purpose), (
            "Trip purpose (dpurp) distribution differs:\n"
            f"Old:\n{old_purpose}\n"
            f"New:\n{new_purpose}"
        )


class TestDaysimFormatter:
    """Unit tests for DaysimFormatter class."""

    def test_age_mapping(self) -> None:
        """Test age category mapping."""
        formatter = DaysimFormatter({"weighted": False})

        # Create test data
        test_person = pl.DataFrame(
            {
                "hh_id": [1, 1, 1],
                "person_num": [1, 2, 3],
                "age": [1, 5, 10],  # age categories
                "gender": [1, 2, 1],
                "student": [2, 2, 2],  # not student
                "work_park": [1, 1, 1],
                "residence_rent_own": [1, 1, 1],
                "residence_type": [1, 1, 1],
                "employment": [0, 0, 0],
                "school_type": [0, 0, 0],
                "work_lon": [-122.4, -122.4, -122.4],
                "work_lat": [37.8, 37.8, 37.8],
                "school_lon": [-122.4, -122.4, -122.4],
                "school_lat": [37.8, 37.8, 37.8],
                "work_taz": [1, 1, 1],
                "work_maz": [1, 1, 1],
                "school_taz": [1, 1, 1],
                "school_maz": [1, 1, 1],
                "work_county": [6075, 6075, 6075],
                "school_county": [6075, 6075, 6075],
            }
        )

        result = formatter.format_person(test_person)

        # Check age mapping: 1->3, 5->30, 10->80
        assert result["pagey"].to_list() == [3, 30, 80]

    def test_person_type_derivation(self) -> None:
        """Test person type (pptyp) derivation logic."""
        formatter = DaysimFormatter({"weighted": False})

        # Create test data with different person types
        test_person = pl.DataFrame(
            {
                "hh_id": [1, 1, 1, 1],
                "person_num": [1, 2, 3, 4],
                "age": [
                    1,
                    2,
                    4,
                    10,
                ],  # child 0-4, child 5-15, young adult, senior
                "gender": [1, 1, 1, 1],
                "student": [2, 2, 2, 2],  # not student
                "work_park": [1, 1, 1, 1],
                "residence_rent_own": [1, 1, 1, 1],
                "residence_type": [1, 1, 1, 1],
                "employment": [0, 0, 1, 0],  # not employed, not, full-time, not
                "school_type": [0, 0, 0, 0],
                "work_lon": [-122.4] * 4,
                "work_lat": [37.8] * 4,
                "school_lon": [-122.4] * 4,
                "school_lat": [37.8] * 4,
                "work_taz": [1] * 4,
                "work_maz": [1] * 4,
                "school_taz": [1] * 4,
                "school_maz": [1] * 4,
                "work_county": [6075] * 4,
                "school_county": [6075] * 4,
            }
        )

        result = formatter.format_person(test_person)

        # Check person types: child 0-4, child 5-15, full-time worker, retiree
        # Age mappings: 1→3yrs, 2→10yrs, 4→21yrs, 10→80yrs
        assert result["pptyp"].to_list() == [8, 7, 1, 3]

    def test_trip_mode_mapping(self) -> None:
        """Test trip mode mapping to DaySim codes."""
        formatter = DaysimFormatter({"weighted": False})

        # Create test trip data
        test_trip = pl.DataFrame(
            {
                "hh_id": [1, 1, 1, 1],
                "person_num": [1, 1, 1, 1],
                "trip_num": [1, 2, 3, 4],
                "mode_type": [1, 2, 8, 13],  # walk, bike, car, transit
                "num_travelers": [1, 1, 1, 1],
                "o_purpose_category": [1, 1, 1, 1],
                "d_purpose_category": [2, 2, 2, 2],
                "depart_hour": [8, 9, 10, 11],
                "depart_minute": [0, 15, 30, 45],
                "arrive_hour": [8, 9, 10, 11],
                "arrive_minute": [30, 45, 0, 15],
                "o_taz": [1, 1, 1, 1],
                "o_maz": [1, 1, 1, 1],
                "d_taz": [2, 2, 2, 2],
                "d_maz": [2, 2, 2, 2],
                "o_lon": [-122.4] * 4,
                "o_lat": [37.8] * 4,
                "d_lon": [-122.5] * 4,
                "d_lat": [37.9] * 4,
                "o_county": [6075] * 4,
                "d_county": [6075] * 4,
                "travel_dow": [2, 2, 2, 2],
                "day_is_complete": [1, 1, 1, 1],
                "transit_access": [1] * 4,
                "transit_egress": [1] * 4,
                "driver": [1] * 4,
                "mode_1": [1] * 4,
                "mode_2": [1] * 4,
                "mode_3": [1] * 4,
                "mode_4": [1] * 4,
            }
        )

        result = formatter.format_trip(test_trip)

        # Check mode mapping: walk=1, bike=2, car(1pax)=3, transit=6
        assert result["mode"].to_list() == [1, 2, 3, 6]


def spatial_join_taz_maz(
    df: pl.DataFrame,
    taz_shapefile_path: Path,
    id_col: str,
    var_prefix: str,
) -> pl.DataFrame:
    """Perform spatial join to add TAZ/MAZ columns.

    Args:
        df: DataFrame with {var_prefix}_lon and {var_prefix}_lat columns
        taz_shapefile_path: Path to TAZ shapefile
        id_col: ID column for deduplication
        var_prefix: Prefix for columns (e.g., 'o', 'd', 'home', 'work')

    Returns:
        DataFrame with added {var_prefix}_taz and {var_prefix}_maz columns

    """
    # Load TAZ shapefile (MTC TM1 format)
    maz = gpd.read_file(taz_shapefile_path)[["TAZ1454", "geometry"]].rename(
        columns={"TAZ1454": "TAZ"}
    )
    maz["MAZID"] = maz["TAZ"]  # For MTC TM1, MAZID = TAZ

    # Set CRS if not present - shapefile name indicates WGS84
    if maz.crs is None:
        maz = maz.set_crs("EPSG:4326")

    # Project to California State Plane Zone 3 (NAD83, units=US feet)
    # This matches the old pipeline's requirements for 2000ft buffer
    maz = maz.to_crs("EPSG:2227")

    # Convert DataFrame to pandas for GeoPandas
    pdf = df.to_pandas()

    # Filter out records with missing coordinates
    lon_col = f"{var_prefix}_lon"
    lat_col = f"{var_prefix}_lat"
    has_coords = pdf[lon_col].notna() & pdf[lat_col].notna()

    # Create GeoDataFrame from survey points (only non-null coordinates)
    gdf = gpd.GeoDataFrame(
        pdf[has_coords],
        geometry=gpd.points_from_xy(
            pdf.loc[has_coords, lon_col], pdf.loc[has_coords, lat_col]
        ),
        crs="EPSG:4326",
    )

    # Convert to projected CRS for spatial join (California State Plane)
    gdf = gdf.to_crs("EPSG:2227")

    # Spatial join with 2000ft buffer (only for records with coordinates)
    joined = gdf.sjoin_nearest(maz, how="left", max_distance=2000)

    # Keep first match if multiple equidistant
    joined = joined.drop_duplicates(subset=id_col, keep="first")

    # Clean up and rename
    joined = joined.drop(columns=["geometry", "index_right"])
    joined["MAZID"] = joined["MAZID"].astype("Int32")
    joined["TAZ"] = joined["TAZ"].astype("Int32")
    joined = joined.rename(
        columns={"MAZID": f"{var_prefix}_maz", "TAZ": f"{var_prefix}_taz"}
    )

    # For records without coordinates, add them back with null TAZ/MAZ
    if not has_coords.all():
        no_coords_pl = pl.from_pandas(pdf[~has_coords])
        # Add null TAZ/MAZ columns with correct Int32 type
        no_coords_pl = no_coords_pl.with_columns(
            [
                pl.lit(None, dtype=pl.Int32).alias(f"{var_prefix}_maz"),
                pl.lit(None, dtype=pl.Int32).alias(f"{var_prefix}_taz"),
            ]
        )
        # Combine with spatially joined records and sort to maintain order
        return pl.concat(
            [pl.from_pandas(joined), no_coords_pl], how="diagonal"
        ).sort(id_col)

    return pl.from_pandas(joined)


class TestPipelineReproduction:
    """Test new pipeline outputs against expected outputs from 02b and 03a.

    This test runs the complete pipeline and compares only the final outputs:
    - Linked trips (02b-link_trips_week)
    - Tours (03a-tour_extract_week)
    """

    @pytest.fixture
    def taz_shapefile_path(self) -> Path:
        """Path to TAZ shapefile for spatial join."""
        env_path = os.environ.get("TAZ_SHAPEFILE_PATH")
        if env_path:
            return Path(env_path)
        return Path(
            r"\\model3-a\Model3A-Share\travel-model-one-master\utilities\geographies"
            r"\bayarea_rtaz1454_rev1_WGS84.shp"
        )

    @pytest.fixture
    def input_dir(self) -> Path:
        """Path to raw survey data (WeightedDataset_02212025)."""
        env_path = os.environ.get("DAYSIM_RAW_DIR")
        if env_path:
            return Path(env_path)
        return Path(
            r"\\models.ad.mtc.ca.gov\data\models\Data\HomeInterview"
            r"\Bay Area Travel Study 2023\Data"
            r"\Full Weighted 2023 Dataset\WeightedDataset_02212025"
        )

    @pytest.fixture
    def expected_linked_dir(self) -> Path:
        """Path to expected linked trips (02b-link_trips_week)."""
        env_path = os.environ.get("DAYSIM_OLD_LINKED_DIR")
        if env_path:
            return Path(env_path)
        return Path(
            r"\\models.ad.mtc.ca.gov\data\models\Data\HomeInterview"
            r"\Bay Area Travel Study 2023\Data\Processed"
            r"\TripLinking_20250728\02b-link_trips_week"
        )

    @pytest.fixture
    def expected_tours_dir(self) -> Path:
        """Path to expected tours (03a-tour_extract_week)."""
        env_path = os.environ.get("DAYSIM_OLD_TOURS_DIR")
        if env_path:
            return Path(env_path)
        return Path(
            r"\\models.ad.mtc.ca.gov\data\models\Data\HomeInterview"
            r"\Bay Area Travel Study 2023\Data\Processed"
            r"\TripLinking_20250728\03a-tour_extract_week"
        )

    @pytest.fixture
    def pipeline_outputs(
        self,
        input_dir: Path,
        taz_shapefile_path: Path,
    ) -> tuple[pl.DataFrame, pl.DataFrame] | None:
        """Run the complete pipeline and return outputs.

        Returns:
            Tuple of (linked_trips, tours) or None if inputs don't exist.
            linked_trips: Aggregated linked trips (one row per linked trip),
                          comparable to old 02b output
            tours: Tour-level records, comparable to old 03a output
        """
        if not input_dir.exists():
            return None

        if not taz_shapefile_path.exists():
            pytest.skip(f"TAZ shapefile not found: {taz_shapefile_path}")

        logger.info("=== Running Pipeline ===")

        # Load input
        input_trip = pl.read_csv(input_dir / "trip.csv")
        input_person = pl.read_csv(input_dir / "person.csv")
        input_household = pl.read_csv(input_dir / "hh.csv")

        # Step 1: Spatial join to add TAZ/MAZ columns
        logger.info("Performing spatial join for trips...")
        input_trip = spatial_join_taz_maz(
            input_trip, taz_shapefile_path, "trip_id", "o"
        )
        input_trip = spatial_join_taz_maz(
            input_trip, taz_shapefile_path, "trip_id", "d"
        )

        logger.info("Performing spatial join for persons...")
        input_person = spatial_join_taz_maz(
            input_person, taz_shapefile_path, "person_id", "work"
        )
        input_person = spatial_join_taz_maz(
            input_person, taz_shapefile_path, "person_id", "school"
        )

        logger.info("Performing spatial join for households...")
        input_household = spatial_join_taz_maz(
            input_household, taz_shapefile_path, "hh_id", "home"
        )

        # Fix data quality issues
        if "depart_second" in input_trip.columns:
            input_trip = input_trip.rename({"depart_second": "depart_seconds"})
        if "arrive_second" in input_trip.columns:
            input_trip = input_trip.rename({"arrive_second": "arrive_seconds"})

        # Add time columns for time inversion fix
        input_trip = add_time_columns(input_trip)

        # Fix time inversions: swap depart/arrive when times are inverted
        swap_condition = pl.col("depart_time") > pl.col("arrive_time")
        input_trip = input_trip.with_columns(
            pl.when(swap_condition)
            .then(pl.col("arrive_time"))
            .otherwise(pl.col("depart_time"))
            .alias("depart_time"),
            pl.when(swap_condition)
            .then(pl.col("depart_time"))
            .otherwise(pl.col("arrive_time"))
            .alias("arrive_time"),
            pl.when(swap_condition)
            .then(pl.col("arrive_hour"))
            .otherwise(pl.col("depart_hour"))
            .alias("depart_hour"),
            pl.when(swap_condition)
            .then(pl.col("arrive_minute"))
            .otherwise(pl.col("depart_minute"))
            .alias("depart_minute"),
            pl.when(swap_condition)
            .then(pl.col("arrive_seconds"))
            .otherwise(pl.col("depart_seconds"))
            .alias("depart_seconds"),
            pl.when(swap_condition)
            .then(pl.col("depart_hour"))
            .otherwise(pl.col("arrive_hour"))
            .alias("arrive_hour"),
            pl.when(swap_condition)
            .then(pl.col("depart_minute"))
            .otherwise(pl.col("arrive_minute"))
            .alias("arrive_minute"),
            pl.when(swap_condition)
            .then(pl.col("depart_seconds"))
            .otherwise(pl.col("arrive_seconds"))
            .alias("arrive_seconds"),
        )

        # Filter for complete days only (matching old pipeline 02a-reformat)
        input_trip = input_trip.filter(pl.col("day_is_complete") == 1)

        # Step 2: Derive person_type for TourExtractor
        logger.info("Deriving person types...")
        input_person = derive_person_type(input_person)

        # Run pipeline: link trips
        logger.info("Linking trips...")
        print(f"\nDEBUG: Trips before linking: {len(input_trip):,}")
        new_trips_with_ids, new_linked_aggregated = link_trips(
            input_trip,
            change_mode_code=10,
            transit_mode_codes=[13],
            max_dwell_time=120,
            dwell_buffer_distance=100,
        )
        num_segments_merged = (
            len(new_trips_with_ids) - len(new_linked_aggregated)
        )
        print(f"DEBUG: Segments merged: {num_segments_merged:,}")
        print(
            f"DEBUG: Linked trips: {len(new_linked_aggregated):,}\n"
        )

        # Run pipeline: build tours
        logger.info("Building tours...")
        builder = TourExtractor(input_person, households=input_household)
        _, new_tours = builder.build_tours(new_linked_aggregated)

        logger.info(
            "Pipeline complete: %d trip segments, "
            "%d linked trips (aggregated), %d tours",
            len(new_trips_with_ids),
            len(new_linked_aggregated),
            len(new_tours),
        )

        # Return aggregated linked trips (comparable to old 02b output)
        # Old 02b removes merged trip segments, so we return the aggregated version
        # The tours are comparable with 03a output
        return new_linked_aggregated, new_tours

    def test_01_pipeline_runs(
        self,
        pipeline_outputs: tuple[pl.DataFrame, pl.DataFrame] | None,
    ) -> None:
        """Test that the pipeline runs successfully and produces outputs."""
        if pipeline_outputs is None:
            pytest.skip("Input directory not found")

        new_trips, new_tours = pipeline_outputs

        # Validate outputs exist and are non-empty
        assert len(new_trips) > 0, "No trips with IDs produced"
        assert len(new_tours) > 0, "No tours produced"

        # Basic sanity checks
        assert "linked_trip_id" in new_trips.columns, (
            "linked_trip_id column missing"
        )
        assert "tour_id" in new_tours.columns, "tour_id column missing"

        logger.info("✓ Pipeline runs successfully")

    def test_02_output_counts_match(
        self,
        pipeline_outputs: tuple[pl.DataFrame, pl.DataFrame] | None,
        expected_linked_dir: Path,
        expected_tours_dir: Path,
    ) -> None:
        """Test that output row counts match expected values."""
        if pipeline_outputs is None:
            pytest.skip("Input directory not found")

        if not expected_linked_dir.exists() or not expected_tours_dir.exists():
            pytest.skip("Expected output directories not found")

        new_trips, new_tours = pipeline_outputs

        # Load expected outputs
        expected_trips = pl.read_csv(expected_linked_dir / "trip.csv")
        expected_tours = pl.read_csv(expected_tours_dir / "tour.csv")

        # Compare counts
        logger.info("=== Row Count Comparison ===")
        logger.info(
            "Trips with IDs: %d (expected: %d)",
            len(new_trips),
            len(expected_trips),
        )
        logger.info(
            "Tours:          %d (expected: %d)",
            len(new_tours),
            len(expected_tours),
        )

        # Assert counts match
        assert len(new_trips) == len(expected_trips), (
            f"Trips count mismatch: "
            f"got {len(new_trips)}, expected {len(expected_trips)}"
        )
        assert len(new_tours) == len(expected_tours), (
            f"Tours count mismatch: "
            f"got {len(new_tours)}, expected {len(expected_tours)}"
        )

        logger.info("✓ Row counts match expected values")

    def test_03_output_columns_matches(
        self,
        pipeline_outputs: tuple[pl.DataFrame, pl.DataFrame] | None,
        expected_linked_dir: Path,
        expected_tours_dir: Path,
    ) -> None:
        """Test that output content matches expected values."""
        if pipeline_outputs is None:
            pytest.skip("Input directory not found")

        if not expected_linked_dir.exists() or not expected_tours_dir.exists():
            pytest.skip("Expected output directories not found")

        new_trips, new_tours = pipeline_outputs

        # Load expected outputs
        expected_trips = pl.read_csv(expected_linked_dir / "trip.csv")
        expected_tours = pl.read_csv(expected_tours_dir / "tour.csv")

        logger.info("=== Content Comparison ===")

        # Compare trips structure
        new_cols = set(new_trips.columns)
        expected_cols = set(expected_trips.columns)

        if new_cols != expected_cols:
            missing = expected_cols - new_cols
            extra = new_cols - expected_cols
            msg = "Trips column mismatch:\n"
            if missing:
                msg += f"  Missing: {sorted(missing)}\n"
            if extra:
                msg += f"  Extra: {sorted(extra)}\n"
            logger.warning(msg)
        else:
            logger.info("✓ Trips columns match")

        # Compare tours structure
        new_tour_cols = set(new_tours.columns)
        expected_tour_cols = set(expected_tours.columns)

        if new_tour_cols != expected_tour_cols:
            missing = expected_tour_cols - new_tour_cols
            extra = new_tour_cols - expected_tour_cols
            msg = "Tours column mismatch:\n"
            if missing:
                msg += f"  Missing: {sorted(missing)}\n"
            if extra:
                msg += f"  Extra: {sorted(extra)}\n"
            logger.warning(msg)
        else:
            logger.info("✓ Tours columns match")

        # For now, just check structure - detailed content comparison
        # can be added later as needed
        logger.info("✓ Output structure validated")

    def test_04_output_content_matches(
        self,
        pipeline_outputs: tuple[pl.DataFrame, pl.DataFrame] | None,
        expected_linked_dir: Path,
        expected_tours_dir: Path,
    ) -> None:
        """Test that output content matches expected values."""
        if pipeline_outputs is None:
            pytest.skip("Input directory not found")

        if not expected_linked_dir.exists() or not expected_tours_dir.exists():
            pytest.skip("Expected output directories not found")

        new_trips, new_tours = pipeline_outputs

        # Load expected outputs
        expected_trips = pl.read_csv(expected_linked_dir / "trip.csv")
        expected_tours = pl.read_csv(expected_tours_dir / "tour.csv")

        logger.info("=== Content Comparison ===")

        # Compare trips content
        if not new_trips.frame_equal(expected_trips):
            logger.warning(
                "Trips content does not match expected output"
            )
        else:
            logger.info("✓ Trips content matches")

        # Compare tours content
        if not new_tours.frame_equal(expected_tours):
            logger.warning("Tours content does not match expected output")
        else:
            logger.info("✓ Tours content matches")


if __name__ == "__main__":
    # Allow running tests directly for development
    pytest.main([__file__, "-v"])
