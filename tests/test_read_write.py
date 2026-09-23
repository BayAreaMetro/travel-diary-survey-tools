"""Tests for read_write module."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import geopandas as gpd
import polars as pl
import pytest
from shapely.geometry import Point

from data_canon.core.dataclass import CanonicalData
from processing.read_write.read_write import load_data, write_data


def _geoframe() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"id": [1, 2], "name": ["A", "B"]},
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:4326",
    )


class TestLoadData:
    """Test load_data function."""

    @pytest.mark.parametrize(
        ("filename", "write_source", "expected_type", "expected_ids"),
        [
            pytest.param(
                "test.csv",
                lambda path: pl.DataFrame({"id": [1, 2, 3]}).write_csv(path),
                pl.DataFrame,
                [1, 2, 3],
                id="csv",
            ),
            pytest.param(
                "test.parquet",
                lambda path: pl.DataFrame({"id": [1, 2, 3]}).write_parquet(path),
                pl.DataFrame,
                [1, 2, 3],
                id="parquet",
            ),
            pytest.param(
                "test.shp",
                lambda path: _geoframe().to_file(path),
                gpd.GeoDataFrame,
                [1, 2],
                id="shapefile",
            ),
            pytest.param(
                "test.geojson",
                lambda path: _geoframe().to_file(path, driver="GeoJSON"),
                gpd.GeoDataFrame,
                [1, 2],
                id="geojson",
            ),
        ],
    )
    def test_load_by_extension(
        self,
        tmp_path,
        filename: str,
        write_source: Callable[[Path], None],
        expected_type: type,
        expected_ids: list[int],
    ):
        """Each supported extension loads into the frame type that suits it."""
        path = tmp_path / filename
        write_source(path)

        result = load_data(input_paths={"test_table": str(path)})

        assert isinstance(result["test_table"], expected_type)
        assert list(result["test_table"]["id"]) == expected_ids

    def test_load_nonexistent_file_raises_error(self, tmp_path):
        """The error says the file is missing and where the path stops resolving."""
        broken = tmp_path / "nonexistent_dir" / "subdir" / "file.csv"

        with pytest.raises(FileNotFoundError, match="does not exist") as excinfo:
            load_data(input_paths={"test": str(broken)})

        assert "Possibly broken at" in str(excinfo.value)

    def test_load_unsupported_format_raises_error(self, tmp_path):
        """Test that unsupported file format raises ValueError."""
        # Create a file with unsupported extension
        unsupported_path = tmp_path / "test.xlsx"
        unsupported_path.write_text("dummy content")

        input_paths = {"test": str(unsupported_path)}

        with pytest.raises(ValueError, match="Unsupported file format"):
            load_data(input_paths=input_paths)


class TestWriteData:
    """Test write_data function."""

    @pytest.mark.parametrize(
        ("table", "payload", "filename", "read_back"),
        [
            pytest.param(
                "households",
                pl.DataFrame({"id": [1, 2, 3]}),
                "output.csv",
                lambda path: pl.read_csv(path)["id"].to_list(),
                id="csv",
            ),
            pytest.param(
                "persons",
                pl.DataFrame({"id": [1, 2, 3]}),
                "output.parquet",
                lambda path: pl.read_parquet(path)["id"].to_list(),
                id="parquet",
            ),
            pytest.param(
                "zones",
                gpd.GeoDataFrame(
                    {"id": [1, 2, 3]},
                    geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
                    crs="EPSG:4326",
                ),
                "output.shp",
                lambda path: gpd.read_file(path)["id"].to_list(),
                id="shapefile",
            ),
            pytest.param(
                "summary",
                "1, 2, 3",
                "output.txt",
                lambda path: [int(part) for part in path.read_text().split(",")],
                id="text",
            ),
        ],
    )
    def test_write_by_extension(
        self,
        tmp_path,
        table: str,
        payload: Any,
        filename: str,
        read_back: Callable[[Path], list[int]],
    ):
        """Each supported extension writes a file that reads back with the same rows."""
        output_path = tmp_path / filename
        canonical_data = CanonicalData()
        setattr(canonical_data, table, payload)

        write_data(
            output_paths={table: str(output_path)},
            canonical_data=canonical_data,
            validate_input=False,
            write_only_canonical=False,
        )

        assert output_path.exists()
        assert read_back(output_path) == [1, 2, 3]

    def test_write_creates_directories(self, tmp_path):
        """Test that write_data creates parent directories."""
        output_path = tmp_path / "subdir1" / "subdir2" / "output.csv"
        canonical_data = CanonicalData()
        canonical_data.trips = pl.DataFrame({"id": [1]})  # pyright: ignore[reportAttributeAccessIssue]

        output_paths = {"trips": str(output_path)}
        write_data(
            output_paths=output_paths,
            canonical_data=canonical_data,
            validate_input=False,
            create_dirs=True,
            write_only_canonical=False,
        )

        assert output_path.exists()
        assert output_path.parent.exists()

    def test_write_without_creating_directories_fails(self, tmp_path):
        """Test that write fails if directories don't exist and create_dirs=False."""
        output_path = tmp_path / "nonexistent" / "output.csv"
        canonical_data = CanonicalData()
        canonical_data.households = pl.DataFrame({"id": [1]})

        output_paths = {"households": str(output_path)}

        # Will raise OS error for missing directory
        with pytest.raises(
            OSError, match=r"No such file or directory|cannot find the path specified"
        ):
            write_data(
                output_paths=output_paths,
                canonical_data=canonical_data,
                validate_input=False,
                create_dirs=False,
                write_only_canonical=False,
            )

    def test_write_unsupported_format_raises_error(self, tmp_path):
        """Test that unsupported output format raises ValueError."""
        output_path = tmp_path / "output.xlsx"
        canonical_data = CanonicalData()
        canonical_data.households = pl.DataFrame({"id": [1]})

        output_paths = {"households": str(output_path)}

        with pytest.raises(ValueError, match="Unsupported file format"):
            write_data(
                output_paths=output_paths,
                canonical_data=canonical_data,
                validate_input=False,
                write_only_canonical=False,
            )
