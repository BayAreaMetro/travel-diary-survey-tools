"""Tests for pipeline caching functionality."""

import contextlib
import threading
from collections.abc import Callable
from pathlib import Path

import geopandas as gpd
import polars as pl
import pytest
from shapely.geometry import Point

from pipeline.cache import PipelineCache
from pipeline.decoration import step


@pytest.fixture
def cache_dir(tmp_path):
    """Provide a temporary cache directory."""
    return tmp_path / "test_cache"


@pytest.fixture
def pipeline_cache(cache_dir):
    """Provide a PipelineCache instance."""
    return PipelineCache(cache_dir=cache_dir)


@pytest.fixture
def sample_dataframe():
    """Provide a sample DataFrame for testing."""
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 3],
            "home_lat": [37.7, 37.8, 37.9],
            "home_lon": [-122.4, -122.5, -122.6],
        }
    )


class TestPipelineCache:
    """Test PipelineCache functionality."""

    def test_cache_initialization(self, cache_dir):
        """Test cache directory is created on initialization."""
        cache = PipelineCache(cache_dir=cache_dir)
        assert cache_dir.exists()
        assert cache.cache_dir == cache_dir

    def test_cache_key_generation(self, pipeline_cache, sample_dataframe):
        """Test cache key is deterministic for same inputs."""
        inputs = {"households": sample_dataframe}
        params = {"param1": "value1"}

        key1 = pipeline_cache.get_cache_key("test_step", inputs, params)
        key2 = pipeline_cache.get_cache_key("test_step", inputs, params)

        assert key1 == key2
        assert len(key1) == 16  # 16 hex characters

    def test_cache_key_changes_with_data(self, pipeline_cache, sample_dataframe):
        """Test cache key changes when data changes."""
        inputs1 = {"households": sample_dataframe}
        inputs2 = {"households": sample_dataframe.with_columns(pl.col("hh_id") + 10)}
        params = {"param1": "value1"}

        key1 = pipeline_cache.get_cache_key("test_step", inputs1, params)
        key2 = pipeline_cache.get_cache_key("test_step", inputs2, params)

        assert key1 != key2

    def test_cache_key_changes_with_params(self, pipeline_cache, sample_dataframe):
        """Test cache key changes when parameters change."""
        inputs = {"households": sample_dataframe}

        key1 = pipeline_cache.get_cache_key("test_step", inputs, {"param1": "value1"})
        key2 = pipeline_cache.get_cache_key("test_step", inputs, {"param1": "value2"})

        assert key1 != key2

    @pytest.mark.parametrize(
        ("inputs", "params"),
        [
            pytest.param({"data": pl.DataFrame({"id": []})}, {"p": "v"}, id="empty-dataframe"),
            pytest.param(None, {"p": "v"}, id="no-inputs"),
            pytest.param({"data": pl.DataFrame({"id": [1]})}, None, id="no-params"),
        ],
    )
    def test_cache_key_is_still_produced_for_degenerate_inputs(
        self, pipeline_cache, inputs, params
    ):
        """An empty frame, a first step with no inputs, a step with no params."""
        assert len(pipeline_cache.get_cache_key("test_step", inputs, params)) == 16

    def test_cache_key_ignores_non_serializable_params(self, pipeline_cache, sample_dataframe):
        """A param that will not serialise is dropped, not hashed and not fatal.

        The key must be the one the same call would produce without it, or a
        pipeline_cache or a lock in the params would make every run a miss.
        """
        inputs = {"data": sample_dataframe}
        good = {"serializable": "value", "another_good": 123}

        with_lock = pipeline_cache.get_cache_key(
            "test_step", inputs, {**good, "non_serializable": threading.Lock()}
        )

        assert with_lock == pipeline_cache.get_cache_key("test_step", inputs, good)

    def test_save_and_load(self, pipeline_cache, sample_dataframe):
        """Test saving and loading cached data."""
        step_name = "test_step"
        cache_key = "test_key_12345678"
        outputs = {"households": sample_dataframe}

        # Save to cache
        pipeline_cache.save(step_name, cache_key, outputs)

        # Load from cache
        loaded = pipeline_cache.load(step_name, cache_key)

        assert loaded is not None
        assert "households" in loaded
        assert loaded["households"].equals(sample_dataframe)

    def test_cache_miss(self, pipeline_cache, sample_dataframe):
        """Neither an unknown step nor an unknown key under a known step loads."""
        pipeline_cache.save("test_step", "real_key", {"data": sample_dataframe})

        assert pipeline_cache.load("nonexistent_step", "nonexistent_key") is None
        assert pipeline_cache.load("test_step", "fake_key") is None

    def test_invalidate_step(self, pipeline_cache, sample_dataframe):
        """Test invalidating cache for a specific step."""
        step_name = "test_step"
        cache_key = "test_key_12345678"
        outputs = {"households": sample_dataframe}

        # Save to cache
        pipeline_cache.save(step_name, cache_key, outputs)
        assert pipeline_cache.load(step_name, cache_key) is not None

        # Invalidate
        pipeline_cache.invalidate(step_name)
        assert pipeline_cache.load(step_name, cache_key) is None

    def test_invalidate_nonexistent_step(self, pipeline_cache):
        """Test invalidating a step that doesn't exist."""
        # Should not raise error
        pipeline_cache.invalidate("nonexistent_step")

        # Directory should not exist
        step_dir = pipeline_cache.cache_dir / "nonexistent_step"
        assert not step_dir.exists()

    @pytest.mark.parametrize(
        "wipe",
        [
            pytest.param(lambda cache: cache.invalidate(), id="invalidate-all"),
            pytest.param(lambda cache: cache.clear(), id="clear"),
        ],
    )
    def test_wiping_the_cache_leaves_no_step_directories(
        self, pipeline_cache, sample_dataframe, wipe: Callable[[PipelineCache], None]
    ):
        """``invalidate()`` and ``clear()`` are separate entry points to the same wipe."""
        pipeline_cache.save("step1", "key1", {"data": sample_dataframe})
        pipeline_cache.save("step2", "key2", {"data": sample_dataframe})
        assert len(pipeline_cache.list_cached_steps()) == 2

        wipe(pipeline_cache)

        assert pipeline_cache.list_cached_steps() == []
        assert pipeline_cache.load("step1", "key1") is None
        assert [d for d in pipeline_cache.cache_dir.iterdir() if d.is_dir()] == []

    def test_list_cached_steps(self, pipeline_cache, sample_dataframe):
        """Test listing cached steps."""
        outputs = {"households": sample_dataframe}

        # Save to cache
        pipeline_cache.save("step1", "key1", outputs)
        pipeline_cache.save("step2", "key2", outputs)

        # List caches
        cached_steps = pipeline_cache.list_cached_steps()

        assert len(cached_steps) == 2
        step_names = {step["step_name"] for step in cached_steps}
        assert step_names == {"step1", "step2"}

    def test_cache_statistics(self, pipeline_cache, sample_dataframe):
        """Hits, misses and the derived rate, from zero through one of each."""
        assert pipeline_cache.get_stats() == {
            "loaded": 0,
            "missing": 0,
            "stale": 0,
            "total": 0,
            "load_rate": 0.0,
        }

        pipeline_cache.save("step1", "key1", {"data": sample_dataframe})
        pipeline_cache.load("step1", "key1")

        assert pipeline_cache.get_stats() == {
            "loaded": 1,
            "missing": 0,
            "stale": 0,
            "total": 1,
            "load_rate": 1.0,
        }

        pipeline_cache.load("nonexistent", "nonexistent")

        assert pipeline_cache.get_stats() == {
            "loaded": 1,
            "missing": 1,
            "stale": 0,
            "total": 2,
            "load_rate": 0.5,
        }

        pipeline_cache.reset_stats()
        assert pipeline_cache.get_stats()["total"] == 0


class TestStepDecoratorCaching:
    """Test @step decorator caching functionality."""

    @pytest.fixture
    def cached_step(self):
        """A cacheable step whose output is checkable."""

        @step(validate_input=False, cache=True)
        def process_data(households: pl.DataFrame) -> dict[str, pl.DataFrame]:
            processed = households.with_columns(
                pl.concat_str([pl.lit("HH_"), pl.col("hh_id").cast(pl.Utf8)]).alias("hh_code")
            )
            return {"households": processed}

        return process_data

    def test_decorator_cache_miss_and_hit(self, cached_step, pipeline_cache, sample_dataframe):
        """The second identical call is served from cache, the first is not."""
        result1 = cached_step(
            households=sample_dataframe, pipeline_cache=pipeline_cache, cache=True
        )
        result2 = cached_step(
            households=sample_dataframe, pipeline_cache=pipeline_cache, cache=True
        )

        assert result1["households"].equals(result2["households"])

        stats = pipeline_cache.get_stats()
        assert stats["loaded"] == 1
        assert stats["missing"] == 1

    def test_decorator_recomputes_when_the_input_changes(self, cached_step, pipeline_cache):
        """A different input frame is a different key, so nothing is served stale."""
        df1 = pl.DataFrame({"hh_id": [1, 2, 3]})
        df2 = pl.DataFrame({"hh_id": [4, 5, 6]})

        result1 = cached_step(households=df1, pipeline_cache=pipeline_cache, cache=True)
        result2 = cached_step(households=df2, pipeline_cache=pipeline_cache, cache=True)

        assert result1["households"]["hh_code"].to_list() == ["HH_1", "HH_2", "HH_3"]
        assert result2["households"]["hh_code"].to_list() == ["HH_4", "HH_5", "HH_6"]
        assert pipeline_cache.get_stats()["loaded"] == 0

    def test_decorator_cache_disabled(self, cached_step, pipeline_cache, sample_dataframe):
        """Test decorator works when caching is disabled."""
        result1 = cached_step(
            households=sample_dataframe, pipeline_cache=pipeline_cache, cache=False
        )
        result2 = cached_step(
            households=sample_dataframe, pipeline_cache=pipeline_cache, cache=False
        )

        # Results should be identical but both calls execute the function
        assert result1["households"].equals(result2["households"])

        # Verify no cache was created
        assert pipeline_cache.get_stats()["loaded"] == 0
        assert pipeline_cache.list_cached_steps() == []

    def test_decorator_without_pipeline_cache(self, cached_step, sample_dataframe):
        """Test decorator works when no pipeline_cache is provided."""
        result = cached_step(households=sample_dataframe, cache=True)
        assert "households" in result
        assert "hh_code" in result["households"].columns


class TestCacheIntegration:
    """Integration tests for caching in full pipeline context."""

    def test_multiple_output_tables_cached(self, pipeline_cache):
        """Test caching works with multiple output tables."""

        @step(validate_input=False, cache=True)
        def split_data(households: pl.DataFrame) -> dict[str, pl.DataFrame]:
            return {
                "households": households.filter(pl.col("hh_id") <= 2),
                "persons": pl.DataFrame({"person_id": [1, 2], "hh_id": [1, 2]}),
            }

        df = pl.DataFrame({"hh_id": [1, 2, 3]})

        # First call
        result1 = split_data(households=df, pipeline_cache=pipeline_cache, cache=True)

        # Second call - should use cache
        result2 = split_data(households=df, pipeline_cache=pipeline_cache, cache=True)

        assert result1["households"].equals(result2["households"])
        assert result1["persons"].equals(result2["persons"])

        # Verify both tables were cached
        cached_steps = pipeline_cache.list_cached_steps()
        assert len(cached_steps) == 1
        assert set(cached_steps[0]["tables"]) == {"households", "persons"}

    def test_cache_with_geodataframe(self, pipeline_cache):
        """Test caching with GeoDataFrame."""
        gdf = gpd.GeoDataFrame(
            {"id": [1, 2], "value": [10, 20]}, geometry=[Point(0, 0), Point(1, 1)], crs="EPSG:4326"
        )

        # Save and load
        pipeline_cache.save("test_step", "geo_key", {"locations": gdf})
        loaded = pipeline_cache.load("test_step", "geo_key")

        assert loaded is not None
        assert "locations" in loaded
        assert isinstance(loaded["locations"], gpd.GeoDataFrame)
        assert loaded["locations"].crs.to_string() == "EPSG:4326"

    @pytest.mark.parametrize(
        "corrupt",
        [
            pytest.param(
                lambda path: (path / "metadata.json").write_text("{ invalid json }"),
                id="unreadable-metadata",
            ),
            pytest.param(
                lambda path: (path / "metadata.json").unlink(),
                id="missing-metadata",
            ),
            pytest.param(
                lambda path: (path / "data.parquet").unlink(),
                id="missing-table-file",
            ),
        ],
    )
    def test_load_with_corrupted_cache(
        self, pipeline_cache, sample_dataframe, corrupt: Callable[[Path], None]
    ):
        """A damaged entry reads as a miss and is counted stale, never half-loaded."""
        step_name = "test_step"
        cache_key = "corrupt_key"
        pipeline_cache.save(step_name, cache_key, {"data": sample_dataframe})

        corrupt(pipeline_cache.cache_dir / step_name / cache_key)

        assert pipeline_cache.load(step_name, cache_key) is None
        assert pipeline_cache.get_stats() == {
            "loaded": 0,
            "missing": 0,
            "stale": 1,
            "total": 1,
            "load_rate": 0.0,
        }

    def test_list_cached_steps_skips_corrupted_metadata(self, pipeline_cache, sample_dataframe):
        """An entry whose metadata will not parse is dropped from the listing."""
        pipeline_cache.save("step1", "key1", {"data": sample_dataframe})
        pipeline_cache.save("step2", "key2", {"data": sample_dataframe})

        (pipeline_cache.cache_dir / "step2" / "key2" / "metadata.json").write_text("{ bad json }")

        assert [s["step_name"] for s in pipeline_cache.list_cached_steps()] == ["step1"]

    def test_save_with_exception_cleans_up(self, pipeline_cache):
        """Test that failed save cleans up partial cache."""
        step_name = "test_step"
        cache_key = "will_fail"

        # Create an object that can't be saved properly
        class SerializationError(RuntimeError):
            def __init__(self):
                super().__init__("Cannot serialize")

        class UnserializableObject:
            def __getstate__(self):
                raise SerializationError

        outputs = {"bad_data": UnserializableObject()}

        # This should catch the exception and clean up
        with contextlib.suppress(Exception):
            pipeline_cache.save(step_name, cache_key, outputs)

        # Cache directory for this key should not exist
        cache_path = pipeline_cache.cache_dir / step_name / cache_key
        assert not cache_path.exists()
