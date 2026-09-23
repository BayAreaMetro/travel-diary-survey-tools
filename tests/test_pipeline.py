"""Tests for the Pipeline runner: status reporting, get_data, args and run()."""

import logging
import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl
import pytest
import yaml

from pipeline.cache import PipelineCache
from pipeline.decoration import step
from pipeline.pipeline import Pipeline


@pytest.fixture
def temp_cache_dir(tmp_path):
    """A cache directory holding step1 (households, persons) and step2 (linked_trips)."""
    cache_dir = tmp_path / ".cache"
    cache = PipelineCache(cache_dir=cache_dir)

    cache.save(
        "step1",
        "abc123",
        {
            "households": pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]}),
            "persons": pl.DataFrame({"id": [1, 2], "value": [10, 20]}),
        },
    )
    cache.save(
        "step2",
        "def456",
        {"linked_trips": pl.DataFrame({"id": [1, 2, 3, 4], "trip": ["A", "B", "C", "D"]})},
    )

    return cache_dir


@pytest.fixture
def temp_config(tmp_path):
    """Create a temporary config file."""
    config = {
        "steps": [
            {"name": "step1", "cache": True},
            {"name": "step2", "cache": True},
            {"name": "step3", "cache": False},
        ]
    }

    config_path = tmp_path / "config.yaml"

    with config_path.open("w") as f:
        yaml.dump(config, f)

    return config_path


def _write_config(tmp_path, config: dict) -> str:
    """Write a config dict to disk and return its path."""
    config_path = tmp_path / "config.yaml"
    with config_path.open("w") as f:
        yaml.dump(config, f)
    return str(config_path)


class TestScanCache:
    """What the constructor works out about each step from the cache on disk."""

    def test_pipeline_status_with_cache(self, temp_config, temp_cache_dir, caplog):
        """Test that pipeline correctly reports status of cached steps."""
        caplog.set_level(logging.INFO)

        pipeline = Pipeline(config_path=str(temp_config), steps=[], caching=temp_cache_dir)

        assert pipeline._step_status["step1"] == {
            "has_cache": True,
            "cache_key": "abc123",
            "cache_enabled": True,
            "tables": ["households", "persons"],
        }
        assert pipeline._step_status["step2"] == {
            "has_cache": True,
            "cache_key": "def456",
            "cache_enabled": True,
            "tables": ["linked_trips"],
        }
        assert pipeline._step_status["step3"] == {
            "has_cache": False,
            "cache_key": None,
            "cache_enabled": False,
            "tables": [],
        }

        assert "Pipeline Status" in caplog.text

    def test_pipeline_status_no_cache(self, temp_config, tmp_path):
        """Test status when no cache exists."""
        cache_dir = tmp_path / "empty_cache"
        cache_dir.mkdir()

        pipeline = Pipeline(config_path=str(temp_config), steps=[], caching=cache_dir)

        assert pipeline._step_status["step1"]["has_cache"] is False
        assert pipeline._step_status["step2"]["has_cache"] is False

    def test_multiple_cache_keys_uses_newest(self, temp_config, temp_cache_dir):
        """Test that when multiple cache keys exist, newest is used."""
        old_cache = temp_cache_dir / "step1" / "old999"
        PipelineCache(cache_dir=temp_cache_dir).save(
            "step1", "old999", {"households": pl.DataFrame({"id": [99], "name": ["old"]})}
        )

        # Make the old cache older by modification time
        old_time = time.time() - 3600  # 1 hour ago
        os.utime(old_cache, (old_time, old_time))

        pipeline = Pipeline(config_path=str(temp_config), steps=[], caching=temp_cache_dir)

        # Should use abc123 (newer), not old999
        assert pipeline._step_status["step1"]["cache_key"] == "abc123"
        assert len(pipeline.get_data("households")) == 3  # From abc123, not 1 from old999

    def test_a_corrupt_newest_cache_shadows_a_good_older_one(self, temp_config, temp_cache_dir):
        """The newest cache directory wins even when its metadata cannot be read.

        A run stopped mid-save leaves exactly this, since ``metadata.json`` is
        written last. The older good cache beside it is then ignored and the table
        reads as absent. Caches only speed up development, so the remedy is to
        clear ``.cache`` after an interrupted run; this pins the behaviour so a
        change to it is deliberate.
        """
        corrupt = temp_cache_dir / "step1" / "corrupt123"
        corrupt.mkdir(parents=True)
        (corrupt / "metadata.json").write_text("{ invalid json }")

        now = time.time()
        os.utime(temp_cache_dir / "step1" / "abc123", (now - 3600, now - 3600))
        os.utime(corrupt, (now, now))

        pipeline = Pipeline(config_path=str(temp_config), steps=[], caching=temp_cache_dir)

        assert pipeline._step_status["step1"] == {
            "has_cache": True,
            "cache_key": "corrupt123",
            "cache_enabled": True,
            "tables": [],
        }

        # The good abc123 cache is now unreachable, and the message says the
        # table is missing rather than that a cache entry is damaged.
        with pytest.raises(ValueError, match="not found in any cached step") as excinfo:
            pipeline.get_data("households")
        assert "Available tables: linked_trips" in str(excinfo.value)

    def test_report_status_with_no_steps(self, tmp_path):
        """Test status report with empty config."""
        pipeline = Pipeline(
            config_path=_write_config(tmp_path, {"steps": []}), steps=[], caching=False
        )

        # Should not crash with empty steps
        pipeline.report_status()

    @pytest.mark.parametrize(
        "build",
        [
            pytest.param(lambda tmp: (str(tmp / "custom_cache"), tmp / "custom_cache"), id="str"),
            pytest.param(lambda tmp: (tmp / "custom_cache", tmp / "custom_cache"), id="path"),
            pytest.param(lambda _: (True, Path(".cache")), id="true"),
        ],
    )
    def test_pipeline_caching_argument_forms(
        self, temp_config, tmp_path, monkeypatch, build: Callable[[Path], tuple[Any, Path]]
    ):
        """A string, a Path or ``True`` all land on a cache rooted where they say.

        ``caching=True`` creates ``.cache`` under the working directory, so the
        working directory is moved before the run rather than the repository
        collecting a cache directory from a test.
        """
        monkeypatch.chdir(tmp_path)
        caching, expected = build(tmp_path)

        pipeline = Pipeline(config_path=str(temp_config), steps=[], caching=caching)

        assert pipeline.cache is not None
        assert pipeline.cache.cache_dir == expected


class TestGetData:
    """Reading a table back out of the cache, by table or by step."""

    def test_get_data_from_latest_step(self, temp_config, temp_cache_dir):
        """Test fetching data from latest step that has the table."""
        pipeline = Pipeline(config_path=str(temp_config), steps=[], caching=temp_cache_dir)

        households = pipeline.get_data("households")

        assert isinstance(households, pl.DataFrame)
        assert households["id"].to_list() == [1, 2, 3]
        assert households["name"].to_list() == ["a", "b", "c"]

        # Fetching also parks the table on the pipeline's canonical data.
        assert pipeline.data.households is households

    def test_get_data_from_specific_step(self, temp_config, temp_cache_dir):
        """Test fetching data from a specific step."""
        pipeline = Pipeline(config_path=str(temp_config), steps=[], caching=temp_cache_dir)

        persons = pipeline.get_data("persons", step="step1")

        assert persons["value"].to_list() == [10, 20]

    @pytest.mark.parametrize(
        ("kwargs", "match", "also_says"),
        [
            pytest.param(
                {"table_name": "nonexistent_table"},
                "not found in any cached step",
                "Available tables: households, linked_trips, persons",
                id="table-in-no-step",
            ),
            pytest.param(
                {"table_name": "households", "step": "step3"},
                "has no cached data",
                "step3",
                id="step-has-no-cache",
            ),
            pytest.param(
                {"table_name": "linked_trips", "step": "step1"},
                "not found in step",
                "Available tables: households, persons",
                id="table-not-in-that-step",
            ),
        ],
    )
    def test_get_data_errors_name_what_is_available(
        self, temp_config, temp_cache_dir, kwargs: dict, match: str, also_says: str
    ):
        """Every failure says what went wrong and what could have been asked for."""
        pipeline = Pipeline(config_path=str(temp_config), steps=[], caching=temp_cache_dir)

        with pytest.raises(ValueError, match=match) as excinfo:
            pipeline.get_data(**kwargs)

        assert also_says in str(excinfo.value)

    def test_get_data_no_caching_enabled(self, temp_config):
        """With caching off, get_data reads the in-memory tables and nothing else."""
        pipeline = Pipeline(config_path=str(temp_config), steps=[], caching=False)

        with pytest.raises(ValueError, match=r"Table 'households' not found in canonical data."):
            pipeline.get_data("households")

        pipeline.data.households = pl.DataFrame({"id": [1]})
        assert pipeline.get_data("households")["id"].to_list() == [1]


class TestParseStepArgs:
    """Assembling a step's keyword arguments from the config and canonical data."""

    def test_parse_step_args_with_canonical_data(self, temp_config):
        """Test parse_step_args passes canonical_data parameter."""

        def step_func(canonical_data):
            pass

        pipeline = Pipeline(config_path=str(temp_config), steps=[step_func], caching=False)

        kwargs = pipeline.parse_step_args("step1", step_func)

        assert kwargs["canonical_data"] is pipeline.data

    def test_parse_step_args_with_table_names(self, temp_config):
        """Test parse_step_args extracts table data from canonical data."""

        def step_func(households, persons):
            pass

        pipeline = Pipeline(config_path=str(temp_config), steps=[step_func], caching=False)

        pipeline.data.households = pl.DataFrame({"id": [1, 2]})
        pipeline.data.persons = pl.DataFrame({"id": [1, 2]})

        kwargs = pipeline.parse_step_args("step_func", step_func)

        assert kwargs["households"] is pipeline.data.households
        assert kwargs["persons"] is pipeline.data.persons

    def test_parse_step_args_missing_required_parameter(self, tmp_path):
        """Test parse_step_args raises error for missing required params."""
        config_path = _write_config(tmp_path, {"steps": [{"name": "test_step", "params": {}}]})

        def step_func(required_param):  # No default value
            pass

        pipeline = Pipeline(config_path=config_path, steps=[step_func], caching=False)

        with pytest.raises(ValueError, match="Missing required parameter 'required_param'"):
            pipeline.parse_step_args("test_step", step_func)

    def test_parse_step_args_with_config_params(self, tmp_path):
        """Test parse_step_args extracts parameters from config."""
        config_path = _write_config(
            tmp_path,
            {"steps": [{"name": "test_step", "params": {"threshold": 0.5, "mode": "strict"}}]},
        )

        def step_func(threshold, mode):
            pass

        pipeline = Pipeline(config_path=config_path, steps=[step_func], caching=False)

        kwargs = pipeline.parse_step_args("test_step", step_func)

        assert kwargs["threshold"] == 0.5
        assert kwargs["mode"] == "strict"

    def test_parse_step_args_with_defaults(self, tmp_path):
        """A parameter the config is silent about is left to the function's default."""
        config_path = _write_config(tmp_path, {"steps": [{"name": "test_step", "params": {}}]})

        def step_func(optional_param="default_value"):
            pass

        pipeline = Pipeline(config_path=config_path, steps=[step_func], caching=False)

        assert "optional_param" not in pipeline.parse_step_args("test_step", step_func)


class TestRun:
    """Executing the configured steps."""

    def test_pipeline_run_with_simple_step(self, tmp_path):
        """Test pipeline.run() executes steps correctly."""
        config_path = _write_config(tmp_path, {"steps": [{"name": "simple_step", "cache": False}]})

        step_executed = []

        def simple_step(canonical_data, **kwargs):  # noqa: ARG001
            step_executed.append(True)
            canonical_data.households = pl.DataFrame({"id": [1, 2, 3]})

        pipeline = Pipeline(config_path=config_path, steps=[simple_step], caching=False)

        result = pipeline.run()

        assert len(step_executed) == 1
        assert result.households["id"].to_list() == [1, 2, 3]

    def test_pipeline_run_missing_step_reports_all_and_runs_nothing(self, tmp_path):
        """An unregistered step aborts the run before any step executes.

        Resolution happens up front, so a config naming a step the runner never
        passed fails immediately rather than part-way through a long pipeline, and
        every unresolved name is listed at once instead of one per re-run.
        """
        config_path = _write_config(
            tmp_path,
            {"steps": [{"name": "good_step"}, {"name": "missing_one"}, {"name": "missing_two"}]},
        )

        executed = []

        def good_step(canonical_data, **kwargs):  # noqa: ARG001
            executed.append("good_step")

        pipeline = Pipeline(config_path=config_path, steps=[good_step], caching=False)

        with pytest.raises(ValueError, match="not found in pipeline steps") as exc_info:
            pipeline.run()

        message = str(exc_info.value)
        # Both unresolved names reported together, not just the first.
        assert "missing_one" in message
        assert "missing_two" in message
        # The registered steps are listed so the fix is obvious from the error.
        assert "good_step" in message
        # Nothing ran: the failure precedes execution rather than interrupting it.
        assert executed == []

    @pytest.mark.parametrize("absolute", [False, True], ids=["relative", "absolute"])
    def test_pipeline_run_with_log_file(self, tmp_path, monkeypatch, absolute: bool):
        """A relative log path lands under ``.cache``; an absolute one is taken as given.

        The working directory is moved first because the relative case creates
        ``.cache/`` wherever the run happens, which used to be the repository.
        """
        monkeypatch.chdir(tmp_path)
        log_file = tmp_path / "test.log" if absolute else Path(".cache") / "pipeline.log"
        configured = str(log_file) if absolute else "pipeline.log"

        config_path = _write_config(
            tmp_path,
            {"log_file": configured, "steps": [{"name": "test_step", "cache": False}]},
        )

        def test_step(canonical_data, **kwargs):  # noqa: ARG001
            canonical_data.households = pl.DataFrame({"id": [1]})

        Pipeline(config_path=config_path, steps=[test_step], caching=False).run()

        assert log_file.exists()

    def test_pipeline_run_with_caching_statistics(self, tmp_path):
        """A second run over the same inputs is served from cache, and says so."""
        cache_dir = tmp_path / "cache"
        config_path = _write_config(tmp_path, {"steps": [{"name": "cached_step", "cache": True}]})

        # Only a dict return is cacheable, so a step that mutates canonical_data
        # in place and returns nothing would never populate the cache at all.
        @step()
        def cached_step(canonical_data):  # noqa: ARG001
            return {"households": pl.DataFrame({"id": [1, 2]})}

        first = Pipeline(config_path=config_path, steps=[cached_step], caching=cache_dir)
        result1 = first.run()

        assert first.cache.get_stats()["loaded"] == 0
        assert first.cache.get_stats()["missing"] == 1

        second = Pipeline(config_path=config_path, steps=[cached_step], caching=cache_dir)
        result2 = second.run()

        assert second.cache.get_stats()["loaded"] == 1
        assert second.cache.get_stats()["missing"] == 0
        assert result2.households.equals(result1.households)
