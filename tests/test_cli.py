"""The ``surveypipe`` command and ``run``, which every runner goes through."""

import os

import pytest
import yaml

from pipeline.cli import main, run

_STEP_FILE = """
import os

import polars as pl
from pipeline.decoration import step


@step(validate_input=False)
def make_households() -> dict[str, pl.DataFrame]:
    return {"households": pl.DataFrame({"root": [os.environ["SURVEY_ROOT"]]})}
"""


@pytest.fixture
def project(tmp_path):
    """A project folder below a .env, with one step from its own file.

    The .env sets ``SURVEY_ROOT``; it's removed afterwards, since load_dotenv
    writes it into the real environment.
    """
    (tmp_path / ".env").write_text("SURVEY_ROOT=from-dotenv\n", encoding="utf-8")
    folder = tmp_path / "projects" / "demo"
    folder.mkdir(parents=True)
    (folder / "steps.py").write_text(_STEP_FILE, encoding="utf-8")
    config = {"output_dir": "out", "steps": [{"name": "make_households", "file": "steps.py"}]}
    (folder / "config.yaml").write_text(yaml.safe_dump(config), encoding="utf-8")
    yield folder
    os.environ.pop("SURVEY_ROOT", None)


def test_run_reads_the_nearest_env_above_the_config(project):
    """A .env in a parent folder of the config reaches the run."""
    data = run(project / "config.yaml")

    assert data.households["root"].to_list() == ["from-dotenv"]


def test_the_command_reads_relative_paths_from_the_config_folder(project, tmp_path, monkeypatch):
    """Launched from another folder, a relative output_dir still lands beside the config.

    The resolved config is saved there, named after the project folder and the
    config file.
    """
    monkeypatch.chdir(tmp_path)

    main(["run", str(project / "config.yaml")])

    saved = project / "out" / "demo_config.yaml"
    assert yaml.safe_load(saved.read_text(encoding="utf-8"))["output_dir"] == "out"
