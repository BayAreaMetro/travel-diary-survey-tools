"""Fixtures for E2E integration tests.

Generates all data on the fly into a temp directory, runs each pipeline
configuration once (session scope), then cleans up.
"""

import json
import shutil
import tempfile
from pathlib import Path

import pytest

E2E_DIR = Path(__file__).parent


def _materialize_data(tmp: Path):
    """Write generated data to temp directory, return the data root."""
    from tests.e2e.generate_toy_data import (
        ZONE_GEOJSON,
        build_pums_dataframes,
        build_survey_dataframes,
    )

    # Survey tables (parquet preserves datetime types)
    survey_dir = tmp / "survey"
    survey_dir.mkdir(parents=True)
    for name, df in build_survey_dataframes().items():
        df.write_parquet(survey_dir / f"{name}.parquet")

    # PUMS (CSV -- expected by compute_weights)
    pums_dir = tmp / "pums"
    pums_dir.mkdir()
    pums_hh, pums_per = build_pums_dataframes()
    pums_hh.write_csv(pums_dir / "psam_h06.csv")
    pums_per.write_csv(pums_dir / "psam_p06.csv")

    # Zones GeoJSON
    zones_dir = tmp / "zones"
    zones_dir.mkdir()
    (zones_dir / "test_zones.geojson").write_text(json.dumps(ZONE_GEOJSON))

    return tmp


def _render_config(template_name: str, data_dir: Path, output_dir: Path) -> Path:
    """Render a YAML config template with concrete paths."""
    raw = (E2E_DIR / template_name).read_text()
    raw = raw.replace("{{ DATA_DIR }}", str(data_dir).replace("\\", "/"))
    raw = raw.replace("{{ OUTPUT_DIR }}", str(output_dir).replace("\\", "/"))
    rendered = output_dir / template_name
    rendered.write_text(raw)
    return rendered


def _run_pipeline(config_template: str, year: str):
    from pipeline.pipeline import Pipeline
    from processing import (
        add_zone_ids,
        compute_weights,
        detect_joint_trips,
        extract_tours,
        format_ctramp,
        format_daysim,
        link_trips,
        load_data,
        write_data,
    )

    tmp = Path(tempfile.mkdtemp(prefix=f"e2e_{year}_"))
    data_dir = _materialize_data(tmp / "data")
    output_dir = tmp / "output"
    output_dir.mkdir()

    config_path = _render_config(config_template, data_dir, output_dir)

    steps = [
        load_data,
        link_trips,
        detect_joint_trips,
        extract_tours,
        add_zone_ids,
        format_ctramp,
        format_daysim,
        compute_weights,
        write_data,
    ]

    from data_canon.models import daysim as daysim_models

    data_models = {
        "households_daysim": daysim_models.HouseholdDaysimModel,
        "persons_daysim": daysim_models.PersonDaysimModel,
        "days_daysim": daysim_models.PersonDayDaysimModel,
        "linked_trips_daysim": daysim_models.LinkedTripDaysimModel,
        "tours_daysim": daysim_models.TourDaysimModel,
    }

    if year == "2023":
        from data_canon.models import ctramp as ctramp_models

        data_models.update(
            {
                "households_ctramp": ctramp_models.HouseholdCTRAMPModel,
                "persons_ctramp": ctramp_models.PersonCTRAMPModel,
                "mandatory_locations_ctramp": ctramp_models.MandatoryLocationCTRAMPModel,
                "individual_tours_ctramp": ctramp_models.IndividualTourCTRAMPModel,
                "individual_trips_ctramp": ctramp_models.IndividualTripCTRAMPModel,
                "joint_tours_ctramp": ctramp_models.JointTourCTRAMPModel,
                "joint_trips_ctramp": ctramp_models.JointTripCTRAMPModel,
            }
        )

    pipeline = Pipeline(
        config_path=config_path,
        steps=steps,
        caching=False,
        data_models=data_models,
    )

    result = pipeline.run()
    return result, output_dir, tmp


@pytest.fixture(scope="session")
def pipeline_2019():
    result, output_dir, tmp = _run_pipeline("config_2019.yaml", "2019")
    yield result, output_dir
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture(scope="session")
def pipeline_2023():
    result, output_dir, tmp = _run_pipeline("config_2023.yaml", "2023")
    yield result, output_dir
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture(scope="session")
def result_2019(pipeline_2019):
    return pipeline_2019[0]


@pytest.fixture(scope="session")
def output_dir_2019(pipeline_2019):
    return pipeline_2019[1]


@pytest.fixture(scope="session")
def result_2023(pipeline_2023):
    return pipeline_2023[0]


@pytest.fixture(scope="session")
def output_dir_2023(pipeline_2023):
    return pipeline_2023[1]
