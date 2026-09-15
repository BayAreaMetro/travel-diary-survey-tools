"""Tests for assembling the run's diagnostics document.

One report covers the whole run, so the cases that matter are the ones about
*how many fits* it describes: a single un-profiled fit must still render, and
several fits must each get their own pane without the run-level sections being
repeated.
"""

import polars as pl
import pytest

from processing.weighting.core.specs import (
    ControlTotals,
    GeographyCoverage,
    ImputationSummary,
    ProfileFit,
    ZoneStatus,
)
from processing.weighting.diagnostics.report import generate_report

TARGETS = ["h_total", "p_total"]
ZONE = "01"


def _control_totals() -> ControlTotals:
    """Targets for one zone at both structural controls."""
    return ControlTotals(
        totals=pl.DataFrame(
            {
                "geo_id": [ZONE, ZONE],
                "control_name": ["h_total", "p_total"],
                "category": ["total", "total"],
                "target_total": [1000.0, 2500.0],
            }
        ),
        pums_hh_count=500,
        pums_person_count=1200,
        geo_ids=[ZONE],
    )


def _fit(profile: str | None, *, n_hh: int = 4) -> ProfileFit:
    """A completed fit carrying the minimum every report section reads."""
    hh_ids = list(range(1, n_hh + 1))
    seed = pl.DataFrame(
        {
            "hh_id": hh_ids,
            "ctrl_geoid": [ZONE] * n_hh,
            "study_geoid": [ZONE] * n_hh,
            "h_total": [1.0] * n_hh,
            "p_total": [2.0] * n_hh,
            "base_weight": [200.0] * n_hh,
        }
    )
    return ProfileFit(
        profile=profile,
        usability_flag_col=f"usable_{profile}" if profile else "survey_complete",
        seed_incidence=seed,
        pre_imputation_incidence=seed,
        imputation_summary=[ImputationSummary("h_size", "household", n_hh, 1, 0.4, 0.8)],
        coverage=GeographyCoverage(profile=profile, n_universe=n_hh, n_placed=n_hh),
        weights=pl.DataFrame({"hh_id": hh_ids, "hh_weight": [250.0] * n_hh}),
        statuses=[
            ZoneStatus(geo_id=ZONE, converged=True, iterations=7, delta=1e-9, max_gamma_diff=1e-9)
        ],
    )


def _tables(profiles: list[str | None]) -> dict[str, pl.DataFrame]:
    """Propagated canonical tables carrying one weight column per profile."""
    households = pl.DataFrame({"hh_id": [1, 2, 3, 4]})
    persons = pl.DataFrame({"person_id": [1, 2, 3, 4], "hh_id": [1, 1, 2, 3]})
    for profile in profiles:
        suffix = f"_{profile}" if profile else ""
        flag = f"usable_{profile}" if profile else "survey_complete"
        households = households.with_columns(
            pl.Series(flag, [True, True, True, False]),
            pl.Series(f"hh_weight{suffix}", [250.0, 250.0, 250.0, 0.0]),
        )
        persons = persons.with_columns(
            pl.Series(flag, [True, True, True, True]),
            pl.Series(f"person_weight{suffix}", [250.0, 250.0, 250.0, 250.0]),
        )
    return {"households": households, "persons": persons}


class TestGenerateReport:
    """The document a run writes, however many profiles it fitted."""

    def test_single_unprofiled_fit_renders(self, tmp_path):
        """A run weighting the whole survey collapses the toggle to one pane."""
        out = generate_report(
            fits={None: _fit(None)},
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables([None]),
            output_path=tmp_path / "diagnostics.html",
        )
        html = out.read_text(encoding="utf-8")
        assert html.count('class="profile-pane"') == 1
        assert 'data-profile="survey"' in html

    def test_one_pane_per_fit(self, tmp_path):
        """Each profile gets a pane and a button; run-level sections appear once."""
        fits = {p: _fit(p) for p in ("ctramp", "daysim", "analysis")}
        out = generate_report(
            fits=fits,
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables(list(fits)),
            output_path=tmp_path / "diagnostics.html",
        )
        html = out.read_text(encoding="utf-8")

        assert html.count('class="profile-pane"') == 3
        for profile in fits:
            assert f'data-profile="{profile}"' in html

        # The run-level sections are written once, not once per fit.
        assert html.count("Profile Comparison") == 1
        assert html.count("Weight Cascade") == 1
        assert html.count("Crosswalk Map") == 1

    def test_each_pane_names_its_profile(self, tmp_path):
        """A cropped screenshot of any per-profile section still says what it shows."""
        out = generate_report(
            fits={"ctramp": _fit("ctramp")},
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables(["ctramp"]),
            output_path=tmp_path / "diagnostics.html",
        )
        html = out.read_text(encoding="utf-8")
        assert html.count('<span class="profile-name">ctramp</span>') >= 3

    def test_cascade_reports_every_profile(self, tmp_path):
        """The cascade is run-level: all profiles in one table, not behind the toggle."""
        fits = {p: _fit(p) for p in ("ctramp", "analysis")}
        out = generate_report(
            fits=fits,
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables(list(fits)),
            output_path=tmp_path / "diagnostics.html",
        )
        html = out.read_text(encoding="utf-8")
        cascade = html.split("Weight Cascade")[1].split("Balancer Performance")[0]
        assert 'colspan="3">ctramp' in cascade
        assert 'colspan="3">analysis' in cascade

    def test_run_meta_is_rendered(self, tmp_path):
        """A report shared detached from its run still identifies the run."""
        out = generate_report(
            fits={"ctramp": _fit("ctramp")},
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables(["ctramp"]),
            output_path=tmp_path / "diagnostics.html",
            run_meta={"PUMS": "2023 &middot; FIPS 06"},
        )
        html = out.read_text(encoding="utf-8")
        assert "2023 &middot; FIPS 06" in html

    def test_no_fits_raises(self, tmp_path):
        """An empty run has nothing to describe; say so rather than write a shell."""
        with pytest.raises(ValueError, match="at least one completed fit"):
            generate_report(
                fits={},
                control_totals=_control_totals(),
                target_names=TARGETS,
                tables={},
                output_path=tmp_path / "diagnostics.html",
            )
