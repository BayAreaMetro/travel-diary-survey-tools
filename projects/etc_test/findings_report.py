"""The catalogue of vendor data checks, and the report that presents them.

**This module does not check anything.**  The checking lives in
``conformity/check_vendor_data.py``; what lives here is the *description* of
each check and the code that turns a table of findings into a document.

The flow is:

1. ``conformity/check_vendor_data.py`` reads the raw vendor tables and emits
   one row per finding: ``check``, ``severity``, ``hh_id``, ``person_num``,
   ``trip_num``, ``detail``.  ``detail`` carries the evidence for that one row
   (the age, the two timestamps, the counts that disagree).
2. This module holds :data:`CHECKS`, one entry per check name, saying what the
   check looks for, **how** it looks for it, and what the finding costs us.
   That is the part a reader -- or the vendor -- needs in order to act on a
   finding or to argue with it.
3. :func:`render_markdown` joins the two and writes the report.

The detection thresholds below are defined here rather than in the checker so
the sentence describing a rule and the number the rule actually uses cannot
drift apart; ``check_vendor_data`` imports them from this module.

Run this file directly to rebuild the report from the findings CSV the
pipeline already wrote -- useful after editing a description here, since it
does not re-run the checks.
"""

import argparse
import json
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import yaml

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "config.yaml"
ISSUES_FILENAME = "data_issues.csv"
CODEBOOK_FILENAME = "codebook.csv"

# --- Detection thresholds -----------------------------------------------------
# Imported by conformity/check_vendor_data.py, so these values are both what
# the checks enforce and what the descriptions below quote.

#: Youngest age at which a driving licence is possible in California.
MIN_DRIVING_AGE = 16
#: How far apart two people's arrivals at one place may be before we stop
#: believing they travelled there together.
COMPANION_TOLERANCE_MINUTES = 2
# Past this gap the nearest companion visit is a different visit altogether, so
# the claim is an unmatched companion rather than a quarrel about the clock.
COMPANION_MATCH_WINDOW_MINUTES = 60
#: Decimal places at which two coordinates count as the same place (~1 metre).
PLACE_MATCH_DECIMALS = 5


SEVERITY_ORDER = ("error", "warning")

SEVERITY_BLURB = {
    "error": (
        "The record cannot be used as filed. It either fails canonical validation "
        "outright or it silently describes travel that did not happen."
    ),
    "warning": (
        "The record survives, but a question we depend on went unanswered, so it "
        "drops out of a later reduction."
    ),
}


@dataclass(frozen=True)
class Check:
    """What one check looks for, how it looks, and why the answer matters.

    Attributes:
        summary: What the finding means, in one sentence.
        detection: The rule as executed -- the columns read, the comparison
            made, and any threshold -- so a finding can be reproduced or
            disputed without reading the code.
        cost: What accepting the record as filed would cost us downstream.
        source: The function in ``conformity/check_vendor_data.py`` that
            implements it.
    """

    summary: str
    detection: str
    cost: str
    source: str


CHECKS: dict[str, Check] = {
    "driver_under_driving_age": Check(
        summary="Somebody below driving age is recorded as the driver of a vehicle trip.",
        detection=(
            f"Take diary rows where `Person Was Driver` is 1 (Driver), join them to the "
            f"person sheet on Sample Number and Person Number, and keep those whose "
            f"`Age` is under {MIN_DRIVING_AGE}."
        ),
        cost=(
            "Driver status feeds vehicle occupancy, auto-ownership models and the "
            "CT-RAMP driver flags, so the trip cannot be assigned as filed."
        ),
        source="check_drivers",
    ),
    "driver_not_licensed": Check(
        summary="The traveller drove but was never reported as a licensed driver.",
        detection=(
            f"The same joined rows, aged {MIN_DRIVING_AGE} or over, whose `Licensed "
            f"Driver` answer is anything other than 1 (Yes) -- including unanswered."
        ),
        cost="Either the licence question or the driver flag is wrong.",
        source="check_drivers",
    ),
    "travel_flag_contradicts_diary": Check(
        summary=(
            "The person-level travel question and the filed diary disagree about "
            "whether this person travelled."
        ),
        detection=(
            "Count each person's diary rows with `Trip Number` above 0 (row 0 records "
            "where the day began and is not a movement). Flag `Travel` = 2 (No) with a "
            "count above zero, and `Travel` = 1 (Yes) with a count of zero."
        ),
        cost=(
            "Day completeness is derived from both answers, so the day gets certified "
            "on a contradiction."
        ),
        source="check_travel_flag",
    ),
    "no_travel_without_reason": Check(
        summary="A non-travel day was reported with no reason given.",
        detection="Persons whose `Travel` is 2 (No) and whose `Why No Travel` is empty.",
        cost=(
            "A day only counts as surveyed when the traveller either filed complete "
            "trips or said why they stayed put, so these days leave the usable sample."
        ),
        source="check_travel_flag",
    ),
    "unlabelled_movement": Check(
        summary="A movement carries timestamps and coordinates but no activity code and no mode.",
        detection=(
            "Diary rows with `Trip Number` above 0 where `Activity Type Code` or "
            "`Mode of Travel` is empty."
        ),
        cost=(
            "Purpose and mode can only be recorded as MISSING, and trips like this "
            "cannot be linked into tours or assigned to a mode."
        ),
        source="check_diary_labelling",
    ),
    "movement_returns_to_origin": Check(
        summary="The movement ends where it began, so it measures as zero distance.",
        detection=(
            "Within each person, sorted by `Trip Number`, compare each row's `Location "
            "Latitude` and `Location Longitude` against the previous row's and flag "
            "exact equality."
        ),
        cost=(
            "Not necessarily an error: a walk or jog around the block legitimately "
            "returns to its origin, and the vendor is right to record it. It is "
            "reported so the reader knows any distance-based logic sees zero length "
            "here, and so a genuinely missing intermediate destination is not hidden "
            "among the loops. Check the mode and activity before treating one as a fault."
        ),
        source="check_geometry",
    ),
    "arrival_before_departure": Check(
        summary="The arrival precedes the departure it followed, which no trip can do.",
        detection=(
            "Combine `Date` with `Arrival Time` and `Departure Time` into timestamps, "
            "then flag rows whose arrival falls before the previous row's departure."
        ),
        cost="Trip duration goes negative and the day's sequence cannot be trusted.",
        source="check_geometry",
    ),
    "carpool_mode_contradicts_party": Check(
        summary=(
            "The mode says household members only, but the same row counts "
            "non-household companions."
        ),
        detection=(
            "Rows whose `Mode of Travel` is 'carpool with only family/household "
            "member(s)' and whose `Number of People` -- the count of companions from "
            "outside the household -- is above zero."
        ),
        cost=("The two answers cannot both be right, and together they set vehicle occupancy."),
        source="check_travel_party",
    ),
    "companion_filed_no_matching_trip": Check(
        summary=(
            "One traveller names a household member as travelling with them, but that "
            "member filed no arrival at the destination."
        ),
        detection=(
            f"Split `HH Members` on ';' into the named companions. For each, look for a "
            f"row that companion filed on the same `Date` at the same place, with "
            f"coordinates rounded to {PLACE_MATCH_DECIMALS} decimals (about a metre, "
            f"which absorbs the precision drift below). Flag when nothing matches."
        ),
        cost=(
            "Joint-trip detection groups members by shared place and time, so the "
            "group cannot form."
        ),
        source="check_travel_party",
    ),
    "companion_arrival_disagrees": Check(
        summary=(
            "Two household members who report travelling together record different "
            "arrival times at the same place."
        ),
        detection=(
            f"Of the companion's visits to that place, keep the one closest in time. If "
            f"that visit is more than {COMPANION_MATCH_WINDOW_MINUTES} minutes away it "
            f"is a different visit and the finding becomes "
            f"`companion_filed_no_matching_trip` instead; within the window, flag when "
            f"the two arrival timestamps differ by more than "
            f"{COMPANION_TOLERANCE_MINUTES} minutes. Each pair is reported once, not "
            f"once per traveller."
        ),
        cost=("Joint-trip detection matches on coincident timing and will not group them."),
        source="check_travel_party",
    ),
    "companion_mode_disagrees": Check(
        summary="Two household members report different modes for the same shared trip.",
        detection=(
            "Compare `Mode of Travel` on the row against the same closest-in-time "
            "matched visit filed by the named companion, and flag any difference."
        ),
        cost="One vehicle cannot be two modes, and mode drives assignment.",
        source="check_travel_party",
    ),
    "place_coordinate_precision_varies": Check(
        summary="One address is stored at more than one coordinate, differing only in precision.",
        detection=(
            f"Group diary rows by household and `Location Address`, then flag addresses "
            f"holding more than one distinct latitude that collapse to a single value "
            f"once rounded to {PLACE_MATCH_DECIMALS} decimals."
        ),
        cost=(
            "Anything that identifies a place by its coordinates -- our joint-trip "
            "detection included -- stops seeing the visits as one place."
        ),
        source="check_place_geocoding",
    ),
    "person_roster_incomplete": Check(
        summary="The household reported more members than it filed person rows for.",
        detection=(
            "Compare each household's `Number Persons` against the count of person "
            "rows carrying its Sample Number."
        ),
        cost="The household is under-enumerated for weighting.",
        source="check_rosters",
    ),
    "vehicle_roster_incomplete": Check(
        summary="The household reported more vehicles than it filed vehicle rows for.",
        detection=(
            "Compare each household's `Vehicles Available` against the count of "
            "vehicle rows carrying its Sample Number."
        ),
        cost="The vehicle roster is incomplete, so vehicle attributes are unavailable.",
        source="check_rosters",
    ),
    "employed_without_workplace": Check(
        summary="Somebody is reported as employed but has nowhere to work.",
        detection=(
            "Persons with `Employment` = 1 (Yes), no `Primary Workplace Latitude`, and "
            "a `Current Work Location` that is either empty or something other than "
            "'work ONLY from home' -- which would explain the missing location."
        ),
        cost="They cannot be given a mandatory work location.",
        source="check_person_blocks",
    ),
    "student_without_school": Check(
        summary="Somebody is reported as a student but has no school recorded.",
        detection=(
            "Persons with `Student Status` = 1 (Yes), no `School Latitude`, no `School "
            "Type`, and `Online School` not 1 (Yes)."
        ),
        cost="They cannot be given a school location.",
        source="check_person_blocks",
    ),
    "missing_household_role": Check(
        summary="No relationship to the primary respondent was recorded.",
        detection="Persons whose `Relationship` is empty.",
        cost="Household structure is unknown for this person.",
        source="check_person_blocks",
    ),
    "missing_age": Check(
        summary="Neither an age nor an age band was recorded.",
        detection="Persons with neither `Age` nor `Age Category`.",
        cost=(
            "The canonical age band has no missing code, so the person cannot be "
            "represented at all."
        ),
        source="check_person_blocks",
    ),
}


def _cell(value: object) -> str:
    """Render one value for a Markdown cell, keeping blanks visibly blank."""
    if value is None or value == "":
        return "_(blank)_"
    return str(value).replace("|", "\\|")


def _parse_evidence(row: dict) -> dict[str, str]:
    """Decode one finding's evidence JSON, tolerating checks that carry none."""
    raw = row.get("evidence")
    if raw is None or raw == "":
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError) as exc:  # a malformed payload must never pass silently
        msg = f"Finding for check {row.get('check')!r} has unreadable evidence: {raw!r}"
        raise ValueError(msg) from exc
    # Nulls are kept: for checks about a missing answer the empty field *is* the
    # evidence, and dropping it would hide the point of the finding.
    return parsed


def _table(rows: pl.DataFrame) -> list[str]:
    """Render the findings of one check as a Markdown table.

    The values that triggered each finding become their own columns, so the
    reader sees the offending record rather than only a sentence about it.
    Column names are the vendor's own, so they can be looked up in the codebook
    appendix and in the vendor data dictionary.
    """
    evidence = [_parse_evidence(row) for row in rows.iter_rows(named=True)]
    fields: list[str] = []
    for item in evidence:
        fields.extend(k for k in item if k not in fields)

    header = ["Household", "Person", "Trip", *fields]
    out = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join("---" for _ in header) + " |",
    ]
    for row, item in zip(rows.iter_rows(named=True), evidence, strict=True):
        out.append(
            "| "
            + " | ".join(
                [
                    str(row["hh_id"]),
                    "" if row["person_num"] is None else str(row["person_num"]),
                    "" if row["trip_num"] is None else str(row["trip_num"]),
                    *(_cell(item.get(f)) for f in fields),
                ]
            )
            + " |"
        )
    return out


def _validate(issues: pl.DataFrame) -> None:
    """Refuse to render anything that would mislead the reader.

    Raises:
        ValueError: The frame did not come from ``check_etc_data``.
        KeyError: A check has no entry in :data:`CHECKS`, so its findings would
            arrive with no statement of what they mean or how they were found.
    """
    missing_columns = sorted({"check", "severity", "hh_id", "detail"} - set(issues.columns))
    if missing_columns:
        msg = (
            f"Findings frame is missing {missing_columns}; it did not come from "
            f"check_etc_data. Columns present: {issues.columns}"
        )
        raise ValueError(msg)

    undescribed = sorted(set(issues["check"].to_list()) - set(CHECKS))
    if undescribed:
        msg = (
            f"No CHECKS entry for {undescribed}. Every check must say what it looks "
            f"for, how it looks, and what it costs before its findings can be reported."
        )
        raise KeyError(msg)

    if issues.is_empty():
        logger.warning(
            "No findings to report. That means either the extract is clean or the "
            "checks matched nothing -- confirm which before reading it as a pass."
        )


def render_markdown(
    issues: pl.DataFrame,
    source: str | None = None,
    codebook_path: Path | None = None,
) -> str:
    """Render the findings table as a Markdown report.

    Args:
        issues: The ``etc_data_issues`` frame from ``check_etc_data``.
        source: Optional description of the extract the findings came from.
        codebook_path: The vendor `codebook.csv`, used to decode the coded
            values the findings quote. When omitted the report says the
            values are undecoded rather than quietly leaving them bare.

    Returns:
        The report as one Markdown string.
    """
    _validate(issues)

    generated = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    counts = Counter(issues["severity"].to_list())
    n_households = issues["hh_id"].n_unique()

    lines = [
        "# ETC vendor test data: data quality findings",
        "",
        f"{issues.height} findings across {n_households} households "
        f"({counts.get('error', 0)} error, {counts.get('warning', 0)} warning).",
        "",
        "Every finding below is a contradiction *within the vendor's own tables*: it "
        "can be checked and corrected without reference to our schema or our codebook. "
        "Separately from this, the vendor's questionnaire cannot express some things "
        "our schema needs; those are recorded in `conformity/mappings.py` and are "
        "requests to make of the vendor rather than errors in the data.",
        "",
        "Each check below states what it looks for, the rule used to find it, and what "
        "accepting the record would cost, so any finding can be reproduced or disputed "
        "without reading the code.",
        "",
        f"Generated {generated}"
        + (f" from {source}." if source else ".")
        + " Regenerate with `python -m projects.etc_test.run`, or rebuild this document "
        "alone with `python projects/etc_test/findings_report.py`.",
        "",
        "## Summary",
        "",
        "| Severity | Check | Findings | Households |",
        "| --- | --- | --- | --- |",
    ]
    summary = (
        issues.group_by(["severity", "check"])
        .agg(pl.len().alias("n"), pl.col("hh_id").n_unique().alias("n_hh"))
        .sort(["severity", "n"], descending=[False, True])
    )
    # Anchors are the heading text lowercased with spaces hyphenated; check names
    # have no spaces, so the underscores carry through to the fragment unchanged.
    lines.extend(
        f"| {row['severity']} | [`{row['check']}`](#{row['check']}) | {row['n']} | {row['n_hh']} |"
        for row in summary.iter_rows(named=True)
    )

    for severity in SEVERITY_ORDER:
        subset = issues.filter(pl.col("severity") == severity)
        if subset.is_empty():
            continue
        lines += ["", f"## {severity.capitalize()}s", "", SEVERITY_BLURB[severity], ""]
        # Follow the summary table's order so the two read as one document.
        ordered = summary.filter(pl.col("severity") == severity)["check"].to_list()
        for name in ordered:
            rows = subset.filter(pl.col("check") == name)
            check = CHECKS[name]
            lines += [
                f"### {name}",
                "",
                f"{rows.height} finding(s). {check.summary}",
                "",
                f"- **How it is detected.** {check.detection}",
                f"- **What it costs.** {check.cost}",
                f"- **Implemented by.** `{check.source}()` in `conformity/check_vendor_data.py`",
                "",
                *_table(rows),
                "",
            ]

    lines += _codebook_section(issues, codebook_path)

    return "\n".join(lines).rstrip() + "\n"


def write_markdown(
    issues: pl.DataFrame,
    path: str | Path,
    source: str | None = None,
    codebook_path: Path | None = None,
) -> Path:
    """Write the Markdown report, creating the parent directory if needed.

    Args:
        issues: The ``etc_data_issues`` frame from ``check_etc_data``.
        path: Destination for the report.
        source: Optional description of the extract the findings came from.
        codebook_path: The vendor `codebook.csv`, used to decode the coded
            values the findings quote. When omitted the report says the
            values are undecoded rather than quietly leaving them bare.

    Returns:
        The path written.
    """
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render_markdown(issues, source, codebook_path), encoding="utf-8")
    logger.info("Wrote %d findings to %s", issues.height, out)
    return out


def _codebook_section(issues: pl.DataFrame, codebook_path: Path | None) -> list[str]:
    """Decode every coded field the findings quote, using the vendor's codebook.

    The findings tables carry the vendor's raw numeric answers, which mean
    nothing on their own -- ``Mode of Travel = 2`` is only intelligible next to
    the vendor's own label for 2. Only the fields actually cited are listed, so
    the appendix stays as short as the findings allow.

    Args:
        issues: The findings frame, read for the field names its evidence cites.
        codebook_path: The vendor ``codebook.csv``. When absent the appendix is
            replaced by a line saying so; it is never silently omitted.
    """
    cited: list[str] = []
    for row in issues.iter_rows(named=True):
        cited.extend(k for k in _parse_evidence(row) if k not in cited)
    if not cited:
        return []

    lines = ["", "## Codebook", ""]
    if codebook_path is None or not codebook_path.exists():
        where = codebook_path or "an unknown location"
        lines += [
            f"The vendor codebook was not found at `{where}`, so the coded values above "
            "are shown as filed and are not decoded here.",
        ]
        return lines

    codebook = pl.read_csv(codebook_path, infer_schema_length=5000)
    lines += [
        "Values quoted in the tables above, decoded with the vendor's own "
        f"`{codebook_path.name}`. Fields not listed here are free text, "
        "coordinates, timestamps, or counts.",
    ]
    decoded = 0
    for field in cited:
        entries = codebook.filter(pl.col("Column Name") == field)
        if entries.is_empty():
            continue
        decoded += 1
        lines += ["", f"### {field}", "", "| Code | Meaning |", "| --- | --- |"]
        lines += [
            f"| {row['Option Key']} | {_cell(row['Value'])} |"
            for row in entries.iter_rows(named=True)
        ]
    if not decoded:
        lines += ["", "_None of the cited fields are coded; all are free values._"]
    return lines


def default_issues_path() -> Path:
    """Find the findings CSV using the same directory the pipeline writes to.

    Reads ``survey_dir`` out of the project config rather than keeping a second
    copy of that path here, so moving the data only means editing one file.
    """
    if not CONFIG_PATH.exists():
        msg = f"No config at {CONFIG_PATH}, so the findings location is unknown. Pass --issues."
        raise SystemExit(msg)

    survey_dir = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")).get("survey_dir")
    if not survey_dir:
        msg = (
            f"{CONFIG_PATH} sets no survey_dir, so the findings location is unknown. Pass --issues."
        )
        raise SystemExit(msg)

    return Path(survey_dir) / ISSUES_FILENAME


def main() -> None:
    """Re-render the report from the findings CSV the pipeline already wrote.

    The pipeline calls :func:`write_markdown` directly; this entry point exists
    so the report can be rebuilt on its own after editing a description above,
    without re-running the checks. With no arguments it uses the paths from
    ``config.yaml``.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--issues",
        type=Path,
        default=None,
        help=f"Findings CSV (default: {ISSUES_FILENAME} in the config's survey_dir)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Destination .md (default: alongside the findings CSV)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    issues_path = args.issues or default_issues_path()
    if not issues_path.exists():
        msg = (
            f"No findings CSV at {issues_path}. Run the pipeline first "
            f"(python -m projects.etc_test.run), or pass --issues."
        )
        raise SystemExit(msg)

    logger.info("Reading findings from %s", issues_path)
    write_markdown(
        pl.read_csv(issues_path),
        args.out or issues_path.with_suffix(".md"),
        codebook_path=issues_path.parent / CODEBOOK_FILENAME,
    )


if __name__ == "__main__":
    main()
