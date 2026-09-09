"""The catalogue of vendor data checks, and the report that presents them.

**This module does not check anything.** The checking lives in
``conformity/check_vendor_data.py``, which emits one row per finding. What lives
here is :data:`CHECKS` -- one entry per check name saying what it looks for, how
it looks, and what the finding costs -- and the code that joins the two into a
document. That description is the part a reader, or the vendor, needs in order
to act on a finding or to argue with it.

Detection thresholds live here rather than in the checker so the sentence
describing a rule and the number the rule enforces cannot drift apart;
``check_vendor_data`` imports them from this module.

Run this file directly to rebuild the report from the findings CSV the pipeline
already wrote, without re-running the checks.
"""

import argparse
import json
import logging
from collections import Counter
from collections.abc import Callable
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

#: Everything the report says before the per-check sections, including the
#: summary table's header. Placeholders are filled by :func:`render_markdown`.
#: Everything the report says before the findings themselves. Placeholders are
#: filled by :func:`render_markdown`.
PREAMBLE = """# ETC vendor test data: data quality findings

**{n} findings across {n_households} households — {n_error} error, {n_warning} warning.**

Every finding is a contradiction *within the vendor's own tables*, checkable \
without reference to our schema. An **error** cannot be used as filed; a \
**warning** survives but drops out of a later reduction. Coded answers are \
decoded inline from the vendor's own `{codebook}`.

Separately, the vendor's questionnaire cannot express some things our schema \
needs; those are recorded in `conformity/mappings.py` and are requests to make \
of the vendor rather than errors in the data.

Generated {generated}{source}

| Severity | Check | Findings | Households |
| --- | --- | --- | --- |"""

#: Closes the collapsed block holding the detection rules.
DETECTION_HEADER = """## How each finding was detected

<details>
<summary>Detection rules, so any finding can be reproduced or disputed \
without reading the code. Click to expand.</summary>
"""


def _cell(value: object) -> str:
    """Render one value for a Markdown cell, keeping blanks visibly blank."""
    if value is None or value == "":
        return "_(blank)_"
    return str(value).replace("|", "\\|")


def _evidence(rows: pl.DataFrame) -> list[dict[str, str]]:
    """Decode each finding's evidence JSON, tolerating checks that carry none.

    Nulls are kept: for a check about a missing answer the empty field *is* the
    evidence, and dropping it would hide the point of the finding.

    Raises:
        ValueError: A payload is malformed, which must never pass silently.
    """
    decoded = []
    for row in rows.iter_rows(named=True):
        raw = row.get("evidence")
        if raw is None or raw == "":
            decoded.append({})
            continue
        try:
            decoded.append(json.loads(raw))
        except (TypeError, ValueError) as exc:
            msg = f"Finding for check {row.get('check')!r} has unreadable evidence: {raw!r}"
            raise ValueError(msg) from exc
    return decoded


def _cited_fields(evidence: list[dict[str, str]]) -> list[str]:
    """The evidence field names, in first-seen order and without repeats."""
    return list(dict.fromkeys(key for item in evidence for key in item))


def _decoder(codebook_path: Path | None) -> Callable[[str, object], str]:
    """Build the function that puts a coded answer's meaning beside the code.

    The findings quote the vendor's raw numeric answers, which mean nothing on
    their own -- ``Mode of Travel = 2`` is only intelligible next to the
    vendor's label for 2. Decoding inline keeps the tables self-explanatory,
    which is what lets the report carry no codebook appendix: a reader needs
    the handful of values actually cited, not every code the field allows.

    Fields the checker derives from a vendor column keep that column's name
    with a prefix (``companion Mode of Travel``), so a prefixed field falls
    back to the column it was derived from.

    Args:
        codebook_path: The vendor ``codebook.csv``. When absent, values are
            rendered as filed.

    Returns:
        A function taking a field name and value, returning the cell text.
    """
    if codebook_path is None or not codebook_path.exists():
        return lambda _field, value: _cell(value)

    codebook = pl.read_csv(codebook_path, infer_schema_length=5000)
    labels = {
        (str(row["Column Name"]), str(row["Option Key"])): str(row["Value"])
        for row in codebook.iter_rows(named=True)
    }

    def decode(field: str, value: object) -> str:
        text = _cell(value)
        if value is None or value == "":
            return text
        # Try the field itself, then the vendor column a derived field came from.
        for name in (field, field.split(" ", 1)[-1]):
            label = labels.get((name, str(value)))
            if label:
                return f"{text} ({label})"
        return text

    return decode


def _table(rows: pl.DataFrame, decode: Callable[[str, object], str]) -> list[str]:
    """Render the findings of one check as a Markdown table.

    The values that triggered each finding become their own columns, so the
    reader sees the offending record rather than only a sentence about it.
    Column names are the vendor's own, so they can be looked up in the vendor
    data dictionary.
    """
    evidence = _evidence(rows)
    fields = _cited_fields(evidence)
    header = ["HH", "Person", "Trip", *fields]
    out = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join("---" for _ in header) + " |",
    ]
    for row, item in zip(rows.iter_rows(named=True), evidence, strict=True):
        cells = [
            str(row["hh_id"]),
            "" if row["person_num"] is None else str(row["person_num"]),
            "" if row["trip_num"] is None else str(row["trip_num"]),
            *(decode(field, item.get(field)) for field in fields),
        ]
        out.append("| " + " | ".join(cells) + " |")
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


def _detection_appendix(names: list[str]) -> list[str]:
    """Collect every cited check's detection rule into one collapsed block.

    Held back rather than repeated under each check: a reader deciding what to
    fix needs the finding, and only a reader disputing one needs the rule.
    """
    lines = DETECTION_HEADER.splitlines()
    for name in names:
        check = CHECKS[name]
        lines += ["", f"**`{name}`** — {check.detection} (`{check.source}()`)"]
    lines += ["", "</details>"]
    return lines


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
            values the findings quote. When omitted the values are rendered as
            filed and the report says so.

    Returns:
        The report as one Markdown string.
    """
    _validate(issues)

    counts = Counter(issues["severity"].to_list())
    # Ties on count are broken by check name so regenerating the report does not
    # reshuffle its sections: two runs over the same findings must diff clean.
    summary = (
        issues.group_by(["severity", "check"], maintain_order=True)
        .agg(pl.len().alias("n"), pl.col("hh_id").n_unique().alias("n_hh"))
        .sort(["severity", "n", "check"], descending=[False, True, False])
    )

    lines = PREAMBLE.format(
        n=issues.height,
        n_households=issues["hh_id"].n_unique(),
        n_error=counts.get("error", 0),
        n_warning=counts.get("warning", 0),
        codebook=codebook_path.name if codebook_path else CODEBOOK_FILENAME,
        generated=datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        source=f" from {source}." if source else ".",
    ).splitlines()
    # Anchors are the heading text lowercased with spaces hyphenated; check names
    # have no spaces, so the underscores carry through to the fragment unchanged.
    lines.extend(
        f"| {row['severity']} | [`{row['check']}`](#{row['check']}) | {row['n']} | {row['n_hh']} |"
        for row in summary.iter_rows(named=True)
    )

    decode = _decoder(codebook_path)
    for severity in SEVERITY_ORDER:
        subset = issues.filter(pl.col("severity") == severity)
        if subset.is_empty():
            continue
        lines += ["", f"## {severity.capitalize()}s"]
        # Follow the summary table's order so the two read as one document.
        for name in summary.filter(pl.col("severity") == severity)["check"].to_list():
            rows = subset.filter(pl.col("check") == name)
            check = CHECKS[name]
            lines += [
                "",
                f"### {name}",
                "",
                f"{check.summary} {check.cost}",
                "",
                *_table(rows, decode),
            ]

    lines += ["", *_detection_appendix(summary["check"].to_list())]
    return "\n".join(lines).rstrip() + "\n"


def write_markdown(
    issues: pl.DataFrame,
    path: str | Path,
    source: str | None = None,
    codebook_path: Path | None = None,
) -> Path:
    """Write :func:`render_markdown` to ``path``, creating the parent directory.

    Returns:
        The path written.
    """
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render_markdown(issues, source, codebook_path), encoding="utf-8")
    logger.info("Wrote %d findings to %s", issues.height, out)
    return out


def default_issues_path() -> Path:
    """Find the findings CSV in the directory the pipeline writes to.

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
    so the report can be rebuilt after editing a description above, without
    re-running the checks. With no arguments it uses the paths from ``config.yaml``.
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
