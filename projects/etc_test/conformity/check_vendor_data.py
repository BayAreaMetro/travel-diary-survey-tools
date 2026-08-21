"""Check the ETC vendor extract for internal contradictions.

These checks read the vendor's *own* tables, before any conversion, and ask
whether the data is consistent with itself.  Nothing here is about our schema
or our codebook: a finding is something the vendor can verify and fix without
reference to how we happen to model travel.

Findings are graded by what they cost us downstream:

``error``
    The record cannot be used as filed.  It either stops canonical validation
    or it silently produces travel that did not happen -- a trip with no
    displacement, a purpose that is nothing but MISSING, a four-year-old at
    the wheel.
``warning``
    The record survives, but a question we rely on went unanswered, so the
    record drops out of some later reduction (a day that cannot be certified
    complete, a worker with no workplace to assign).

The step returns the findings as a table so they can be written out and sent
back to the vendor; it never raises, because the point is to enumerate every
problem in one pass rather than stop at the first.
"""

import logging
from pathlib import Path

import polars as pl
from conformity.expressions import code_expr, timestamp_expr
from findings_report import (
    CODEBOOK_FILENAME,
    COMPANION_MATCH_WINDOW_MINUTES,
    COMPANION_TOLERANCE_MINUTES,
    MIN_DRIVING_AGE,
    PLACE_MATCH_DECIMALS,
    write_markdown,
)

from pipeline.decoration import step

logger = logging.getLogger(__name__)

# MIN_DRIVING_AGE, COMPANION_TOLERANCE_MINUTES and PLACE_MATCH_DECIMALS are
# imported from findings_report, which documents the rules they parameterise --
# so the number enforced here is always the number the report quotes.

V_YES = 1
V_NO = 2
# "Carpool with only family/household member(s)" -- excludes outsiders by
# definition, so a non-household companion count contradicts it.
MODE_HOUSEHOLD_CARPOOL = 2
# Work locations that imply somewhere to travel to (everything but "works
# only from home").
WORK_LOCATION_REMOTE_ONLY = 2

FINDING_COLUMNS = [
    "check",
    "severity",
    "hh_id",
    "person_num",
    "trip_num",
    "detail",
    "evidence",
]


def _finding(
    df: pl.DataFrame,
    check: str,
    severity: str,
    detail: pl.Expr,
    *,
    trip_num: bool = False,
    evidence: dict[str, pl.Expr] | None = None,
) -> pl.DataFrame:
    """Shape any filtered vendor frame into the common findings layout.

    Args:
        df: Rows that failed the check, still in vendor column names.
        check: Check name; must have a matching entry in ``findings_report.CHECKS``.
        severity: ``"error"`` or ``"warning"``.
        detail: Expression producing the one-line prose description.
        trip_num: Whether the finding is about a specific trip row.
        evidence: Field label to expression for the values that triggered the
            finding. Serialised to JSON so the report can table the offending
            records themselves rather than only describing them. Labels that are
            vendor column names get decoded against the vendor codebook.
    """
    evidence_expr = (
        pl.struct([e.cast(pl.String).alias(label) for label, e in evidence.items()])
        .struct.json_encode()
        .alias("evidence")
        if evidence
        else pl.lit(None, pl.String).alias("evidence")
    )
    return df.select(
        pl.lit(check).alias("check"),
        pl.lit(severity).alias("severity"),
        code_expr("Sample Number").alias("hh_id")
        if "Sample Number" in df.columns
        else code_expr("HH Sample Number").alias("hh_id"),
        (code_expr("Person Number") if "Person Number" in df.columns else pl.lit(None, pl.Int64))
        .cast(pl.Int64)
        .alias("person_num"),
        (code_expr("Trip Number") if trip_num else pl.lit(None, pl.Int64))
        .cast(pl.Int64)
        .alias("trip_num"),
        detail.cast(pl.String).alias("detail"),
        evidence_expr,
    )


def check_drivers(persons: pl.DataFrame, trips: pl.DataFrame) -> list[pl.DataFrame]:
    """Flag anyone recorded as driving who cannot legally have been driving."""
    driving = trips.filter(code_expr("Person Was Driver") == V_YES).join(
        persons.select(["Sample Number", "Person Number", "Age", "Licensed Driver"]),
        on=["Sample Number", "Person Number"],
        how="left",
    )
    underage = driving.filter(code_expr("Age") < MIN_DRIVING_AGE)
    unlicensed = driving.filter(
        (code_expr("Age") >= MIN_DRIVING_AGE) & (code_expr("Licensed Driver") != V_YES)
    )
    return [
        _finding(
            underage,
            "driver_under_driving_age",
            "error",
            pl.format(
                "recorded as the driver at age {} (licensed driver answer: {})",
                pl.col("Age"),
                pl.col("Licensed Driver").fill_null(pl.lit("not asked")),
            ),
            trip_num=True,
            evidence={
                "Age": pl.col("Age"),
                "Person Was Driver": pl.col("Person Was Driver"),
                "Licensed Driver": pl.col("Licensed Driver"),
                "Mode of Travel": pl.col("Mode of Travel"),
            },
        ),
        _finding(
            unlicensed,
            "driver_not_licensed",
            "warning",
            pl.format("recorded as the driver but not reported as a licensed driver"),
            trip_num=True,
            evidence={
                "Age": pl.col("Age"),
                "Person Was Driver": pl.col("Person Was Driver"),
                "Licensed Driver": pl.col("Licensed Driver"),
            },
        ),
    ]


def check_travel_flag(persons: pl.DataFrame, trips: pl.DataFrame) -> list[pl.DataFrame]:
    """Compare the person-level travel answer against the filed diary."""
    moved = (
        trips.filter(code_expr("Trip Number") > 0)
        .group_by(["Sample Number", "Person Number"])
        .agg(pl.len().alias("_n_movements"))
    )
    joined = persons.join(moved, on=["Sample Number", "Person Number"], how="left").with_columns(
        pl.col("_n_movements").fill_null(0)
    )

    contradicted = joined.filter(
        ((code_expr("Travel") == V_NO) & (pl.col("_n_movements") > 0))
        | ((code_expr("Travel") == V_YES) & (pl.col("_n_movements") == 0))
    )
    unexplained = joined.filter(
        (code_expr("Travel") == V_NO) & code_expr("Why No Travel").is_null()
    )
    return [
        _finding(
            contradicted,
            "travel_flag_contradicts_diary",
            "error",
            pl.format(
                "reported travel = {} but the diary holds {} movement rows",
                pl.col("Travel"),
                pl.col("_n_movements"),
            ),
            evidence={
                "Travel": pl.col("Travel"),
                "movement rows filed": pl.col("_n_movements"),
                "Why No Travel": pl.col("Why No Travel"),
            },
        ),
        _finding(
            unexplained,
            "no_travel_without_reason",
            "warning",
            pl.lit("reported no travel but gave no reason, so the day cannot be certified"),
            evidence={
                "Travel": pl.col("Travel"),
                "Why No Travel": pl.col("Why No Travel"),
                "movement rows filed": pl.col("_n_movements"),
            },
        ),
    ]


def check_diary_labelling(trips: pl.DataFrame) -> list[pl.DataFrame]:
    """Flag movements the diary never described."""
    movements = trips.filter(code_expr("Trip Number") > 0)
    unlabelled = movements.filter(
        code_expr("Activity Type Code").is_null() | code_expr("Mode of Travel").is_null()
    )
    return [
        _finding(
            unlabelled,
            "unlabelled_movement",
            "error",
            pl.format(
                "movement with no activity code ({}) or mode ({}); purpose and mode "
                "can only be recorded as MISSING",
                pl.col("Activity Type Code").fill_null(pl.lit("null")),
                pl.col("Mode of Travel").fill_null(pl.lit("null")),
            ),
            trip_num=True,
            evidence={
                "Activity Type Code": pl.col("Activity Type Code"),
                "Mode of Travel": pl.col("Mode of Travel"),
                "Location Address": pl.col("Location Address"),
                "Arrival Time": pl.col("Arrival Time"),
                "Departure Time": pl.col("Departure Time"),
            },
        )
    ]


def check_geometry(trips: pl.DataFrame) -> list[pl.DataFrame]:
    """Flag movements that did not move, and clocks that run backwards."""
    ordered = trips.sort(["Sample Number", "Person Number", "Trip Number"])
    person = ["Sample Number", "Person Number"]
    paired = ordered.with_columns(
        pl.col("Location Latitude").shift(1).over(person).alias("_o_lat"),
        pl.col("Location Longitude").shift(1).over(person).alias("_o_lon"),
        timestamp_expr(ordered, "Date", "Departure Time").shift(1).over(person).alias("_depart"),
        timestamp_expr(ordered, "Date", "Arrival Time").alias("_arrive"),
    ).filter(code_expr("Trip Number") > 0)

    stationary = paired.filter(
        (pl.col("Location Latitude") == pl.col("_o_lat"))
        & (pl.col("Location Longitude") == pl.col("_o_lon"))
    )
    backwards = paired.filter(pl.col("_arrive") < pl.col("_depart"))
    return [
        _finding(
            stationary,
            "movement_returns_to_origin",
            "warning",
            pl.format(
                "returns to the coordinates it departed from, so anything measuring "
                "distance sees a zero-length trip (departed {}, arrived {})",
                pl.col("_depart"),
                pl.col("_arrive"),
            ),
            trip_num=True,
            evidence={
                "Mode of Travel": pl.col("Mode of Travel"),
                "Activity Type Code": pl.col("Activity Type Code"),
                "Departure Time": pl.col("_depart"),
                "Arrival Time": pl.col("_arrive"),
                "Location Latitude": pl.col("Location Latitude"),
                "Location Longitude": pl.col("Location Longitude"),
            },
        ),
        _finding(
            backwards,
            "arrival_before_departure",
            "error",
            pl.format("arrives {} having departed {}", pl.col("_arrive"), pl.col("_depart")),
            trip_num=True,
            evidence={
                "Departure Time": pl.col("_depart"),
                "Arrival Time": pl.col("_arrive"),
            },
        ),
    ]


def check_travel_party(trips: pl.DataFrame) -> list[pl.DataFrame]:
    """Cross-check the two questions that describe who else was in the vehicle.

    ``HH Members`` names household companions and ``Number of People`` counts
    everyone else, so the two together should agree with the reported mode and
    with what those companions filed in their own diaries.
    """
    outsiders_in_household_carpool = trips.filter(
        (code_expr("Mode of Travel") == MODE_HOUSEHOLD_CARPOOL)
        & (code_expr("Number of People") > 0)
    )

    # Expand each named household companion into its own row, then look for the
    # trip that companion filed to the same place.  Places are matched on
    # rounded coordinates because the vendor stores the same point at differing
    # precision from row to row (see check_place_geocoding).
    claimed = (
        trips.filter(pl.col("HH Members").is_not_null())
        .with_columns(pl.col("HH Members").cast(pl.String).str.split(";").alias("_companions"))
        .explode("_companions")
        .with_columns(
            pl.col("_companions")
            .str.strip_chars()
            .cast(pl.Int64, strict=False)
            .alias("_companion"),
            pl.col("Location Latitude").round(PLACE_MATCH_DECIMALS).alias("_lat"),
            pl.col("Location Longitude").round(PLACE_MATCH_DECIMALS).alias("_lon"),
        )
        .with_columns(timestamp_expr(trips, "Date", "Arrival Time").alias("_arrive"))
    )
    companion_trips = trips.select(
        "Sample Number",
        "Date",
        pl.col("Person Number").alias("_companion"),
        pl.col("Location Latitude").round(PLACE_MATCH_DECIMALS).alias("_lat"),
        pl.col("Location Longitude").round(PLACE_MATCH_DECIMALS).alias("_lon"),
        timestamp_expr(trips, "Date", "Arrival Time").alias("_c_arrive"),
        pl.col("Mode of Travel").alias("_c_mode"),
    )

    # A companion may visit the same place more than once in a day, so keep the
    # visit closest in time and judge the claim against that one.
    gap = (pl.col("_arrive") - pl.col("_c_arrive")).dt.total_minutes().abs()
    row = ["Sample Number", "Person Number", "Trip Number", "_companion"]
    matched = (
        claimed.join(
            companion_trips, on=["Sample Number", "Date", "_companion", "_lat", "_lon"], how="left"
        )
        .with_columns(gap.alias("_gap"))
        .sort("_gap", nulls_last=True)
        .group_by(row, maintain_order=True)
        .first()
    )

    # A visit further away than the match window is a different visit, so the
    # companion filed nothing for *this* movement; only visits inside the window
    # can disagree about the clock or the mode.
    in_window = pl.col("_c_arrive").is_not_null() & (
        pl.col("_gap") <= COMPANION_MATCH_WINDOW_MINUTES
    )
    # Both travellers file the same disagreement about each other, so key each
    # one on the unordered pair and keep a single row for it.
    matched = matched.with_columns(
        pl.min_horizontal(code_expr("Person Number"), pl.col("_companion")).alias("_pair_lo"),
        pl.max_horizontal(code_expr("Person Number"), pl.col("_companion")).alias("_pair_hi"),
    )
    pair = ["Sample Number", "Date", "_lat", "_lon", "_pair_lo", "_pair_hi"]

    absent = matched.filter(~in_window)
    disagreeing_time = matched.filter(
        in_window & (pl.col("_gap") > COMPANION_TOLERANCE_MINUTES)
    ).unique(subset=pair, keep="first", maintain_order=True)
    disagreeing_mode = matched.filter(
        in_window & (code_expr("Mode of Travel") != pl.col("_c_mode").cast(pl.Int64))
    ).unique(subset=pair, keep="first", maintain_order=True)
    return [
        _finding(
            outsiders_in_household_carpool,
            "carpool_mode_contradicts_party",
            "error",
            pl.format(
                "mode says the carpool held household members only, yet {} non-household "
                "companion(s) are counted on the same row",
                pl.col("Number of People"),
            ),
            trip_num=True,
            evidence={
                "Mode of Travel": pl.col("Mode of Travel"),
                "Number of People": pl.col("Number of People"),
                "HH Members": pl.col("HH Members"),
            },
        ),
        _finding(
            absent,
            "companion_filed_no_matching_trip",
            "error",
            pl.format(
                "names person {} as travelling along, but that person filed no arrival "
                "at this destination within {} minutes",
                pl.col("_companion"),
                pl.lit(COMPANION_MATCH_WINDOW_MINUTES),
            ),
            trip_num=True,
            evidence={
                "HH Members": pl.col("HH Members"),
                "Location Address": pl.col("Location Address"),
                "Arrival Time": pl.col("_arrive"),
                "companion person": pl.col("_companion"),
                "companion arrival": pl.col("_c_arrive"),
            },
        ),
        _finding(
            disagreeing_time,
            "companion_arrival_disagrees",
            "error",
            pl.format(
                "arrives {} but person {} records arriving at the same place {}",
                pl.col("_arrive"),
                pl.col("_companion"),
                pl.col("_c_arrive"),
            ),
            trip_num=True,
            evidence={
                "Location Address": pl.col("Location Address"),
                "Arrival Time": pl.col("_arrive"),
                "companion person": pl.col("_companion"),
                "companion arrival": pl.col("_c_arrive"),
                "gap (minutes)": pl.col("_gap"),
            },
        ),
        _finding(
            disagreeing_mode,
            "companion_mode_disagrees",
            "error",
            pl.format(
                "reports mode {} for a trip person {} reports as mode {}",
                pl.col("Mode of Travel"),
                pl.col("_companion"),
                pl.col("_c_mode"),
            ),
            trip_num=True,
            evidence={
                "Location Address": pl.col("Location Address"),
                "Mode of Travel": pl.col("Mode of Travel"),
                "companion person": pl.col("_companion"),
                "companion Mode of Travel": pl.col("_c_mode"),
            },
        ),
    ]


def check_place_geocoding(trips: pl.DataFrame) -> list[pl.DataFrame]:
    """Flag one place stored at more than one coordinate.

    Repeat visits to a single address should carry a single coordinate. When
    they do not, anything that identifies a place by its coordinates -- our
    joint-trip detection among them -- stops seeing the visits as the same
    place.
    """
    drift = (
        trips.filter(pl.col("Location Address").is_not_null())
        .group_by(["Sample Number", "Location Address"])
        .agg(
            pl.col("Location Latitude").n_unique().alias("_n_lat"),
            pl.col("Location Longitude").n_unique().alias("_n_lon"),
            pl.col("Location Latitude").round(PLACE_MATCH_DECIMALS).n_unique().alias("_n_rounded"),
            pl.col("Location Latitude").unique().alias("_lats"),
            pl.col("Location Longitude").unique().alias("_lons"),
        )
        # Only a precision difference: identical once rounded to ~1 metre.
        .filter((pl.col("_n_lat") > 1) & (pl.col("_n_rounded") == 1))
    )
    return [
        _finding(
            drift,
            "place_coordinate_precision_varies",
            "warning",
            pl.format(
                "address '{}' is filed at {} different latitudes that agree to ~1 metre, "
                "so the same place does not compare equal between rows",
                pl.col("Location Address"),
                pl.col("_n_lat"),
            ),
            evidence={
                "Location Address": pl.col("Location Address"),
                "latitudes filed": pl.col("_lats")
                .list.eval(pl.element().cast(pl.String))
                .list.join(", "),
                "longitudes filed": pl.col("_lons")
                .list.eval(pl.element().cast(pl.String))
                .list.join(", "),
            },
        )
    ]


def check_rosters(
    households: pl.DataFrame, persons: pl.DataFrame, vehicles: pl.DataFrame
) -> list[pl.DataFrame]:
    """Compare the counts a household reported against the rows it filed."""
    counts = (
        households.join(
            persons.group_by("Sample Number").len().rename({"len": "_person_rows"}),
            left_on="HH Sample Number",
            right_on="Sample Number",
            how="left",
        )
        .join(
            vehicles.group_by("Sample Number").len().rename({"len": "_vehicle_rows"}),
            left_on="HH Sample Number",
            right_on="Sample Number",
            how="left",
        )
        .with_columns(pl.col("_person_rows").fill_null(0), pl.col("_vehicle_rows").fill_null(0))
    )
    return [
        _finding(
            counts.filter(code_expr("Number Persons") != pl.col("_person_rows")),
            "person_roster_incomplete",
            "warning",
            pl.format(
                "reports {} household members but filed {} person rows",
                pl.col("Number Persons"),
                pl.col("_person_rows"),
            ),
            evidence={
                "Number Persons": pl.col("Number Persons"),
                "person rows filed": pl.col("_person_rows"),
            },
        ),
        _finding(
            counts.filter(code_expr("Vehicles Available") != pl.col("_vehicle_rows")),
            "vehicle_roster_incomplete",
            "warning",
            pl.format(
                "reports {} vehicles but filed {} vehicle rows",
                pl.col("Vehicles Available"),
                pl.col("_vehicle_rows"),
            ),
            evidence={
                "Vehicles Available": pl.col("Vehicles Available"),
                "vehicle rows filed": pl.col("_vehicle_rows"),
            },
        ),
    ]


def check_person_blocks(persons: pl.DataFrame) -> list[pl.DataFrame]:
    """Flag question blocks that were opened but never filled in."""
    employed_nowhere = persons.filter(
        (code_expr("Employment") == V_YES)
        & pl.col("Primary Workplace Latitude").is_null()
        & (
            code_expr("Current Work Location").is_null()
            | (code_expr("Current Work Location") != WORK_LOCATION_REMOTE_ONLY)
        )
    )
    student_nowhere = persons.filter(
        (code_expr("Student Status") == V_YES)
        & pl.col("School Latitude").is_null()
        & code_expr("School Type").is_null()
        & (code_expr("Online School") != V_YES)
    )
    no_relationship = persons.filter(code_expr("Relationship").is_null())
    no_age = persons.filter(code_expr("Age").is_null() & code_expr("Age Category").is_null())
    return [
        _finding(
            employed_nowhere,
            "employed_without_workplace",
            "warning",
            pl.lit("reported as employed but has neither a workplace location nor a work pattern"),
            evidence={
                "Employment": pl.col("Employment"),
                "Current Work Location": pl.col("Current Work Location"),
                "Primary Workplace Latitude": pl.col("Primary Workplace Latitude"),
            },
        ),
        _finding(
            student_nowhere,
            "student_without_school",
            "warning",
            pl.lit("reported as a student but has no school location, school type, or online flag"),
            evidence={
                "Student Status": pl.col("Student Status"),
                "School Type": pl.col("School Type"),
                "Online School": pl.col("Online School"),
                "School Latitude": pl.col("School Latitude"),
            },
        ),
        _finding(
            no_relationship,
            "missing_household_role",
            "warning",
            pl.lit("no relationship to the primary respondent, so household structure is unknown"),
            evidence={
                "Relationship": pl.col("Relationship"),
                "Age": pl.col("Age"),
            },
        ),
        _finding(
            no_age,
            "missing_age",
            "error",
            pl.lit("neither an age nor an age band; the canonical age band has no missing code"),
            evidence={
                "Age": pl.col("Age"),
                "Age Category": pl.col("Age Category"),
            },
        ),
    ]


@step()
def check_etc_data(
    etc_households: pl.DataFrame,
    etc_persons: pl.DataFrame,
    etc_vehicles: pl.DataFrame,
    etc_trips: pl.DataFrame,
    findings_report_path: str | None = None,
    findings_csv_path: str | None = None,
) -> dict[str, pl.DataFrame]:
    """Enumerate internal contradictions in the raw vendor tables.

    Args:
        etc_households: Vendor "1 Household" sheet.
        etc_persons: Vendor "2 Person" sheet.
        etc_vehicles: Vendor "3 Vehicle" sheet.
        etc_trips: Vendor "4 Trips" sheet.
        findings_report_path: Optional path for a Markdown write-up of the
            findings, grouped by check with the note explaining what each one
            costs. Skipped when unset.
        findings_csv_path: Optional path for the same findings as one row per
            finding. Written here rather than through ``write_data`` so the
            findings land even when a later step cannot run.

    Returns:
        Dictionary with ``etc_data_issues``: one row per finding, with the
        check name, severity, the household/person/trip it belongs to, and a
        sentence of evidence.
    """
    findings = [
        *check_drivers(etc_persons, etc_trips),
        *check_travel_flag(etc_persons, etc_trips),
        *check_diary_labelling(etc_trips),
        *check_geometry(etc_trips),
        *check_travel_party(etc_trips),
        *check_place_geocoding(etc_trips),
        *check_rosters(etc_households, etc_persons, etc_vehicles),
        *check_person_blocks(etc_persons),
    ]
    issues = pl.concat(findings, how="vertical").select(FINDING_COLUMNS)
    issues = issues.sort(["severity", "check", "hh_id", "person_num", "trip_num"])

    n_errors = issues.filter(pl.col("severity") == "error").height
    logger.info(
        "Vendor data check: %d findings (%d error, %d warning) across %d households",
        issues.height,
        n_errors,
        issues.height - n_errors,
        issues["hh_id"].n_unique(),
    )
    for row in issues.group_by(["severity", "check"], maintain_order=True).len().iter_rows():
        logger.info("  %-7s %-38s %d", *row)

    if findings_report_path:
        source = (
            f"{len(etc_households)} households, {len(etc_persons)} persons, "
            f"{len(etc_trips)} diary rows"
        )
        write_markdown(
            issues,
            findings_report_path,
            source=source,
            codebook_path=Path(findings_report_path).parent / CODEBOOK_FILENAME,
        )
    else:
        logger.warning(
            "No findings_report_path configured, so no Markdown report was written. "
            "Set it under this step's params in config.yaml."
        )

    if findings_csv_path:
        csv_path = Path(findings_csv_path)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        issues.write_csv(csv_path)
        logger.info("Wrote %d findings to %s", issues.height, csv_path)
    else:
        logger.warning(
            "No findings_csv_path configured, so the findings table was not written "
            "to disk. Set it under this step's params in config.yaml."
        )

    return {"etc_data_issues": issues}
