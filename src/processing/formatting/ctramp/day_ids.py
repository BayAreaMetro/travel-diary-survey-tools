"""CT-RAMP ids that carry the survey day.

CT-RAMP has no day dimension, so a multi-day respondent becomes one household
and person record per day, with the day encoded into ``hh_id`` and
``person_id``.
"""

import logging

import polars as pl

logger = logging.getLogger(__name__)


def incorporate_day_into_ids(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    tours: pl.DataFrame,
    linked_trips: pl.DataFrame,
    joint_trips: pl.DataFrame,
    days: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Expand records to person-day level and encode the survey day into CTRAMP IDs.

    CTRAMP treats each model run as a single day. Since BATS respondents have
    multiple survey days, each person-day must appear as a distinct "household"
    and "person" in the CTRAMP input files so tours and trips can be attributed
    to the correct day without double-counting.

    ID encoding (preserves the canonical hierarchical structure):
        - CTRAMP hh_id  = hh_id * 100 + day_num   (unique per household-day)
        - CTRAMP person_id = day_id                (unique per person-day)

    Both formulas are invertible: the original IDs can always be recovered from
    the CTRAMP IDs.

    Args:
        households: Canonical households (one row per household).
        persons: Canonical persons (one row per person).
        tours: Canonical tours (one row per tour, already has day_id).
        linked_trips: Canonical linked trips (one row per trip, already has day_id).
        joint_trips: Canonical joint trips (one row per joint trip, with the
            household's ``travel_date``).
        days: Canonical person-days table with person_id, hh_id, day_id columns.

    Returns:
        Tuple of (households, persons, tours, linked_trips, joint_trips) where
        each table is expanded to the person-day level and hh_id / person_id
        reflect the day-encoded IDs.
    """
    # day_num is encoded in the last two digits of day_id
    # hh_day_id = hh_id * 100 + day_num = hh_id * 100 + (day_id % 100)
    day_id_col = pl.col("day_id")
    day_num_expr = day_id_col % 100

    # Build a person-day map: original person_id →
    # (ctramp_person_id, ctramp_hh_id[, telecommute_time]).
    # telecommute_time is carried through when present so format_persons can use
    # it directly as the WFH indicator instead of relying on job_type alone.
    _day_cols = [
        pl.col("person_id"),
        day_id_col.alias("ctramp_person_id"),
        (pl.col("hh_id") * 100 + day_num_expr).alias("ctramp_hh_id"),
    ]
    if "telecommute_time" in days.columns:
        _day_cols.append(pl.col("telecommute_time"))
    person_day_map = days.select(_day_cols)

    # Count survey days per household and per person for weight scaling.
    # When a household/person is expanded to X day-records, each copy must
    # carry weight W/X so the sum of weights is preserved across the expansion.
    #
    # NOTE: num_hh_days must count unique HOUSEHOLD-DAY combinations, not total
    # person-day rows.  A 3-person household surveyed for 2 days has 6 rows in
    # `days` but only 2 household-days, so we unique on (hh_id, ctramp_hh_id)
    # before counting.
    hh_day_unique = days.select(
        pl.col("hh_id"),
        (pl.col("hh_id") * 100 + day_num_expr).alias("ctramp_hh_id"),
    ).unique(["hh_id", "ctramp_hh_id"])
    hh_num_days = hh_day_unique.group_by("hh_id").agg(pl.len().alias("num_hh_days"))
    person_num_days = days.group_by("person_id").agg(pl.len().alias("num_person_days"))

    # Build a household-day map: original hh_id → [ctramp_hh_id, num_hh_days]
    hh_day_map = hh_day_unique.join(hh_num_days, on="hh_id")

    n_hh_before = len(households)
    n_per_before = len(persons)

    # Expand households: one row per household-day; scale hh_weight accordingly
    households = (
        households.join(hh_day_map, on="hh_id", how="inner")
        .drop("hh_id")
        .rename({"ctramp_hh_id": "hh_id"})
    )
    if "hh_weight" in households.columns:
        households = households.with_columns(
            (pl.col("hh_weight") / pl.col("num_hh_days")).alias("hh_weight")
        )
    households = households.drop("num_hh_days")

    # Expand persons: one row per person-day; scale person_weight accordingly
    persons = (
        persons.join(person_day_map, on="person_id", how="inner")
        .join(person_num_days, on="person_id", how="left")
        .drop(["hh_id", "person_id"])
        .rename({"ctramp_hh_id": "hh_id", "ctramp_person_id": "person_id"})
    )
    if "person_weight" in persons.columns:
        persons = persons.with_columns(
            (pl.col("person_weight") / pl.col("num_person_days")).alias("person_weight")
        )
    persons = persons.drop("num_person_days")

    logger.info(
        "Expanded to person-day level: %d households → %d household-days, "
        "%d persons → %d person-days",
        n_hh_before,
        len(households),
        n_per_before,
        len(persons),
    )

    # The `days` table has already been filtered to valid (non-zero-weight) days
    # (e.g. Mon-Thu only).  Tours, trips, and joint trips must also be restricted
    # to those days so that every tour/trip record can be joined back to a person
    # record.  Use a semi-join on day_id so we drop rows without adding columns.
    valid_day_ids = days.select("day_id")

    # Update tours: drop non-target days, then encode CTRAMP IDs
    if len(tours) > 0 and "day_id" in tours.columns:
        tours = tours.join(valid_day_ids, on="day_id", how="semi").with_columns(
            (pl.col("hh_id") * 100 + pl.col("day_id") % 100).alias("hh_id"),
            pl.col("day_id").alias("person_id"),
        )

    # Update linked_trips: drop non-target days, then encode CTRAMP IDs
    if len(linked_trips) > 0 and "day_id" in linked_trips.columns:
        linked_trips = linked_trips.join(valid_day_ids, on="day_id", how="semi").with_columns(
            (pl.col("hh_id") * 100 + pl.col("day_id") % 100).alias("hh_id"),
            pl.col("day_id").alias("person_id"),
        )

    # Update joint_trips: a joint trip belongs to its household's date, so it takes
    # the day number every member's day on that date shares. The inner join also
    # drops joint trips on dates that are not target days, as the semi-joins above do.
    if len(joint_trips) > 0:
        joint_trips = (
            joint_trips.join(
                _household_date_numbers(days), on=["hh_id", "travel_date"], how="inner"
            )
            .with_columns((pl.col("hh_id") * 100 + pl.col("_day_num")).alias("hh_id"))
            .drop("_day_num")
        )

    return households, persons, tours, linked_trips, joint_trips


def _household_date_numbers(days: pl.DataFrame) -> pl.DataFrame:
    """Return each household date's day number, the last two digits of its days' ids.

    Every member's day on one date carries the same number, so a household date
    has exactly one. If two members number the same date differently, a joint
    trip on it has no single CT-RAMP household-day, so that raises.

    Raises:
        ValueError: If a household date carries more than one day number.
    """
    numbers = days.select(
        "hh_id", "travel_date", (pl.col("day_id") % 100).alias("_day_num")
    ).unique()
    split = numbers.group_by("hh_id", "travel_date").len().filter(pl.col("len") > 1)
    if not split.is_empty():
        msg = (
            f"{split.height} household date(s) carry more than one day number across "
            f"their members' days, so a joint trip on them has no single CT-RAMP "
            f"household-day. Sample: {split.drop('len').head(5).to_dicts()}."
        )
        raise ValueError(msg)
    return numbers
