"""Conform the ETC vendor extract to the canonical survey schema.

This is a ONE-OFF conversion, it is NOT INTENDED FOR PRODUCTION USE!

It is purely to test the vendor's far enough into the pipeline to judge whether
the data itself is sound. It is not the long-term plan: the intent is to hand
the vendor our schema and have them deliver against it, so every mapping that
loses information is a request to make of them rather than a translation to
maintain forever.  Those losses are recorded in :mod:`mappings`.

Two structural conversions do real work rather than renaming:

* **Trips.**  The vendor files an *activity/place log*: one row per place
  visited, carrying the arrival at that place and the later departure from it.
  Trip ``0`` is where the day began (a departure, no arrival).  Canonical
  ``unlinked_trips`` wants one row per *movement*, so each row is paired with
  its predecessor -- origin from row ``n-1``, destination and travel
  attributes from row ``n`` -- and row ``0`` drops out.
* **Days.**  The vendor ships no day table.  Person-days are derived from the
  diary, and persons who reported no travel get a day built from the household
  survey date so they still appear in the denominator.

The vendor codebook lives in :mod:`mappings` and the expression helpers in
:mod:`expressions`; :func:`report_gaps` at the end of the step logs which
canonical fields the extract could not fill.
"""

import logging

import polars as pl
from conformity.expressions import (
    age_category_expr,
    code_expr,
    mode_detail_expr,
    mode_type_expr,
    purpose_expr,
    race_ethnicity_exprs,
    recode_expr,
    text_expr,
    timestamp_expr,
    yes_no_expr,
)
from conformity.mappings import (
    COMMUTE_FREQ,
    DRIVER,
    EDUCATION,
    EMPLOYMENT_STATUS,
    GENDER,
    ID_STRIDE,
    INCOME_BIN,
    INCOME_MIDPOINT,
    INDUSTRY,
    JOB_TYPE,
    MADE_TRAVEL,
    MODE,
    NO_TRAVEL_REASON,
    NOT_EMPLOYED,
    OCCUPATION,
    PURPOSE_CATEGORY,
    RELATIONSHIP,
    RENT_OWN,
    SCHOOL_TYPE,
    TELEWORK_FREQ,
    TNC_TYPE,
    V_NO,
    V_PROXY_RETRIEVAL,
    V_YES,
    WORK_PARKING,
    WORK_PARKING_DISCOUNT_CODES,
    WORK_PARKING_FREE_CODES,
)

from data_canon.codebook.days import MadeTravel
from data_canon.codebook.households import IncomeBroad, ResidenceRentOwn, ResidenceType
from data_canon.codebook.persons import Education, Employment, Gender, SchoolType, Student
from data_canon.codebook.trips import Driver, ModeType, Purpose, PurposeCategory
from data_canon.models.survey import (
    HouseholdModel,
    PersonDayModel,
    PersonModel,
    UnlinkedTripModel,
)
from pipeline.decoration import step
from utils.helpers import expr_haversine

logger = logging.getLogger(__name__)


def conform_households(etc_households: pl.DataFrame) -> pl.DataFrame:
    """Build the canonical households table from the vendor household sheet."""
    logger.info("Conforming %d vendor households", len(etc_households))

    return etc_households.select(
        code_expr("HH Sample Number").alias("hh_id"),
        pl.col("HH Latitude").cast(pl.Float64).alias("home_lat"),
        pl.col("HH Longitude").cast(pl.Float64).alias("home_lon"),
        recode_expr(
            etc_households,
            "Rent or Own Residence",
            RENT_OWN,
            ResidenceRentOwn.MISSING.value,
            keep_null=False,
        ).alias("residence_rent_own"),
        # The vendor instrument carries no dwelling-type question at all.
        pl.lit(ResidenceType.MISSING.value, dtype=pl.Int64).alias("residence_type"),
        recode_expr(etc_households, "Income", INCOME_MIDPOINT, None).alias("income"),
        recode_expr(
            etc_households, "Income", INCOME_BIN, IncomeBroad.MISSING.value, keep_null=False
        ).alias("income_bin"),
        code_expr("Vehicles Available").fill_null(0).alias("num_vehicles"),
        code_expr("Bikes").alias("num_bikes"),
        code_expr("Number Persons").alias("num_people_reported"),
        pl.lit(None, dtype=pl.Float64).alias("hh_weight"),
        # Placeholder: cascade_completeness derives this upward from the days.
        pl.lit(value=True).alias("complete"),
    )


def conform_persons(etc_persons: pl.DataFrame) -> pl.DataFrame:
    """Build the canonical persons table from the vendor person sheet.

    Two columns beyond the canonical schema, ``made_travel`` and
    ``no_travel_reason``, are carried out of here so :func:`conform_days` can
    tell a declared non-travel day from an unreported one.
    """
    logger.info("Conforming %d vendor persons", len(etc_persons))
    race, ethnicity = race_ethnicity_exprs()

    hh_id = code_expr("Sample Number")
    person_num = code_expr("Person Number")

    # Employment status is only asked of workers; everyone else is described
    # by the "Not Employed" follow-up.
    employed = recode_expr(
        etc_persons, "Employment Status", EMPLOYMENT_STATUS, Employment.MISSING.value
    )
    not_employed = recode_expr(
        etc_persons, "Not Employed", NOT_EMPLOYED, Employment.UNEMPLOYED_NOT_LOOKING.value
    )
    employment = (
        pl.when(code_expr("Employment") == V_YES)
        .then(employed)
        .when(code_expr("Employment") == V_NO)
        .then(not_employed)
        .otherwise(pl.lit(Employment.MISSING.value))
        .fill_null(Employment.MISSING.value)
        .cast(pl.Int64)
    )

    # The vendor asks whether someone is a student and whether school is
    # online, but never full- versus part-time, so enrolment is reported as
    # full time and that split is simply unmeasured.
    student = (
        pl.when(code_expr("Student Status") == V_NO)
        .then(pl.lit(Student.NONSTUDENT.value))
        .when((code_expr("Student Status") == V_YES) & (code_expr("Online School") == V_YES))
        .then(pl.lit(Student.FULLTIME_ONLINE.value))
        .when(code_expr("Student Status") == V_YES)
        .then(pl.lit(Student.FULLTIME_INPERSON.value))
        .otherwise(pl.lit(Student.MISSING.value))
        .cast(pl.Int64)
    )

    school_type = (
        pl.when(code_expr("Home Schooled") == V_YES)
        .then(pl.lit(SchoolType.HOME_SCHOOL.value))
        .otherwise(recode_expr(etc_persons, "School Type", SCHOOL_TYPE, SchoolType.MISSING.value))
        .cast(pl.Int64)
    )

    parking = code_expr("Pay For Parking At Work")
    free_parking = yes_no_expr(parking.is_in(WORK_PARKING_FREE_CODES), parking)
    discount_parking = yes_no_expr(parking.is_in(WORK_PARKING_DISCOUNT_CODES), parking)

    return etc_persons.select(
        (hh_id * ID_STRIDE + person_num).alias("person_id"),
        hh_id.alias("hh_id"),
        person_num.alias("person_num"),
        age_category_expr(etc_persons).alias("age"),
        code_expr("Age").alias("age_years"),
        recode_expr(etc_persons, "Gender", GENDER, Gender.MISSING.value, keep_null=False).alias(
            "gender"
        ),
        recode_expr(etc_persons, "Relationship", RELATIONSHIP, None).alias("relationship"),
        recode_expr(
            etc_persons, "Educational Attainment", EDUCATION, Education.MISSING.value
        ).alias("education"),
        race.alias("race"),
        ethnicity.alias("ethnicity"),
        employment.alias("employment"),
        student.alias("student"),
        school_type.alias("school_type"),
        recode_expr(etc_persons, "Primary Occupation", OCCUPATION, None).alias("occupation"),
        recode_expr(etc_persons, "Primary Industry", INDUSTRY, None).alias("industry"),
        recode_expr(etc_persons, "Current Work Location", JOB_TYPE, None).alias("job_type"),
        recode_expr(etc_persons, "Pay For Parking At Work", WORK_PARKING, None).alias("work_park"),
        recode_expr(etc_persons, "Travel to Work", MODE, None).alias("work_mode"),
        recode_expr(etc_persons, "Commute Frequency", COMMUTE_FREQ, None).alias("commute_freq"),
        recode_expr(etc_persons, "Telecommute Frequency", TELEWORK_FREQ, None).alias(
            "telework_freq"
        ),
        pl.col("Primary Workplace Latitude").cast(pl.Float64).alias("work_lat"),
        pl.col("Primary Workplace Longitude").cast(pl.Float64).alias("work_lon"),
        pl.col("School Latitude").cast(pl.Float64).alias("school_lat"),
        pl.col("School Longitude").cast(pl.Float64).alias("school_lon"),
        # The vendor asks one parking question, so "offered" and "used" are the
        # same answer: it cannot tell a subsidy taken from one declined.
        free_parking.alias("commute_subsidy_provide_free_parking"),
        discount_parking.alias("commute_subsidy_provide_discounted_parking"),
        free_parking.alias("commute_subsidy_use_free_parking"),
        discount_parking.alias("commute_subsidy_use_discounted_parking"),
        recode_expr(etc_persons, "Licensed Driver", {1: 1, 2: 0}, None).alias("can_drive"),
        (code_expr("Travel Data Retrieval Mode") == V_PROXY_RETRIEVAL).alias("is_proxy"),
        # Every enumerated person was asked to file a diary.
        pl.lit(value=True).alias("surveyable"),
        pl.lit(0, dtype=pl.Int64).alias("num_days_complete"),
        pl.lit(None, dtype=pl.Float64).alias("person_weight"),
        # Placeholder: cascade_completeness derives this upward from the days.
        pl.lit(value=True).alias("complete"),
        recode_expr(etc_persons, "Travel", MADE_TRAVEL, MadeTravel.MISSING.value).alias(
            "made_travel"
        ),
        recode_expr(etc_persons, "Why No Travel", NO_TRAVEL_REASON, None).alias("no_travel_reason"),
    )


def conform_trips(etc_trips: pl.DataFrame) -> pl.DataFrame:
    """Reshape the vendor place log into canonical unlinked trips.

    Each vendor row is a place: it carries the arrival there and the later
    departure from it.  A canonical trip is the movement *between* two such
    rows, so every row is paired with its predecessor within the person-day.
    Row 0, where the day began, has no predecessor and drops out.
    """
    logger.info("Reshaping %d vendor place rows into trips", len(etc_trips))

    hh_id = code_expr("Sample Number")
    person_num = code_expr("Person Number")
    trips = etc_trips.with_columns(
        hh_id.alias("hh_id"),
        (hh_id * ID_STRIDE + person_num).alias("person_id"),
        text_expr(etc_trips, "Date").alias("travel_date_str"),
        timestamp_expr(etc_trips, "Date", "Arrival Time").alias("_arrive"),
        timestamp_expr(etc_trips, "Date", "Departure Time").alias("_depart"),
        purpose_expr(etc_trips, "Activity Type Code", "Type of Place").alias("_purpose"),
        mode_detail_expr(etc_trips, "Mode of Travel").alias("_mode"),
        mode_type_expr(etc_trips, "Mode of Travel").alias("_mode_type"),
    ).sort(["hh_id", "person_id", "travel_date_str", "Trip Number"])

    group = ["hh_id", "person_id", "travel_date_str"]
    trips = trips.with_columns(
        # Origin facts come from the previous place in the same person-day.
        pl.col("Location Latitude").shift(1).over(group).alias("o_lat"),
        pl.col("Location Longitude").shift(1).over(group).alias("o_lon"),
        pl.col("_purpose").shift(1).over(group).alias("o_purpose"),
        pl.col("_depart").shift(1).over(group).alias("depart_time"),
        pl.col("Trip Number").shift(1).over(group).alias("_prev_trip_num"),
    ).filter(pl.col("_prev_trip_num").is_not_null())

    # A diary that runs past midnight comes back with an arrival earlier than
    # the departure, because every row carries the same calendar date.
    overnight = pl.col("_arrive") < pl.col("depart_time")
    n_overnight = trips.filter(overnight).height
    if n_overnight:
        logger.info("Rolling %d trips arriving after midnight onto the next day", n_overnight)
    trips = trips.with_columns(
        pl.when(overnight)
        .then(pl.col("_arrive") + pl.duration(days=1))
        .otherwise(pl.col("_arrive"))
        .alias("arrive_time")
    )

    # Travel party: the person, the household members named on the row, and
    # any non-household companions the vendor counted separately.
    n_hh_companions = (
        pl.col("HH Members").cast(pl.String).str.split(";").list.len().fill_null(0).cast(pl.Int64)
    )
    num_travelers = 1 + n_hh_companions + code_expr("Number of People").fill_null(0)

    day_num = pl.col("travel_date_str").rank("dense").over("person_id").cast(pl.Int64)
    day_id = pl.col("person_id") * ID_STRIDE + day_num
    trip_num = code_expr("Trip Number")

    out = trips.select(
        (day_id * ID_STRIDE + trip_num).alias("unlinked_trip_id"),
        day_id.alias("day_id"),
        pl.col("person_id"),
        pl.col("hh_id"),
        pl.col("travel_date_str"),
        # link_trips carries the day of week up onto the linked trips.
        pl.col("travel_date_str")
        .str.to_date("%Y-%m-%d", strict=False)
        .dt.weekday()
        .cast(pl.Int64)
        .alias("travel_dow"),
        trip_num.alias("trip_num"),
        pl.col("o_lat").cast(pl.Float64),
        pl.col("o_lon").cast(pl.Float64),
        pl.col("Location Latitude").cast(pl.Float64).alias("d_lat"),
        pl.col("Location Longitude").cast(pl.Float64).alias("d_lon"),
        pl.col("o_purpose"),
        pl.col("_purpose").alias("d_purpose"),
        pl.col("depart_time"),
        pl.col("arrive_time"),
        pl.col("_mode_type").alias("mode_type"),
        pl.col("_mode").alias("mode_1"),
        pl.lit(None, dtype=pl.Int64).alias("mode_2"),
        pl.lit(None, dtype=pl.Int64).alias("mode_3"),
        pl.lit(None, dtype=pl.Int64).alias("mode_4"),
        recode_expr(trips, "Taxi Ride Service App Used", TNC_TYPE, None).alias("tnc_type"),
        # Non-vehicle movements are never asked who drove, and canonical `driver`
        # has no null: an unasked question is MISSING, not an absent value.
        recode_expr(
            trips, "Person Was Driver", DRIVER, Driver.MISSING.value, keep_null=False
        ).alias("driver"),
        num_travelers.alias("num_travelers"),
        pl.lit(None, dtype=pl.Float64).alias("unlinked_trip_weight"),
    )

    # Purpose categories follow from the purposes just assigned.
    out = out.with_columns(
        [
            pl.col(side)
            .replace_strict(
                PURPOSE_CATEGORY, default=PurposeCategory.MISSING.value, return_dtype=pl.Int64
            )
            .alias(f"{side}_category")
            for side in ("o_purpose", "d_purpose")
        ]
    )

    # The vendor ships neither distance nor duration, so both are derived here.
    # The distance is straight-line, not a network path.
    out = out.with_columns(
        expr_haversine(pl.col("o_lat"), pl.col("o_lon"), pl.col("d_lat"), pl.col("d_lon")).alias(
            "distance_meters"
        ),
        (pl.col("arrive_time") - pl.col("depart_time"))
        .dt.total_minutes()
        .alias("duration_minutes"),
    )

    # A trip counts as surveyed only when the movement is fully described.
    return out.with_columns(
        (
            pl.col("depart_time").is_not_null()
            & pl.col("arrive_time").is_not_null()
            & pl.col("o_lat").is_not_null()
            & pl.col("d_lat").is_not_null()
            & pl.col("d_purpose").ne(Purpose.MISSING.value)
            & pl.col("mode_type").ne(ModeType.MISSING.value)
        ).alias("complete")
    )


def conform_days(
    persons: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
    etc_households: pl.DataFrame,
) -> pl.DataFrame:
    """Derive person-days from the diary, covering non-travellers as well.

    The vendor ships no day table.  Days observed in the diary come from the
    trips; a person who reported no travel still needs a day, which is dated
    from the household survey date so they stay in the denominator.
    """
    diary_days = unlinked_trips.group_by(["hh_id", "person_id", "day_id", "travel_date_str"]).agg(
        pl.len().alias("num_trips"),
        pl.col("complete").all().alias("_all_trips_surveyed"),
    )

    hh_dates = etc_households.select(
        code_expr("HH Sample Number").alias("hh_id"),
        text_expr(etc_households, "Date of Household Survey").alias("travel_date_str"),
    )

    # Persons with no diary day at all get the household's survey date and the
    # first day slot, which is free because they filed no diary.
    absent = (
        persons.select("hh_id", "person_id")
        .join(diary_days.select("person_id"), on="person_id", how="anti")
        .join(hh_dates, on="hh_id", how="left")
        .with_columns(
            (pl.col("person_id") * ID_STRIDE + 1).alias("day_id"),
            pl.lit(0, dtype=pl.UInt32).alias("num_trips"),
            pl.lit(value=False).alias("_all_trips_surveyed"),
        )
    )
    if absent.height:
        logger.info("Building %d day rows for persons with no diary entries", absent.height)

    days = pl.concat([diary_days, absent], how="diagonal").join(
        persons.select("person_id", "made_travel", "no_travel_reason"), on="person_id", how="left"
    )

    # A day counts as surveyed when every trip on it was fully described, or
    # when the person declared why they did not travel.
    declared_no_travel = (pl.col("made_travel") == MadeTravel.NO.value) & pl.col(
        "no_travel_reason"
    ).is_not_null()
    complete = (
        pl.when(pl.col("num_trips") > 0)
        .then(pl.col("_all_trips_surveyed"))
        .otherwise(declared_no_travel)
        .fill_null(value=False)
    )

    return days.select(
        pl.col("day_id"),
        pl.col("person_id"),
        pl.col("hh_id"),
        pl.col("travel_date_str").str.to_datetime("%Y-%m-%d", strict=False).alias("travel_date"),
        pl.col("travel_date_str")
        .str.to_date("%Y-%m-%d", strict=False)
        .dt.weekday()
        .cast(pl.Int64)
        .alias("travel_dow"),
        pl.col("num_trips").cast(pl.Int64),
        pl.col("made_travel"),
        pl.col("no_travel_reason"),
        # No day_weight column at all: this project runs no weighting, and an
        # all-null weight column reads as "weighted, every day zero", which the
        # CT-RAMP day filter takes literally and drops the whole extract.
        complete.alias("complete"),
    )


def report_gaps(tables: dict[str, pl.DataFrame]) -> None:
    """Log which canonical fields the vendor extract cannot populate.

    This step exists to evaluate a vendor format, so a missing or wholly-null
    canonical field is a finding to record rather than a reason to stop.
    """
    models = {
        "households": HouseholdModel,
        "persons": PersonModel,
        "days": PersonDayModel,
        "unlinked_trips": UnlinkedTripModel,
    }
    for name, model in models.items():
        df = tables[name]
        missing = sorted(f for f in model.model_fields if f not in df.columns)
        empty = sorted(
            col
            for col in df.columns
            if col in model.model_fields and df[col].null_count() == df.height
        )
        logger.info("%s: %d rows x %d columns", name, df.height, df.width)
        if missing:
            logger.warning("%s: canonical fields absent: %s", name, missing)
        if empty:
            logger.warning("%s: canonical fields present but entirely null: %s", name, empty)


@step()
def conform_etc(
    etc_households: pl.DataFrame,
    etc_persons: pl.DataFrame,
    etc_trips: pl.DataFrame,
    etc_vehicles: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """Conform the ETC vendor tables to the canonical schema.

    Args:
        etc_households: Vendor "1 Household" sheet.
        etc_persons: Vendor "2 Person" sheet.
        etc_trips: Vendor "4 Trips" sheet, an activity/place log.
        etc_vehicles: Vendor "3 Vehicle" sheet.  Reconciled against the
            household vehicle count and otherwise unused: the canonical schema
            has no vehicle table, so body type, fuel type and ownership are
            dropped here.

    Returns:
        Dictionary with the canonical households, persons, days and
        unlinked_trips tables.
    """
    households = conform_households(etc_households)
    persons = conform_persons(etc_persons)
    unlinked_trips = conform_trips(etc_trips)
    days = conform_days(persons, unlinked_trips, etc_households)

    # The vehicle roster should hold one row per vehicle the household counted.
    rostered = etc_vehicles.group_by(code_expr("Sample Number").alias("hh_id")).len()
    mismatch = (
        households.join(rostered, on="hh_id", how="left")
        .with_columns(pl.col("len").fill_null(0))
        .filter(pl.col("len") != pl.col("num_vehicles"))
        .select("hh_id", pl.col("len").alias("rostered"), pl.col("num_vehicles").alias("reported"))
    )
    if mismatch.height:
        logger.warning(
            "%d households have a vehicle roster disagreeing with their reported count: %s",
            mismatch.height,
            mismatch.to_dicts(),
        )

    # made_travel / no_travel_reason were only needed to build the days.
    persons = persons.drop("made_travel", "no_travel_reason")
    unlinked_trips = unlinked_trips.drop("travel_date_str")

    tables = {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
    }
    report_gaps(tables)
    return tables
