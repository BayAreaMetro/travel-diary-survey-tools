"""Trip formatting for CT-RAMP output.

Transforms canonical trip data into CT-RAMP model format, including:
- Individual trips (trips on individual tours)
- Joint trips (trips on joint tours)
"""

import logging

import polars as pl

from data_canon.codebook.ctramp import map_mode_to_ctramp, map_purpose_to_ctramp

from .ctramp_config import CTRAMPConfig

logger = logging.getLogger(__name__)


def format_individual_trip(
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
    persons: pl.DataFrame,
    households: pl.DataFrame,
    config: CTRAMPConfig | None = None,
) -> pl.DataFrame:
    """Format individual trips to CT-RAMP specification.

    Transforms linked trip data (for individual tours only) to CT-RAMP format.

    Args:
        linked_trips: DataFrame with canonical linked trip fields
        tours: DataFrame with tour context (purpose, mode, category)
        persons: DataFrame with person_type
        households: DataFrame with income
        config: CT-RAMP configuration with income thresholds

    Returns:
        DataFrame with CT-RAMP individual trip fields

    Notes:
        - Excludes trips on joint tours
        - Creates stop_id sequence starting at 1 per half-tour
        - Excludes model-only fields (parking costs, value of time, etc.)
    """
    logger.info("Formatting individual trip data for CT-RAMP")

    # Handle empty input DataFrames
    if len(tours) == 0 or len(linked_trips) == 0:
        logger.info("No tours or trips provided")
        return pl.DataFrame()

    # Filter to trips on individual tours only
    individual_tour_ids = tours.filter(
        pl.col("joint_tour_id").is_null()
    ).select("tour_id")
    individual_trips = linked_trips.filter(
        pl.col("tour_id").is_in(individual_tour_ids)
    )

    # Join with tour context
    individual_trips = individual_trips.join(
        tours.select(["tour_id", "tour_purpose", "tour_mode", "tour_category"]),
        on="tour_id",
        how="left",
    )

    # Join with persons and households
    individual_trips = individual_trips.join(
        persons.select(["person_id", "person_type"]),
        on="person_id",
        how="left",
    ).join(
        households.select(["hh_id", "income"]),
        on="hh_id",
        how="left",
    )

    # Create stop_id: sequence number within each half-tour (outbound/inbound)
    # Starting at 1 for each half-tour
    individual_trips = individual_trips.sort(
        ["tour_id", "half_tour", "depart_time"]
    ).with_columns(
        pl.col("depart_time")
        .rank("dense")
        .over(["tour_id", "half_tour"])
        .cast(pl.Int64)
        .alias("stop_id")
    )

    # Map inbound flag (0=outbound, 1=inbound)
    individual_trips = individual_trips.with_columns(
        pl.when(pl.col("half_tour") == "inbound")
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias("inbound")
    )

    # Map origin and destination purposes
    individual_trips = individual_trips.with_columns(
        [
            map_purpose_to_ctramp(
                pl.col("o_purpose"),
                pl.col("income"),
                pl.col("student_category").fill_null("Not student"),
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
            ).alias("orig_purpose"),
            map_purpose_to_ctramp(
                pl.col("d_purpose"),
                pl.col("income"),
                pl.col("student_category").fill_null("Not student"),
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
            ).alias("dest_purpose"),
        ]
    )

    # Map trip mode
    individual_trips = individual_trips.with_columns(
        map_mode_to_ctramp(
            pl.col("mode_type"),
            pl.col("num_travelers").fill_null(1),
        ).alias("trip_mode")
    )

    # Convert times to minutes after midnight
    individual_trips = individual_trips.with_columns(
        [
            (
                pl.col("depart_time").dt.hour() * 60
                + pl.col("depart_time").dt.minute()
            ).alias("depart_minutes"),
            (
                pl.col("arrive_time").dt.hour() * 60
                + pl.col("arrive_time").dt.minute()
            ).alias("arrive_minutes"),
        ]
    )

    # Select and rename to CTRAMP columns
    individual_trips_ctramp = individual_trips.select(
        [
            pl.col("hh_id"),
            pl.col("person_id"),
            pl.col("person_num"),
            pl.col("tour_id"),
            pl.col("stop_id"),
            pl.col("inbound"),
            pl.col("tour_purpose"),
            pl.col("orig_purpose"),
            pl.col("dest_purpose"),
            pl.col("o_taz").cast(pl.Int64).alias("orig_taz"),
            pl.col("d_taz").cast(pl.Int64).alias("dest_taz"),
            pl.col("trip_mode"),
            pl.col("depart_minutes").cast(pl.Int64),
            pl.col("arrive_minutes").cast(pl.Int64),
        ]
    )

    logger.info(
        "Formatted %d individual trip records", len(individual_trips_ctramp)
    )
    return individual_trips_ctramp


def format_joint_trip(
    joint_trips: pl.DataFrame,
    tours: pl.DataFrame,
    households: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format joint trips to CT-RAMP specification.

    Transforms joint trip data using mean coordinates from joint_trips table.

    Args:
        joint_trips: DataFrame with aggregated joint trip data (mean locations)
        tours: DataFrame with tour context
        households: DataFrame with income
        config: CT-RAMP configuration with income thresholds

    Returns:
        DataFrame with CT-RAMP joint trip fields

    Notes:
        - Uses mean coordinates from joint_trips aggregation table
        - Deduplicates to one row per joint_trip_id
    """
    logger.info("Formatting joint trip data for CT-RAMP")

    if len(joint_trips) == 0:
        logger.info("No joint trips found")
        return pl.DataFrame()

    # Join with tour context
    joint_trips_formatted = joint_trips.join(
        tours.select(
            ["tour_id", "joint_tour_id", "tour_purpose", "tour_category"]
        ),
        on="tour_id",
        how="left",
    )

    # Filter to only trips on joint tours
    joint_trips_formatted = joint_trips_formatted.filter(
        pl.col("joint_tour_id").is_not_null()
    )

    # Join with households
    joint_trips_formatted = joint_trips_formatted.join(
        households.select(["hh_id", "income"]),
        on="hh_id",
        how="left",
    )

    # Map purposes and mode using mean locations
    joint_trips_formatted = joint_trips_formatted.with_columns(
        [
            map_purpose_to_ctramp(
                pl.col("o_purpose"),
                pl.col("income"),
                pl.lit("Not student"),
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
            ).alias("OrigPurpose"),
            map_purpose_to_ctramp(
                pl.col("d_purpose"),
                pl.col("income"),
                pl.lit("Not student"),
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
            ).alias("DestPurpose"),
            map_mode_to_ctramp(
                pl.col("mode_type"),
                pl.col("num_participants").fill_null(2),
            ).alias("TripMode"),
        ]
    )

    # Convert times
    joint_trips_formatted = joint_trips_formatted.with_columns(
        [
            (
                pl.col("depart_time").dt.hour() * 60
                + pl.col("depart_time").dt.minute()
            ).alias("DepartMinutes"),
            (
                pl.col("arrive_time").dt.hour() * 60
                + pl.col("arrive_time").dt.minute()
            ).alias("ArriveMinutes"),
        ]
    )

    # Select final columns
    joint_trips_ctramp = joint_trips_formatted.select(
        [
            pl.col("hh_id").alias("HHID"),
            pl.col("joint_tour_id").alias("JointTourNum"),
            pl.col("joint_trip_id").alias("JointTripNum"),
            pl.col("tour_category").alias("TourCategory"),
            pl.col("tour_purpose").alias("TourPurpose"),
            pl.col("OrigPurpose"),
            pl.col("DestPurpose"),
            pl.col("o_taz_mean").cast(pl.Int64).alias("OrigTAZ"),
            pl.col("d_taz_mean").cast(pl.Int64).alias("DestTAZ"),
            pl.col("TripMode"),
            pl.col("DepartMinutes").cast(pl.Int64),
            pl.col("ArriveMinutes").cast(pl.Int64),
            pl.col("num_participants").cast(pl.Int64).alias("NumParticipants"),
        ]
    )

    logger.info("Formatted %d joint trip records", len(joint_trips_ctramp))
    return joint_trips_ctramp
