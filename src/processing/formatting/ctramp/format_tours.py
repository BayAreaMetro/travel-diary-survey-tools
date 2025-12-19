"""Tour formatting for CT-RAMP output.

Transforms canonical tour data into CT-RAMP model format, including:
- Individual tours (non-joint tours)
- Joint tours (household member group tours)
"""

import logging

import polars as pl

from data_canon.codebook.ctramp import (
    TourComposition,
    map_mode_to_ctramp,
    map_purpose_to_ctramp,
)
from data_canon.codebook.persons import AgeCategory

from .ctramp_config import CTRAMPConfig

logger = logging.getLogger(__name__)


def format_individual_tour(
    tours: pl.DataFrame,
    persons: pl.DataFrame,
    households: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format individual tours to CT-RAMP specification.

    Transforms tour data (excluding joint tours) to CT-RAMP format.

    Args:
        tours: DataFrame with canonical tour fields including:
            - tour_id, hh_id, person_id, person_num
            - tour_category, tour_purpose
            - o_taz, d_taz
            - depart_time, arrive_time
            - tour_mode
            - joint_tour_id (for filtering)
            - parent_tour_id (for subtour counting)
        persons: DataFrame with person_type for joining
        households: DataFrame with income for purpose mapping
        config: CT-RAMP configuration with income thresholds

    Returns:
        DataFrame with CT-RAMP individual tour fields:
        - hh_id, person_id, person_num, person_type
        - tour_id, tour_category, tour_purpose
        - orig_taz, dest_taz
        - start_hour, end_hour
        - tour_mode
        - atWork_freq (subtour count)
        - num_ob_stops, num_ib_stops

    Notes:
        - Excludes joint tours (joint_tour_id IS NULL)
        - Excludes all model-only fields (random numbers, wait times, logsums)
    """
    logger.info("Formatting individual tour data for CT-RAMP")

    # Filter to individual tours only (not joint)
    individual_tours = tours.filter(pl.col("joint_tour_id").is_null())

    # Join with persons to get person_type and with households for income
    individual_tours = individual_tours.join(
        persons.select(["person_id", "person_type"]),
        on="person_id",
        how="left",
    ).join(
        households.select(["hh_id", "income"]),
        on="hh_id",
        how="left",
    )

    # Calculate subtour count (at-work tours)
    subtour_counts = (
        tours.filter(pl.col("parent_tour_id").is_not_null())
        .group_by("parent_tour_id")
        .agg(pl.len().alias("atWork_freq"))
    )

    individual_tours = individual_tours.join(
        subtour_counts,
        left_on="tour_id",
        right_on="parent_tour_id",
        how="left",
    ).with_columns(pl.col("atWork_freq").fill_null(0))

    # Map tour purpose to CTRAMP format
    individual_tours = individual_tours.with_columns(
        map_purpose_to_ctramp(
            pl.col("tour_purpose"),
            pl.col("income"),
            pl.col("student_category").fill_null("Not student"),
            config.income_low_threshold,
            config.income_med_threshold,
            config.income_high_threshold,
        ).alias("tour_purpose_ctramp")
    )

    # Convert times to hour integers (5am-11pm = 5-23)
    individual_tours = individual_tours.with_columns(
        [
            pl.col("depart_time").dt.hour().alias("start_hour"),
            pl.col("arrive_time").dt.hour().alias("end_hour"),
        ]
    )

    # Map mode to CTRAMP integer codes
    individual_tours = individual_tours.with_columns(
        map_mode_to_ctramp(
            pl.col("mode_type"),
            pl.col("num_travelers").fill_null(1),
        ).alias("tour_mode_ctramp")
    )

    msg = "Calculating number of outboard and inboard stops "
    raise NotImplementedError(msg)
    # This requires trip-to-tour linkage which needs to be implemented

    # Select and rename to CTRAMP columns
    individual_tours_ctramp = individual_tours.select(
        [
            pl.col("hh_id"),
            pl.col("person_id"),
            pl.col("person_num"),
            pl.col("person_type"),
            pl.col("tour_id"),
            pl.col("tour_category"),
            pl.col("tour_purpose_ctramp").alias("tour_purpose"),
            pl.col("o_taz").cast(pl.Int64).alias("orig_taz"),
            pl.col("d_taz").cast(pl.Int64).alias("dest_taz"),
            pl.col("start_hour").cast(pl.Int64),
            pl.col("end_hour").cast(pl.Int64),
            pl.col("tour_mode_ctramp").alias("tour_mode"),
            pl.col("atWork_freq").cast(pl.Int64),
            # Placeholder stop counts - TODO: calculate from trips
            pl.lit(0).alias("num_ob_stops"),
            pl.lit(0).alias("num_ib_stops"),
        ]
    )

    logger.info(
        "Formatted %d individual tour records", len(individual_tours_ctramp)
    )
    return individual_tours_ctramp


def format_joint_tour(
    tours: pl.DataFrame,
    persons: pl.DataFrame,
    households: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format joint tours to CT-RAMP specification.

    Transforms joint tour data with participant aggregation.

    Args:
        tours: DataFrame with canonical tour fields
        persons: DataFrame with person ages for composition determination
        households: DataFrame with income
        config: CT-RAMP configuration with income thresholds

    Returns:
        DataFrame with CT-RAMP joint tour fields

    Notes:
        - Filters to joint tours only (joint_tour_id IS NOT NULL)
        - Aggregates participants into space-separated string
        - Determines composition from participant ages
    """
    logger.info("Formatting joint tour data for CT-RAMP")

    # Filter to joint tours only
    joint_tours = tours.filter(pl.col("joint_tour_id").is_not_null())

    if len(joint_tours) == 0:
        logger.info("No joint tours found")
        return pl.DataFrame()

    # Group by joint_tour_id to aggregate participants
    # This assumes tours table has person_num for each participant
    participants_agg = joint_tours.group_by("joint_tour_id").agg(
        [
            pl.col("hh_id").first(),
            pl.col("person_num")
            .sort()
            .cast(pl.Utf8)
            .str.join(" ")
            .alias("Participants"),
            pl.col("tour_category").first(),
            pl.col("tour_purpose").first(),
            pl.col("o_taz").first(),
            pl.col("d_taz").first(),
            pl.col("depart_time").first(),
            pl.col("arrive_time").first(),
            pl.col("mode_type").first(),
            pl.col("num_travelers").first(),
        ]
    )

    # Join with persons to determine composition
    participants_with_ages = joint_tours.join(
        persons.select(["person_id", "age"]),
        on="person_id",
        how="left",
    )

    composition_agg = (
        participants_with_ages.group_by("joint_tour_id")
        .agg(
            [
                (pl.col("age") < AgeCategory.AGE_18_TO_24.value)
                .any()
                .alias("has_children"),
                (pl.col("age") >= AgeCategory.AGE_18_TO_24.value)
                .any()
                .alias("has_adults"),
            ]
        )
        .with_columns(
            pl.when(pl.col("has_children") & pl.col("has_adults"))
            .then(pl.lit(TourComposition.ADULTS_AND_CHILDREN.value))
            .when(pl.col("has_children"))
            .then(pl.lit(TourComposition.CHILDREN_ONLY.value))
            .otherwise(pl.lit(TourComposition.ADULTS_ONLY.value))
            .alias("Composition")
        )
    )

    # Join aggregations
    joint_tours_formatted = participants_agg.join(
        composition_agg.select(["joint_tour_id", "Composition"]),
        on="joint_tour_id",
        how="left",
    )

    # Join with households for income
    joint_tours_formatted = joint_tours_formatted.join(
        households.select(["hh_id", "income"]),
        on="hh_id",
        how="left",
    )

    # Map purpose and mode
    joint_tours_formatted = joint_tours_formatted.with_columns(
        [
            map_purpose_to_ctramp(
                pl.col("tour_purpose"),
                pl.col("income"),
                pl.lit("Not student"),  # Joint tours typically not for school
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
            ).alias("TourPurpose"),
            map_mode_to_ctramp(
                pl.col("mode_type"),
                pl.col("num_travelers").fill_null(2),
            ).alias("TourMode"),
        ]
    )

    # Convert times
    joint_tours_formatted = joint_tours_formatted.with_columns(
        [
            pl.col("depart_time").dt.hour().alias("StartHour"),
            pl.col("arrive_time").dt.hour().alias("EndHour"),
        ]
    )

    # Select final columns
    joint_tours_ctramp = joint_tours_formatted.select(
        [
            pl.col("hh_id").alias("HHID"),
            pl.col("joint_tour_id").alias("JointTourNum"),
            pl.col("tour_category").alias("TourCategory"),
            pl.col("TourPurpose"),
            pl.col("Composition"),
            pl.col("Participants"),
            pl.col("o_taz").cast(pl.Int64).alias("OrigTAZ"),
            pl.col("d_taz").cast(pl.Int64).alias("DestTAZ"),
            pl.col("StartHour").cast(pl.Int64),
            pl.col("EndHour").cast(pl.Int64),
            pl.col("TourMode"),
        ]
    )

    logger.info("Formatted %d joint tour records", len(joint_tours_ctramp))
    return joint_tours_ctramp
