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
from data_canon.codebook.tours import TourDirection

from .ctramp_config import CTRAMPConfig

logger = logging.getLogger(__name__)


def format_individual_tour(
    tours: pl.DataFrame,
    trips: pl.DataFrame,
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
        trips: DataFrame with canonical trip fields including:
            - tour_id, tour_direction (1=outbound, 2=inbound, 3=subtour)
        persons: DataFrame with person_type for joining
        households: DataFrame with income for purpose mapping
        config: CT-RAMP configuration with income thresholds
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
    # Handle empty tours DataFrame
    if len(tours) == 0:
        individual_tours = tours
    else:
        individual_tours = tours.filter(pl.col("joint_tour_id").is_null())

    # Join with persons to get person type and with households for income
    individual_tours = individual_tours.join(
        persons.select(["person_id", "type"]),
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
            pl.col("tour_mode"),
            pl.col("num_travelers").fill_null(1),
        ).alias("tour_mode_ctramp")
    )

    # Calculate number of outbound and inbound stops from trips
    # Count trips by tour_direction
    # Note: For subtours, they get their own tour_id and count stops
    # Handle empty trips DataFrame
    if len(trips) == 0:
        outbound_stops = pl.DataFrame(
            {"tour_id": [], "num_ob_stops": []},
            schema={"tour_id": pl.Int32, "num_ob_stops": pl.UInt32},
        )
        inbound_stops = pl.DataFrame(
            {"tour_id": [], "num_ib_stops": []},
            schema={"tour_id": pl.Int32, "num_ib_stops": pl.UInt32},
        )
    else:
        outbound_stops = (
            trips.filter(
                pl.col("tour_direction") == TourDirection.OUTBOUND.value
            )
            .group_by("tour_id")
            .agg(pl.len().alias("num_ob_stops"))
        )

        inbound_stops = (
            trips.filter(
                pl.col("tour_direction") == TourDirection.INBOUND.value
            )
            .group_by("tour_id")
            .agg(pl.len().alias("num_ib_stops"))
        )

    # Join stop counts to tours
    individual_tours = (
        individual_tours.join(outbound_stops, on="tour_id", how="left")
        .join(inbound_stops, on="tour_id", how="left")
        .with_columns(
            [
                pl.col("num_ob_stops").fill_null(0),
                pl.col("num_ib_stops").fill_null(0),
            ]
        )
    )

    # Validate that no tours have zero trips
    zero_trip_tours = individual_tours.filter(
        (pl.col("num_ob_stops") == 0) & (pl.col("num_ib_stops") == 0)
    )
    if len(zero_trip_tours) > 0:
        tour_ids = zero_trip_tours["tour_id"].to_list()
        msg = (
            f"Found {len(zero_trip_tours)} tours with zero trips. "
            f"Tour IDs: {tour_ids[:10]}{'...' if len(tour_ids) > 10 else ''}"  # noqa: PLR2004
        )
        raise ValueError(msg)

    # Select and rename to CTRAMP columns
    individual_tours_ctramp = individual_tours.select(
        [
            pl.col("hh_id"),
            pl.col("person_id"),
            pl.col("person_num"),
            pl.col("type").alias("person_type"),
            pl.col("tour_id"),
            pl.col("tour_category"),
            pl.col("tour_purpose_ctramp").alias("tour_purpose"),
            pl.col("o_taz").cast(pl.Int64).alias("orig_taz"),
            pl.col("d_taz").cast(pl.Int64).alias("dest_taz"),
            pl.col("start_hour").cast(pl.Int64),
            pl.col("end_hour").cast(pl.Int64),
            pl.col("tour_mode_ctramp").alias("tour_mode"),
            pl.col("atWork_freq").cast(pl.Int64),
            pl.col("num_ob_stops").cast(pl.Int64),
            pl.col("num_ib_stops").cast(pl.Int64),
        ]
    )

    logger.info(
        "Formatted %d individual tour records", len(individual_tours_ctramp)
    )
    return individual_tours_ctramp


def format_joint_tour(
    tours: pl.DataFrame,
    trips: pl.DataFrame,
    persons: pl.DataFrame,
    households: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format joint tours to CT-RAMP specification.

    Transforms joint tour data with participant aggregation.

    Args:
        tours: DataFrame with canonical tour fields
        trips: DataFrame with canonical trip fields
        persons: DataFrame with person ages for composition determination
        households: DataFrame with income
        config: CT-RAMP configuration with income thresholds and age_adult

    Returns:
        DataFrame with CT-RAMP joint tour fields

    Notes:
        - Filters to joint tours only (joint_tour_id IS NOT NULL)
        - Aggregates participants into space-separated string
        - Determines composition from participant ages
    """
    logger.info("Formatting joint tour data for CT-RAMP")

    # Handle empty tours DataFrame
    if len(tours) == 0:
        logger.info("No tours provided")
        return pl.DataFrame()

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
            pl.col("tour_mode").first(),
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
                (pl.col("age") < config.age_adult).any().alias("has_children"),
                (pl.col("age") >= config.age_adult).any().alias("has_adults"),
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
                pl.col("tour_mode"),
                pl.col("num_travelers").fill_null(
                    config.default_joint_tour_travelers
                ),
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

    # Calculate number of outbound and inbound stops from trips
    # For joint tours, we need to match on joint_tour_id
    # Note: joint_tour_id should exist in canonical data (may be null)
    if len(trips) == 0:
        outbound_stops = pl.DataFrame(
            {"joint_tour_id": [], "NumObStops": []},
            schema={"joint_tour_id": pl.Int32, "NumObStops": pl.UInt32},
        )
        inbound_stops = pl.DataFrame(
            {"joint_tour_id": [], "NumIbStops": []},
            schema={"joint_tour_id": pl.Int32, "NumIbStops": pl.UInt32},
        )
    else:
        outbound_stops = (
            trips.filter(
                pl.col("joint_tour_id").is_not_null()
                & (pl.col("tour_direction") == TourDirection.OUTBOUND.value)
            )
            .group_by("joint_tour_id")
            .agg(pl.len().alias("NumObStops"))
        )

        inbound_stops = (
            trips.filter(
                pl.col("joint_tour_id").is_not_null()
                & (pl.col("tour_direction") == TourDirection.INBOUND.value)
            )
            .group_by("joint_tour_id")
            .agg(pl.len().alias("NumIbStops"))
        )

    # Join stop counts to joint tours
    joint_tours_formatted = (
        joint_tours_formatted.join(
            outbound_stops, on="joint_tour_id", how="left"
        )
        .join(inbound_stops, on="joint_tour_id", how="left")
        .with_columns(
            [
                pl.col("NumObStops").fill_null(0),
                pl.col("NumIbStops").fill_null(0),
            ]
        )
    )

    # Validate that no joint tours have zero trips
    if len(trips) > 0:
        zero_trip_tours = joint_tours_formatted.filter(
            (pl.col("NumObStops") == 0) & (pl.col("NumIbStops") == 0)
        )
        if len(zero_trip_tours) > 0:
            tour_ids = zero_trip_tours["joint_tour_id"].to_list()
            msg = (
                f"Found {len(zero_trip_tours)} joint tours with zero trips. "
                f"Joint tour IDs: {tour_ids[:10]}"
                f"{'...' if len(tour_ids) > 10 else ''}"  # noqa: PLR2004
            )
            raise ValueError(msg)

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
            pl.col("NumObStops").cast(pl.Int64),
            pl.col("NumIbStops").cast(pl.Int64),
        ]
    )

    logger.info("Formatted %d joint tour records", len(joint_tours_ctramp))
    return joint_tours_ctramp
