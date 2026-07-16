"""Tour formatting for CT-RAMP.

Transforms canonical tour data into CT-RAMP model format, including:

- Individual tours (non-joint tours)
- Joint tours (household member group tours)
"""

import logging

import polars as pl

from data_canon.codebook.ctramp import (
    AtWorkFreq,
    CTRAMPEmploymentCategory,
    CTRAMPPersonType,
    CTRAMPTourCategory,
    CTRAMPTourPurpose,
    TourComposition,
)
from data_canon.codebook.persons import SchoolType
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import PurposeCategory
from processing.formatting.ctramp.mappings import (
    EMPLOYMENT_TO_CTRAMP,
    aggregate_transit_submode,
    ctramp_mode_expression,
    ctramp_purpose_category_expression,
    ctramp_student_category_expression,
)

from .ctramp_config import CTRAMPConfig
from .format_persons import enrich_persons_with_person_type

logger = logging.getLogger(__name__)


def format_individual_tour(
    tours_canonical: pl.DataFrame,
    linked_trips_canonical: pl.DataFrame,
    persons_canonical: pl.DataFrame,
    households_ctramp: pl.DataFrame,
    config: CTRAMPConfig,
    unlinked_trips_canonical: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Format individual tours to CT-RAMP specification.

    Transforms tour data (excluding joint tours) to CT-RAMP format.
    Key Transformations:

    - Filtering: Excludes joint tours (processes only tours with
      joint_tour_id IS NULL)
    - Purpose Mapping: Converts canonical purpose categories to CT-RAMP tour
      purposes
    - Mode Translation: Maps canonical mode types to CT-RAMP mode codes
    - Time-of-Day: Extracts start/end hours from departure/arrival times
    - Stop Counts: Computes outbound and inbound stop counts from linked trips
        - Subtour Frequency: Encodes atWork_freq using CT-RAMP categorical values
            based on work subtour pattern
    - Excludes: Model simulation fields (random numbers, wait times, logsums)

    Args:
        tours_canonical: Canonical tours DataFrame with tour_id, hh_id, person_id,
            person_num, tour_category, tour_purpose, o_taz, d_taz, origin_depart_time,
            origin_arrive_time, tour_mode, joint_tour_id (for filtering), parent_tour_id
            (for subtour counting)
        linked_trips_canonical: Canonical trips DataFrame with tour_id, tour_direction
            (1=outbound, 2=inbound, 3=subtour)
        persons_canonical: Canonical persons DataFrame with person_id, person_num,
            person_type (for mode mapping), school_type (for purpose mapping) are optional
            but re-derived if missing or invalid
        households_ctramp: Formatted CT-RAMP households DataFrame with hh_id, income
        config: CT-RAMP configuration with income thresholds

    Returns:
        DataFrame with CT-RAMP individual tour fields:

            - hh_id, person_id, person_num, person_type
            - tour_id, tour_category, tour_purpose
            - orig_taz, dest_taz
            - start_hour, end_hour
            - tour_mode
            - atWork_freq (subtour frequency category)
            - num_ob_stops, num_ib_stops

    Notes:
        - Excludes joint tours (joint_tour_id IS NULL)
        - Excludes all model-only fields (random numbers, wait times, logsums)
    """
    logger.info("Formatting individual tour data for CT-RAMP")

    # Reclassify invalid joint tours before splitting individual vs joint records.
    tours_canonical = _identify_misclassified_joint_tours(tours_canonical)

    # Derive/validate person_type in persons_with_type before joining to tours
    if "person_type" not in persons_canonical.columns or "type" not in persons_canonical.columns:
        logger.info("Deriving person_type for tour formatting")
        # Pre-compute student_category and employment_category for consistency
        if "student_category" not in persons_canonical.columns:
            persons_canonical = persons_canonical.with_columns(
                ctramp_student_category_expression(school_taz_col="school_taz").alias(
                    "student_category"
                )
            )
        if "employment_category" not in persons_canonical.columns:
            persons_canonical = persons_canonical.with_columns(
                pl.col("employment")
                .replace_strict(
                    EMPLOYMENT_TO_CTRAMP,
                    default=CTRAMPEmploymentCategory.NOT_EMPLOYED.value,
                )
                .alias("employment_category")
            )
        persons_canonical = enrich_persons_with_person_type(persons_canonical)

    # Filter to individual tours only (not joint)
    individual_tours = tours_canonical.filter(pl.col("joint_tour_id").is_null())

    # Join with persons for person_type and school_type,
    # and households for income
    individual_tours = individual_tours.join(
        persons_canonical.select(["person_id", "person_num", "person_type", "school_type"]),
        on="person_id",
        how="left",
    ).join(
        households_ctramp.select(["hh_id", "income"]),
        on="hh_id",
        how="left",
    )

    # Bring parent tour purpose onto each row so at-work labels can be gated
    # by both subtour status and the parent tour being WORK.
    parent_tour_purposes = individual_tours.select(
        [
            pl.col("tour_id").alias("_parent_tour_id_for_join"),
            pl.col("tour_purpose").alias("_parent_tour_purpose"),
        ]
    )
    individual_tours = individual_tours.join(
        parent_tour_purposes,
        left_on="parent_tour_id",
        right_on="_parent_tour_id_for_join",
        how="left",
    )

    # Map tour purpose to CTRAMP format
    individual_tours = individual_tours.with_columns(
        ctramp_purpose_category_expression(
            pl.col("tour_purpose"),
            pl.col("income"),
            pl.col("school_type"),
            pl.col("person_type"),
            config.income_low_threshold,
            config.income_med_threshold,
            config.income_high_threshold,
            pl.col("_parent_tour_purpose"),
            pl.col("parent_tour_id") != pl.col("tour_id"),
        ).alias("tour_purpose_ctramp")
    )

    # Map tour_category to string labels
    # Note: tour_purpose here is the canonical PurposeCategory enum, not the mapped CTRAMP string
    mandatory_purposes = [
        PurposeCategory.WORK.value,
        PurposeCategory.SCHOOL.value,
    ]

    individual_tours = individual_tours.with_columns(
        pl.when(
            (pl.col("parent_tour_id") != pl.col("tour_id"))
            & (pl.col("_parent_tour_purpose") == PurposeCategory.WORK.value)
        )
        .then(pl.lit(CTRAMPTourCategory.AT_WORK.value))
        .when(pl.col("tour_purpose").is_in(mandatory_purposes))
        .then(pl.lit(CTRAMPTourCategory.MANDATORY.value))
        .otherwise(pl.lit(CTRAMPTourCategory.INDIVIDUAL_NON_MANDATORY.value))
        .alias("tour_category_ctramp")
    ).drop("_parent_tour_purpose")

    # Build subtour mix summaries by parent tour, then map to CT-RAMP categories:
    # 0=not work tour, 1=no subtour, 2=one eat, 3=one business,
    # 4=one maintenance, 5=two business, 6=eat and business.
    subtour_counts = (
        individual_tours.filter(pl.col("parent_tour_id") != pl.col("tour_id"))
        .group_by("parent_tour_id")
        .agg(
            [
                pl.len().alias("_subtour_total"),
                (pl.col("tour_purpose_ctramp") == CTRAMPTourPurpose.ATWORK_EAT.value)
                .sum()
                .alias("_subtour_eat_count"),
                (pl.col("tour_purpose_ctramp") == CTRAMPTourPurpose.ATWORK_BUSINESS.value)
                .sum()
                .alias("_subtour_business_count"),
                (pl.col("tour_purpose_ctramp") == CTRAMPTourPurpose.ATWORK_MAINT.value)
                .sum()
                .alias("_subtour_maint_count"),
            ]
        )
    )

    individual_tours = (
        individual_tours.join(
            subtour_counts,
            left_on="tour_id",
            right_on="parent_tour_id",
            how="left",
        )
        .with_columns(
            # Only work tours use CT-RAMP atWork_freq categories.
            pl.when(pl.col("tour_purpose") != PurposeCategory.WORK.value)
            .then(pl.lit(AtWorkFreq.NONE_NOT_WORK.value))
            .when(pl.col("_subtour_total").fill_null(0) == 0)
            .then(pl.lit(AtWorkFreq.NO_SUBTOUR.value))
            .when((pl.col("_subtour_total") == 1) & (pl.col("_subtour_eat_count") == 1))
            .then(pl.lit(AtWorkFreq.ONE_EAT.value))
            .when((pl.col("_subtour_total") == 1) & (pl.col("_subtour_business_count") == 1))
            .then(pl.lit(AtWorkFreq.ONE_BUSINESS.value))
            .when((pl.col("_subtour_total") == 1) & (pl.col("_subtour_maint_count") == 1))
            .then(pl.lit(AtWorkFreq.ONE_MAINT.value))
            .when((pl.col("_subtour_total") == 2) & (pl.col("_subtour_business_count") == 2))
            .then(pl.lit(AtWorkFreq.TWO_BUSINESS.value))
            .when(
                (pl.col("_subtour_total") == 2)
                & (pl.col("_subtour_business_count") == 1)
                & (pl.col("_subtour_eat_count") == 1)
            )
            .then(pl.lit(AtWorkFreq.ONE_EAT_ONE_BUSINESS.value))
            .otherwise(pl.lit(AtWorkFreq.OTHER.value))
            .alias("atWork_freq")
        )
        .drop(
            [
                "_subtour_total",
                "_subtour_eat_count",
                "_subtour_business_count",
                "_subtour_maint_count",
            ]
        )
    )

    # Convert times to hour integers (5am-11pm = 5-23)
    individual_tours = individual_tours.with_columns(
        [
            pl.col("origin_depart_time").dt.hour().alias("start_hour"),
            pl.col("origin_arrive_time").dt.hour().alias("end_hour"),
        ]
    )

    # Map mode to CTRAMP integer codes
    # Get max num_travelers from trips for each tour to properly map occupancy-based modes
    if len(linked_trips_canonical) > 0:
        tour_travelers = linked_trips_canonical.group_by("tour_id").agg(
            pl.col("num_travelers").max().alias("max_num_travelers")
        )
        individual_tours = individual_tours.join(tour_travelers, on="tour_id", how="left")
        # For tours with no trips (shouldn't happen), default to 1
        num_travelers_expr = pl.col("max_num_travelers").fill_null(1)
        
        # Get access/egress for tour's transit trip - Access and egress is based on the first transit trip
        tour_access_egress = (
            linked_trips_canonical.filter(pl.col("access_mode").is_not_null())
            .sort(["tour_id", "depart_time", "arrive_time"])
            .group_by("tour_id")
            .agg(
                [
                    pl.col("access_mode").first().alias("tour_access_mode"),
                    pl.col("egress_mode").first().alias("tour_egress_mode")
                ]
            )
        )
        individual_tours = individual_tours.join(tour_access_egress, on="tour_id", how="left")
        access_expr = pl.col("tour_access_mode")
        egress_expr = pl.col("tour_egress_mode")
    else:
        num_travelers_expr = pl.lit(1)
        access_expr = None
        egress_expr = None


    # Derive the highest transit submode per tour from detailed unlinked-trip modes
    if unlinked_trips_canonical is not None and len(unlinked_trips_canonical) > 0:
        submode_by_indiv_tour = aggregate_transit_submode(
            unlinked_trips_canonical, "tour_id"
        )
        individual_tours = individual_tours.join(
            submode_by_indiv_tour, on="tour_id", how="left"
        )
        indiv_submode_expr = pl.col("transit_submode")
    else:
        indiv_submode_expr = None

    individual_tours = individual_tours.with_columns(
        ctramp_mode_expression(
            pl.col("tour_mode"),
            num_travelers_expr,
            access_expr,
            egress_expr,
            indiv_submode_expr
        ).alias("tour_mode_ctramp")
    )

    # Calculate number of outbound and inbound stops from trips
    # Stops = trips - 1 (number of intermediate destinations, not total trips)
    # A direct home->work tour has 1 trip but 0 stops
    # A home->store->work tour has 2 trips and 1 stop
    if len(linked_trips_canonical) > 0:
        outbound_stops = (
            linked_trips_canonical.filter(pl.col("tour_direction") == TourDirection.OUTBOUND.value)
            .group_by("tour_id")
            .agg((pl.len() - 1).alias("num_ob_stops"))
        )

        inbound_stops = (
            linked_trips_canonical.filter(pl.col("tour_direction") == TourDirection.INBOUND.value)
            .group_by("tour_id")
            .agg((pl.len() - 1).alias("num_ib_stops"))
        )
    else:
        # Handle empty trips DataFrame - create empty aggregation results
        outbound_stops = pl.DataFrame(
            {"tour_id": [], "num_ob_stops": []},
            schema={"tour_id": pl.Int64, "num_ob_stops": pl.UInt32},
        )
        inbound_stops = pl.DataFrame(
            {"tour_id": [], "num_ib_stops": []},
            schema={"tour_id": pl.Int64, "num_ib_stops": pl.UInt32},
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
    # Tours without any trip records will not appear in the trips dataframe
    # Check if any individual tours are missing from the trips data
    tours_with_trips = (
        linked_trips_canonical.select("tour_id").unique()
        if len(linked_trips_canonical) > 0
        else pl.DataFrame({"tour_id": []}, schema={"tour_id": pl.Int64})
    )
    zero_trip_tours = individual_tours.filter(
        ~pl.col("tour_id").is_in(tours_with_trips["tour_id"].implode())
        & (pl.col("subtour_num") == 0)  # Exclude subtours from this check
    )

    if len(zero_trip_tours) > 0:
        tour_ids = zero_trip_tours["tour_id"].to_list()
        msg = (
            f"Found {len(zero_trip_tours)} tours with zero trips. "
            f"Tour IDs: {tour_ids[:10]}{'...' if len(tour_ids) > 10 else ''}"  # noqa: PLR2004
        )
        raise ValueError(msg)

    # Assign CT-RAMP tour_id: 0-based per person for parent tours, with at-work
    # subtours encoded as a two-digit integer <1-based parent tour #><subtour #>.
    individual_tours = _assign_ctramp_tour_ids(individual_tours)

    # Format columns to CTRAMP specifications
    individual_tours = individual_tours.with_columns(
        [
            pl.col("_ctramp_tour_id").alias("tour_id"),  # CTRAMP 0-based per-person tour id
            pl.col("tour_id").alias("_tour_id_canonical"),  # Temp column for joining with trips
            pl.col("tour_category_ctramp").alias("tour_category"),
            pl.col("tour_purpose_ctramp").alias("tour_purpose"),
            pl.col(f"o_{config.taz_field}").cast(pl.Int64).alias("orig_taz"),
            pl.col(f"d_{config.taz_field}").cast(pl.Int64).alias("dest_taz"),
            pl.col("start_hour").cast(pl.Int64),
            pl.col("end_hour").cast(pl.Int64),
            pl.col("tour_mode_ctramp").alias("tour_mode"),
            pl.col("atWork_freq").cast(pl.Int64),
            pl.col("num_ob_stops").cast(pl.Int64),
            pl.col("num_ib_stops").cast(pl.Int64),
        ]
    ).drop("_ctramp_tour_id")

    # Add sampleRate if tour_weight exists
    if "tour_weight" in individual_tours.columns:
        individual_tours = individual_tours.with_columns(
            pl.when(pl.col("tour_weight") > 0)
            .then(pl.col("tour_weight").pow(-1))
            .otherwise(None)
            .alias("sampleRate")
        )

    logger.info("Formatted %d individual tour records", len(individual_tours))
    return individual_tours


def format_joint_tour(
    tours_canonical: pl.DataFrame,
    linked_trips_canonical: pl.DataFrame,
    persons_canonical: pl.DataFrame,
    households_ctramp: pl.DataFrame,
    config: CTRAMPConfig,
    unlinked_trips_canonical: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Format joint tours to CT-RAMP specification.

    Transforms joint tour data with participant aggregation.
    Key Transformations:

    - Identifies tours shared by multiple household members using joint_tour_id
    - Composition: Classifies as adults-only, children-only, or mixed based on
      configurable age threshold
    - Participant List: Maintains ordered list of person_num for all tour
      participants
    - Same Fields: Purpose, mode, time-of-day, stops, destinations (like
      individual tours)

    Args:
        tours_canonical: Canonical tours DataFrame with tour_id, joint_tour_id, hh_id,
            person_id, person_num, tour_category, tour_purpose, o_taz, d_taz,
            origin_depart_time, origin_arrive_time, tour_mode, subtour_num
        linked_trips_canonical: Canonical trips DataFrame with joint_tour_id, tour_direction,
            person_id
        persons_canonical: Canonical persons DataFrame with person_id, person_num,
            age_category (for composition determination)
        households_ctramp: Formatted CT-RAMP households DataFrame with hh_id, income
        config: CT-RAMP configuration with income thresholds and age_adult category

    Returns:
        DataFrame with CT-RAMP joint tour fields

    Notes:
        - Filters to joint tours only (joint_tour_id IS NOT NULL)
        - Aggregates participants into space-separated string
        - Determines composition from participant ages
    """
    logger.info("Formatting joint tour data for CT-RAMP")

    # Reclassify invalid joint tours before building joint-tour outputs.
    tours_canonical = _identify_misclassified_joint_tours(tours_canonical)

    # Handle empty tours DataFrame
    if len(tours_canonical) == 0:
        logger.info("No tours provided")
        return pl.DataFrame()

    # Filter to joint tours only
    joint_tours = tours_canonical.filter(pl.col("joint_tour_id").is_not_null())

    if len(joint_tours) == 0:
        logger.info("No joint tours found")
        return pl.DataFrame()

    # Join person_num for sorting participants
    joint_tours = joint_tours.join(
        persons_canonical.select(["person_id", "person_num", "age"]),
        on="person_id",
        how="left",
    )

    # Group by joint_tour_id to aggregate participants
    # This assumes tours table has person_num for each participant
    participants_agg = joint_tours.group_by("joint_tour_id").agg(
        [
            pl.col("hh_id").first(),
            pl.col("person_num").sort().cast(pl.Utf8).str.join(" ").alias("Participants"),
            pl.col("tour_category").first(),
            pl.col("tour_purpose").first(),
            pl.col(f"o_{config.taz_field}").first(),
            pl.col(f"d_{config.taz_field}").first(),
            pl.col("origin_depart_time").first(),
            pl.col("origin_arrive_time").first(),
            pl.col("tour_mode").first(),
            pl.col("subtour_num").first(),
        ]
    )

    # Join with persons to determine composition
    participants_with_ages = joint_tours.join(
        persons_canonical.select(["person_id", "age"]),
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

    # Join with households for income and weight
    hh_cols = ["hh_id", "income"]
    if "hh_weight" in households_ctramp.columns:
        hh_cols.append("hh_weight")
    joint_tours_formatted = joint_tours_formatted.join(
        households_ctramp.select(hh_cols),
        on="hh_id",
        how="left",
    )

    # Calculate all trip-based metrics in a single aggregation
    # Stops = trips - 1 (number of intermediate destinations)
    # Ensure stops never goes below 0 (when there are 0 trips in a direction)
    if len(linked_trips_canonical) > 0:
        joint_trip_stats = (
            linked_trips_canonical.filter(pl.col("joint_tour_id").is_not_null())
            .sort(["joint_tour_id", "depart_time", "arrive_time"])
            .group_by("joint_tour_id")
            .agg(
                [
                    pl.col("person_id").n_unique().alias("num_travelers"),
                    (
                        pl.when(pl.col("tour_direction") == TourDirection.OUTBOUND.value)
                        .then(1)
                        .sum()
                        - 1
                    )
                    .clip(lower_bound=0)
                    .alias("num_ob_stops"),
                    (
                        pl.when(pl.col("tour_direction") == TourDirection.INBOUND.value)
                        .then(1)
                        .sum()
                        - 1
                    )
                    .clip(lower_bound=0)
                    .alias("num_ib_stops"),
                    pl.when(pl.col("tour_direction") == TourDirection.SUBTOUR.value)
                    .then(1)
                    .sum()
                    .alias("num_subtour_stops"),
                    # Access/egress from joint tour's first transit trip
                    pl.col("access_mode")
                    .filter(pl.col("access_mode").is_not_null())
                    .first()
                    .alias("tour_access_mode"),
                    pl.col("egress_mode")
                    .filter(pl.col("egress_mode").is_not_null())
                    .first()
                    .alias("tour_egress_mode")
                ]
            )
        )

        joint_tours_formatted = joint_tours_formatted.join(
            joint_trip_stats, on="joint_tour_id", how="left"
        ).with_columns(
            [
                pl.col("num_ob_stops").fill_null(0),
                pl.col("num_ib_stops").fill_null(0),
                pl.col("num_subtour_stops").fill_null(0),
                pl.lit(None).alias("tour_access_mode"),
                pl.lit(None).alias("tour_egress_mode")
            ]
        )
    else:
        joint_tours_formatted = joint_tours_formatted.with_columns(
            [
                pl.lit(None).cast(pl.Int64).alias("num_travelers"),
                pl.lit(0).alias("num_ob_stops"),
                pl.lit(0).alias("num_ib_stops"),
                pl.lit(0).alias("num_subtour_stops"),
            ]
        )

    # Derive the highest transit submode per joint tour from detailed unlinked-trip modes
    if unlinked_trips_canonical is not None and len(unlinked_trips_canonical) > 0:
        submode_by_joint_tour = aggregate_transit_submode(
            unlinked_trips_canonical, "joint_tour_id"
        )
        joint_tours_formatted = joint_tours_formatted.join(
            submode_by_joint_tour, on="joint_tour_id", how="left"
        )
        joint_submode_expr = pl.col("transit_submode")
    else:
        joint_submode_expr = None

    # Map purpose and mode
    joint_tours_formatted = joint_tours_formatted.with_columns(
        [
            ctramp_purpose_category_expression(
                pl.col("tour_purpose"),
                pl.col("income"),
                pl.lit(SchoolType.MISSING.value),  # Joint tours not for school
                pl.lit(CTRAMPPersonType.NON_WORKER.value),  # Joint tours not for work
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
            ).alias("tour_purpose_ctramp"),
            ctramp_mode_expression(
                pl.col("tour_mode"),
                pl.col("num_travelers"),
                None,  # Tours don't have access/egress modes
                None,
                joint_submode_expr,
            ).alias("tour_mode_ctramp"),
        ]
    )

    # Convert times
    joint_tours_formatted = joint_tours_formatted.with_columns(
        [
            pl.col("origin_depart_time").dt.hour().alias("start_hour"),
            pl.col("origin_arrive_time").dt.hour().alias("end_hour"),
        ]
    )

    # Map tour_category to string labels (joint tours are always JOINT_NON_MANDATORY)
    joint_tours_formatted = joint_tours_formatted.with_columns(
        pl.lit(CTRAMPTourCategory.JOINT_NON_MANDATORY).alias("tour_category")
    )

    # Validate that no joint tours have zero trips
    # Check if joint_tour_id appears in trips at all, not just stop counts
    if len(linked_trips_canonical) > 0:
        joint_tours_with_trips = (
            linked_trips_canonical.filter(pl.col("joint_tour_id").is_not_null())
            .select("joint_tour_id")
            .unique()
        )
        if len(joint_tours_with_trips) > 0:
            zero_trip_tours = joint_tours_formatted.filter(
                ~pl.col("joint_tour_id").is_in(joint_tours_with_trips["joint_tour_id"].implode())
            )
            if len(zero_trip_tours) > 0:
                tour_ids = zero_trip_tours["joint_tour_id"].to_list()
                msg = (
                    f"Found {len(zero_trip_tours)} joint tours with zero trips. "
                    f"Joint tour IDs: {tour_ids[:10]}"
                    f"{'...' if len(tour_ids) > 10 else ''}"  # noqa: PLR2004
                )
                raise ValueError(msg)

    # CT-RAMP joint tour_id is 0-based per household (0=first joint tour, ...).
    # The canonical joint_tour_id is a household-scoped 1-based enumerator, so a
    # dense rank within the household minus 1 yields the CT-RAMP numbering.
    joint_tours_formatted = joint_tours_formatted.with_columns(
        (pl.col("joint_tour_id").rank("dense").over("hh_id") - 1)
        .cast(pl.Int64)
        .alias("_ctramp_joint_tour_id")
    )

    # Select final columns with snake_case names
    select_cols = [
        pl.col("hh_id"),
        pl.col("_ctramp_joint_tour_id").alias("tour_id"),
        pl.col("tour_category"),
        pl.col("tour_purpose_ctramp").alias("tour_purpose"),
        pl.col("Composition").alias("tour_composition"),
        pl.col("Participants").alias("tour_participants"),
        pl.col(f"o_{config.taz_field}").cast(pl.Int64).alias("orig_taz"),
        pl.col(f"d_{config.taz_field}").cast(pl.Int64).alias("dest_taz"),
        pl.col("start_hour").cast(pl.Int64),
        pl.col("end_hour").cast(pl.Int64),
        pl.col("tour_mode_ctramp").alias("tour_mode"),
        pl.col("num_ob_stops").cast(pl.Int64),
        pl.col("num_ib_stops").cast(pl.Int64),
    ]
    if "hh_weight" in joint_tours_formatted.columns:
        select_cols.append(pl.col("hh_weight").alias("tour_weight"))
    joint_tours_ctramp = joint_tours_formatted.select(select_cols)

    logger.info("Formatted %d joint tour records", len(joint_tours_ctramp))
    return joint_tours_ctramp


def _assign_ctramp_tour_ids(individual_tours: pl.DataFrame) -> pl.DataFrame:
    """Assign CT-RAMP ``tour_id`` values to individual tours.

    CT-RAMP numbers a person's home-based tours 0-based (first tour is 0, second
    is 1, ...). At-work subtours are encoded as a two-digit integer where the
    first digit is the 1-based parent tour number and the second digit is the
    subtour sequence number (e.g. ``12`` is the second subtour on the person's
    first tour, whose own ``tour_id`` is ``0``).

    Subtours are identified the same way as elsewhere in this module: a tour
    whose ``parent_tour_id`` is non-null and points to a *different* tour. Parent
    tours either have a null ``parent_tour_id`` or one equal to their own
    ``tour_id``. A subtour whose parent cannot be resolved to a base tour (for
    example, the parent was filtered out because it belongs to a joint tour) is
    treated as a base tour so that every row receives a valid, non-null
    ``tour_id``.

    Args:
        individual_tours: Canonical individual tours with ``person_id``, canonical
            ``tour_id``, and ``parent_tour_id``.

    Returns:
        DataFrame with a ``_ctramp_tour_id`` column added.
    """
    is_subtour_candidate = pl.col("parent_tour_id").is_not_null() & (
        pl.col("parent_tour_id") != pl.col("tour_id")
    )
    individual_tours = individual_tours.with_columns(
        is_subtour_candidate.alias("_is_subtour_candidate")
    )

    # Tour ids that can serve as parents (i.e. are not themselves subtours).
    parent_keys = individual_tours.filter(~pl.col("_is_subtour_candidate"))["tour_id"]

    # A tour is only encoded as a subtour when its parent resolves to a base tour.
    # Orphan subtours (parent filtered out / missing) fall back to base tours.
    individual_tours = individual_tours.with_columns(
        (
            pl.col("_is_subtour_candidate")
            & pl.col("parent_tour_id").is_in(parent_keys.implode())
        ).alias("_is_subtour")
    )

    # Base tours: 0-based index per person, ordered by canonical tour_id
    # (which is monotonic in day and tour sequence).
    parent_idx = (
        individual_tours.filter(~pl.col("_is_subtour"))
        .select(["person_id", "tour_id"])
        .with_columns(
            (pl.col("tour_id").rank("ordinal").over("person_id") - 1)
            .cast(pl.Int64)
            .alias("_ctramp_parent_idx")
        )
        .select(
            pl.col("tour_id").alias("_parent_key"),
            pl.col("_ctramp_parent_idx"),
        )
    )

    # Subtours: 1-based sequence within their parent tour, ordered by tour_id.
    subtour_seq = (
        individual_tours.filter(pl.col("_is_subtour"))
        .select(["tour_id", "parent_tour_id"])
        .with_columns(
            pl.col("tour_id")
            .rank("ordinal")
            .over("parent_tour_id")
            .cast(pl.Int64)
            .alias("_subtour_seq")
        )
        .select(["tour_id", "_subtour_seq"])
    )

    # Base tours key on their own tour_id; subtours key on their parent_tour_id.
    individual_tours = (
        individual_tours.with_columns(
            pl.when(pl.col("_is_subtour"))
            .then(pl.col("parent_tour_id"))
            .otherwise(pl.col("tour_id"))
            .alias("_parent_key")
        )
        .join(parent_idx, on="_parent_key", how="left")
        .join(subtour_seq, on="tour_id", how="left")
    )

    individual_tours = individual_tours.with_columns(
        pl.when(pl.col("_is_subtour"))
        .then((pl.col("_ctramp_parent_idx") + 1) * 10 + pl.col("_subtour_seq"))
        .otherwise(pl.col("_ctramp_parent_idx"))
        .cast(pl.Int64)
        .alias("_ctramp_tour_id")
    ).drop(
        [
            "_is_subtour_candidate",
            "_is_subtour",
            "_parent_key",
            "_ctramp_parent_idx",
            "_subtour_seq",
        ]
    )

    return individual_tours


def _identify_misclassified_joint_tours(tours_canonical: pl.DataFrame) -> pl.DataFrame:
    """Reclassify invalid joint tours by clearing their ``joint_tour_id``.

    Misclassified joint tours include:

    - WORK tours
    - SCHOOL tours
    - ESCORT tours
    - Joint tour groups containing multiple distinct purposes

    Args:
        tours_canonical: Canonical tours DataFrame with tour_id, joint_tour_id, hh_id,
            person_id, person_num, tour_category, tour_purpose, o_taz, d_taz,
            origin_depart_time, origin_arrive_time, tour_mode, subtour_num

    Returns:
        Tours table with invalid joint tour groups converted to individual tours.
    """
    if len(tours_canonical) == 0 or "joint_tour_id" not in tours_canonical.columns:
        return tours_canonical

    joint_tours = tours_canonical.filter(pl.col("joint_tour_id").is_not_null())
    if len(joint_tours) == 0:
        return tours_canonical

    misclassified_joint_ids = (
        joint_tours.group_by("joint_tour_id")
        .agg(
            [
                pl.col("tour_purpose").n_unique().alias("_n_unique_purposes"),
                (pl.col("tour_purpose") == PurposeCategory.WORK.value).any().alias("_has_work"),
                (pl.col("tour_purpose") == PurposeCategory.SCHOOL.value).any().alias("_has_school"),
                (pl.col("tour_purpose") == PurposeCategory.ESCORT.value).any().alias("_has_escort"),
            ]
        )
        .filter(
            pl.col("_has_work")
            | pl.col("_has_school")
            | pl.col("_has_escort")
            | (pl.col("_n_unique_purposes") > 1)
        )
        .select("joint_tour_id")
    )

    if len(misclassified_joint_ids) == 0:
        return tours_canonical

    logger.info(
        "Reclassifying %d misclassified joint tours to individual tours",
        len(misclassified_joint_ids),
    )

    return tours_canonical.with_columns(
        pl.when(pl.col("joint_tour_id").is_in(misclassified_joint_ids["joint_tour_id"].implode()))
        .then(pl.lit(None).cast(tours_canonical.schema["joint_tour_id"]))
        .otherwise(pl.col("joint_tour_id"))
        .alias("joint_tour_id")
    )
