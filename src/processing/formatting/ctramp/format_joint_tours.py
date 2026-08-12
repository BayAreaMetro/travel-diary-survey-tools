"""Format canonical joint tours for CT-RAMP."""

import logging
from collections.abc import Collection

import polars as pl

from data_canon.codebook.ctramp import (
    CTRAMPPersonType,
    CTRAMPTourCategory,
    TourComposition,
)
from data_canon.codebook.persons import SchoolType
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import PurposeCategory
from processing.weighting.core.hierarchy import MEMBER_COUNT_COL

from .ctramp_config import CTRAMPConfig
from .mode_mappings import (
    aggregate_transit_submode,
    ctramp_mode_expression,
)
from .purpose_mappings import ctramp_purpose_category_expression

logger = logging.getLogger(__name__)


def format_joint_tour(
    tours_canonical: pl.DataFrame,
    linked_trips_canonical: pl.DataFrame,
    unlinked_trips_canonical: pl.DataFrame,
    joint_tours_canonical: pl.DataFrame,
    persons_canonical: pl.DataFrame,
    households_ctramp: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format canonical joint tours to CT-RAMP's household-level schema."""
    logger.info("Formatting joint tour data for CT-RAMP")
    tours_canonical = identify_misclassified_joint_tours(tours_canonical)

    if tours_canonical.is_empty():
        logger.info("No tours provided")
        return pl.DataFrame()

    joint_tours = tours_canonical.filter(pl.col("joint_tour_id").is_not_null())
    if joint_tours.is_empty():
        logger.info("No joint tours found")
        return pl.DataFrame()

    joint_tours = joint_tours.join(
        persons_canonical.select(["person_id", "person_num", "age"]),
        on="person_id",
        how="left",
    )
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
    joint_tours_formatted = participants_agg.join(
        composition_agg.select(["joint_tour_id", "Composition"]),
        on="joint_tour_id",
        how="left",
    )

    # Income only: a joint record's weight and rate come from its own columns, never
    # from hh_weight, which the cascade has since split and redistributed.
    joint_tours_formatted = joint_tours_formatted.join(
        households_ctramp.select(["hh_id", "income"]),
        on="hh_id",
        how="left",
    )

    if not linked_trips_canonical.is_empty():
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
                    pl.col("access_mode")
                    .filter(pl.col("access_mode").is_not_null())
                    .first()
                    .alias("tour_access_mode"),
                    pl.col("egress_mode")
                    .filter(pl.col("egress_mode").is_not_null())
                    .first()
                    .alias("tour_egress_mode"),
                ]
            )
        )
        joint_tours_formatted = joint_tours_formatted.join(
            joint_trip_stats, on="joint_tour_id", how="left"
        ).with_columns(
            [
                pl.col("num_ob_stops").fill_null(0),
                pl.col("num_ib_stops").fill_null(0),
                pl.lit(None).alias("tour_access_mode"),
                pl.lit(None).alias("tour_egress_mode"),
            ]
        )
    else:
        joint_tours_formatted = joint_tours_formatted.with_columns(
            [
                pl.lit(None).cast(pl.Int64).alias("num_travelers"),
                pl.lit(0).alias("num_ob_stops"),
                pl.lit(0).alias("num_ib_stops"),
            ]
        )

    if not unlinked_trips_canonical.is_empty():
        submode_by_joint_tour = aggregate_transit_submode(unlinked_trips_canonical, "joint_tour_id")
        joint_tours_formatted = joint_tours_formatted.join(
            submode_by_joint_tour, on="joint_tour_id", how="left"
        )
        joint_submode_expr = pl.col("transit_submode")
        tnc_type = pl.col("tnc_type")
    else:
        joint_submode_expr = None
        tnc_type = None

    joint_tours_formatted = joint_tours_formatted.with_columns(
        [
            ctramp_purpose_category_expression(
                pl.col("tour_purpose"),
                pl.col("income"),
                pl.lit(SchoolType.MISSING.value),
                pl.lit(CTRAMPPersonType.NON_WORKER.value),
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
            ).alias("tour_purpose_ctramp"),
            ctramp_mode_expression(
                pl.col("tour_mode"),
                pl.col("num_travelers"),
                pl.col("tour_access_mode"),
                pl.col("tour_egress_mode"),
                joint_submode_expr,
                tnc_type,
            ).alias("tour_mode_ctramp"),
        ]
    ).with_columns(
        [
            pl.col("origin_depart_time").dt.hour().alias("start_hour"),
            pl.col("origin_arrive_time").dt.hour().alias("end_hour"),
            pl.lit(CTRAMPTourCategory.JOINT_NON_MANDATORY.value).alias("tour_category"),
        ]
    )

    _validate_joint_tours_have_trips(joint_tours_formatted, linked_trips_canonical)

    joint_tours_formatted = joint_tours_formatted.with_columns(
        (pl.col("joint_tour_id").rank("dense").over("hh_id") - 1)
        .cast(pl.Int64)
        .alias("_ctramp_joint_tour_id")
    )

    # The joint tour weight is the SUM of its member tours, so its inverse is not a
    # sample rate; the per-tour rate is the inverse of the member mean, recovered
    # exactly as num_represented_members / weight. Derived from this record's own
    # columns -- weights do not copy down the hierarchy unchanged.
    weight_cols: list[pl.Expr] = []
    if "joint_tour_weight" in joint_tours_canonical.columns:
        has_count = MEMBER_COUNT_COL in joint_tours_canonical.columns
        carried = ["joint_tour_id", "joint_tour_weight", *([MEMBER_COUNT_COL] if has_count else [])]
        joint_tours_formatted = joint_tours_formatted.join(
            joint_tours_canonical.select(carried),
            on="joint_tour_id",
            how="left",
        )
        weight_cols = joint_weight_columns(joint_tours_canonical.columns, "joint_tour_weight")

    return joint_tours_formatted.select(
        [
            pl.col("hh_id"),
            # WARNING: Canonical processing uses joint_tour_id. CT-RAMP's legacy
            # joint-tour schema calls this household-scoped number tour_id.
            # Rename only at the output boundary to avoid conflating the identifiers.
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
            *weight_cols,
        ]
    )


def joint_weight_columns(available: Collection[str], weight_col: str) -> list[pl.Expr]:
    """Carry a joint weight through, with its sample rate where recoverable.

    A joint weight is the SUM over its participants, so its inverse is *not* a
    sample rate: CT-RAMP re-applies the party multiplier itself
    (``num_participants/sampleRate``), and inverting the sum would count the party
    twice. The per-member rate is the inverse of the member mean, recovered exactly
    as ``MEMBER_COUNT_COL / weight`` -- from the record's own columns, since weights
    do not copy down the hierarchy unchanged.

    Args:
        available: Columns present on the frame the expressions will run against.
        weight_col: The joint weight column to carry through.

    Returns:
        The weight expression, plus ``sampleRate`` when the member count is
        available; empty when there is no weight to carry.
    """
    if weight_col not in available:
        return []
    cols = [pl.col(weight_col)]
    if MEMBER_COUNT_COL in available:
        cols.append(
            pl.when(pl.col(weight_col) > 0)
            .then(pl.col(MEMBER_COUNT_COL) / pl.col(weight_col))
            .otherwise(None)
            .alias("sampleRate")
        )
    else:
        # Supplied joint weights skip the cascade that publishes the count, so the
        # member mean cannot be recovered. Emit no rate rather than a wrong one.
        logger.warning(
            "%s has no %s: emitting the weight without sampleRate", weight_col, MEMBER_COUNT_COL
        )
    return cols


def identify_misclassified_joint_tours(tours_canonical: pl.DataFrame) -> pl.DataFrame:
    """Convert inadmissible joint-tour groups to individual tours.

    CT-RAMP joint tours cannot be work, school, escort, or groups whose members
    have different purposes. Clearing ``joint_tour_id`` preserves that travel as
    individual tours instead of dropping it.
    """
    if tours_canonical.is_empty() or "joint_tour_id" not in tours_canonical.columns:
        return tours_canonical

    joint_tours = tours_canonical.filter(pl.col("joint_tour_id").is_not_null())
    if joint_tours.is_empty():
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

    if misclassified_joint_ids.is_empty():
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


def _validate_joint_tours_have_trips(joint_tours: pl.DataFrame, linked_trips: pl.DataFrame) -> None:
    """Raise when a joint tour has no corresponding linked trip."""
    if linked_trips.is_empty():
        return
    joint_tours_with_trips = (
        linked_trips.filter(pl.col("joint_tour_id").is_not_null()).select("joint_tour_id").unique()
    )
    if joint_tours_with_trips.is_empty():
        return
    zero_trip_tours = joint_tours.filter(
        ~pl.col("joint_tour_id").is_in(joint_tours_with_trips["joint_tour_id"].implode())
    )
    if zero_trip_tours.is_empty():
        return
    tour_ids = zero_trip_tours["joint_tour_id"].to_list()
    msg = (
        f"Found {len(zero_trip_tours)} joint tours with zero trips. "
        f"Joint tour IDs: {tour_ids[:10]}"
        f"{'...' if len(tour_ids) > 10 else ''}"  # noqa: PLR2004
    )
    raise ValueError(msg)
