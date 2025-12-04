"""Helper functions for creating globally unique IDs in the pipeline.

This module provides utilities for creating composite IDs from hierarchical
identifier columns. IDs follow a hierarchical concatenation structure:
    hh_id (8 digits) -> person_id (+ 2 digits) -> day_id (+ 2 digits) ->
    linked_trip_id (+ 2 digits) -> etc.

Each level is created by appending zero-padded digits to the parent level,
forming a character string that uniquely identifies entities at each level
of the hierarchy.

ID vs Num Columns:
    - ID columns (e.g., day_id, person_id): Pre-concatenated hierarchical
      identifiers that contain the full path from root (hh_id) to current level.
      Multiple rows may share the same parent ID (e.g., multiple trips per day).
    - Num columns (e.g., day_num, person_num): Sequence numbers within a parent
      scope. These reset for each new parent (e.g., each person has
      day_num 1, 2, 3).
"""

import logging

import polars as pl

logger = logging.getLogger(__name__)


def create_concatenated_id(
    df: pl.DataFrame,
    output_col: str,
    parent_id_col: str,
    sequence_col: str,
    sequence_padding: int = 2,
) -> pl.DataFrame:
    """Create hierarchical ID by concatenating parent ID with padded sequence.

    This function creates IDs following a hierarchical structure where each
    level is formed by appending zero-padded sequence numbers to the parent:
        - hh_id: "23000075" [Can variable length, typically 8 digits]
        - person_id: "2300007501" [10-digits] (hh_id + 2-digit person_num)
        - day_id: "230000750101" [12-digits] (person_id + 2-digit day_num)
        - unlinked_trip_id: "23000075010101" [14-digits]
            (day_id + 2-digit trip_num)
        - linked_trip_id: "23000075010102" [14-digits]
            (day_id + 2-digit trip_num)
        - tour_id : "23000075010101" [16-digits]
            (day_id + 2-digit parent tour_num + 2-digit sub-tour_num)

    The parent_id_col may already contain the full hierarchical path
    (pre-concatenated), making this a simple append operation.

    Args:
        df: Input DataFrame
        output_col: Name for the new ID column to create
        parent_id_col: Column containing the parent ID (may be pre-concatenated)
        sequence_col: Column containing sequence number at this level
        sequence_padding: Number of digits for zero-padding sequence
            (default: 2)

    Returns:
        DataFrame with new ID column added

    Examples:
        # Create person_id from hh_id + person_num
        >>> df = create_concatenated_id(
        ...     df,
        ...     output_col="person_id",
        ...     parent_id_col="hh_id",
        ...     sequence_col="person_num",
        ...     sequence_padding=2
        ... )

        # Create day_id from person_id + day_num
        >>> df = create_concatenated_id(
        ...     df,
        ...     output_col="day_id",
        ...     parent_id_col="person_id",
        ...     sequence_col="day_num",
        ...     sequence_padding=2
        ... )

        # Create linked_trip_id from day_id + linked_trip_num
        >>> df = create_concatenated_id(
        ...     df,
        ...     output_col="linked_trip_id",
        ...     parent_id_col="day_id",
        ...     sequence_col="linked_trip_num",
        ...     sequence_padding=2
        ... )
    """
    if df.is_empty():
        logger.info(
            "Empty DataFrame: adding %s column with null values",
            output_col,
        )
        return df.with_columns(pl.lit(None).cast(pl.Utf8).alias(output_col))

    logger.info(
        "Creating %s = %s + %s (padded to %d digits)",
        output_col,
        parent_id_col,
        sequence_col,
        sequence_padding,
    )

    id_expr = pl.col(parent_id_col).cast(pl.Utf8) + pl.col(sequence_col).cast(
        pl.Utf8
    ).str.pad_start(sequence_padding, "0")

    return df.with_columns(id_expr.alias(output_col))


def create_linked_trip_id(
    df: pl.DataFrame,
    day_id_col: str = "day_id",
    sequence_col: str = "linked_trip_num",
    sequence_padding: int = 2,
) -> pl.DataFrame:
    """Create linked_trip_id by appending sequence to day_id.

    Convenience wrapper for create_concatenated_id() specifically for
    linked trip IDs. Assumes day_id is already pre-concatenated with
    the full hierarchy (hh_id + person_num + day_num).

    Args:
        df: Input DataFrame containing day_id and sequence columns
        day_id_col: Column name for day ID (default: "day_id")
        sequence_col: Column name for sequence number
            (default: "linked_trip_num")
        sequence_padding: Number of digits for padding (default: 2)

    Returns:
        DataFrame with "linked_trip_id" column added

    Example:
        >>> trips = trips.with_columns(
        ...     pl.col("new_trip_flag")
        ...       .cum_sum()
        ...       .over("person_id")
        ...       .alias("linked_trip_num")
        ... )
        >>> trips = create_linked_trip_id(trips)
    """
    return create_concatenated_id(
        df,
        output_col="linked_trip_id",
        parent_id_col=day_id_col,
        sequence_col=sequence_col,
        sequence_padding=sequence_padding,
    )


def create_tour_id(
    df: pl.DataFrame,
    day_id_col: str = "day_id",
    tour_num_col: str = "tour_num_in_day",
    subtour_num_col: str | None = None,
    padding: int = 2,
) -> pl.DataFrame:
    """Create tour_id with nested parent tour and subtour structure.

    Tour IDs have a special nested structure:
        tour_id = day_id + 2-digit tour_num + 2-digit subtour_num

    For parent tours, subtour_num is 0 (or can be omitted).
    For subtours, subtour_num is > 0.

    Examples:
        "23000075010100" - parent tour (tour #1, subtour #0)
        "23000075010101" - subtour #1 of tour #1
        "23000075010102" - subtour #2 of tour #1
        "23000075010200" - parent tour #2

    Args:
        df: Input DataFrame containing day_id and tour number columns
        day_id_col: Column name for day ID (default: "day_id")
        tour_num_col: Column name for tour number within day
            (default: "tour_num_in_day")
        subtour_num_col: Optional column name for subtour number.
            If None, subtour number will be 0 (parent tours only).
            (default: None)
        padding: Number of digits for padding each component (default: 2)

    Returns:
        DataFrame with "tour_id" column added

    Raises:
        ValueError: If validate=True and day_id contains duplicate values

    Examples:
        # Create parent tour IDs (subtour_num = 0)
        >>> tours = create_tour_id(tours)

        # Create tour IDs with subtours
        >>> tours = create_tour_id(
        ...     tours,
        ...     subtour_num_col="subtour_num"
        ... )
    """
    if df.is_empty():
        logger.info("Empty DataFrame: adding tour_id with null values")
        return df.with_columns(pl.lit(None).cast(pl.Utf8).alias("tour_id"))

    # Build the tour ID expression
    if subtour_num_col is None:
        # Parent tours only: tour_id = day_id + tour_num + "00"
        logger.info(
            "Creating parent tour_id = %s + %s + 00",
            day_id_col,
            tour_num_col,
        )
        tour_id_expr = (
            pl.col(day_id_col).cast(pl.Utf8)
            + pl.col(tour_num_col).cast(pl.Utf8).str.pad_start(padding, "0")
            + "00"
        )
    else:
        # Tours with subtours: tour_id = day_id + tour_num + subtour_num
        logger.info(
            "Creating tour_id = %s + %s + %s",
            day_id_col,
            tour_num_col,
            subtour_num_col,
        )
        tour_id_expr = (
            pl.col(day_id_col).cast(pl.Utf8)
            + pl.col(tour_num_col).cast(pl.Utf8).str.pad_start(padding, "0")
            + pl.col(subtour_num_col).cast(pl.Utf8).str.pad_start(padding, "0")
        )

    return df.with_columns(tour_id_expr.alias("tour_id"))
