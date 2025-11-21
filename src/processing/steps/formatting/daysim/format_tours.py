"""Tour formatting for DaySim output."""

import logging

import polars as pl

from .mappings import PURPOSE_MAP

logger = logging.getLogger(__name__)


def format_tours(tours: pl.DataFrame) -> pl.DataFrame:
    """Format tour data to DaySim specification.

    Transforms canonical tour data into DaySim tour format with proper
    field mappings and time conversions.

    Args:
        tours: DataFrame with canonical tour fields

    Returns:
        DataFrame with DaySim tour fields
    """
    logger.info("Formatting tour data")

    # Extract household, person, and day IDs from composite keys
    tours_daysim = tours.with_columns(
        hhno=(pl.col("person_id") // 100),
        pno=(pl.col("person_id") % 100),
        day=(pl.col("day_id") % 10),
    )

    # Map tour identifiers and purpose
    tours_daysim = tours_daysim.with_columns(
        tour=pl.col("tour_sequence_num"),
        parent=pl.when(pl.col("parent_tour_id").is_null())
        .then(pl.lit(0))
        .otherwise(pl.col("parent_tour_id")),
        pdpurp=pl.col("primary_dest_purpose").replace(PURPOSE_MAP),
    )

    # Convert times to DaySim format (minutes after midnight)
    tours_daysim = tours_daysim.with_columns(
        tlvorig=(
            pl.col("origin_depart_time").dt.hour() * 60
            + pl.col("origin_depart_time").dt.minute()
        ),
        tardest=(
            pl.col("dest_arrive_time").dt.hour() * 60
            + pl.col("dest_arrive_time").dt.minute()
        ),
        tlvdest=(
            pl.col("dest_depart_time").dt.hour() * 60
            + pl.col("dest_depart_time").dt.minute()
        ),
        tarorig=(
            pl.col("origin_arrive_time").dt.hour() * 60
            + pl.col("origin_arrive_time").dt.minute()
        ),
    )

    # Map location types to DaySim address type codes
    # 1=home, 2=work, 3=school, 4=escort, 5=other
    tours_daysim = tours_daysim.with_columns(
        toadtyp=pl.when(pl.col("o_location_type") == "home")
        .then(pl.lit(1))
        .when(pl.col("o_location_type") == "work")
        .then(pl.lit(2))
        .when(pl.col("o_location_type") == "school")
        .then(pl.lit(3))
        .otherwise(pl.lit(5)),
        tdadtyp=pl.when(pl.col("d_location_type") == "home")
        .then(pl.lit(1))
        .when(pl.col("d_location_type") == "work")
        .then(pl.lit(2))
        .when(pl.col("d_location_type") == "school")
        .then(pl.lit(3))
        .otherwise(pl.lit(5)),
    )

    # Set location coordinates and mode
    tours_daysim = tours_daysim.with_columns(
        toxco=pl.col("o_lon"),
        toyco=pl.col("o_lat"),
        tdxco=pl.col("d_lon"),
        tdyco=pl.col("d_lat"),
        tmodetp=pl.col("tour_mode"),
    )

    # Add DaySim-specific fields (placeholders and defaults)
    tours_daysim = tours_daysim.with_columns(
        # Tour structure fields
        jtindex=pl.lit(0),  # Joint tour index (not supported)
        subtours=pl.lit(0),  # Work-based subtours count
        # Zone/parcel fields (not available in canonical)
        topcl=pl.lit(-1),  # Origin parcel
        totaz=pl.lit(-1),  # Origin TAZ
        tdpcl=pl.lit(-1),  # Destination parcel
        tdtaz=pl.lit(-1),  # Destination TAZ
        # Travel characteristics (not available)
        tpathtp=pl.lit(1),  # Path type (default to full network)
        tautotime=pl.lit(-1.0),  # Auto time
        tautocost=pl.lit(-1.0),  # Auto cost
        tautodist=pl.lit(-1.0),  # Auto distance
        # Stop counts
        tripsh1=pl.col("num_outbound_stops") + 1,  # Outbound trip segments
        tripsh2=pl.col("num_inbound_stops") + 1,  # Inbound trip segments
        # Half-tour indices (not used)
        phtindx1=pl.lit(0),
        phtindx2=pl.lit(0),
        fhtindx1=pl.lit(0),
        fhtindx2=pl.lit(0),
        # Expansion factor
        toexpfac=pl.lit(1.0),
    )

    # Select DaySim tour fields
    tour_cols = [
        "hhno",
        "pno",
        "day",
        "tour",
        "jtindex",
        "parent",
        "subtours",
        "pdpurp",
        "tlvorig",
        "tardest",
        "tlvdest",
        "tarorig",
        "toadtyp",
        "tdadtyp",
        "topcl",
        "totaz",
        "tdpcl",
        "tdtaz",
        "toxco",
        "toyco",
        "tdxco",
        "tdyco",
        "tmodetp",
        "tpathtp",
        "tautotime",
        "tautocost",
        "tautodist",
        "tripsh1",
        "tripsh2",
        "phtindx1",
        "phtindx2",
        "fhtindx1",
        "fhtindx2",
        "toexpfac",
    ]

    return (
        tours_daysim
        .select(tour_cols)
        .sort(by=["hhno", "pno", "day", "tour"])
    )
