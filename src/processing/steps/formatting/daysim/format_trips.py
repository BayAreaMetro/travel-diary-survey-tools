"""Trip formatting for DaySim output."""

import logging

import polars as pl

from data_canon.codebook.daysim import (
    DaysimDriverPassenger,
    DaysimMode,
    DaysimPathType,
    VehicleOccupancy,
)
from data_canon.codebook.trips import (
    Driver,
    Mode,
    ModeType,
)

from .mappings import (
    DROVE_ACCESS_EGRESS,
    PURPOSE_MAP,
)

logger = logging.getLogger(__name__)


def format_linked_trips(linked_trips: pl.DataFrame) -> pl.DataFrame:
    """Format linked trip data to DaySim specification.

    Computes DaySim mode, path type, and driver/passenger codes from linked
    trip data.

    Args:
        linked_trips: DataFrame with canonical linked trip fields

    Returns:
        DataFrame with DaySim trip fields
    """
    logger.info("Formatting linked trip data")

    # Rename columns to DaySim naming convention
    trips_daysim = linked_trips.rename({
        "hh_id": "hhno",
        "person_num": "pno",
        "trip_num": "tripno",
        "travel_dow": "day",
        "o_taz": "otaz",
        "o_maz": "opcl",
        "d_taz": "dtaz",
        "d_maz": "dpcl",
        "o_lon": "oxcord",
        "o_lat": "oycord",
        "d_lon": "dxcord",
        "d_lat": "dycord",
        "o_purpose_category": "opurp",
        "d_purpose_category": "dpurp",
    })

    # Apply basic transformations
    trips_daysim = trips_daysim.with_columns(
        # Fill null coordinates with -1
        pl.col(["oxcord", "oycord", "dxcord", "dycord"]).fill_null(-1),
        # Convert times to DaySim format (HHMM)
        deptm=(pl.col("depart_hour") * 100 + pl.col("depart_minute")),
        arrtm=(pl.col("arrive_hour") * 100 + pl.col("arrive_minute")),
        # Map purposes
        opurp=pl.col("opurp").replace(PURPOSE_MAP),
        dpurp=pl.col("dpurp").replace(PURPOSE_MAP),
    )

    # Compute DaySim mode
    trips_daysim = trips_daysim.with_columns(
        mode=pl.when(pl.col("mode_type") == ModeType.WALK.value)
        .then(pl.lit(DaysimMode.WALK.value))
        .when(
            pl.col("mode_type").is_in([
                ModeType.BIKE.value,
                ModeType.BIKESHARE.value,
                ModeType.SCOOTERSHARE.value,
            ])
        )
        .then(pl.lit(DaysimMode.BIKE.value))
        .when(
            pl.col("mode_type").is_in([
                ModeType.CAR.value,
                ModeType.CARSHARE.value,
            ])
        )
        .then(
            pl.when(pl.col("num_travelers") == VehicleOccupancy.SOV.value)
            .then(pl.lit(DaysimMode.SOV.value))
            .when(pl.col("num_travelers") == VehicleOccupancy.HOV2.value)
            .then(pl.lit(DaysimMode.HOV2.value))
            .when(pl.col("num_travelers") > VehicleOccupancy.HOV3_MIN.value)
            .then(pl.lit(DaysimMode.HOV3.value))
        )
        .when(
            pl.col("mode_type").is_in([
                ModeType.TAXI.value,
                ModeType.TNC.value,
            ])
        )
        .then(pl.lit(DaysimMode.TNC.value))
        .when(pl.col("mode_type") == ModeType.SCHOOL_BUS.value)
        .then(pl.lit(DaysimMode.SCHOOL_BUS.value))
        .when(pl.col("mode_type") == ModeType.SHUTTLE_VANPOOL.value)
        .then(pl.lit(DaysimMode.HOV3.value))  # shuttle/vanpool as HOV3+
        .when(
            pl.col("mode_type").is_in([
                ModeType.FERRY.value,
                ModeType.TRANSIT.value,
            ])
            | (
                (pl.col("mode_type") == ModeType.LONG_DISTANCE.value)
                & (pl.col("mode_1") == Mode.RAIL_INTERCITY.value)
            )
        )
        .then(
            pl.when(
                pl.col("transit_access").is_in(DROVE_ACCESS_EGRESS)
                | pl.col("transit_egress").is_in(DROVE_ACCESS_EGRESS)
            )
            .then(pl.lit(DaysimMode.DRIVE_TRANSIT.value))
            .otherwise(pl.lit(DaysimMode.WALK_TRANSIT.value))
        )
        .otherwise(pl.lit(DaysimMode.OTHER.value))
    )

    # Compute DaySim path type
    trips_daysim = trips_daysim.with_columns(
        path=pl.when(pl.col("mode_type") == ModeType.CAR.value)
        .then(pl.lit(DaysimPathType.FULL_NETWORK.value))
        .when(
            pl.col("mode").is_in([
                DaysimMode.WALK_TRANSIT.value,
                DaysimMode.DRIVE_TRANSIT.value,
            ])
        )
        .then(
            pl.when(
                pl.any_horizontal(
                    pl.col("mode_1", "mode_2", "mode_3", "mode_4")
                    == Mode.FERRY.value
                )
            )
            .then(pl.lit(DaysimPathType.FERRY.value))
            .when(
                pl.any_horizontal(
                    pl.col("mode_1", "mode_2", "mode_3", "mode_4")
                    == Mode.BART.value
                )
            )
            .then(pl.lit(DaysimPathType.BART.value))
            .when(
                pl.any_horizontal(
                    pl.col("mode_1", "mode_2", "mode_3", "mode_4").is_in([
                        Mode.RAIL_INTERCITY.value,
                        Mode.RAIL_OTHER.value,
                        Mode.BUS_EXPRESS.value,
                    ])
                )
            )
            .then(pl.lit(DaysimPathType.PREMIUM.value))
            .when(
                pl.any_horizontal(
                    pl.col("mode_1", "mode_2", "mode_3", "mode_4").is_in([
                        Mode.MUNI_METRO.value,
                        Mode.RAIL.value,
                        Mode.STREETCAR.value,
                    ])
                )
            )
            .then(pl.lit(DaysimPathType.LRT.value))
            .otherwise(pl.lit(DaysimPathType.BUS.value))
        )
        .otherwise(pl.lit(DaysimPathType.NONE.value))
    )

    # Compute DaySim driver/passenger code
    trips_daysim = trips_daysim.with_columns(
        dorp=pl.when(
            pl.col("mode").is_in([
                DaysimMode.SOV.value,
                DaysimMode.HOV2.value,
                DaysimMode.HOV3.value,
            ])
        )
        .then(
            pl.when(
                pl.col("driver").is_in([
                    Driver.DRIVER.value,
                    Driver.BOTH.value,
                ])
            )
            .then(pl.lit(DaysimDriverPassenger.DRIVER.value))
            .when(pl.col("driver") == Driver.PASSENGER.value)
            .then(pl.lit(DaysimDriverPassenger.PASSENGER.value))
            .otherwise(pl.lit(DaysimDriverPassenger.MISSING.value))
        )
        .when(pl.col("mode") == DaysimMode.TNC.value)
        .then(
            pl.when(pl.col("num_travelers") == VehicleOccupancy.SOV.value)
            .then(pl.lit(DaysimDriverPassenger.TNC_ALONE.value))
            .when(pl.col("num_travelers") == VehicleOccupancy.HOV2.value)
            .then(pl.lit(DaysimDriverPassenger.TNC_2.value))
            .when(pl.col("num_travelers") > VehicleOccupancy.HOV3_MIN.value)
            .then(pl.lit(DaysimDriverPassenger.TNC_3PLUS.value))
        )
        .otherwise(pl.lit(DaysimDriverPassenger.NA.value))
    )

    # Add default expansion factor
    trips_daysim = trips_daysim.with_columns(
        trexpfac=pl.lit(1.0),
    )

    # Select DaySim trip fields
    trip_cols = [
        "hhno",
        "pno",
        "day",
        "tripno",
        "opurp",
        "dpurp",
        "opcl",
        "otaz",
        "dpcl",
        "dtaz",
        "mode",
        "path",
        "dorp",
        "deptm",
        "arrtm",
        "oxcord",
        "oycord",
        "dxcord",
        "dycord",
        "trexpfac",
    ]

    return (
        trips_daysim
        .select(trip_cols)
        .sort(by=["hhno", "pno", "day", "tripno"])
    )

