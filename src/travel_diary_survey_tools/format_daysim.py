"""Module for formatting data to DaySim model format.

This module transforms travel survey data from the standardized pipeline format
into the specific format required by the DaySim activity-based travel demand
model. DaySim requires specific data structures and coding schemes for person
types, household characteristics, trip purposes, and transportation modes.

The formatter handles:
- Person demographic mapping (age, gender, worker/student types)
- Household attributes (income, dwelling type, vehicle ownership)
- Trip purpose and mode recoding to DaySim conventions
- Geographic filtering (Bay Area 9-county region)
- Survey weighting and completeness calculations
"""

import logging
from enum import IntEnum
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)


class AgeThreshold(IntEnum):
    """Age thresholds for person type classification."""

    CHILD_PRESCHOOL = 5  # Child age 0-4
    CHILD_SCHOOL = 16  # Child age 5-15
    HIGH_SCHOOL = 18  # High school age
    UNIVERSITY = 25  # University age
    RETIREMENT = 65  # Retirement age


class ModeType(IntEnum):
    """Survey mode type codes."""

    WALK = 1
    BIKE = 2
    BIKE_SHARE = 3
    SCOOTER = 4
    TAXI = 5
    TNC = 6
    CAR = 8
    CARSHARE = 9
    SCHOOL_BUS = 10
    SHUTTLE_VANPOOL = 11
    FERRY = 12
    TRANSIT = 13
    LONG_DISTANCE = 14


class TransitMode(IntEnum):
    """Specific transit mode codes."""

    BART = 30
    INTERCITY_RAIL = 41
    OTHER_RAIL = 42
    MUNI_METRO = 53
    EXPRESS_BUS = 55
    CABLE_CAR = 68
    FERRY = 78
    RAIL = 105


class TravelerCount(IntEnum):
    """Number of travelers threshold."""

    SINGLE = 1
    TWO = 2


class DriverStatus(IntEnum):
    """Driver status codes."""

    DRIVER = 1
    PASSENGER = 2
    SWITCHED = 3


class DaysimMode(IntEnum):
    """DaySim output mode codes."""

    WALK = 1
    BIKE = 2
    DRIVE_ALONE = 3
    HOV2 = 4
    HOV3_PLUS = 5
    WALK_TRANSIT = 6
    DRIVE_TRANSIT = 7
    SCHOOL_BUS = 8
    TNC = 9


# Bay Area 9-county FIPS codes (California FIPS prefix: 6)
COUNTY_FIPS = 6 * 1000 + np.array([1, 13, 41, 55, 75, 81, 85, 95, 97])


class DaysimFormatter:
    """Transform survey data to DaySim model input format."""

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialize formatter with configuration.

        Args:
            config: Configuration dictionary with mapping options

        """
        self.config = config
        self.weighted = config.get("weighted", True)

        # Define mapping dictionaries
        self._init_mappings()

    def _init_mappings(self) -> None:
        """Initialize all mapping dictionaries for DaySim codes."""
        # Person attribute mappings
        self.age_dict = {
            1: 3,
            2: 10,
            3: 16,  # 16-17
            4: 21,
            5: 30,
            6: 40,
            7: 50,
            8: 60,
            9: 70,
            10: 80,
            11: 90,  # 85+
        }

        self.gender_dict = {
            1: 2,  # female
            2: 1,  # male
            4: 3,  # non-binary
            997: 3,  # other/self-describe
            995: 9,  # missing
            999: 9,  # prefer not to answer
        }

        self.student_dict = {
            0: 1,  # full-time, some/all in-person
            1: 2,  # part-time, some/all in-person
            2: 0,  # not student
            3: 2,  # part-time, remote only
            4: 1,  # full-time, remote only
            995: -1,  # missing
        }

        self.work_park_dict = {
            1: 0,  # free parking at work
            2: 0,  # employer pays all costs -> free parking
            3: 1,  # paid parking
            4: 1,  # paid parking
            995: -1,  # missing
            996: -1,  # missing
            997: -1,  # never drive to work
            998: -1,  # don't know
        }

        self.residence_rent_own_dict = {
            1: 1,  # own
            2: 2,  # rent
            3: 3,  # provided by military -> other
            4: 3,  # provided by family/relative/friend rent-free -> other
            997: 3,  # other
            995: 9,  # missing
            999: 9,  # prefer not to answer -> missing
        }

        self.residence_type_dict = {
            1: 1,  # detached house
            2: 2,  # rowhouse/townhouse -> duplex/triplex/rowhouse
            3: 3,  # duplex/triplex/quads 2-4 units -> apt/condo
            4: 3,  # apt/condos 5-49 units
            5: 3,  # apt/condos 50+ units
            6: 3,  # senior/age-restricted apt/condos
            7: 4,  # manufactured/mobile home, trailer
            9: 5,  # dorm, group quarters, inst housing
            995: 9,  # missing
            997: 6,  # other
        }

        # Household income mappings
        self.income_detailed_dict = {
            999: -1,
            1: 7500,
            2: 20000,
            3: 30000,
            4: 42500,
            5: 62500,
            6: 87500,
            7: 125000,
            8: 175000,
            9: 225000,
            10: 350000,  # 250k+
        }

        self.income_followup_dict = {
            999: -1,
            1: 12500,
            2: 37500,
            3: 62500,
            4: 87500,
            5: 150000,
            6: 250000,  # 200k+
        }

        # Trip purpose mappings
        self.purpose_dict = {
            -1: -1,  # not imputable -> missing
            995: -1,  # missing -> missing
            1: 0,  # home -> home
            2: 1,  # work -> work
            3: 4,  # work-related -> personal business
            4: 2,  # school -> school
            5: 2,  # school related -> school
            6: 3,  # escort -> escort
            7: 5,  # shop -> shop
            8: 6,  # meal -> meal
            9: 7,  # socrec -> socrec
            10: 4,  # errand -> pers.bus
            11: 10,  # change mode -> change mode
            12: 11,  # overnight non-home -> other
            13: 11,  # other -> other
        }

    def format_person(
        self,
        person: pl.DataFrame,
        day_completeness: pl.DataFrame | None = None,
    ) -> pl.DataFrame:
        """Format person data to DaySim specification.

        Args:
            person: Person DataFrame from pipeline
            day_completeness: Optional day completeness data

        Returns:
            DaySim-formatted person DataFrame

        """
        person_out_cols = [
            "hhno",
            "pno",
            "pptyp",
            "pagey",
            "pgend",
            "pwtyp",
            "pwpcl",
            "pwtaz",
            "pstyp",
            "pspcl",
            "pstaz",
            "ppaidprk",
            "pwxcord",
            "pwycord",
            "psxcord",
            "psycord",
            "pownrent",
            "prestype",
        ]

        # Add completeness columns if provided
        if day_completeness is not None:
            person_out_cols.extend(
                [
                    "mon_complete",
                    "tue_complete",
                    "wed_complete",
                    "thu_complete",
                    "fri_complete",
                    "sat_complete",
                    "sun_complete",
                    "num_days_complete_3dayweekday",
                    "num_days_complete_4dayweekday",
                    "num_days_complete_5dayweekday",
                    "num_days_complete",
                ]
            )

        if self.weighted:
            person_out_cols.append("person_weight")

        # Rename columns to DaySim conventions
        person = person.rename(
            {
                "hh_id": "hhno",
                "person_num": "pno",
                "work_lon": "pwxcord",
                "work_lat": "pwycord",
                "school_lon": "psxcord",
                "school_lat": "psycord",
                "work_taz": "pwtaz",
                "work_maz": "pwpcl",
                "school_taz": "pstaz",
                "school_maz": "pspcl",
            }
        )

        # Cast TAZ/MAZ columns to int
        person = person.cast(
            {
                "pwtaz": pl.Int64,
                "pwpcl": pl.Int64,
                "pstaz": pl.Int64,
                "pspcl": pl.Int64,
            }
        )

        # Apply basic mappings
        person = person.with_columns(
            [
                pl.col(["pwxcord", "pwycord", "psxcord", "psycord"]).fill_null(
                    -1
                ),
                pl.col("age").replace(self.age_dict).alias("pagey"),
                pl.col("gender").replace(self.gender_dict).alias("pgend"),
                pl.col("student")
                .replace(self.student_dict)
                .fill_null(0)
                .alias("pstyp"),
                pl.col("work_park")
                .replace(self.work_park_dict)
                .alias("ppaidprk"),
                pl.col("residence_rent_own")
                .replace(self.residence_rent_own_dict)
                .alias("pownrent"),
                pl.col("residence_type")
                .replace(self.residence_type_dict)
                .alias("prestype"),
            ]
        )

        # If num_days_complete exists, handle missing values
        if "num_days_complete" in person.columns:
            person = person.with_columns(
                pl.col("num_days_complete").replace({995: 0})
            )

        # Derive person type (pptyp)
        person = person.with_columns(
            pl.when(pl.col("pagey") < AgeThreshold.CHILD_PRESCHOOL)
            .then(pl.lit(8))  # child 0-4
            .when(pl.col("pagey") < AgeThreshold.CHILD_SCHOOL)
            .then(pl.lit(7))  # child 5-15
            # only if age >= 16:
            .when(pl.col("employment").is_in([1, 3, 8]))  # employed full-time
            .then(pl.lit(1))  # full-time worker
            # all cases below: if not full-time employed and age >= 16:
            .when(
                (pl.col("pagey") < AgeThreshold.HIGH_SCHOOL)  # and age >= 16
                & (pl.col("student").is_in([0, 1, 3, 4]))  # student
            )
            .then(pl.lit(6))  # high school 16+
            .when(
                (pl.col("pagey") < AgeThreshold.UNIVERSITY)  # and age >= 16
                & (pl.col("school_type").is_in([4, 7]))  # home/high school
                & (pl.col("student").is_in([0, 1, 3, 4]))
            )
            .then(pl.lit(6))  # high school 16+
            # logic below is for age 18-65
            .when(pl.col("student").is_in([0, 1, 3, 4]))  # student
            .then(pl.lit(5))  # university student
            .when(
                pl.col("employment").is_in([2, 3, 7])
            )  # part-time/self/unpaid
            .then(pl.lit(2))  # part-time worker
            .when(pl.col("pagey") < AgeThreshold.RETIREMENT)
            .then(pl.lit(4))  # non-working age < 65
            .otherwise(pl.lit(3))  # non-working age 65+
            .alias("pptyp")
        )

        # Derive worker type (pwtyp)
        person = person.with_columns(
            pl.when(pl.col("pptyp").is_in([1, 2]))
            .then(pl.col("pptyp"))  # direct mapping for full/part-time workers
            .when(
                pl.col("pptyp").is_in([5, 6])  # student
                & pl.col("employment").is_in([1, 2, 3])  # employed
            )
            .then(pl.lit(2))  # paid part time
            .otherwise(pl.lit(0))
            .alias("pwtyp")
        )

        # Filter work/school locations to Bay Area only
        person = person.with_columns(
            [
                pl.when(
                    pl.col("work_county").is_in(COUNTY_FIPS.tolist())
                    & (pl.col("pwtyp") != 0)
                )
                .then(pl.col("pwtaz"))
                .otherwise(pl.lit(-1))
                .alias("pwtaz"),
                pl.when(
                    pl.col("work_county").is_in(COUNTY_FIPS.tolist())
                    & (pl.col("pwtyp") != 0)
                )
                .then(pl.col("pwpcl"))
                .otherwise(pl.lit(-1))
                .alias("pwpcl"),
                pl.when(pl.col("pwtyp") != 0)
                .then(pl.col("pwxcord"))
                .otherwise(pl.lit(-1))
                .alias("pwxcord"),
                pl.when(pl.col("pwtyp") != 0)
                .then(pl.col("pwycord"))
                .otherwise(pl.lit(-1))
                .alias("pwycord"),
                pl.when(
                    pl.col("school_county").is_in(COUNTY_FIPS.tolist())
                    & (pl.col("pstyp") != 0)
                )
                .then(pl.col("pstaz"))
                .otherwise(pl.lit(-1))
                .alias("pstaz"),
                pl.when(
                    pl.col("school_county").is_in(COUNTY_FIPS.tolist())
                    & (pl.col("pstyp") != 0)
                )
                .then(pl.col("pspcl"))
                .otherwise(pl.lit(-1))
                .alias("pspcl"),
                pl.when(pl.col("pstyp") != 0)
                .then(pl.col("psxcord"))
                .otherwise(pl.lit(-1))
                .alias("psxcord"),
                pl.when(pl.col("pstyp") != 0)
                .then(pl.col("psycord"))
                .otherwise(pl.lit(-1))
                .alias("psycord"),
            ]
        )

        # Join with day completeness if provided
        if day_completeness is not None:
            person = person.join(
                day_completeness, on=["hhno", "pno"], how="left"
            )

        return person.select(person_out_cols).sort(by=["hhno", "pno"])

    def format_household(
        self,
        household: pl.DataFrame,
        person: pl.DataFrame,
    ) -> pl.DataFrame:
        """Format household data to DaySim specification.

        Args:
            household: Household DataFrame from pipeline
            person: Person DataFrame (for ownership/residence attributes)

        Returns:
            DaySim-formatted household DataFrame

        """
        hh_out_cols = [
            "hhno",
            "hhsize",
            "hhvehs",
            "hhincome",
            "hownrent",
            "hrestype",
            "hhparcel",
            "hhtaz",
            "hxcord",
            "hycord",
        ]

        if self.weighted:
            hh_out_cols.append("hh_weight")

        # Get ownership/residence from first person in household
        person_attrs = (
            person.select("hhno", "pownrent", "prestype")
            .group_by("hhno")
            .first()
            .rename({"pownrent": "hownrent", "prestype": "hrestype"})
        )

        # Rename columns
        household = household.rename(
            {
                "hh_id": "hhno",
                "home_maz": "hhparcel",
                "home_taz": "hhtaz",
                "home_lon": "hxcord",
                "home_lat": "hycord",
                "num_people": "hhsize",
                "num_vehicles": "hhvehs",
            }
        )

        # Process income
        household = household.with_columns(
            [
                pl.col("income_detailed").replace(self.income_detailed_dict),
                pl.col("income_followup").replace(self.income_followup_dict),
            ]
        )

        household = household.with_columns(
            pl.when(pl.col("income_detailed") > 0)
            .then(pl.col("income_detailed"))
            .otherwise(pl.col("income_followup"))
            .alias("hhincome")
        )

        # Join with person attributes
        household = household.join(person_attrs, on="hhno", how="left")

        return household.select(hh_out_cols).sort(by="hhno")

    def format_trip(self, trip: pl.DataFrame) -> pl.DataFrame:
        """Format trip data to DaySim specification.

        Args:
            trip: Trip DataFrame from pipeline

        Returns:
            DaySim-formatted trip DataFrame

        """
        trip_out_cols = [
            "hhno",
            "pno",
            "tripno",
            "dow",
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
            "mode_type",
        ]

        if self.weighted:
            trip_out_cols.append("trip_weight")

        # Rename columns
        trip = trip.rename(
            {
                "hh_id": "hhno",
                "person_num": "pno",
                "o_taz": "otaz",
                "o_maz": "opcl",
                "d_taz": "dtaz",
                "d_maz": "dpcl",
                "o_lon": "oxcord",
                "o_lat": "oycord",
                "d_lon": "dxcord",
                "d_lat": "dycord",
                "trip_num": "tripno",
                "travel_dow": "dow",
            }
        )

        # Cast TAZ/MAZ columns to int
        trip = trip.cast(
            {
                "otaz": pl.Int64,
                "opcl": pl.Int64,
                "dtaz": pl.Int64,
                "dpcl": pl.Int64,
            }
        )

        # Filter to complete person-days only
        if "day_is_complete" in trip.columns:
            trip = trip.filter(pl.col("day_is_complete") == 1)

        # Fill null coordinates
        trip = trip.with_columns(
            pl.col(["oxcord", "oycord", "dxcord", "dycord"]).fill_null(-1)
        )

        # Convert time to DaySim format (HHMM)
        trip = trip.with_columns(
            [
                (pl.col("depart_hour") * 100 + pl.col("depart_minute")).alias(
                    "deptm"
                ),
                (pl.col("arrive_hour") * 100 + pl.col("arrive_minute")).alias(
                    "arrtm"
                ),
            ]
        )

        # Filter TAZ/MAZ to Bay Area only
        trip = trip.with_columns(
            [
                pl.when(pl.col("o_county").is_in(COUNTY_FIPS.tolist()))
                .then(pl.col("otaz"))
                .otherwise(pl.lit(-1))
                .alias("otaz"),
                pl.when(pl.col("o_county").is_in(COUNTY_FIPS.tolist()))
                .then(pl.col("opcl"))
                .otherwise(pl.lit(-1))
                .alias("opcl"),
                pl.when(pl.col("d_county").is_in(COUNTY_FIPS.tolist()))
                .then(pl.col("dtaz"))
                .otherwise(pl.lit(-1))
                .alias("dtaz"),
                pl.when(pl.col("d_county").is_in(COUNTY_FIPS.tolist()))
                .then(pl.col("dpcl"))
                .otherwise(pl.lit(-1))
                .alias("dpcl"),
            ]
        )

        # Map purposes
        trip = trip.with_columns(
            [
                pl.col("o_purpose_category")
                .replace(self.purpose_dict)
                .alias("opurp"),
                pl.col("d_purpose_category")
                .replace(self.purpose_dict)
                .alias("dpurp"),
            ]
        )

        # Map DaySim mode
        trip = self._map_trip_mode(trip)

        # Map path type (transit service type)
        trip = self._map_trip_path(trip)

        # Map driver/passenger
        trip = self._map_trip_dorp(trip)

        return trip.select(trip_out_cols).sort(by=["hhno", "pno", "tripno"])

    def _map_trip_mode(self, trip: pl.DataFrame) -> pl.DataFrame:
        """Map mode_type to DaySim mode codes.

        DaySim modes:
        0-other, 1-walk, 2-bike, 3-DA, 4-hov2, 5-hov3,
        6-walktran, 7-drivetran, 8-schbus, 9-tnc

        """
        return trip.with_columns(
            pl.when(pl.col("mode_type") == ModeType.WALK)
            .then(pl.lit(1))
            .when(
                pl.col("mode_type").is_in(
                    [ModeType.BIKE, ModeType.BIKE_SHARE, ModeType.SCOOTER]
                )
            )
            .then(pl.lit(2))
            .when(pl.col("mode_type").is_in([ModeType.CAR, ModeType.CARSHARE]))
            .then(
                pl.when(pl.col("num_travelers") == TravelerCount.SINGLE)
                .then(pl.lit(3))  # drive alone
                .when(pl.col("num_travelers") == TravelerCount.TWO)
                .then(pl.lit(4))  # HOV2
                .when(pl.col("num_travelers") > TravelerCount.TWO)
                .then(pl.lit(5))  # HOV3+
            )
            .when(pl.col("mode_type").is_in([ModeType.TAXI, ModeType.TNC]))
            .then(pl.lit(9))
            .when(pl.col("mode_type") == ModeType.SCHOOL_BUS)
            .then(pl.lit(8))
            .when(pl.col("mode_type") == ModeType.SHUTTLE_VANPOOL)
            .then(pl.lit(5))  # HOV3+
            .when(
                pl.col("mode_type").is_in([ModeType.FERRY, ModeType.TRANSIT])
                | (
                    (pl.col("mode_type") == ModeType.LONG_DISTANCE)
                    & (pl.col("mode_1") == TransitMode.INTERCITY_RAIL)
                )
            )
            .then(
                pl.when(
                    pl.col("transit_access").is_in([6, 7, 8, 9, 10])
                    | pl.col("transit_egress").is_in([6, 7, 8, 9, 10])
                )
                .then(pl.lit(7))  # drive-transit
                .otherwise(pl.lit(6))  # walk-transit
            )
            .otherwise(pl.lit(0))  # other
            .alias("mode")
        )

    def _map_trip_path(self, trip: pl.DataFrame) -> pl.DataFrame:
        """Map transit modes to DaySim path types.

        DaySim path types:
        0-none, 1-fullnetwork, 2-no-toll, 3-bus, 4-lrt,
        5-premium, 6-BART, 7-ferry

        """
        return trip.with_columns(
            pl.when(pl.col("mode_type") == ModeType.CAR)
            .then(pl.lit(1))  # full network
            .when(pl.col("mode").is_in([6, 7]))  # transit modes
            .then(
                pl.when(
                    pl.any_horizontal(
                        pl.col("mode_1", "mode_2", "mode_3", "mode_4")
                        == TransitMode.FERRY
                    )
                )
                .then(pl.lit(7))  # ferry
                .when(
                    pl.any_horizontal(
                        pl.col("mode_1", "mode_2", "mode_3", "mode_4")
                        == TransitMode.BART
                    )
                )
                .then(pl.lit(6))  # BART
                .when(
                    pl.any_horizontal(
                        pl.col("mode_1", "mode_2", "mode_3", "mode_4").is_in(
                            [
                                TransitMode.INTERCITY_RAIL,
                                TransitMode.OTHER_RAIL,
                                TransitMode.EXPRESS_BUS,
                            ]
                        )
                    )
                )
                .then(pl.lit(5))  # premium
                .when(
                    pl.any_horizontal(
                        pl.col("mode_1", "mode_2", "mode_3", "mode_4").is_in(
                            [
                                TransitMode.MUNI_METRO,
                                TransitMode.RAIL,
                                TransitMode.CABLE_CAR,
                            ]
                        )
                    )
                )
                .then(pl.lit(4))  # LRT
                .otherwise(pl.lit(3))  # bus
            )
            .otherwise(pl.lit(0))  # none
            .alias("path")
        )

    def _map_trip_dorp(self, trip: pl.DataFrame) -> pl.DataFrame:
        """Map driver/passenger status for auto trips.

        DaySim dorp codes:
        1-driver, 2-passenger, 3-N/A, 9-missing,
        11-TNC single, 12-TNC shared 2, 13-TNC shared 3+

        """
        return trip.with_columns(
            pl.when(pl.col("mode").is_in([3, 4, 5]))  # auto modes
            .then(
                pl.when(pl.col("driver").is_in([1, 3]))  # driver or switched
                .then(pl.lit(1))  # driver
                .when(pl.col("driver") == DriverStatus.PASSENGER)
                .then(pl.lit(2))  # passenger
                .otherwise(pl.lit(9))  # missing
            )
            .when(pl.col("mode") == DaysimMode.TNC)
            .then(
                pl.when(pl.col("num_travelers") == TravelerCount.SINGLE)
                .then(pl.lit(11))
                .when(pl.col("num_travelers") == TravelerCount.TWO)
                .then(pl.lit(12))
                .when(pl.col("num_travelers") > TravelerCount.TWO)
                .then(pl.lit(13))
            )
            .otherwise(pl.lit(3))  # N/A
            .alias("dorp")
        )

    def load_day_completeness(self, day_path: Path | str) -> pl.DataFrame:
        """Load and process day completeness data for person weighting.

        Args:
            day_path: Path to day-level survey data

        Returns:
            Person completeness DataFrame with day-specific indicators

        """
        return (
            pl.read_csv(
                day_path,
                columns=["person_id", "is_complete", "travel_dow"],
            )
            .pivot(index="person_id", on="travel_dow", values="is_complete")
            .fill_null(0)
            .with_columns(
                [
                    # person_id in day file is hhno*100 + pno
                    (pl.col("person_id") // 100).alias("hhno"),
                    (pl.col("person_id") % 100).alias("pno"),
                    pl.sum_horizontal(["2", "3", "4"]).alias(
                        "num_days_complete_3dayweekday"
                    ),
                    pl.sum_horizontal(["1", "2", "3", "4"]).alias(
                        "num_days_complete_4dayweekday"
                    ),
                    pl.sum_horizontal(["1", "2", "3", "4", "5"]).alias(
                        "num_days_complete_5dayweekday"
                    ),
                ]
            )
            .with_columns(
                pl.col("num_days_complete_5dayweekday").alias(
                    "num_days_complete"
                )
            )
            .select(
                [
                    "hhno",
                    "pno",
                    "1",
                    "2",
                    "3",
                    "4",
                    "5",
                    "6",
                    "7",
                    "num_days_complete_3dayweekday",
                    "num_days_complete_4dayweekday",
                    "num_days_complete_5dayweekday",
                    "num_days_complete",
                ]
            )
            .rename(
                {
                    "1": "mon_complete",
                    "2": "tue_complete",
                    "3": "wed_complete",
                    "4": "thu_complete",
                    "5": "fri_complete",
                    "6": "sat_complete",
                    "7": "sun_complete",
                }
            )
        )
