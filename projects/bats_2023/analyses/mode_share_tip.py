import polars as pl
from pathlib import Path
from data_canon.codebook.trips import ModeType
from data_canon.codebook.households import IncomeDetailed

# ============================================================================
# CONFIGURATION
# ============================================================================

output_path = Path(r"E:\BATS2023_TIP_03052026\tip")
output_path.mkdir(parents=True, exist_ok=True)

# ============================================================================
# LOAD DATA
# ============================================================================

households = pl.read_csv(r"E:\BATS2023_TIP_03052026\survey\households_2023.csv")
persons = pl.read_csv(r"E:\BATS2023_TIP_03052026\survey\persons_2023.csv")
linked_trips = pl.read_csv(r"E:\BATS2023_TIP_03052026\survey\linked_trips_2023.csv")

# ============================================================================
# RECODE MAPS
# ============================================================================

mode_map = ModeType.to_dict()
income_map = IncomeDetailed.to_dict()

mode_group_map = {
    1: "active",   # Walk
    2: "active",   # Bike
    3: "active",   # Bikeshare
    4: "active",   # Scootershare
    5: "roadway",  # Taxi
    6: "roadway",  # TNC
    7: "other",    # Other
    8: "roadway",  # Car
    9: "roadway",  # Carshare
    10: "transit", # School bus
    11: "transit", # Shuttle/vanpool
    12: "transit", # Ferry
    13: "transit", # Transit
    14: "transit", # Long distance
    995: "missing",
}

county_map = {
    "6001": "Alameda",
    "6013": "Contra Costa",
    "6041": "Marin",
    "6055": "Napa",
    "6075": "San Francisco",
    "6081": "San Mateo",
    "6085": "Santa Clara",
    "6095": "Solano",
    "6097": "Sonoma",
}

# ============================================================================
# PREPARE DEMOGRAPHIC ATTRIBUTES
# ============================================================================

hh_attrs = (
    households
    .select(["hh_id", "home_county", "income_detailed"])
    .with_columns([
        pl.col("home_county").cast(pl.Utf8).replace(county_map).alias("county"),
        pl.col("income_detailed").replace_strict(income_map, default="Missing").alias("income"),
    ])
)

person_attrs = (
    persons
    .select(["person_id", "hh_id", "age", "race_imputed", "ethnicity_imputed"])
    .with_columns([
        pl.when(pl.col("age") >= 9)
          .then(pl.lit("65 and Over"))
          .otherwise(pl.lit("Under 65"))
          .alias("age_group"),
        pl.when(pl.col("ethnicity_imputed") == "hispanic")
          .then(pl.lit("hispanic"))
          .otherwise(pl.col("race_imputed"))
          .alias("race_eth"),
    ])
)

# ============================================================================
# BUILD ENRICHED TRIP TABLE
# ============================================================================

trips_enriched = (
    linked_trips
    .with_columns([
        pl.col("mode_type").replace_strict(mode_group_map).alias("mode_group"),
        pl.col("mode_type").replace_strict(mode_map).alias("mode"),
        (pl.col("distance_meters") / 1609.34).alias("distance_miles"),
    ])
    .join(hh_attrs, on="hh_id", how="left")
    .join(person_attrs.select(["person_id", "hh_id", "age_group", "race_eth"]), on=["person_id", "hh_id"], how="left")
)

# ============================================================================
# HELPER
# ============================================================================


def summarize(df, group_cols):
    return (
        df.group_by(group_cols)
        .agg([
            pl.len().alias("trip_count"),
            pl.sum("linked_trip_weight").alias("weighted_trips"),
            (pl.col("linked_trip_weight") * pl.col("distance_miles")).sum().alias("passenger_miles"),
        ])
        .with_columns([
            (pl.col("weighted_trips") / pl.col("weighted_trips").sum() * 100).alias("weighted_trip_share_pct"),
            (pl.col("passenger_miles") / pl.col("passenger_miles").sum() * 100).alias("passenger_mile_share_pct"),
        ])
        .sort(group_cols)
    )

# ============================================================================
# OVERALL MODE SHARE
# ============================================================================

mode_share = (
    trips_enriched
    .group_by("mode")
    .agg([
        pl.len().alias("trip_count"),
        pl.sum("linked_trip_weight").alias("weighted_trips"),
        (pl.col("linked_trip_weight") * pl.col("distance_miles")).sum().alias("passenger_miles"),
    ])
    .with_columns(
        (pl.col("weighted_trips") / pl.col("weighted_trips").sum() * 100).alias("share_pct")
    )
    .sort("mode")
)
mode_share.write_csv(output_path / "mode_share_tip.csv")

mode_group_share = (
    trips_enriched
    .group_by("mode_group")
    .agg([
        pl.len().alias("trip_count"),
        pl.sum("linked_trip_weight").alias("weighted_trips"),
        (pl.col("linked_trip_weight") * pl.col("distance_miles")).sum().alias("passenger_miles"),
    ])
    .with_columns(
        (pl.col("weighted_trips") / pl.col("weighted_trips").sum() * 100).alias("share_pct")
    )
    .sort("mode_group")
)
mode_group_share.write_csv(output_path / "mode_group_share_tip.csv")

# ============================================================================
# COUNTY x DEMOGRAPHIC x MODE BREAKDOWNS
# ============================================================================

summarize(trips_enriched, ["county", "income", "mode_group"]).write_csv(output_path / "trips_county_income_mode.csv")
summarize(trips_enriched, ["county", "race_eth", "mode_group"]).write_csv(output_path / "trips_county_race_mode.csv")
summarize(trips_enriched, ["county", "age_group", "mode_group"]).write_csv(output_path / "trips_county_age_mode.csv")

print("Done. Outputs written to", output_path)