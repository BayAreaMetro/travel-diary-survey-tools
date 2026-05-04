from pathlib import Path

import polars as pl

import svy

# ============================================================================
# CONFIGURATION
# ============================================================================

CONF_LEVEL = 0.90
Z_VALUE = 1.645  # 90% confidence interval z-value
CV_THRESHOLD = 0.30
MIN_UNWEIGHTED_N = 30
CI_WIDTH_THRESHOLD = 0.40

input_dir = Path(r"E:\BATS2023_TIP_03052026\survey")
output_dir = Path(r"E:\BATS2023_TIP_03052026\tip_svy")
output_dir.mkdir(parents=True, exist_ok=True)

# ============================================================================
# RECODE MAPS
# ============================================================================

mode_map = {
    1: "Walk",
    2: "Bike",
    3: "Bikeshare",
    4: "Scootershare",
    5: "Taxi",
    6: "TNC",
    7: "Other",
    8: "Car",
    9: "Carshare",
    10: "School bus",
    11: "Shuttle/vanpool",
    12: "Ferry",
    13: "Transit",
    14: "Long distance passenger",
    995: "Missing Response",
}

mode_group_map = {
    1: "active",
    2: "active",
    3: "active",
    4: "active",
    5: "roadway",
    6: "roadway",
    7: "other",
    8: "roadway",
    9: "roadway",
    10: "transit",
    11: "transit",
    12: "transit",
    13: "transit",
    14: "transit",
    995: "missing",
}

income_map = {
    1: "Under $15,000",
    2: "$15,000-$24,999",
    3: "$25,000-$34,999",
    4: "$35,000-$49,999",
    5: "$50,000-$74,999",
    6: "$75,000-$99,999",
    7: "$100,000-$149,999",
    8: "$150,000-$199,999",
    9: "$200,000-$249,999",
    10: "$250,000 or more",
    999: "Prefer not to answer",
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

MODE_GROUPS = ["active", "roadway", "transit", "other", "missing"]

# ============================================================================
# LOAD DATA
# ============================================================================

households = pl.read_csv(input_dir / "households_2023.csv")
persons = pl.read_csv(input_dir / "persons_2023.csv")
linked_trips = pl.read_csv(input_dir / "linked_trips_2023.csv")

# ============================================================================
# PREPARE DEMOGRAPHIC ATTRIBUTES
# ============================================================================

hh_attrs = (
    households
    .select(["hh_id", "home_county", "income_detailed", "sample_segment"])
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
# BUILD ANALYSIS TABLE
# ============================================================================

trips_enriched = (
    linked_trips
    .filter(pl.col("linked_trip_weight") > 0)
    .with_columns([
        pl.col("mode_type").replace_strict(mode_group_map, default="other").alias("mode_group"),
        pl.col("mode_type").replace_strict(mode_map, default="Unknown").alias("mode"),
        (pl.col("distance_meters") / 1609.34).alias("distance_miles"),
    ])
    .join(hh_attrs, on="hh_id", how="left")
    .join(
        person_attrs.select(["person_id", "hh_id", "age_group", "race_eth"]),
        on=["person_id", "hh_id"],
        how="left",
    )
)

for mode_group in MODE_GROUPS:
    trips_enriched = trips_enriched.with_columns(
        (pl.col("mode_group") == mode_group).cast(pl.Int8).alias(f"is_{mode_group}")
    )

# Save an enriched copy for inspection/debugging
trips_enriched.write_csv(output_dir / "linked_trips_enriched_for_svy.csv")

# ============================================================================
# CURRENT POINT-ESTIMATE OUTPUTS
# These are valid weighted estimates, but NOT design-based SE/MoE/CIs.
# ============================================================================

def summarize_point_estimates(df: pl.DataFrame, group_cols: list[str]) -> pl.DataFrame:
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

mode_share = (
    trips_enriched
    .group_by("mode")
    .agg([
        pl.len().alias("trip_count"),
        pl.sum("linked_trip_weight").alias("weighted_trips"),
        (pl.col("linked_trip_weight") * pl.col("distance_miles")).sum().alias("passenger_miles"),
    ])
    .with_columns([
        (pl.col("weighted_trips") / pl.col("weighted_trips").sum() * 100).alias("weighted_trip_share_pct"),
        (pl.col("passenger_miles") / pl.col("passenger_miles").sum() * 100).alias("passenger_mile_share_pct"),
    ])
    .sort("mode")
)
mode_share.write_csv(output_dir / "mode_share_tip.csv")

mode_group_share = (
    trips_enriched
    .group_by("mode_group")
    .agg([
        pl.len().alias("trip_count"),
        pl.sum("linked_trip_weight").alias("weighted_trips"),
        (pl.col("linked_trip_weight") * pl.col("distance_miles")).sum().alias("passenger_miles"),
    ])
    .with_columns([
        (pl.col("weighted_trips") / pl.col("weighted_trips").sum() * 100).alias("weighted_trip_share_pct"),
        (pl.col("passenger_miles") / pl.col("passenger_miles").sum() * 100).alias("passenger_mile_share_pct"),
    ])
    .sort("mode_group")
)
mode_group_share.write_csv(output_dir / "mode_group_share_tip.csv")

summarize_point_estimates(trips_enriched, ["county", "income", "mode_group"]).write_csv(
    output_dir / "trips_county_income_mode.csv"
)
summarize_point_estimates(trips_enriched, ["county", "race_eth", "mode_group"]).write_csv(
    output_dir / "trips_county_race_mode.csv"
)
summarize_point_estimates(trips_enriched, ["county", "age_group", "mode_group"]).write_csv(
    output_dir / "trips_county_age_mode.csv"
)


# helper to attach counts and reliability fields
def add_counts_and_flags(
    share_df: pl.DataFrame,
    source_df: pl.DataFrame,
    group_cols: list[str],
) -> pl.DataFrame:
    domain_cols = [col for col in group_cols if col != "mode_group"]

    weighted_counts = (
        source_df
        .group_by(group_cols)
        .agg([
            pl.sum("linked_trip_weight").alias("weighted_count"),
            pl.len().alias("unweighted_count"),
        ])
    )

    if domain_cols:
        totals = (
            source_df
            .group_by(domain_cols)
            .agg([
                pl.sum("linked_trip_weight").alias("total_weighted"),
                pl.len().alias("total_unweighted"),
            ])
        )
        result = (
            share_df
            .join(weighted_counts, on=group_cols, how="left")
            .join(totals, on=domain_cols, how="left")
        )
    else:
        total_weighted = source_df["linked_trip_weight"].sum()
        total_unweighted = source_df.height
        result = (
            share_df
            .join(weighted_counts, on=group_cols, how="left")
            .with_columns([
                pl.lit(total_weighted).alias("total_weighted"),
                pl.lit(total_unweighted).alias("total_unweighted"),
            ])
        )

    result = result.with_columns([
        pl.col("weighted_count").fill_null(0.0),
        pl.col("unweighted_count").fill_null(0),
    ])    

    result = result.with_columns([
        pl.when(pl.col("weighted_share") != 0)
        .then((pl.col("se") / pl.col("weighted_share")).abs())
        .otherwise(None)
        .alias("coeff_of_var"),
        (pl.col("ci_upper") - pl.col("ci_lower")).alias("ci_width"),
        pl.lit(CONF_LEVEL).alias("confidence_level"),
    ]).with_columns([
        (pl.col("coeff_of_var") > CV_THRESHOLD).fill_null(False).alias("cv_flag"),
        (pl.col("unweighted_count") < MIN_UNWEIGHTED_N).alias("sample_size_flag"),
        (pl.col("ci_width") > CI_WIDTH_THRESHOLD).alias("ci_width_flag"),
        ((pl.col("ci_lower") < 0) | (pl.col("ci_upper") > 1)).alias("extreme_values_flag"),
    ]).with_columns([
        (
            pl.col("cv_flag")
            | pl.col("sample_size_flag")
            | pl.col("ci_width_flag")
            | pl.col("extreme_values_flag")
        ).alias("suppress"),
        pl.when(pl.col("cv_flag"))
        .then(pl.lit("Poor (High CV >30%)"))
        .when(pl.col("sample_size_flag"))
        .then(pl.lit("Poor (Small sample n<30)"))
        .when(pl.col("ci_width_flag"))
        .then(pl.lit("Poor (Wide CI >40pp)"))
        .when(pl.col("extreme_values_flag"))
        .then(pl.lit("Poor (Invalid range)"))
        .otherwise(pl.lit("Acceptable"))
        .alias("estimate_reliability"),
    ])

    return result



# ============================================================================
# SVY: OVERALL MODE GROUP SHARES WITH SE / CI / MOE
# ============================================================================

design = svy.Design(
    stratum="sample_segment",
    psu="hh_id",
    ssu="person_id",
    wgt="linked_trip_weight",
)

sample = svy.Sample(data=trips_enriched, design=design)

overall_results = []
for mode_group in MODE_GROUPS:
    result = sample.estimation.mean(y=f"is_{mode_group}")
    result_pl = result.to_polars().with_columns([
        pl.lit(mode_group).alias("mode_group"),
        (pl.col("se") * Z_VALUE).alias("moe"),
    ])
    overall_results.append(result_pl)

mode_group_share_design = (
    pl.concat(overall_results)
    .rename({
        "est": "weighted_share",
        "lci": "ci_lower",
        "uci": "ci_upper",
    })
    .select([
        "mode_group",
        "weighted_share",
        "se",
        "moe",
        "ci_lower",
        "ci_upper",
    ])
    .sort("mode_group")
)

mode_group_share_design = add_counts_and_flags(
    mode_group_share_design,
    trips_enriched,
    ["mode_group"],
)

mode_group_share_design.write_csv(output_dir / "mode_group_share_with_design_se.csv")



# the county × income preparation step
trips_county_income = (
    trips_enriched
    .filter(
        pl.col("county").is_not_null() &
        pl.col("income").is_not_null() &
        pl.col("sample_segment").is_not_null() &
        pl.col("hh_id").is_not_null() &
        pl.col("linked_trip_weight").is_not_null()
    )
)

# Build a new survey design and sample for that domain table
design_county_income = svy.Design(
    stratum="sample_segment",
    psu="hh_id",
    ssu="person_id",
    wgt="linked_trip_weight",
)

sample_county_income = svy.Sample(data=trips_county_income, design=design_county_income)

# a second helper that uses that domain sample
def estimate_mode_group_shares_for_county_income() -> None:
    domain_results = []

    for mode_group in MODE_GROUPS:
        result = sample_county_income.estimation.mean(
            y=f"is_{mode_group}",
            by=["county", "income"],
        )
        result_pl = result.to_polars().with_columns([
            pl.lit(mode_group).alias("mode_group"),
            (pl.col("se") * Z_VALUE).alias("moe"),
        ])
        domain_results.append(result_pl)

    county_income_share_design = (
        pl.concat(domain_results)
        .rename({
            "est": "weighted_share",
            "lci": "ci_lower",
            "uci": "ci_upper",
        })
        .select([
            "county",
            "income",
            "mode_group",
            "weighted_share",
            "se",
            "moe",
            "ci_lower",
            "ci_upper",
        ])
        .sort(["county", "income", "mode_group"])
    )

    county_income_share_design = add_counts_and_flags(
        county_income_share_design,
        trips_county_income,
        ["county", "income", "mode_group"],
    )

    county_income_share_design.write_csv(
        output_dir / "trips_county_income_mode_share_with_design_se.csv"
    )

# call the function
estimate_mode_group_shares_for_county_income()



# the county × race preparation step
trips_county_race = (
    trips_enriched
    .filter(
        pl.col("county").is_not_null() &
        pl.col("race_eth").is_not_null() &
        pl.col("sample_segment").is_not_null() &
        pl.col("hh_id").is_not_null() &
        pl.col("linked_trip_weight").is_not_null()
    )
)

# Build a new survey design and sample for that domain table
design_county_race = svy.Design(
    stratum="sample_segment",
    psu="hh_id",
    ssu="person_id",
    wgt="linked_trip_weight",
)

sample_county_race = svy.Sample(data=trips_county_race, design=design_county_race)

# the county x race function
def estimate_mode_group_shares_for_county_race() -> None:
    domain_results = []

    for mode_group in MODE_GROUPS:
        result = sample_county_race.estimation.mean(
            y=f"is_{mode_group}",
            by=["county", "race_eth"],
        )
        result_pl = result.to_polars().with_columns([
            pl.lit(mode_group).alias("mode_group"),
            (pl.col("se") * Z_VALUE).alias("moe"),
        ])
        domain_results.append(result_pl)

    county_race_share_design = (
        pl.concat(domain_results)
        .rename({
            "est": "weighted_share",
            "lci": "ci_lower",
            "uci": "ci_upper",
        })
        .select([
            "county",
            "race_eth",
            "mode_group",
            "weighted_share",
            "se",
            "moe",
            "ci_lower",
            "ci_upper",
        ])
        .sort(["county", "race_eth", "mode_group"])
    )

    county_race_share_design = add_counts_and_flags(
        county_race_share_design,
        trips_county_race,
        ["county", "race_eth", "mode_group"],
    )

    county_race_share_design.write_csv(
        output_dir / "trips_county_race_mode_share_with_design_se.csv"
    )

# call the county x race function
estimate_mode_group_shares_for_county_race()


# the county × age preparation step
trips_county_age = (
    trips_enriched
    .filter(
        pl.col("county").is_not_null() &
        pl.col("age_group").is_not_null() &
        pl.col("sample_segment").is_not_null() &
        pl.col("hh_id").is_not_null() &
        pl.col("linked_trip_weight").is_not_null()
    )
)

# Build a new survey design and sample for that domain table
design_county_age = svy.Design(
    stratum="sample_segment",
    psu="hh_id",
    ssu="person_id",
    wgt="linked_trip_weight",
)

sample_county_age = svy.Sample(data=trips_county_age, design=design_county_age)


# the county x age function
def estimate_mode_group_shares_for_county_age() -> None:
    domain_results = []

    for mode_group in MODE_GROUPS:
        result = sample_county_age.estimation.mean(
            y=f"is_{mode_group}",
            by=["county", "age_group"],
        )
        result_pl = result.to_polars().with_columns([
            pl.lit(mode_group).alias("mode_group"),
            (pl.col("se") * Z_VALUE).alias("moe"),
        ])
        domain_results.append(result_pl)

    county_age_share_design = (
        pl.concat(domain_results)
        .rename({
            "est": "weighted_share",
            "lci": "ci_lower",
            "uci": "ci_upper",
        })
        .select([
            "county",
            "age_group",
            "mode_group",
            "weighted_share",
            "se",
            "moe",
            "ci_lower",
            "ci_upper",
        ])
        .sort(["county", "age_group", "mode_group"])
    )

    county_age_share_design = add_counts_and_flags(
        county_age_share_design,
        trips_county_age,
        ["county", "age_group", "mode_group"],
    )

    county_age_share_design.write_csv(
        output_dir / "trips_county_age_mode_share_with_design_se.csv"
    )

# call the county x age function
estimate_mode_group_shares_for_county_age()

print(f"Done. Outputs written to {output_dir}")
print(
    "Design-based outputs written to "
    "mode_group_share_with_design_se.csv, "
    "trips_county_income_mode_share_with_design_se.csv, "
    "trips_county_race_mode_share_with_design_se.csv, "
    "trips_county_age_mode_share_with_design_se.csv"
)