# This script is not currently runnable from the repo's default environment yet, because
# travel-diary-survey-tools pins numpy<1.26, while svy requires numpy>=2.0.
# For now it is being run in a separate environment and read pipeline CSV outputs by path.

from pathlib import Path

import polars as pl

import svy

# ============================================================================
# CONFIGURATION
# ============================================================================

# File paths
input_dir = Path(r"E:\BATS2023_TIP_11052026\survey")
output_dir = Path(r"E:/Box/Modeling and Surveys/Surveys/Requests/TIP_investment analysis_2027/BATS2023_TIP_14052026/tip_svy")

output_dir.mkdir(parents=True, exist_ok=True)

# Column names for race and ethnicity variables
# They need to be the ones imputed from the weighting process
RACE_COL = "race_imputed_rmove_only"
ETHNICITY_COL = "ethnicity_imputed_rmove_only"

# Statistical parameters for reliability assessment
CONF_LEVEL = 0.90
ALPHA = 1 - CONF_LEVEL

CV_THRESHOLD = 0.30
MIN_UNWEIGHTED_N = 30
CI_WIDTH_THRESHOLD = 0.40


# ============================================================================
# LOAD DATA
# ============================================================================

households = pl.read_csv(input_dir / "households_2023.csv")
persons = pl.read_csv(input_dir / "persons_2023.csv")
linked_trips = pl.read_csv(input_dir / "linked_trips_2023.csv")


# ============================================================================
# RECODE MAPS
# ============================================================================

# mode grouping: collapse mode_type categories from linked_trips table
# see: ModeType enum in src/data_canon/codebook/trips.py
# canonical_field_name = "mode_type"
mode_group_map = {
    1: "3. active",    # Walk
    2: "3. active",    # Bike
    3: "3. active",    # Bikeshare
    4: "3. active",    # Scootershare
    5: "1. roadway",   # Taxi
    6: "1. roadway",   # TNC
    # 7: "other",     # Other
    8: "1. roadway",   # Car
    9: "1. roadway",   # Carshare
    10: "2. transit",  # School bus
    11: "2. transit",  # Shuttle/vanpool
    12: "2. transit",  # Ferry
    13: "2. transit",  # Transit
    14: "2. transit",  # Long distance passenger
    # 995: "missing", # Missing Response
}

# income grouping: collapse income_bin categories from households table
# see: IncomeBroad enum in src/data_canon/codebook/households.py
# canonical_field_name = "income_bin"
income_map = {
    1: "1. Under $50,000",      # Under $25,000
    2: "1. Under $50,000",      # $25,000-$49,999
    3: "2. $50,000-$99,999",    # $50,000-$74,999
    4: "2. $50,000-$99,999",    # $75,000-$99,999
    5: "3. $100,000-$199,999",  # $100,000-$199,999
    6: "4. $200,000 or more",   # $200,000 or more
    # 995: "Missing",
    # 999: "Prefer not to answer",
}

# age grouping: collapse age categories from persons table
# see: AgeCategory enum in src/data_canon/codebook/persons.py 
# canonical_field_name = "age"
age_group_map = {
    1: "Under 65",    # Under 5
    2: "Under 65",    # 5 to 15
    3: "Under 65",    # 16 to 17
    4: "Under 65",    # 18 to 24
    5: "Under 65",    # 25 to 34
    6: "Under 65",    # 35 to 44
    7: "Under 65",    # 45 to 54
    8: "Under 65",    # 55 to 64
    9: "65 and Over", # 65 to 74
    10: "65 and Over", # 75 to 84
    11: "65 and Over", # 85 and up
}

# ethnicity grouping: Hispanic identity takes priority
# see: ethnicity_user_reported column in persons table
# will be combined with race_map via coalesce to create race_eth variable
ethnicity_map = {
    "hispanic": "1. Hispanic (All Races)",
    "not_hispanic": None,  # will use race instead
    # "missing": None,       
}

# race grouping: label cleanup for race_user_input
# see: race_user_input column in persons table
# used when ethnicity is not Hispanic (via coalesce)
race_map = {
    "white": "4. White (Non-Hispanic)",
    "afam": "2. Black (Non-Hispanic)",
    "asian_pacific": "3. Asian/Pacific Islander (Non-Hispanic)",
    "other": "5. Other (Non-Hispanic)",
    # "missing": "Missing (Non-Hispanic)",
}

# county grouping: map FIPS codes to county names (combine smaller counties)
county_map = {
    "6001": "Alameda",
    "6013": "Contra Costa",
    "6041": "Marin and Sonoma",  # Marin
    "6097": "Marin and Sonoma",  # Sonoma
    "6055": "Napa and Solano",   # Napa
    "6095": "Napa and Solano",   # Solano
    "6075": "San Francisco",
    "6081": "San Mateo",
    "6085": "Santa Clara",
}

# ============================================================================
# PREPARE DEMOGRAPHIC ATTRIBUTES
# ============================================================================

hh_attrs = (
    households
    .select(["hh_id", "home_county", "income_bin", "sample_segment"])
    .with_columns([
        pl.col("home_county").cast(pl.Utf8).replace(county_map).alias("county"),
        pl.col("income_bin").replace_strict(income_map, default=None).alias("income"),
    ])
)

person_attrs = (
    persons
    .select(["person_id", "hh_id", "age", RACE_COL, ETHNICITY_COL])
    .with_columns([
        pl.col("age").replace_strict(age_group_map, default=None).alias("age_group"),
        pl.coalesce([
            pl.col(ETHNICITY_COL).replace(ethnicity_map),
            pl.col(RACE_COL).replace(race_map)
        ]).alias("race_eth"),
    ])
)

# ============================================================================
# BUILD ANALYSIS TABLE
# ============================================================================

trips_enriched = (
    linked_trips
    .filter(pl.col("linked_trip_weight") > 0)
    .with_columns([
        pl.col("mode_type").replace_strict(mode_group_map, default=None).alias("mode_group"),
        (pl.col("distance_meters") / 1609.34).alias("distance_miles"),
    ])
    .filter(pl.col("mode_group").is_not_null())
    .join(hh_attrs, on="hh_id", how="left")
    .join(
        person_attrs.select(["person_id", "hh_id", "age_group", "race_eth"]),
        on=["person_id", "hh_id"],
        how="left",
    )
    .with_columns([
        (pl.col("linked_trip_weight") * pl.col("distance_miles")).alias("pmt_weight"),
    ])
)

# Create indicator columns for each mode group (needed for design-based estimation)
# this is equivalent to specifying MODE_GROUPS = ["1. roadway", "2. transit", "3. active", "other"]
MODE_GROUPS = sorted(trips_enriched["mode_group"].unique().to_list())

for mode_group in MODE_GROUPS:
    trips_enriched = trips_enriched.with_columns(
        (pl.col("mode_group") == mode_group).cast(pl.Int8).alias(f"is_{mode_group}")
    )

# Save an enriched copy for inspection/debugging
trips_enriched.write_csv(output_dir / "linked_trips_enriched_for_svy.csv")

# ============================================================================
# Helper to attach counts and reliability fields
# ============================================================================
def add_counts_and_flags(
    share_df: pl.DataFrame,
    source_df: pl.DataFrame,
    group_cols: list[str],
    metric_type: str = "trip",
) -> pl.DataFrame:
    """Add counts and reliability flags.
    
    Args:
        share_df: DataFrame with estimated shares
        source_df: Source data with trip records
        group_cols: Grouping columns including mode_group
        metric_type: "trip" for trip counts or "pmt" for passenger-miles
    """
    domain_cols = [col for col in group_cols if col != "mode_group"]

    if metric_type == "trip":
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
    else:  # metric_type == "pmt"
        weighted_counts = (
            source_df
            .group_by(group_cols)
            .agg([
                (pl.col("linked_trip_weight") * pl.col("distance_miles")).sum().alias("weighted_count"),
                pl.len().alias("unweighted_count"),
            ])
        )

        if domain_cols:
            totals = (
                source_df
                .group_by(domain_cols)
                .agg([
                    (pl.col("linked_trip_weight") * pl.col("distance_miles")).sum().alias("total_weighted"),
                    pl.len().alias("total_unweighted"),
                ])
            )
            result = (
                share_df
                .join(weighted_counts, on=group_cols, how="left")
                .join(totals, on=domain_cols, how="left")
            )
        else:
            total_weighted = (source_df["linked_trip_weight"] * source_df["distance_miles"]).sum()
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
# Helper: estimate mode shares by demographic domain with design-based SE
# ============================================================================
def estimate_domain_shares(
    filtered_trips: pl.DataFrame,
    domain_cols: list[str],
    output_filename: str,
    metric_type: str = "trip",
) -> None:
    """Estimate mode shares by domain with design-based standard errors.
    
    Args:
        filtered_trips: Filtered trip data
        domain_cols: Demographic grouping columns
        output_filename: Output CSV filename
        metric_type: "trip" for trip counts or "pmt" for passenger-miles
    """
    
    # Create design and sample based on metric type
    weight_col = "linked_trip_weight" if metric_type == "trip" else "pmt_weight"
    design = svy.Design(
        stratum="sample_segment",
        psu="hh_id",
        ssu="person_id",
        wgt=weight_col,
    )
    sample = svy.Sample(data=filtered_trips, design=design)
    
    # Calculate shares for each mode group
    domain_results = []
    for mode_group in MODE_GROUPS:
        result = sample.estimation.mean(
            y=f"is_{mode_group}",
            by=domain_cols,
            alpha=ALPHA,
        )
        result_pl = result.to_polars().with_columns([
            pl.lit(mode_group).alias("mode_group"),
        ])
        domain_results.append(result_pl)
    
    # Combine results and add metadata
    share_design = (
        pl.concat(domain_results)
        .rename({
            "est": "weighted_share",
            "lci": "ci_lower",
            "uci": "ci_upper",
        })
        .select([
            *domain_cols,
            "mode_group",
            "weighted_share",
            "se",
            "ci_lower",
            "ci_upper",
        ])
        .sort([*domain_cols, "mode_group"])
    )
    
    share_design = add_counts_and_flags(
        share_design,
        filtered_trips,
        [*domain_cols, "mode_group"],
        metric_type=metric_type,
    )
    
    share_design.write_csv(output_dir / output_filename)




# ============================================================================
# OVERALL MODE GROUP SHARES WITH SE and CI 
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
    result = sample.estimation.mean(y=f"is_{mode_group}", alpha=ALPHA)
    result_pl = result.to_polars().with_columns([
        pl.lit(mode_group).alias("mode_group"),
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
        "ci_lower",
        "ci_upper",
    ])
    .sort("mode_group")
)

mode_group_share_design = add_counts_and_flags(
    mode_group_share_design,
    trips_enriched,
    ["mode_group"],
    metric_type="trip",
)

mode_group_share_design.write_csv(output_dir / "trip_mode_share_with_se.csv")


# ============================================================================
# OVERALL MODE GROUP SHARES (PMT) WITH SE and CI 
# ============================================================================

design_pmt = svy.Design(
    stratum="sample_segment",
    psu="hh_id",
    ssu="person_id",
    wgt="pmt_weight",
)

sample_pmt = svy.Sample(data=trips_enriched, design=design_pmt)

overall_pmt_results = []
for mode_group in MODE_GROUPS:
    result = sample_pmt.estimation.mean(y=f"is_{mode_group}", alpha=ALPHA)
    result_pl = result.to_polars().with_columns([
        pl.lit(mode_group).alias("mode_group"),
    ])
    overall_pmt_results.append(result_pl)

mode_group_share_pmt_design = (
    pl.concat(overall_pmt_results)
    .rename({
        "est": "weighted_share",
        "lci": "ci_lower",
        "uci": "ci_upper",
    })
    .select([
        "mode_group",
        "weighted_share",
        "se",
        "ci_lower",
        "ci_upper",
    ])
    .sort("mode_group")
)

mode_group_share_pmt_design = add_counts_and_flags(
    mode_group_share_pmt_design,
    trips_enriched,
    ["mode_group"],
    metric_type="pmt",
)

mode_group_share_pmt_design.write_csv(output_dir / "mode_group_share_pmt_with_se.csv")



# ============================================================================
# DOMAIN-SPECIFIC MODE SHARES WITH SE and CI
# ============================================================================

# County × Income
trips_county_income = trips_enriched.filter(
    pl.col("county").is_not_null() &
    pl.col("income").is_not_null() &
    pl.col("sample_segment").is_not_null() &
    pl.col("hh_id").is_not_null() &
    pl.col("person_id").is_not_null() &  
    pl.col("linked_trip_weight").is_not_null()
)
estimate_domain_shares(
    trips_county_income,
    ["county", "income"],
    "trips_county_income_mode_share_with_se.csv",
    metric_type="trip",
)

# County × Race/Ethnicity
trips_county_race = trips_enriched.filter(
    pl.col("county").is_not_null() &
    pl.col("race_eth").is_not_null() &
    pl.col("sample_segment").is_not_null() &
    pl.col("hh_id").is_not_null() &
    pl.col("person_id").is_not_null() &  
    pl.col("linked_trip_weight").is_not_null()
)
estimate_domain_shares(
    trips_county_race,
    ["county", "race_eth"],
    "trips_county_race_mode_share_with_se.csv",
    metric_type="trip",
)

# County × Age
trips_county_age = trips_enriched.filter(
    pl.col("county").is_not_null() &
    pl.col("age_group").is_not_null() &
    pl.col("sample_segment").is_not_null() &
    pl.col("hh_id").is_not_null() &
    pl.col("person_id").is_not_null() &  
    pl.col("linked_trip_weight").is_not_null()
)
estimate_domain_shares(
    trips_county_age,
    ["county", "age_group"],
    "trips_county_age_mode_share_with_se.csv",
    metric_type="trip",
)


# ============================================================================
# DOMAIN-SPECIFIC MODE SHARES (PMT) WITH SE and CI
# ============================================================================

# County × Income (PMT)
estimate_domain_shares(
    trips_county_income,
    ["county", "income"],
    "trips_county_income_mode_pmt_share_with_se.csv",
    metric_type="pmt",
)

# County × Race/Ethnicity (PMT)
estimate_domain_shares(
    trips_county_race,
    ["county", "race_eth"],
    "trips_county_race_mode_pmt_share_with_se.csv",
    metric_type="pmt",
)

# County × Age (PMT)
estimate_domain_shares(
    trips_county_age,
    ["county", "age_group"],
    "trips_county_age_mode_pmt_share_with_se.csv",
    metric_type="pmt",
)