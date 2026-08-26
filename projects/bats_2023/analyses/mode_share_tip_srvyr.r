# Mode Share Analysis with srvyr - Replicating Python svy Output
# This script produces the same output as the Python script mode_share_tip_svy.py


# ============================================================================
# LOAD LIBRARIES
# ============================================================================

library(tidyverse)
library(srvyr)

# ============================================================================
# CONFIGURATION
# ============================================================================

# File paths
input_dir <- "E:/BATS2023_TIP_11052026/survey"
output_dir <- "E:/Box/Modeling and Surveys/Surveys/Requests/TIP_investment analysis_2027/BATS2023_TIP_14052026/tip_srvyr"

# Create output directory if it doesn't exist
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# Statistical parameters for reliability assessment
CONF_LEVEL <- 0.90
ALPHA <- 1 - CONF_LEVEL
CV_THRESHOLD <- 0.30
MIN_UNWEIGHTED_N <- 30
CI_WIDTH_THRESHOLD <- 0.40

# ============================================================================
# LOAD DATA
# ============================================================================

households <- read_csv(file.path(input_dir, "households_2023.csv"))
persons <- read_csv(file.path(input_dir, "persons_2023.csv"))
linked_trips <- read_csv(file.path(input_dir, "linked_trips_2023.csv"))

# ============================================================================
# RECODE MAPS
# ============================================================================

# Column names for race and ethnicity variables
RACE_COL <- "race_imputed_rmove_only"
ETHNICITY_COL <- "ethnicity_imputed_rmove_only"

# Mode grouping: collapse mode_type categories from linked_trips table
mode_group_map <- c(
  "1" = "3. active",    # Walk
  "2" = "3. active",    # Bike
  "3" = "3. active",    # Bikeshare
  "4" = "3. active",    # Scootershare
  "5" = "1. roadway",   # Taxi
  "6" = "1. roadway",   # TNC
  "8" = "1. roadway",   # Car
  "9" = "1. roadway",   # Carshare
  "10" = "2. transit",  # School bus
  "11" = "2. transit",  # Shuttle/vanpool
  "12" = "2. transit",  # Ferry
  "13" = "2. transit",  # Transit
  "14" = "2. transit"   # Long distance passenger
)

# Income grouping
income_map <- c(
  "1" = "1. Under $50,000",
  "2" = "1. Under $50,000",
  "3" = "2. $50,000-$99,999",
  "4" = "2. $50,000-$99,999",
  "5" = "3. $100,000-$199,999",
  "6" = "4. $200,000 or more"
)

# Age grouping
age_group_map <- c(
  "1" = "Under 65",
  "2" = "Under 65",
  "3" = "Under 65",
  "4" = "Under 65",
  "5" = "Under 65",
  "6" = "Under 65",
  "7" = "Under 65",
  "8" = "Under 65",
  "9" = "65 and Over",
  "10" = "65 and Over",
  "11" = "65 and Over"
)

# Ethnicity mapping (Hispanic takes priority)
ethnicity_map <- c(
  "hispanic" = "1. Hispanic (All Races)",
  "not_hispanic" = NA_character_
)

# Race mapping (used when ethnicity is not Hispanic)
race_map <- c(
  "white" = "4. White (Non-Hispanic)",
  "afam" = "2. Black (Non-Hispanic)",
  "asian_pacific" = "3. Asian/Pacific Islander (Non-Hispanic)",
  "other" = "5. Other (Non-Hispanic)"
)

# County mapping
county_map <- c(
  "6001" = "Alameda",
  "6013" = "Contra Costa",
  "6041" = "Marin and Sonoma",
  "6097" = "Marin and Sonoma",
  "6055" = "Napa and Solano",
  "6095" = "Napa and Solano",
  "6075" = "San Francisco",
  "6081" = "San Mateo",
  "6085" = "Santa Clara"
)

# ============================================================================
# PREPARE DEMOGRAPHIC ATTRIBUTES
# ============================================================================

hh_attrs <- households %>%
  select(hh_id, home_county, income_bin, sample_segment) %>%
  mutate(
    county = recode(as.character(home_county), !!!county_map, .default = NA_character_),
    income = recode(as.character(income_bin), !!!income_map, .default = NA_character_)
  )

person_attrs <- persons %>%
  select(hh_id, person_id, age, all_of(c(RACE_COL, ETHNICITY_COL))) %>%
  mutate(
    age_group = recode(as.character(age), !!!age_group_map, .default = NA_character_),
    ethnicity_mapped = recode(!!sym(ETHNICITY_COL), !!!ethnicity_map, .default = NA_character_),
    race_mapped = recode(!!sym(RACE_COL), !!!race_map, .default = NA_character_),
    race_eth = coalesce(ethnicity_mapped, race_mapped)
  ) %>%
  select(hh_id, person_id, age_group, race_eth)

# ============================================================================
# BUILD ANALYSIS TABLE
# ============================================================================

trips_enriched <- linked_trips %>%
  # Filter to trips with positive weights
  filter(linked_trip_weight > 0) %>%
  # Create mode_group from mode_type
  mutate(
    mode_group = recode(as.character(mode_type), !!!mode_group_map, .default = NA_character_),
    distance_miles = distance_meters / 1609.34
  ) %>%
  # Filter to non-null mode groups
  filter(!is.na(mode_group)) %>%
  # Join with household attributes
  left_join(hh_attrs, by = "hh_id") %>%
  # Join with person attributes
  left_join(person_attrs, by = c("hh_id", "person_id")) %>%
  # Calculate PMT weight
  mutate(
    pmt_weight = linked_trip_weight * distance_miles
  )

# Get unique mode groups (sorted)
MODE_GROUPS <- sort(unique(trips_enriched$mode_group))

# Create indicator variables for each mode group
for (mode_group in MODE_GROUPS) {
  col_name <- paste0("is_", mode_group)
  trips_enriched[[col_name]] <- as.integer(trips_enriched$mode_group == mode_group)
}

# Save enriched trips for debugging
write_csv(trips_enriched, file.path(output_dir, "linked_trips_enriched_for_srvyr.csv"))

# ============================================================================
# HELPER FUNCTION: ADD COUNTS AND FLAGS
# ============================================================================

add_counts_and_flags <- function(share_df, source_df, group_cols, metric_type = "trip") {
  # Determine domain columns (all except mode_group)
  domain_cols <- setdiff(group_cols, "mode_group")
  
  # Calculate weighted and unweighted counts
  if (metric_type == "trip") {
    weighted_counts <- source_df %>%
      group_by(across(all_of(group_cols))) %>%
      summarize(
        weighted_count = sum(linked_trip_weight),
        unweighted_count = n(),
        .groups = "drop"
      )
  } else {  # PMT
    weighted_counts <- source_df %>%
      group_by(across(all_of(group_cols))) %>%
      summarize(
        weighted_count = sum(linked_trip_weight * distance_miles),
        unweighted_count = n(),
        .groups = "drop"
      )
  }
  
  # Calculate totals
  if (length(domain_cols) > 0) {
    if (metric_type == "trip") {
      totals <- source_df %>%
        group_by(across(all_of(domain_cols))) %>%
        summarize(
          total_weighted = sum(linked_trip_weight),
          total_unweighted = n(),
          .groups = "drop"
        )
    } else {  # PMT
      totals <- source_df %>%
        group_by(across(all_of(domain_cols))) %>%
        summarize(
          total_weighted = sum(linked_trip_weight * distance_miles),
          total_unweighted = n(),
          .groups = "drop"
        )
    }
    
    result <- share_df %>%
      left_join(weighted_counts, by = group_cols) %>%
      left_join(totals, by = domain_cols)
  } else {
    # No domain columns (overall analysis)
    if (metric_type == "trip") {
      total_weighted <- sum(source_df$linked_trip_weight)
    } else {  # PMT
      total_weighted <- sum(source_df$linked_trip_weight * source_df$distance_miles)
    }
    total_unweighted <- nrow(source_df)
    
    result <- share_df %>%
      left_join(weighted_counts, by = group_cols) %>%
      mutate(
        total_weighted = total_weighted,
        total_unweighted = total_unweighted
      )
  }
  
  # Fill missing counts with 0
  result <- result %>%
    mutate(
      weighted_count = coalesce(weighted_count, 0),
      unweighted_count = coalesce(as.numeric(unweighted_count), 0)
    )
  
  # Calculate reliability metrics
  result <- result %>%
    mutate(
      coeff_of_var = if_else(
        weighted_share != 0,
        abs(se / weighted_share),
        NA_real_
      ),
      ci_width = ci_upper - ci_lower,
      confidence_level = CONF_LEVEL,
      cv_flag = coalesce(coeff_of_var > CV_THRESHOLD, FALSE),
      sample_size_flag = unweighted_count < MIN_UNWEIGHTED_N,
      ci_width_flag = ci_width > CI_WIDTH_THRESHOLD,
      extreme_values_flag = (ci_lower < 0) | (ci_upper > 1),
      suppress = cv_flag | sample_size_flag | ci_width_flag | extreme_values_flag,
      estimate_reliability = case_when(
        cv_flag ~ "Poor (High CV >30%)",
        sample_size_flag ~ "Poor (Small sample n<30)",
        ci_width_flag ~ "Poor (Wide CI >40pp)",
        extreme_values_flag ~ "Poor (Invalid range)",
        TRUE ~ "Acceptable"
      )
    ) %>%
    # Preserve sort order by re-sorting on group columns
    arrange(across(all_of(group_cols)))
  
  return(result)
}

# ============================================================================
# HELPER FUNCTION: ESTIMATE DOMAIN SHARES
# ============================================================================

estimate_domain_shares <- function(filtered_trips, domain_cols, output_filename, metric_type = "trip") {
  # Set up weight column
  weight_col <- if (metric_type == "trip") "linked_trip_weight" else "pmt_weight"
  
  # Create survey design
  trips_svy <- filtered_trips %>%
    as_survey_design(
      strata = sample_segment,
      ids = c(hh_id, person_id),
      weights = !!sym(weight_col),
      nest = TRUE
    )
  
  # Calculate shares for each mode group
  domain_results <- list()
  
  for (mg in MODE_GROUPS) {
    col_name <- paste0("is_", mg)
    
    # Build formula dynamically
    if (length(domain_cols) > 0) {
      result <- trips_svy %>%
        group_by(across(all_of(domain_cols))) %>%
        summarize(
          weighted_share = survey_mean(
            !!sym(col_name),
            vartype = c("se", "ci"),
            level = CONF_LEVEL,
            na.rm = TRUE
          ),
          .groups = "drop"
        ) %>%
        rename(
          se = weighted_share_se,
          ci_lower = weighted_share_low,
          ci_upper = weighted_share_upp
        ) %>%
        mutate(mode_group = mg)
    }
    
    domain_results[[mg]] <- result
  }
  
  # Combine and format
  share_design <- bind_rows(domain_results) %>%
    select(all_of(c(domain_cols, "mode_group", "weighted_share", "se", "ci_lower", "ci_upper"))) %>%
    arrange(across(all_of(c(domain_cols, "mode_group"))))
  
  # Add counts and flags
  share_design <- add_counts_and_flags(
    share_design,
    filtered_trips,
    c(domain_cols, "mode_group"),
    metric_type = metric_type
  )
  
  # Write to CSV
  write_csv(share_design, file.path(output_dir, output_filename))
  
  cat(sprintf("✓ Saved: %s\n", output_filename))
}


# ============================================================================
# OVERALL MODE GROUP SHARES WITH SE AND CI
# ============================================================================

cat("\n=== Calculating Overall Mode Group Shares (Trips) ===\n")

# Create survey design for trips
trips_svy <- trips_enriched %>%
  as_survey_design(
    strata = sample_segment,
    ids = c(hh_id, person_id),
    weights = linked_trip_weight,
    nest = TRUE
  )

# Calculate mean for each mode group indicator
mode_results <- list()

for (mg in MODE_GROUPS) {
  col_name <- paste0("is_", mg)
  
  result <- trips_svy %>%
    summarize(
      weighted_share = survey_mean(
        !!sym(col_name),
        vartype = c("se", "ci"),
        level = CONF_LEVEL,
        na.rm = TRUE
      )
    ) %>%
    rename(
      se = weighted_share_se,
      ci_lower = weighted_share_low,
      ci_upper = weighted_share_upp
    ) %>%
    mutate(mode_group = mg)
  
  mode_results[[mg]] <- result
}

# Combine results
mode_group_share_design <- bind_rows(mode_results) %>%
  select(mode_group, weighted_share, se, ci_lower, ci_upper) %>%
  arrange(mode_group)

# Add counts and flags
mode_group_share_design <- add_counts_and_flags(
  mode_group_share_design,
  trips_enriched,
  "mode_group",
  metric_type = "trip"
)

# Write to CSV
write_csv(
  mode_group_share_design,
  file.path(output_dir, "trip_mode_share_with_se_srvyr.csv")
)

cat("✓ Saved: trip_mode_share_with_se_srvyr.csv\n")


# ============================================================================
# OVERALL MODE GROUP SHARES (PMT) WITH SE AND CI
# ============================================================================

cat("\n=== Calculating Overall Mode Group Shares (PMT) ===\n")

# Create survey design for PMT
trips_svy_pmt <- trips_enriched %>%
  as_survey_design(
    strata = sample_segment,
    ids = c(hh_id, person_id),
    weights = pmt_weight,
    nest = TRUE
  )

# Calculate PMT shares
pmt_results <- list()

for (mg in MODE_GROUPS) {
  col_name <- paste0("is_", mg)
  
  result <- trips_svy_pmt %>%
    summarize(
      weighted_share = survey_mean(
        !!sym(col_name),
        vartype = c("se", "ci"),
        level = CONF_LEVEL,
        na.rm = TRUE
      )
    ) %>%
    rename(
      se = weighted_share_se,
      ci_lower = weighted_share_low,
      ci_upper = weighted_share_upp
    ) %>%
    mutate(mode_group = mg)
  
  pmt_results[[mg]] <- result
}

# Combine results
mode_group_share_pmt_design <- bind_rows(pmt_results) %>%
  select(mode_group, weighted_share, se, ci_lower, ci_upper) %>%
  arrange(mode_group)

# Add counts and flags
mode_group_share_pmt_design <- add_counts_and_flags(
  mode_group_share_pmt_design,
  trips_enriched,
  "mode_group",
  metric_type = "pmt"
)

# Write to CSV
write_csv(
  mode_group_share_pmt_design,
  file.path(output_dir, "mode_group_share_pmt_with_se_srvyr.csv")
)

cat("✓ Saved: mode_group_share_pmt_with_se_srvyr.csv\n")


# ============================================================================
# DOMAIN-SPECIFIC MODE SHARES WITH SE AND CI
# ============================================================================

cat("\n=== Calculating Domain-Specific Mode Shares (Trips) ===\n")

# County × Income
trips_county_income <- trips_enriched %>%
  filter(
    !is.na(county),
    !is.na(income),
    !is.na(sample_segment),
    !is.na(hh_id),
    !is.na(person_id),
    !is.na(linked_trip_weight)
  )

estimate_domain_shares(
  trips_county_income,
  c("county", "income"),
  "trips_county_income_mode_share_with_se_srvyr.csv",
  metric_type = "trip"
)

# County × Race/Ethnicity
trips_county_race <- trips_enriched %>%
  filter(
    !is.na(county),
    !is.na(race_eth),
    !is.na(sample_segment),
    !is.na(hh_id),
    !is.na(person_id),
    !is.na(linked_trip_weight)
  )

estimate_domain_shares(
  trips_county_race,
  c("county", "race_eth"),
  "trips_county_race_mode_share_with_se_srvyr.csv",
  metric_type = "trip"
)

# County × Age
trips_county_age <- trips_enriched %>%
  filter(
    !is.na(county),
    !is.na(age_group),
    !is.na(sample_segment),
    !is.na(hh_id),
    !is.na(person_id),
    !is.na(linked_trip_weight)
  )

estimate_domain_shares(
  trips_county_age,
  c("county", "age_group"),
  "trips_county_age_mode_share_with_se_srvyr.csv",
  metric_type = "trip"
)


# ============================================================================
# DOMAIN-SPECIFIC MODE SHARES (PMT) WITH SE AND CI
# ============================================================================

cat("\n=== Calculating Domain-Specific Mode Shares (PMT) ===\n")

# County × Income (PMT)
estimate_domain_shares(
  trips_county_income,
  c("county", "income"),
  "trips_county_income_mode_pmt_share_with_se_srvyr.csv",
  metric_type = "pmt"
)

# County × Race/Ethnicity (PMT)
estimate_domain_shares(
  trips_county_race,
  c("county", "race_eth"),
  "trips_county_race_mode_pmt_share_with_se_srvyr.csv",
  metric_type = "pmt"
)

# County × Age (PMT)
estimate_domain_shares(
  trips_county_age,
  c("county", "age_group"),
  "trips_county_age_mode_pmt_share_with_se_srvyr.csv",
  metric_type = "pmt"
)

cat("\n=== Analysis Complete! ===\n")
cat("All outputs saved to:", output_dir, "\n")