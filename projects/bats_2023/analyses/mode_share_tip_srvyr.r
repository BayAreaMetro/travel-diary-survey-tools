# Mode Share Analysis with srvyr - Replicating Python svy Output
# This script produces the same output as mode_group_share_with_se.csv
# from the Python script mode_share_tip_svy.py


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
output_dir <- "E:/BATS2023_TIP_11052026/tip_srvyr"

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

# ============================================================================
# BUILD ANALYSIS TABLE
# ============================================================================

# First, get household attributes (sample_segment)
hh_attrs <- households %>%
  select(hh_id, sample_segment)

trips_enriched <- linked_trips %>%
  # Filter to trips with positive weights
  filter(linked_trip_weight > 0) %>%
  # Create mode_group from mode_type
  mutate(
    mode_group = recode(as.character(mode_type), !!!mode_group_map, .default = NA_character_)
  ) %>%
  # Filter to non-null mode groups
  filter(!is.na(mode_group)) %>%
  # Join with households to get sample_segment
  left_join(hh_attrs, by = "hh_id") %>%
  # Select only needed columns for the overall analysis
  select(hh_id, person_id, mode_type, mode_group, linked_trip_weight, sample_segment)

# Get unique mode groups (sorted)
MODE_GROUPS <- sort(unique(trips_enriched$mode_group))

# Create indicator variables for each mode group
for (mode_group in MODE_GROUPS) {
  col_name <- paste0("is_", mode_group)
  trips_enriched[[col_name]] <- as.integer(trips_enriched$mode_group == mode_group)
}


# ============================================================================
# DEFINE SURVEY DESIGN
# ============================================================================

# Create survey design object with 3-stage design
# Stage 1: Stratum (sample_segment)
# Stage 2: PSU (hh_id)
# Stage 3: SSU (person_id)
trips_svy <- trips_enriched %>%
  as_survey_design(
    strata = sample_segment,
    ids = c(hh_id, person_id),
    weights = linked_trip_weight,
  )

# Add this after creating trips_svy to diagnose
print(paste("Number of observations:", nrow(trips_enriched)))
print(paste("Number of strata:", length(unique(trips_enriched$sample_segment))))
print(paste("Number of PSUs:", length(unique(trips_enriched$hh_id))))

# ============================================================================
# CALCULATE MODE SHARES WITH DESIGN-BASED SE
# ============================================================================

# Calculate mean for each mode group indicator
mode_results <- list()

for (mg in MODE_GROUPS) {
  col_name <- paste0("is_", mg)
  
  result <- trips_svy %>%
    summarize(
      weighted_share = survey_mean(
        .data[[col_name]],
        vartype = c("se", "ci"),
        level = CONF_LEVEL,
        na.rm = TRUE
      )
    ) %>%
    mutate(mode_group = mg)
  
  mode_results[[mg]] <- result
}

# Combine results
mode_group_share_design <- bind_rows(mode_results) %>%
  rename(
    se = weighted_share_se,
    ci_lower = weighted_share_low,
    ci_upper = weighted_share_upp
  )

# ============================================================================
# ADD COUNTS AND METADATA
# ============================================================================

# Calculate weighted and unweighted counts for each mode group
weighted_counts <- trips_enriched %>%
  group_by(mode_group) %>%
  summarize(
    weighted_count = sum(linked_trip_weight),
    unweighted_count = n(),
    .groups = "drop"
  )

# Calculate totals (same for all mode groups)
total_weighted <- sum(trips_enriched$linked_trip_weight)
total_unweighted <- nrow(trips_enriched)

# Join counts to results
mode_group_share_design <- mode_group_share_design %>%
  left_join(weighted_counts, by = "mode_group") %>%
  mutate(
    total_weighted = total_weighted,
    total_unweighted = total_unweighted,
    # Fill any missing counts with 0
    weighted_count = coalesce(weighted_count, 0),
    unweighted_count = coalesce(as.numeric(unweighted_count), 0)
  )

# ============================================================================
# CALCULATE RELIABILITY METRICS
# ============================================================================

mode_group_share_design <- mode_group_share_design %>%
  mutate(
    # Coefficient of variation
    coeff_of_var = if_else(
      weighted_share != 0,
      abs(se / weighted_share),
      NA_real_
    ),
    # CI width
    ci_width = ci_upper - ci_lower,
    # Confidence level
    confidence_level = CONF_LEVEL,
    # Flags
    cv_flag = coalesce(coeff_of_var > CV_THRESHOLD, FALSE),
    sample_size_flag = unweighted_count < MIN_UNWEIGHTED_N,
    ci_width_flag = ci_width > CI_WIDTH_THRESHOLD,
    extreme_values_flag = (ci_lower < 0) | (ci_upper > 1),
    # Suppress if any flag is TRUE
    suppress = cv_flag | sample_size_flag | ci_width_flag | extreme_values_flag,
    # Reliability label (priority order matching Python)
    estimate_reliability = case_when(
      cv_flag ~ "Poor (High CV >30%)",
      sample_size_flag ~ "Poor (Small sample n<30)",
      ci_width_flag ~ "Poor (Wide CI >40pp)",
      extreme_values_flag ~ "Poor (Invalid range)",
      TRUE ~ "Acceptable"
    )
  )

# ============================================================================
# FORMAT AND EXPORT
# ============================================================================

# Select and order columns to match Python output
mode_group_share_design <- mode_group_share_design %>%
  select(
    mode_group,
    weighted_share,
    se,
    ci_lower,
    ci_upper,
    weighted_count,
    unweighted_count,
    total_weighted,
    total_unweighted,
    coeff_of_var,
    ci_width,
    confidence_level,
    cv_flag,
    sample_size_flag,
    ci_width_flag,
    extreme_values_flag,
    suppress,
    estimate_reliability
  ) %>%
  arrange(mode_group)

# Write to CSV
write_csv(
  mode_group_share_design,
  file.path(output_dir, "mode_group_share_with_se_srvyr.csv")
)

# Print results
cat("\nMode Group Share Analysis Complete\n")
cat("Output saved to:", file.path(output_dir, "mode_group_share_with_se_srvyr.csv"), "\n\n")
print(mode_group_share_design)