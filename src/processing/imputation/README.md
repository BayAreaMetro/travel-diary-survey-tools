[← Back to Main README](../../../README.md)

# Imputation Module

This module provides data imputation capabilities for handling missing values in travel diary survey data using established statistical methods.

## Overview

Missing data is common in travel surveys (e.g., missing income, race, ethnicity, etc.). This module provides:
- **KNN Imputation**: Single-column imputation using K-Nearest Neighbors similarity matching
- **Random Forest Imputation**: Single-column imputation using supervised Random Forest models (auto-selects classifier vs regressor)
- **MICE Imputation**: Multi-column imputation using Multiple Imputation by Chained Equations (handles correlated variables)
- **Diagnostic Tracking**: Optional flag columns to track which values were imputed
- **Quality Validation**: Optional k-fold cross-validation to assess imputation accuracy

## Pipeline Step

### `imputation`

Imputes missing values across any canonical data tables (households, persons, days, unlinked_trips, linked_trips, tours).

**Inputs:**
- Any canonical tables (pl.DataFrame, optional)
- `impute_columns`: Configuration for imputation by table — each config includes a `method` key (`knn`, `rf`, or `mice`) and method-specific parameters (dict, optional)
- `create_flags`: Whether to create `{column}_imputed` flag columns (bool, default: True)
- `validate_imputation`: Optional validation configuration (dict, optional)

**Outputs:**
- Dictionary containing imputed tables with same names as inputs

**Configuration Example:**

```yaml
steps:
  - name: imputation
    validate: true
    params:
      impute_columns:
        households:
          - method: knn
            column: income_broad
            missing_values: [MISSING, PNTA]
            n_neighbors: 5
            neighbor_weights: distance

        persons:
          - method: knn
            column: gender
            n_neighbors: 5
            neighbor_weights: distance
            join_tables: [households]
            categorical_features: [income_bin, residence_type]
          - method: rf
            column: education
            missing_values: [MISSING]
            n_estimators: 200
            max_depth: 15
            numeric_features: [age]
            categorical_features: [employment, occupation]
          - method: mice
            columns: [race, ethnicity]
            missing_values:
              race: [MISSING]
              ethnicity: [MISSING, PNTA]
            max_iter: 10

        unlinked_trips:
          - method: knn
            column: mode
            n_neighbors: 5
            neighbor_weights: distance
          - method: mice
            columns: [depart_hour, arrive_hour, duration]
            max_iter: 10

      # Global settings
      random_state: 42
      create_flags: true

      # Optional: k-fold validation to assess quality
      validate_imputation:
        enabled: true
        n_folds: 5
        sample_pct: 5.0
```

Configs are grouped by method and executed in a fixed order (KNN → RF → MICE)
across all tables, so later phases can use values filled in earlier phases.

## Handling Missing Values with Enum Labels

Survey data often uses special codes for missing values (e.g., 995 for "Missing Response", 999 for "Prefer not to answer"). The imputation module automatically resolves enum labels to their numeric values and replaces them with nulls before imputation.

**Specifying Missing Values:**

Use enum member names (labels) rather than numeric values in the config:

```yaml
impute_columns:
  households:
    - method: knn
      column: income_broad
      missing_values: [MISSING, PNTA]  # Enum labels
      n_neighbors: 5
```

This will:
1. Look up the `IncomeBroad` enum from the codebook
2. Resolve `MISSING` → 995 and `PNTA` → 999
3. Replace those values with null before imputation
4. Impute the nulls using KNN

**For MICE with multiple columns:**

```yaml
impute_columns:
  persons:
    - method: mice
      columns: [race, ethnicity]
      missing_values:
        race: [MISSING]            # Only MISSING for race
        ethnicity: [MISSING, PNTA]  # Both MISSING and PNTA for ethnicity
      max_iter: 10
```

Or use a single list to apply the same missing values to all columns:

```yaml
impute_columns:
  persons:
    - method: mice
      columns: [race, ethnicity]
      missing_values: [MISSING, PNTA]  # Applied to all columns
      max_iter: 10
```

**Enum Resolution:**

The module automatically:
- Maps the table name to the appropriate codebook module (e.g., `households` → `data_canon.codebook.households`)
- Finds the enum class with matching `canonical_field_name` (e.g., `income_broad` → `IncomeBroad`)
- Resolves enum member names to their values (e.g., `MISSING` → 995)
- Replaces those values with null in the DataFrame


## Cross-Table Features (`join_tables`)

By default, imputation models only use features from the same table. The `join_tables` option allows you to incorporate features from parent tables (e.g., household attributes when imputing person-level fields), which can significantly improve imputation quality.

**How it works:**

1. Before imputation, columns from the specified parent table(s) are left-joined onto the child table using known foreign key relationships (e.g., `persons` → `households` via `hh_id`)
2. For each target column, a `hh_mode_{column}` feature is auto-generated — the mode of that column among *other* household members (exclude-self). This captures within-household correlation (e.g., siblings sharing race/ethnicity).
3. The auto-generated `hh_mode_*` columns are automatically appended to `categorical_features`.
4. After imputation, all joined/aggregated columns are stripped — the output schema is unchanged.

**Configuration:**

Add `join_tables` to a KNN or MICE config block, and reference the parent columns in your feature lists:

```yaml
impute_columns:
  persons:
    - method: knn
      column: gender
      n_neighbors: 5
      join_tables: [households]
      categorical_features: [age, employment, income_bin, residence_type]
      #                                       ^^^^^^^^^^  ^^^^^^^^^^^^^^
      #                              These come from the households table
    - method: mice
      columns: [race, ethnicity]
      join_tables: [households]
      categorical_features: [age, employment, income_bin, residence_type]
      max_iter: 10
```

**Supported relationships:**

| Child Table | Parent Table | Join Key |
|---|---|---|
| persons | households | hh_id |
| days | persons | person_id |
| days | households | hh_id |
| unlinked_trips | days / persons / households | day_id / person_id / hh_id |
| linked_trips | days / persons / households | day_id / person_id / hh_id |
| tours | persons / households | person_id / hh_id |

**Notes:**
- Joined columns that already exist on the child table are skipped (no duplicates)
- `hh_mode_*` features are only generated when `person_id` and `hh_id` columns exist
- For single-person households, `hh_mode_*` will be null (no other members to reference)
- Validation also uses the same joins, so cross-validation metrics reflect the full feature set


## Child-to-Parent Aggregation (`aggregate_from`)

The reverse of `join_tables`: aggregate child table data up to a parent table. This is useful when imputing parent-level fields that depend on household composition (e.g., predicting household income from the employment/education mix of its members).

**How it works:**

1. For each child table and each field listed under `pivot_count`, the module groups child rows by the parent's foreign key and creates one column per unique value, counting occurrences.
2. Generated columns are named `{child_table}_count_{field}_{value}` (e.g., `persons_count_employment_1`, `persons_count_employment_2`).
3. All generated columns are automatically added to `numeric_features` — you don't reference them in your config.
4. The sum of a field's pivot columns equals the household size, so a separate count is unnecessary.
5. After imputation, all generated columns are stripped — the output schema is unchanged.

**Configuration:**

```yaml
impute_columns:
  households:
    - method: mice
      columns: [income_bin]
      aggregate_from:
        persons:
          pivot_count: [employment, education, student]
      categorical_features: [residence_type, residence_rent_own]
      max_iter: 10
```

This creates columns like `persons_count_employment_1`, `persons_count_employment_2`, `persons_count_education_1`, etc. — all auto-added to `numeric_features`.

**Notes:**
- Uses the same FK relationships as `join_tables` (looked up in reverse)
- Parent rows with no children get 0 for all pivot columns
- Be mindful of feature explosion: a field with 20 unique values creates 20 columns
- Can be combined with `join_tables` in the same config block if needed
- Validation also uses the same aggregation, so cross-validation metrics reflect the full feature set



## Imputation Methods

### KNN (K-Nearest Neighbors)

**Best for:** Single columns with missing values

**How it works:**
1. For each row with a missing value, find the K most similar records (based on all numeric features)
2. Impute the missing value using the weighted average (or mode) of the K neighbors
3. `neighbor_weights='distance'`: Closer neighbors weighted more heavily
4. `neighbor_weights='uniform'`: All K neighbors weighted equally

**Parameters:**
- `column`: Name of column to impute
- `n_neighbors`: Number of similar records to use (default: 5)
- `neighbor_weights`: 'distance' or 'uniform' (default: 'distance')

**Example use cases:**
- Missing trip mode when other trip attributes are known
- Missing person age when household/demographic info is available
- Missing trip distance when other spatial/temporal features exist

### MICE (Multiple Imputation by Chained Equations)

**Best for:** Multiple correlated columns with missing values

**How it works:**
1. Initialize missing values with simple imputation (mean/mode)
2. For each column with missing values:
   - Treat it as the target variable
   - Use other columns as predictors in a regression model
   - Predict and update missing values
3. Repeat iteratively until convergence (max_iter rounds)
4. Particularly effective when variables are correlated (e.g., depart_time, arrive_time, duration)

**Parameters:**
- `columns`: List of column names to impute together
- `max_iter`: Maximum number of imputation rounds (default: 10)

**Example use cases:**
- Missing time fields (depart_hour, arrive_hour, duration) - highly correlated
- Missing spatial coordinates (origin_lat, origin_lon) - spatially correlated
- Missing sociodemographic variables (income, education, employment) - often correlated

### Random Forest

**Best for:** Single columns with complex non-linear relationships or mixed feature types

**How it works:**
1. Split rows into known (have value) and missing (need imputation)
2. Train a Random Forest model on the known rows using all features
3. Automatically selects `RandomForestClassifier` for categorical targets (integer/string)
   or `RandomForestRegressor` for continuous targets (float)
4. Predict missing values using the trained model
5. Handles NaN in features by filling with column medians

**Parameters:**
- `column`: Name of column to impute
- `n_estimators`: Number of trees in the forest (default: 100)
- `max_depth`: Maximum tree depth (default: None = unlimited)

**Example use cases:**
- Missing education level when employment, occupation, and age are available
- Missing income category with many mixed-type predictors
- Cases where KNN struggles with non-linear decision boundaries

## Diagnostic Flags

When `create_flags: true` (default), the module creates boolean columns tracking imputed values:
- `{column}_imputed`: True if the value in {column} was imputed, False otherwise
- Example: `mode_imputed`, `distance_imputed`, `age_imputed`

**Use cases:**
- Quality control: identify records with imputed values
- Sensitivity analysis: compare results with/without imputed records
- Downstream modeling: include imputation status as a feature

## Validation (Optional)

Optional k-fold cross-validation assesses imputation quality by:
1. Sampling X% of **non-missing** values (user-specified, e.g., 5%)
2. Artificially masking these values
3. Imputing them using k-fold cross-validation
4. Comparing imputed vs. actual values
5. Computing and logging metrics

**Metrics by data type:**
- **Categorical columns** (e.g., mode, purpose): Accuracy, Precision, Recall, F1-Score
- **Continuous columns** (e.g., distance, duration): RMSE, MAE, R²

**Configuration:**
```yaml
# Global random_state applies to both imputation and validation
random_state: 42

validate_imputation:
  enabled: true
  n_folds: 5           # Number of CV folds (default: 5)
  sample_pct: 5.0      # % of complete values to test (default: 5%)
```

**Example output:**
```
============================================================
Imputation Validation Results
============================================================

Column: mode (categorical, n=250 test samples)
  Accuracy:  0.876
  Precision: 0.883
  Recall:    0.876
  F1-Score:  0.872

Column: distance (continuous, n=250 test samples)
  RMSE: 2.34
  MAE:  1.82
  R²:   0.721
============================================================
```

**Note:** Validation adds computational overhead. Recommended for development/testing, optional for production pipelines.

## Technical Details

### Data Type Handling
- **Numeric columns**: Imputed directly using KNN/RF/MICE
- **Categorical integer columns** (e.g., enum codes 1-6): Automatically encoded to dense 0..N codes before imputation, decoded back to original codes after. This ensures non-contiguous codes (e.g., 1, 2, 3, 995, 999) don't distort distance calculations.
- **Categorical string columns** (e.g., `"Hispanic"`, `"White"`): Automatically encoded to integer codes for MICE, decoded back to original labels after imputation. No manual pre-processing required.

### Feature Selection
- Features are explicitly configured per imputation block using `numeric_features` and `categorical_features`
- `numeric_features`: Used as-is (continuous values — e.g., `num_trips`, `num_vehicles`)
- `categorical_features`: One-hot encoded into binary columns for distance/regression calculations
- Cross-table features can be pulled in via `join_tables` (parent→child) or `aggregate_from` (child→parent)
- **Tip**: Use `numeric_features` for ordinal/count variables and `categorical_features` for unordered enums. Putting high-cardinality integers (e.g., raw age) in `categorical_features` causes feature explosion and slow performance.

### Missing Data Assumptions
- **KNN**: Assumes similar records (based on other features) have similar missing values
- **MICE**: Assumes Missing At Random (MAR) - missingness may depend on observed values but not on the missing value itself
- If data is Missing Not At Random (MNAR), results may be biased

### Performance Considerations
- KNN: O(n log n) complexity, scales well to medium-large datasets
- Random Forest: Trains on known values only; handles mixed types well but can be memory-intensive with many trees
- MICE: Iterative, can be slow for many columns or large datasets
- Validation: Adds computational overhead (k-fold = k times the imputation time)

## Integration with Pipeline

The imputation step integrates seamlessly with the pipeline:
- Uses `@step()` decorator for automatic validation
- Accepts any canonical tables as input
- Returns modified tables with same names
- Validation controlled by config: `validate: true/false`

**Typical pipeline position:**
```yaml
steps:
  - name: load_data
  - name: custom_cleaning
  - name: imputation        # ← After cleaning, before linking
  - name: link_trips
  - name: joint_trips
  - name: extract_tours
```

## Limitations and Future Enhancements

**Current limitations:**
- No stratified imputation (uses all records as donor pool; no `group_by` option)
- No support for exogenous data sources (PUMS, land use data)
- One-hot encoding of high-cardinality categoricals can cause feature explosion and slow MICE convergence (move ordinal/count variables to `numeric_features` to mitigate)

### Potential Enhancements

- **Stratified imputation**: Fit separate models within groups (e.g., by `hh_id`) to enforce within-group consistency (e.g., siblings share race/ethnicity). Would add a `group_by` option to config blocks.
- **Hierarchical cross-validation**: Hold out entire groups (households/persons) during validation instead of individual records, for more realistic quality assessment.
- **Survey weight integration**: Weight the donor pool by survey expansion weights during KNN/RF/MICE.
- **Multiple imputation**: Generate multiple imputed datasets for uncertainty quantification.
- **Custom donor pools**: Restrict imputation to specific subsets (e.g., same region or time period).
- **Exogenous data sources**: Incorporate external data (PUMS, land use) as additional features.
- **Hot-deck methods**: Alternative imputation approaches for specific use cases.
- **Parallel processing**: Speed up validation and large-table imputation.

## Dependencies

- `scikit-learn>=1.5.0`: Core imputation algorithms (KNNImputer, IterativeImputer, RandomForest)
- `polars`: DataFrame operations
- `numpy`: Numerical operations

## References

- van Buuren, S., & Groothuis-Oudshoorn, K. (2011). mice: Multivariate Imputation by Chained Equations in R. *Journal of Statistical Software*, 45(3), 1-67.
- Troyanskaya, O., et al. (2001). Missing value estimation methods for DNA microarrays. *Bioinformatics*, 17(6), 520-525.
