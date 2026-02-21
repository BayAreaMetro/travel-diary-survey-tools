[← Back to Main README](../../../README.md)

# Imputation Module

This module provides data imputation capabilities for handling missing values in travel diary survey data using established statistical methods.

## Overview

Missing data is common in travel surveys (e.g., missing income, race, ethnicity, etc.). This module provides:
- **KNN Imputation**: Single-column imputation using K-Nearest Neighbors similarity matching
- **MICE Imputation**: Multi-column imputation using Multiple Imputation by Chained Equations (handles correlated variables)
- **Diagnostic Tracking**: Optional flag columns to track which values were imputed
- **Quality Validation**: Optional k-fold cross-validation to assess imputation accuracy

## Pipeline Step

### `imputation`

Imputes missing values across any canonical data tables (households, persons, days, unlinked_trips, linked_trips, tours).

**Inputs:**
- Any canonical tables (pl.DataFrame, optional)
- `knn_columns`: Configuration for KNN imputation by table (dict, optional)
- `mice_groups`: Configuration for MICE imputation by table (dict, optional)
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
      # KNN: impute single columns using similar records
      knn_columns:
        households:
          - column: income_broad
            missing_values: [MISSING, PNTA]  # Enum labels to treat as missing
            n_neighbors: 5
            neighbor_weights: distance

        unlinked_trips:
          - column: mode
            n_neighbors: 5
            neighbor_weights: distance  # or 'uniform'
          - column: distance
            n_neighbors: 10
            neighbor_weights: uniform

        persons:
          - column: age
            n_neighbors: 5
            neighbor_weights: distance

      # MICE: impute correlated column groups together
      mice_groups:
        persons:
          - columns: [race, ethnicity]
            missing_values:
              race: [MISSING]
              ethnicity: [MISSING, PNTA]
            max_iter: 10

        unlinked_trips:
          - columns: [depart_hour, arrive_hour, duration]
            max_iter: 10
          - columns: [origin_lat, origin_lon]
            max_iter: 10

      # Global settings
      random_state: 42          # Random seed for reproducibility
      create_flags: true        # Create diagnostic columns (default: true)

      # Optional: k-fold validation to assess quality
      validate_imputation:
        enabled: true
        n_folds: 5           # Number of cross-validation folds
        sample_pct: 5.0      # % of non-missing values to test
```

## Handling Missing Values with Enum Labels

Survey data often uses special codes for missing values (e.g., 995 for "Missing Response", 999 for "Prefer not to answer"). The imputation module automatically resolves enum labels to their numeric values and replaces them with nulls before imputation.

**Specifying Missing Values:**

Use enum member names (labels) rather than numeric values in the config:

```yaml
knn_columns:
  households:
    - column: income_broad
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
mice_groups:
  persons:
    - columns: [race, ethnicity]
      missing_values:
        race: [MISSING]            # Only MISSING for race
        ethnicity: [MISSING, PNTA]  # Both MISSING and PNTA for ethnicity
      max_iter: 10
```

Or use a single list to apply the same missing values to all columns:

```yaml
mice_groups:
  persons:
    - columns: [race, ethnicity]
      missing_values: [MISSING, PNTA]  # Applied to all columns
      max_iter: 10
```

**Enum Resolution:**

The module automatically:
- Maps the table name to the appropriate codebook module (e.g., `households` → `data_canon.codebook.households`)
- Finds the enum class with matching `canonical_field_name` (e.g., `income_broad` → `IncomeBroad`)
- Resolves enum member names to their values (e.g., `MISSING` → 995)
- Replaces those values with null in the DataFrame



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
- `random_state`: Random seed for reproducibility (default: None)

**Example use cases:**
- Missing time fields (depart_hour, arrive_hour, duration) - highly correlated
- Missing spatial coordinates (origin_lat, origin_lon) - spatially correlated
- Missing sociodemographic variables (income, education, employment) - often correlated

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
- **Numeric columns**: Imputed directly using KNN/MICE
- **Categorical columns** (integer codes): Treated as numeric, rounded after imputation
- **String columns**: Not yet supported (convert to numeric codes first)

### Feature Selection
- KNN/MICE use **all numeric columns** in the table as features
- More features generally improve imputation quality
- Consider pre-processing to ensure relevant features are numeric

### Missing Data Assumptions
- **KNN**: Assumes similar records (based on other features) have similar missing values
- **MICE**: Assumes Missing At Random (MAR) - missingness may depend on observed values but not on the missing value itself
- If data is Missing Not At Random (MNAR), results may be biased

### Performance Considerations
- KNN: O(n log n) complexity, scales well to medium-large datasets
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
- No hierarchical imputation (tables imputed independently)
- No cross-table feature joins
- No stratified imputation (uses all records as donor pool)
- No support for exogenous data sources (PUMS, land use data)
- Categorical string columns must be pre-converted to numeric codes

### Hierarchical Survey Imputation

A more advanced imputation mode that respects the hierarchical structure of survey data (Households → Persons → Days → Trips) and leverages cross-table relationships.

**Use case:** Demographics fields like income, race, and ethnicity have strong household-level correlation. For example:
- Household income relates to person income, number of workers, dwelling type
- Children's race/ethnicity should match parents within same household
- Trip mode choice influenced by person demographics and household characteristics

**Proposed approach:**

```yaml
mice_groups:
  persons:
    - columns: [race, ethnicity, income]
      mode: hierarchical          # Opt-in hierarchical mode
      join_features:              # User-specified parent features
        households: [household_income, dwelling_type, num_workers]
      group_by: household_id      # Stratify imputation by household
      max_iter: 10

  unlinked_trips:
    - columns: [mode, purpose]
      mode: hierarchical
      join_features:
        persons: [age, has_license, income]
        households: [household_income, num_vehicles]
      group_by: person_id         # Stratify by person
      max_iter: 10
```

**Key features:**
1. **Cross-table feature joins**: Automatically join parent table columns as additional predictors for MICE models (uses canonical foreign key relationships)
2. **Stratified imputation**: Fit separate models within groups (e.g., by household_id) to maintain within-group consistency
3. **Hierarchical cross-validation**: Hold out entire groups (households/persons) during validation, not individual records
4. **Integrity validation**: Fail fast if orphaned records detected (e.g., person without household_id)

**Design decisions:**
- **Opt-in**: `mode: hierarchical` required; default `mode: standard` behaves like current implementation
- **User-specified joins**: Explicit `join_features` configuration prevents unexpected feature explosion
- **Configurable grouping**: User controls stratification level (`group_by: household_id`, `person_id`, `day_id`, or none)
- **Referential integrity**: Check foreign key constraints before imputation, error on violations

**Benefits:**
- Preserves household/person consistency (siblings get similar demographics)
- Leverages known relationships for better imputation quality
- More realistic validation (tests ability to impute entire households/persons)
- Aligns with survey statistical best practices

**Limitations:**
- More complex configuration
- Higher memory usage (joined features)
- Requires clean referential integrity in data
- May be slower for large datasets with many joins

### Other Potential Enhancements

- **Survey weight integration**: Optionally weight donor pool by survey weights during KNN/MICE
- **Model-based imputation**: Regression, random forest, or other ML approaches
- **Multiple imputation**: Create multiple imputed datasets for uncertainty quantification
- **String/categorical support**: Native handling of string columns (currently numeric only)
- **Custom donor pools**: Restrict imputation to specific subsets (e.g., only impute from same region/time period)
- **Exogenous data sources**: Incorporate external data (PUMS, land use) for imputation
- **Hot-deck methods**: Alternative imputation approaches for specific use cases
- **Parallel processing**: Speed up validation and large-table imputation

## Dependencies

- `scikit-learn>=1.5.0`: Core imputation algorithms (KNNImputer, IterativeImputer)
- `polars`: DataFrame operations
- `numpy`: Numerical operations

## References

- van Buuren, S., & Groothuis-Oudshoorn, K. (2011). mice: Multivariate Imputation by Chained Equations in R. *Journal of Statistical Software*, 45(3), 1-67.
- Troyanskaya, O., et al. (2001). Missing value estimation methods for DNA microarrays. *Bioinformatics*, 17(6), 520-525.
