[← Back to Main README](../../../README.md)

# Joint Trips Pipeline Steps

This module detects joint trips where multiple household members travel together by identifying trips with similar spatial and temporal characteristics.

## Pipeline Steps

### `detect_joint_trips`

Identifies shared household trips using similarity matching based on origin-destination-time patterns.

**Inputs:**
- `linked_trips`: Journey records with coordinates and timing (pl.DataFrame)
  - Required columns: linked_trip_id, hh_id, person_id, o/d coordinates, depart/arrive times
- `households`: Household table for pre-filtering (pl.DataFrame)
- `method`: Detection method - `"buffer"` or `"mahalanobis"` (default: "buffer")
- `time_threshold_minutes`: Max time difference for buffer method (default: 15.0)
- `space_threshold_meters`: Max spatial distance for buffer method (default: 100.0)
- `covariance`: Covariance matrix for mahalanobis method (optional)
- `confidence_level`: Statistical confidence level for mahalanobis (default: 0.90)
- `log_discrepancies`: Whether to log trips with reported vs detected traveler mismatches (default: False)

**Outputs:**
- Dictionary containing:
  - `linked_trips`: Original trips with added `joint_trip_id` column
  - `joint_trips`: Aggregated table of shared trips with participant lists

**Core Algorithm:**

**Phase 1: Household Pre-filtering**
1. Filter to households with 2+ members who took trips
2. Reduces search space to only households where joint trips are possible

**Phase 2: Pairwise Distance Calculation**
1. Within each multi-person household:
2. Compute pairwise distances between all trip combinations using 4D space:
   - Origin coordinates (o_lon, o_lat)
   - Destination coordinates (d_lon, d_lat)
   - Departure time
   - Arrival time
3. Store distances in condensed matrix format for efficiency

**Phase 3: Similarity Filtering**

**Buffer Method (default):**
- Filter trip pairs where:
  - Spatial distance (haversine) ≤ `space_threshold_meters` for both origin AND destination
  - Absolute time difference ≤ `time_threshold_minutes` for both departure AND arrival
- Simple, interpretable thresholds

**Mahalanobis Method:**
- Calculate statistical distance using covariance matrix:
  - Accounts for correlated variations in space/time
  - Compares to chi-squared distribution at `confidence_level`
- More sophisticated, calibrated to actual joint trip patterns and can more flexibly capture joint trips than a fixed threshold (e.g., 3 of 4 metrics are tight matches but one is *slightly* outside threshold, buffer would miss but Mahalanobis may capture)

**Phase 4: Clique Detection**
1. Build graph where nodes = trips, edges = similar trip pairs
2. Detect maximal cliques (groups of mutually-similar trips)
3. Handle overlapping cliques by selecting disjoint set with maximum coverage
4. Each clique represents one joint trip event

**Phase 5: Joint Trip Aggregation**
1. Assign unique `joint_trip_id` to each clique
2. Create joint_trips table with:
   - Representative location/time (mean or mode of participants)
   - `person_list`: Array of participating person IDs
   - `trip_list`: Array of individual linked_trip_ids
   - `num_participants`: Count of travelers
3. Validate against reported `num_travelers` field if available

**Notes:**
- Only compares trips within same household (joint trips across households not detected)
- Mahalanobis method requires calibrated covariance matrix (see `calibrate_joint_trip_covariance.py`)
- Clique detection ensures transitivity: if A travels with B, and B with C, then A,B,C form one joint trip
- Handles survey reporting errors where respondents over/under-report number of travelers
- Non-joint trips retain `joint_trip_id = NULL`
