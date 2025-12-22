
# CT-RAMP Formatting

This module transforms canonical survey data into CT-RAMP (Coordinated Travel - Regional Activity Modeling Platform) format for use with activity-based travel demand models.

## Current Implementation

The module provides full formatting for:

- **Households** (`format_households.py`):
   - Income conversion to $2000 midpoint values (configurable)
   - TAZ and walk-to-transit subzone mapping
   - Vehicle counts (human-driven and autonomous)
   - Excludes random number fields (simulation-specific)

- **Persons** (`format_persons.py`):
   - Person type classification based on age, employment, and student status
   - Gender mapping to m/f format
   - Free parking eligibility from commute subsidies
   - Value of time calculation
   - Activity pattern, tour frequency, and work-from-home fields are derived from tour data if provided; otherwise, sensible placeholders are used

- **Mandatory Locations** (`format_mandatory_location.py`):
   - Work and school location records for eligible persons

- **Individual Tours** (`format_tours.py`):
   - Formats all non-joint tours with purpose, mode, time-of-day, and destination fields

- **Joint Tours** (`format_tours.py`):
   - Detects and formats tours taken by multiple household members, including composition and participant list

- **Individual Trips** (`format_trips.py`):
   - Formats trips for individual tours, including stop-level details

- **Joint Trips** (`format_trips.py`):
   - Formats trips for joint tours

All outputs conform to the CT-RAMP model specifications. See [src/data_canon/models/ctramp.py](../../data_canon/models/ctramp.py) for data model details.

## Usage

```python
from processing.formatting.ctramp import format_ctramp

result = format_ctramp(
      persons=canonical_persons,
      households=canonical_households,
      linked_trips=canonical_linked_trips,
      tours=canonical_tours,
      joint_trips=canonical_joint_trips,
      income_low_threshold=60000,         # Example: $60k
      income_med_threshold=150000,        # Example: $150k
      income_high_threshold=250000,       # Example: $250k
      income_base_year_dollars=2000,      # Example: $2000 base year
      income_under_minimum=5000,          # Example: $5k for under-minimum
      drop_missing_taz=True
)

households_ctramp = result["households_ctramp"]
persons_ctramp = result["persons_ctramp"]
mandatory_location_ctramp = result["mandatory_location_ctramp"]
individual_tour_ctramp = result["individual_tour_ctramp"]
joint_tour_ctramp = result["joint_tour_ctramp"]
individual_trip_ctramp = result["individual_trip_ctramp"]
joint_trip_ctramp = result["joint_trip_ctramp"]
```

### Configuration Options

The following configuration parameters are required by `format_ctramp`:

- `income_low_threshold`, `income_med_threshold`, `income_high_threshold`: Dollar thresholds for income brackets
- `income_base_year_dollars`: Base year for income values
- `income_under_minimum`: Value for "under minimum" income categories
- `drop_missing_taz`: If True, households without valid TAZ are dropped

## Field Decisions


### Excluded Fields

**Random Number Fields**: The following fields are excluded as they are simulation-specific and not meaningful for survey data:
- `ao_rn`, `fp_rn`, `cdap_rn`, `imtf_rn`, `imtod_rn`, `immc_rn`, `jtf_rn`, `jtl_rn`, `jtod_rn`, `jmc_rn`, `inmtf_rn`, `inmtl_rn`, `inmtod_rn`, `inmmc_rn`, `awf_rn`, `awl_rn`, `awtod_rn`, `awmc_rn`, `stf_rn`, `stl_rn`

**Other Excluded Fields**:
- `auto_suff`: Incorrectly coded per CT-RAMP documentation

### Placeholder Fields

If tour data is not provided, the following fields are set to placeholder values:
- `activity_pattern`: 'H' (home)
- `imf_choice`: 0 (no mandatory tours)
- `inmf_choice`: 1 (minimum valid)
- `wfh_choice`: 0 (no work from home)
- `jtf_choice`: -4 (joint tour analysis not available)


## Data Models Reference

See [src/data_canon/models/ctramp.py](../../data_canon/models/ctramp.py) for complete field definitions:
- `HouseholdCTRAMPModel`
- `PersonCTRAMPModel`
- `MandatoryLocationCTRAMPModel`
- `IndividualTourCTRAMPModel`
- `JointTourCTRAMPModel`
- `IndividualTripCTRAMPModel`
- `JointTripCTRAMPModel`

## See Also

- [DaySim formatting](../daysim/README.md) - Similar formatting for DaySim model
- [CT-RAMP codebook](../../data_canon/codebook/ctramp.py) - Enumeration definitions
- [CT-RAMP models](../../data_canon/models/ctramp.py) - Data model specifications
