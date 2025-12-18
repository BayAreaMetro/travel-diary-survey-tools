# CT-RAMP Formatting

This module transforms canonical survey data into CT-RAMP (Coordinated Travel - Regional Activity Modeling Platform) format for use with activity-based travel demand models.

## Current Implementation

The current implementation includes formatting for:

### Households (`format_households.py`)
- Income conversion to $2000 midpoint values
- TAZ and walk-to-transit subzone mapping
- Vehicle counts (human-driven and autonomous)
- **Excludes**: Random number fields (ao_rn, fp_rn, cdap_rn, etc.) as they are simulation-specific

### Persons (`format_persons.py`)
- Person type classification based on age, employment, and student status
- Gender mapping to m/f format
- Free parking eligibility from commute subsidies
- Value of time calculation
- **Placeholder values** for:
  - `activity_pattern`: Set to 'H' (home)
  - `imf_choice`: Set to 0 (no mandatory tours)
  - `inmf_choice`: Set to 1 (minimum valid)
  - `wfh_choice`: Set to 0 (no work from home)

## Future Work Required

### Joint Tours
CT-RAMP has a fundamentally different tour structure than DaySim:
- **Joint tours**: Tours taken by multiple household members together
- **Tour composition**: Adults only, children only, or mixed
- Requires modifications to the tour extraction algorithm to identify and link joint tours

### Individual Tours (`IndividualTourCTRAMPModel`)
Will need to create `format_individual_tours.py` to handle:
- Tour purpose (work, school, escort, shop, maint, eat, visit, discr, atwork)
- Tour mode and category
- Outbound/inbound stops
- Time-of-day fields (start/end times)
- Destination TAZ and subzone
- Linking tours to persons

### Joint Tours (`JointTourCTRAMPModel`)
Will need to create `format_joint_tours.py` to handle:
- Identifying tours taken by multiple household members
- Tour composition (adults/children/mixed)
- Participant list
- Similar tour attributes as individual tours

### Trips/Stops
CT-RAMP refers to intermediate stops rather than individual trips:
- **Stops**: Intermediate destinations within a tour
- Will need to aggregate/transform linked trips into stops
- Stop purpose, location, time-of-day
- Mode to/from stop

### Tour Extraction Algorithm Changes

The current `extract_tours` algorithm needs modifications:
1. **Joint tour detection**: Identify when multiple household members travel together
   - Same origin/destination
   - Overlapping time windows
   - Same mode (potentially)
2. **Tour linking**: Link persons to joint tours
3. **Stop extraction**: Different from trip extraction
   - Stops are tour-relative, not independent trips
4. **Activity pattern derivation**: Determine M/N/H patterns from tours
5. **Tour frequency calculation**: Count mandatory/non-mandatory tours per person

## Implementation Strategy

### Phase 1: Complete (Households & Persons)
- [x] Basic household and person formatting
- [x] Person type classification
- [x] Income and demographic mapping

### Phase 2: Individual Tours (Future)
- [ ] Modify tour extraction to identify individual vs joint tours
- [ ] Create `format_individual_tours.py`
- [ ] Derive activity patterns from tours
- [ ] Calculate tour frequencies (imf_choice, inmf_choice)
- [ ] Map tour purposes and modes

### Phase 3: Joint Tours (Future)
- [ ] Implement joint tour detection logic
- [ ] Create `format_joint_tours.py`
- [ ] Handle tour composition classification
- [ ] Link multiple persons to joint tours

### Phase 4: Stops/Trips (Future)
- [ ] Transform linked trips to stops
- [ ] Create stop location and timing fields
- [ ] Handle stop purpose mapping

## Field Decisions

### Excluded Fields

**Random Number Fields**: The following fields are excluded as they are simulation-specific and not meaningful for survey data:
- `ao_rn`: Auto ownership model
- `fp_rn`: Free parking model
- `cdap_rn`: Coordinated daily activity pattern
- `imtf_rn`, `imtod_rn`, `immc_rn`: Individual mandatory tour frequency/TOD/mode
- `jtf_rn`, `jtl_rn`, `jtod_rn`, `jmc_rn`: Joint tour frequency/location/TOD/mode
- `inmtf_rn`, `inmtl_rn`, `inmtod_rn`, `inmmc_rn`: Individual non-mandatory
- `awf_rn`, `awl_rn`, `awtod_rn`, `awmc_rn`: At-work subtour
- `stf_rn`, `stl_rn`: Stop frequency/location

**Other Excluded Fields**:
- `auto_suff`: Incorrectly coded per CT-RAMP documentation

### Placeholder Fields

These fields require tour data and are currently set to placeholder values:
- `activity_pattern`: Requires analyzing tour purposes → currently 'H' (home)
- `imf_choice`: Requires counting mandatory tours → currently 0
- `inmf_choice`: Requires counting non-mandatory tours → currently 1
- `wfh_choice`: Requires analyzing work tours → currently 0
- `jtf_choice`: Requires joint tour analysis → currently -4

## Data Models Reference

See [src/data_canon/models/ctramp.py](../../data_canon/models/ctramp.py) for complete field definitions:
- `HouseholdCTRAMPModel`
- `PersonCTRAMPModel`
- `MandatoryLocationCTRAMPModel`
- `IndividualTourCTRAMPModel`
- `JointTourCTRAMPModel`
- `IndividualTripCTRAMPModel`
- `JointTripCTRAMPModel`

## Usage

```python
from processing.formatting.ctramp import format_ctramp

result = format_ctramp(
    persons=canonical_persons,
    households=canonical_households,
    drop_missing_taz=True
)

households_ctramp = result["households_ctramp"]
persons_ctramp = result["persons_ctramp"]
```

## See Also

- [DaySim formatting](../daysim/README.md) - Similar formatting for DaySim model
- [CT-RAMP codebook](../../data_canon/codebook/ctramp.py) - Enumeration definitions
- [CT-RAMP models](../../data_canon/models/ctramp.py) - Data model specifications
