# Codebook Enumerations

This directory contains automatically generated codebook enumerations for all travel diary survey tables.

## Structure

Each table has its own Python module with `LabeledEnum` classes for each coded variable:

- `hh.py` - Household table codebooks
- `person.py` - Person table codebooks  
- `day.py` - Day table codebooks
- `trip.py` - Trip table codebooks
- `vehicle.py` - Vehicle table codebooks

## Usage

```python
from travel_diary_survey_tools.data.codebook import person, hh

# Access enum values
age = person.Age.VALUE_25_TO_34
print(f"Value: {age.value}")  # Output: Value: 5
print(f"Label: {age.label}")  # Output: Label: 25 to 34

# Iterate over all values
for income in hh.IncomeBroad:
    print(f"{income.name}: {income.label}")

# Lookup by value
income = hh.IncomeBroad.from_value(3)
print(income.label)  # Output: $50,000-$74,999

# Lookup by label
age = person.Age.from_label("Under 5")
print(age.value)  # Output: 1

# Get field name for pydantic model mapping
age_enum = person.Age.VALUE_25_TO_34
print(age_enum.field_name)  # Output: age
print(age_enum.label)  # Output: 25 to 34

# Create data dict for pydantic model
data = {age_enum.field_name: age_enum.value}  # {'age': 5}
```

## LabeledEnum Base Class

All codebook enumerations inherit from `LabeledEnum`, which provides:

- **`.value`** - The integer or string code value
- **`.label`** - The human-readable description
- **`.field_name`** - The canonical field name for mapping to pydantic models
- **`.from_value(value)`** - Class method to lookup by value
- **`.from_label(label)`** - Class method to lookup by label
- **`.get_field_name()`** - Class method to get the canonical field name
- **`__int__()`** - Convert to integer value
- **`__str__()`** - Convert to label string

## Example Enumerations

### Person Age
```python
class Age(LabeledEnum):
    """age value labels."""
    UNDER_5 = (1, "Under 5")
    VALUE_5_TO_15 = (2, "5 to 15")
    VALUE_16_TO_17 = (3, "16 to 17")
    # ... etc
```

### Household Income
```python
class IncomeBroad(LabeledEnum):
    """income_broad value labels."""
    UNDER_DOLLAR_25000 = (1, "Under $25,000")
    DOLLAR_25000_DOLLAR_49999 = (2, "$25,000-$49,999")
    # ... etc
```

## Regenerating Codebooks

If the value labels CSV is updated, regenerate the codebooks by running the generation script from the repository root:

```python
# See generate_codebooks.py in the repository root for the generation script
```

## Source Data

All codebooks are generated from `value_labels (1).csv`, which contains the canonical mapping of:
- **table** - The table name (hh, person, day, trip, vehicle)
- **variable** - The column/variable name
- **value** - The numeric or string code
- **label** - The human-readable label

# Data Codebook

This codebook provides detailed documentation for all fields in the canonical travel survey data tables.

Generated automatically from Pydantic model definitions.

## Table: `households`

**Model:** `HouseholdModel`
**Description:** Household attributes (minimal for tour building).

| Field | Type | Constraints | Required In Steps | Created In Step | Default |
| --- | --- | --- | --- | --- | --- |
| `hh_id` | int | ≥ 1 | All steps |  | PydanticUndefined |
| `home_lat` | float | ≥ -90, ≤ 90 | `final_check` |  | PydanticUndefined |
| `home_lon` | float | ≥ -180, ≤ 180 | `final_check` |  | PydanticUndefined |

## Table: `persons`

**Model:** `PersonModel`
**Description:** Person attributes for tour building.

| Field | Type | Constraints | Required In Steps | Created In Step | Default |
| --- | --- | --- | --- | --- | --- |
| `person_id` | int | ≥ 1 | All steps |  | PydanticUndefined |
| `hh_id` | int | ≥ 1 | All steps |  | PydanticUndefined |
| `age` | int | None | ≥ 0 | `final_check` |  |  |
| `work_lat` | float | None | ≥ -90, ≤ 90 | `final_check` |  |  |
| `work_lon` | float | None | ≥ -180, ≤ 180 | `final_check` |  |  |
| `school_lat` | float | None | ≥ -90, ≤ 90 | `final_check` |  |  |
| `school_lon` | float | None | ≥ -180, ≤ 180 | `final_check` |  |  |
| `person_type` | int | ≥ 1 | `final_check` |  |  |

## Table: `days`

**Model:** `PersonDayModel`
**Description:** Daily activity pattern summary with clear purpose-specific counts.

| Field | Type | Constraints | Required In Steps | Created In Step | Default |
| --- | --- | --- | --- | --- | --- |
| `person_id` | int | ≥ 1 | All steps |  | PydanticUndefined |
| `day_id` | int | ≥ 1 | All steps |  | PydanticUndefined |
| `hh_id` | int | ≥ 1 | All steps |  | PydanticUndefined |
| `travel_dow` | int | ≥ 1, ≤ 7 | `final_check` |  | PydanticUndefined |

## Table: `unlinked_trips`

**Model:** `UnlinkedTripModel`
**Description:** Trip data model for validation.

| Field | Type | Constraints | Required In Steps | Created In Step | Default |
| --- | --- | --- | --- | --- | --- |
| `trip_id` | int | ≥ 1 | All steps |  | PydanticUndefined |
| `day_id` | int | ≥ 1 | All steps |  | PydanticUndefined |
| `person_id` | int | ≥ 1 | All steps |  | PydanticUndefined |
| `hh_id` | int | ≥ 1 | All steps |  | PydanticUndefined |
| `linked_trip_id` | int | None | ≥ 1 | `extract_tours`, `final_check` |  |  |
| `tour_id` | int | None | ≥ 1 | `extract_tours`, `final_check` |  |  |
| `depart_date` | str |  |  |  | PydanticUndefined |
| `depart_hour` | int | ≥ 0, ≤ 23 | `final_check` |  | PydanticUndefined |
| `depart_minute` | int | ≥ 0, ≤ 59 | `final_check` |  | PydanticUndefined |
| `depart_seconds` | int | ≥ 0, ≤ 59 | `final_check` |  | PydanticUndefined |
| `arrive_date` | str |  |  |  | PydanticUndefined |
| `arrive_hour` | int | ≥ 0, ≤ 23 | `final_check` |  | PydanticUndefined |
| `arrive_minute` | int | ≥ 0, ≤ 59 | `final_check` |  | PydanticUndefined |
| `arrive_seconds` | int | ≥ 0, ≤ 59 | `final_check` |  | PydanticUndefined |
| `o_purpose_category` | int |  |  |  | PydanticUndefined |
| `d_purpose_category` | int |  |  |  | PydanticUndefined |
| `mode_type` | int |  |  |  | PydanticUndefined |
| `duration_minutes` | float | ≥ 0 | `final_check` |  | PydanticUndefined |
| `distance_miles` | float | ≥ 0 | `final_check` |  | PydanticUndefined |
| `depart_time` | datetime.datetime | None |  | `link_trip`, `final_check` |  |  |
| `arrive_time` | datetime.datetime | None |  | `link_trip`, `final_check` |  |  |

## Table: `linked_trips`

**Model:** `LinkedTripModel`
**Description:** Linked Trip data model for validation.

| Field | Type | Constraints | Required In Steps | Created In Step | Default |
| --- | --- | --- | --- | --- | --- |
| `day_id` | int | ≥ 1 |  | `link_trip` | PydanticUndefined |
| `person_id` | int | ≥ 1 |  | `link_trip` | PydanticUndefined |
| `hh_id` | int | ≥ 1 |  | `link_trip` | PydanticUndefined |
| `linked_trip_id` | int | None | ≥ 1 |  | `link_trip` |  |
| `tour_id` | int | None | ≥ 1 |  | `extract_tours` |  |
| `depart_date` | str |  |  |  | PydanticUndefined |
| `depart_hour` | int | ≥ 0, ≤ 23 |  | `link_trip` | PydanticUndefined |
| `depart_minute` | int | ≥ 0, ≤ 59 |  | `link_trip` | PydanticUndefined |
| `depart_seconds` | int | ≥ 0, ≤ 59 |  | `link_trip` | PydanticUndefined |
| `arrive_date` | str |  |  | `link_trip` | PydanticUndefined |
| `arrive_hour` | int | ≥ 0, ≤ 23 |  | `link_trip` | PydanticUndefined |
| `arrive_minute` | int | ≥ 0, ≤ 59 |  | `link_trip` | PydanticUndefined |
| `arrive_seconds` | int | ≥ 0, ≤ 59 |  | `link_trip` | PydanticUndefined |
| `o_purpose_category` | int |  |  | `link_trip` | PydanticUndefined |
| `d_purpose_category` | int |  |  | `link_trip` | PydanticUndefined |
| `mode_type` | int |  |  | `link_trip` | PydanticUndefined |
| `duration_minutes` | float | ≥ 0 |  | `link_trip` | PydanticUndefined |
| `distance_miles` | float | ≥ 0 |  | `link_trip` | PydanticUndefined |
| `depart_time` | datetime.datetime | None |  |  | `link_trip` |  |
| `arrive_time` | datetime.datetime | None |  |  | `link_trip` |  |
| `is_primary_dest_trip` | bool | None |  |  | `extract_tours` |  |

## Table: `tours`

**Model:** `TourModel`
**Description:** Tour-level records with clear, descriptive step_field names.

| Field | Type | Constraints | Required In Steps | Created In Step | Default |
| --- | --- | --- | --- | --- | --- |
| `tour_id` | int | ≥ 1 |  | `extract_tours` | PydanticUndefined |
| `person_id` | int | ≥ 1 |  | `extract_tours` | PydanticUndefined |
| `day_id` | int | ≥ 1 |  | `extract_tours` | PydanticUndefined |
| `tour_sequence_num` | int | ≥ 1 | `final_check` |  | PydanticUndefined |
| `tour_category` | str |  |  |  | PydanticUndefined |
| `parent_tour_id` | int | None | ≥ 1 |  | `extract_tours` |  |
| `primary_purpose` | int | ≥ 1 |  | `extract_tours` | PydanticUndefined |
| `primary_dest_purpose` | int | ≥ 1 |  | `extract_tours` | PydanticUndefined |
| `purpose_priority` | int | ≥ 1 |  | `extract_tours` | PydanticUndefined |
| `origin_depart_time` | datetime |  |  | `extract_tours` | PydanticUndefined |
| `dest_arrive_time` | datetime |  |  | `extract_tours` | PydanticUndefined |
| `dest_depart_time` | datetime |  |  | `extract_tours` | PydanticUndefined |
| `origin_arrive_time` | datetime |  |  | `extract_tours` | PydanticUndefined |
| `o_lat` | float | ≥ -90, ≤ 90 |  | `extract_tours` | PydanticUndefined |
| `o_lon` | float | ≥ -180, ≤ 180 |  | `extract_tours` | PydanticUndefined |
| `d_lat` | float | ≥ -90, ≤ 90 |  | `extract_tours` | PydanticUndefined |
| `d_lon` | float | ≥ -180, ≤ 180 |  | `extract_tours` | PydanticUndefined |
| `o_location_type` | str |  |  | `extract_tours` | PydanticUndefined |
| `d_location_type` | str |  |  | `extract_tours` | PydanticUndefined |
| `tour_mode` | int | ≥ 1 |  | `extract_tours` | PydanticUndefined |
| `outbound_mode` | int | ≥ 1 |  | `extract_tours` | PydanticUndefined |
| `inbound_mode` | int | ≥ 1 |  | `extract_tours` | PydanticUndefined |
| `num_outbound_stops` | int | ≥ 0 |  | `extract_tours` | PydanticUndefined |
| `num_inbound_stops` | int | ≥ 0 |  | `extract_tours` | PydanticUndefined |
| `is_primary_tour` | bool |  |  | `extract_tours` | PydanticUndefined |
| `tour_starts_at_origin` | bool |  |  | `extract_tours` | PydanticUndefined |
| `tour_ends_at_origin` | bool |  |  | `extract_tours` | PydanticUndefined |
