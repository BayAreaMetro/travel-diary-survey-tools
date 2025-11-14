# Data Validation Framework

A comprehensive, layered validation framework for travel survey data that ensures data quality through multiple validation stages.

## Overview

The validation framework provides 5 layers of validation:

1. **Column Constraints** - Uniqueness checks on key columns
2. **Foreign Key Constraints** - Relational integrity between tables
3. **Row Validation** - Pydantic model validation for types and business rules
4. **Custom Validators** - User-defined validation logic
5. **Required Children** - Bidirectional FK checks ensuring parents have children

## Quick Start

```python
from travel_diary_survey_tools.data.dataclass import CanonicalData
import polars as pl

# Create canonical data structure
data = CanonicalData()

# Load your data
data.households = pl.read_csv("households.csv")
data.persons = pl.read_csv("persons.csv")

# Validate tables
data.validate("households")
data.validate("persons")

# Check validation status
# This avoids redundant re-validation between pipeline steps
# Status is returned to false when data is modified
if data.is_validated("households"):
    print("Households validated successfully!")
```

## Validation Layers

### 1. Column Constraints

Automatically checks uniqueness on primary key columns.

**Built-in constraints:**
- `households`: `hh_id` must be unique
- `persons`: `person_id` must be unique
- `days`: `day_id` must be unique
- `unlinked_trips`: `trip_id` must be unique
- `linked_trips`: `linked_trip_id` must be unique
- `tours`: `tour_id` must be unique

**Example:**
```python
# This will pass
data.households = pl.DataFrame({
    "hh_id": [1, 2, 3],  # All unique
    "home_taz": [100, 200, 300],
    "income": [50000, 75000, 100000],
    "hh_size": [2, 3, 4],
    "num_vehicles": [1, 2, 2],
})
data.validate("households")  # ✓ Success

# This will fail
data.households = pl.DataFrame({
    "hh_id": [1, 2, 2],  # Duplicate ID!
    "home_taz": [100, 200, 300],
    # ...
})
data.validate("households")  # ✗ ValidationError: Duplicate hh_id values
```

See [examples/validation_unique.py](examples/validation_unique.py) for full example.

### 2. Foreign Key Constraints

Ensures referential integrity between related tables.

**Built-in FK relationships:**
- `persons.hh_id` → `households.hh_id`
- `days.person_id` → `persons.person_id`
- `days.hh_id` → `households.hh_id`
- `unlinked_trips.person_id` → `persons.person_id`
- `unlinked_trips.day_id` → `days.day_id`
- `linked_trips.person_id` → `persons.person_id`
- `linked_trips.tour_id` → `tours.tour_id`
- `tours.person_id` → `persons.person_id`

**Example:**
```python
data.households = pl.DataFrame({
    "hh_id": [1, 2, 3],
    # ...
})

# This will pass
data.persons = pl.DataFrame({
    "person_id": [101, 102, 103],
    "hh_id": [1, 2, 3],  # All reference valid households
    # ...
})
data.validate("persons")  # ✓ Success

# This will fail
data.persons = pl.DataFrame({
    "person_id": [101, 102, 103],
    "hh_id": [1, 2, 999],  # 999 doesn't exist!
    # ...
})
data.validate("persons")  # ✗ ValidationError: Orphaned FK
```

**Graceful handling:**
- Skips validation if parent table is `None`
- Skips validation if FK column doesn't exist yet (for forward references)

See [examples/validation_foreign_keys.py](examples/validation_foreign_keys.py) for full example.

### 3. Row Validation

Uses Pydantic models to validate data types, enums, and business logic for each row.

**Built-in models:**
- `HouseholdModel`: Income, household size, vehicle counts
- `PersonModel`: Age, gender, worker/student status
- `DayModel`: Travel date, day of week, trip counts
- `UnlinkedTripModel`: Trip purpose, mode, times
- `LinkedTripModel`: Includes origin/destination locations
- `TourModel`: Tour purpose, structure, complexity

**Example:**
```python
# This will pass
data.persons = pl.DataFrame({
    "person_id": [101, 102],
    "hh_id": [1, 2],
    "age": [35, 42],  # Valid ages
    "gender": ["male", "female"],  # Valid enum values
    "worker": [True, True],  # Boolean
    "student": [False, False],
})
data.validate("persons")  # ✓ Success

# This will fail
data.persons = pl.DataFrame({
    "person_id": [101, 102],
    "hh_id": [1, 2],
    "age": [-5, 200],  # Invalid ages
    "gender": ["male", "alien"],  # Invalid enum
    "worker": [True, "maybe"],  # Wrong type
    "student": [False, False],
})
data.validate("persons")  # ✗ ValidationError: Type/enum violations
```

See [examples/validation_row.py](examples/validation_row.py) for full example.

### 4. Custom Validators

User-defined validation functions for business logic that spans rows or tables.

**How to add custom checks:**

1. Define your check function in `checks.py`:
```python
# src/travel_diary_survey_tools/data/checks.py
def check_arrival_after_departure(unlinked_trips: pl.DataFrame) -> list[str]:
    """Ensure arrive_time is after depart_time for all trips."""
    errors = []
    bad_trips = unlinked_trips.filter(
        pl.col("arrive_time") < pl.col("depart_time")
    )
    if len(bad_trips) > 0:
        trip_ids = bad_trips["trip_id"].to_list()[:5]
        errors.append(
            f"Found {len(bad_trips)} trips where arrive_time < depart_time. "
            f"Sample trip IDs: {trip_ids}"
        )
    return errors
```

2. Register it in the `CUSTOM_VALIDATORS` dictionary at the top of `checks.py`:
```python
# src/travel_diary_survey_tools/data/checks.py (at the top)
CUSTOM_VALIDATORS = {
    "unlinked_trips": [check_arrival_after_departure],
    "linked_trips": [],
}
```

3. The check automatically runs when validating that table:
```python
data.validate("unlinked_trips")  # Runs check_arrival_after_departure
```

**Alternative: Runtime registration with decorator:**
```python
@data.register_validator("households")
def check_income_reasonable(households: pl.DataFrame) -> list[str]:
    """Dynamic registration at runtime."""
    errors = []
    # validation logic...
    return errors
```

Note: Using `CUSTOM_VALIDATORS` in `checks.py` is preferred for maintainability
```

**Multi-table validator:**
```python
@data.register_validator("persons")
def check_household_size_consistency(
    persons: pl.DataFrame,
    households: pl.DataFrame,
) -> list[str]:
    """Check that hh_size matches actual person count."""
    errors = []
    
    actual_sizes = persons.group_by("hh_id").agg(pl.len().alias("actual"))
    merged = households.join(actual_sizes, on="hh_id", how="left")
    mismatches = merged.filter(pl.col("hh_size") != pl.col("actual"))
    
    if len(mismatches) > 0:
        ids = mismatches["hh_id"].to_list()
        errors.append(
            f"Household size mismatch for hh_ids: {ids[:5]}"
            f"{' ...' if len(ids) > 5 else ''}"
        )
    
    return errors

data.validate("persons")  # Uses both persons and households
```

**Spatial/sequence validators:**
```python
@data.register_validator("tours")
def check_no_teleportation(
    tours: pl.DataFrame,
    linked_trips: pl.DataFrame,
) -> list[str]:
    """Check that trips in a tour don't teleport unreasonably."""
    errors = []
    
    for tour_id in tours["tour_id"]:
        tour_trips = (
            linked_trips
            .filter(pl.col("tour_id") == tour_id)
            .sort("trip_num")
        )
        
        for i in range(len(tour_trips) - 1):
            dest = tour_trips[i]["d_taz"]
            next_orig = tour_trips[i + 1]["o_taz"]
            
            if dest != next_orig:
                errors.append(
                    f"Tour {tour_id}: Trip {i+1} destination ({dest}) "
                    f"!= Trip {i+2} origin ({next_orig})"
                )
    
    return errors
```

**Register on multiple tables:**
```python
@data.register_validator("households", "persons", "days")
def check_no_nulls_in_keys(*args, **kwargs) -> list[str]:
    """Check that no primary/foreign keys have nulls."""
    # Validator runs when any of the three tables are validated
    return []
```

See [examples/validation_custom.py](examples/validation_custom.py) for full examples.

### 5. Required Children (Bidirectional FK)

Ensures parent records have required child records.

**Built-in requirements:**
- Every `household` must have at least one `person`
- Every `person` must have at least one `day`

**Example:**
```python
data.households = pl.DataFrame({
    "hh_id": [1, 2, 3],
    # ...
})

# This will pass
data.persons = pl.DataFrame({
    "person_id": [101, 102, 103],
    "hh_id": [1, 2, 3],  # All households have a person
    # ...
})
data.validate("households")  # ✓ Success

# This will fail
data.persons = pl.DataFrame({
    "person_id": [101, 102],
    "hh_id": [1, 2],  # hh_id=3 has no persons!
    # ...
})
data.validate("households")  # ✗ ValidationError: Missing required children
```

See [examples/validation_required_children.py](examples/validation_required_children.py) for full example.

## Validation Status Tracking

The framework automatically tracks which tables have been validated and resets validation status when tables are modified.

```python
# Initial state: nothing validated
data = CanonicalData()
print(data.get_validation_status())
# {'households': False, 'persons': False, ...}

# Load and validate
data.households = pl.read_csv("households.csv")
data.validate("households")
print(data.is_validated("households"))  # True

# Modify table - validation status resets automatically
data.households = data.households.with_columns(
    pl.col("income").mul(1.1)
)
print(data.is_validated("households"))  # False (needs revalidation)

# Revalidate
data.validate("households")
print(data.is_validated("households"))  # True
```

## Pipeline Integration

The validation framework integrates seamlessly with the pipeline decorator:

```python
from travel_diary_survey_tools.pipeline import step

@step(
    inputs=["households", "persons"],
    outputs=["persons_enriched"],
    validate_inputs=True,  # Validate before processing
    validate_outputs=True,  # Validate after processing
)
def enrich_persons(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    **kwargs,
) -> dict[str, pl.DataFrame]:
    # Inputs automatically validated before this runs
    persons_enriched = persons.join(
        households.select(["hh_id", "income"]),
        on="hh_id",
    )
    # Outputs automatically validated after return
    return {"persons_enriched": persons_enriched}
```

## Configuration

### Adding Custom Constraints

Extend the default configurations in your `CanonicalData` instance:

```python
data = CanonicalData()

# Add uniqueness constraint on multiple columns
data._unique_constraints["persons"].append("email")

# Add new FK relationship
data._foreign_keys["my_custom_table"] = ["persons", "households"]

# Add required children relationship
data._required_children["days"] = [("unlinked_trips", "day_id")]
```

### Modifying FK Column Mapping

If you have custom table names, update the FK column mapping:

```python
from travel_diary_survey_tools.data.validation import _TABLE_TO_FK_COLUMN

_TABLE_TO_FK_COLUMN["my_custom_table"] = "custom_id"
```

## Error Handling

All validation errors raise `ValidationError` with structured information:

```python
from travel_diary_survey_tools.data.validation import ValidationError

try:
    data.validate("households")
except ValidationError as e:
    print(f"Table: {e.table}")        # Which table failed
    print(f"Rule: {e.rule}")          # Which validation rule
    print(f"Message: {e.message}")    # Error details
    print(f"Column: {e.column}")      # Column involved (if applicable)
    print(f"Row ID: {e.row_id}")      # Row identifier (if applicable)
```

## Best Practices

1. **Validate early and often** - Run validation after each transformation step
2. **Use custom validators** for business logic specific to your domain
3. **Leverage validation status** - Check `is_validated()` to avoid redundant validation
4. **Register validators at module level** - Define validators once, use everywhere
5. **Return empty list for success** - Custom validators should return `[]` when passing
6. **Provide informative messages** - Include context and sample data in error messages
7. **Use multi-table validators** - Check cross-table consistency where needed

## Examples

Complete working examples are available in the `examples/` directory:

- [validation_unique.py](examples/validation_unique.py) - Uniqueness constraints
- [validation_foreign_keys.py](examples/validation_foreign_keys.py) - FK constraints
- [validation_row.py](examples/validation_row.py) - Pydantic row validation
- [validation_custom.py](examples/validation_custom.py) - Custom validators
- [validation_required_children.py](examples/validation_required_children.py) - Bidirectional FK
- [validation_complete.py](examples/validation_complete.py) - Full workflow

## Testing

Run the validation test suite:

```bash
pytest tests/test_validation.py -v
```

See [test_validation.py](../../../../tests/test_validation.py) for comprehensive test examples.
