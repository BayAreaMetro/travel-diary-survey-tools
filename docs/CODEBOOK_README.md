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
    AGE_UNDER_5 = (1, "Under 5")
    AGE_5_TO_15 = (2, "5 to 15")
    AGE_16_TO_17 = (3, "16 to 17")
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
