"""Utility for adding optional fields in test data builders.

Expects raw values, not enums. Callers must use .value on enums.
"""


def add_optional_fields_batch(record: dict, **fields) -> None:
    """Add fields to record only if they are not None.

    Modifies record in-place. Expects raw values, not enums.

    Args:
        record: Dictionary to add fields to (modified in-place)
        **fields: Keyword arguments of field_name=value pairs (raw values,
            not enums)

    Example:
        record = {"person_id": 101}
        add_optional_fields_batch(
            record,
            work_lat=37.75,
            work_lon=None,  # Not added
            work_taz=200
        )
        # record is now: {"person_id": 101, "work_lat": 37.75,
        # "work_taz": 200}
    """
    record.update({k: v for k, v in fields.items() if v is not None})
