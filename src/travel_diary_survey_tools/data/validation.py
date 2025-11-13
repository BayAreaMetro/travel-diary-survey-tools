from polars import pl
from pydantic import BaseModel
from .models import (
    PersonModel,
    HouseholdModel,
    TripModel,
    LinkedTripModel,
    TourModel,
)

# DataFrame Validation Functions ----------------------------------------------
def validate_dataframe(
    df: pl.DataFrame,
    model: type[BaseModel],
    sample_size: int | None = None,
) -> pl.DataFrame:
    """Validate a Polars DataFrame using a Pydantic model.

    Args:
        df: DataFrame to validate
        model: Pydantic model class to validate against
        sample_size: If provided, only validate a sample of rows (for performance)

    Returns:
        The original DataFrame (unchanged)

    Raises:
        ValidationError: If any row fails validation

    Example:
        >>> persons_df = pl.read_csv("persons.csv")
        >>> validate_dataframe(persons_df, PersonModel, sample_size=100)
    """
    rows_to_check = df.sample(n=sample_size) if sample_size else df
    
    for i, row in enumerate(rows_to_check.iter_rows(named=True)):
        try:
            model(**row)
        except Exception as e:
            row_idx = i if not sample_size else "sampled"
            msg = f"Validation failed at row {row_idx}: {e}"
            raise ValueError(msg) from e
    
    return df


# Convenience validation functions
def validate_persons(df: pl.DataFrame, sample_size: int | None = None) -> pl.DataFrame:
    """Validate persons DataFrame."""
    return validate_dataframe(df, PersonModel, sample_size)


def validate_households(df: pl.DataFrame, sample_size: int | None = None) -> pl.DataFrame:
    """Validate households DataFrame."""
    return validate_dataframe(df, HouseholdModel, sample_size)


def validate_trips(df: pl.DataFrame, sample_size: int | None = None) -> pl.DataFrame:
    """Validate trips DataFrame."""
    return validate_dataframe(df, TripModel, sample_size)


def validate_linked_trips(df: pl.DataFrame, sample_size: int | None = None) -> pl.DataFrame:
    """Validate linked trips DataFrame."""
    return validate_dataframe(df, LinkedTripModel, sample_size)


def validate_tours(df: pl.DataFrame, sample_size: int | None = None) -> pl.DataFrame:
    """Validate tours DataFrame."""
    return validate_dataframe(df, TourModel, sample_size)
