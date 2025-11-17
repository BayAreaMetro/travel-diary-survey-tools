"""Decorators for pipeline steps with automatic validation."""
import functools
import inspect
import logging
from collections.abc import Callable
from typing import Any

import polars as pl

from data_canon import CanonicalData

logger = logging.getLogger(__name__)

# Canonical table names that can be validated
CANONICAL_TABLES = set(CanonicalData.__annotations__.keys())


def step(
    *,
    validate_input: bool = True,
    validate_output: bool = False,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator for pipeline steps with automatic validation.

    This decorator validates canonical data inputs and/or outputs using
    the Pydantic models defined in data.models. Only parameters/returns
    that match canonical table names (households, persons, days,
    unlinked_trips, linked_trips, tours) are validated.

    Validation is skipped for tables that have already been validated
    if a CanonicalData instance is passed as 'canonical_data' parameter.

    The default value is to only validate inputs to avoid duplicate validation.
    Recommend putting a final step full_check step at the end of the pipeline
    to validate all tables after all processing is complete.

    Args:
        validate_input: If True, validate input DataFrames that match
            canonical table names
        validate_output: If True, validate output DataFrames that match
            canonical table names (for dict returns) or the single
            DataFrame if return type matches a canonical table name

    Example:
        >>> @step(validate_input=True, validate_output=True)
        ... def link_trips(
        ...     unlinked_trips: pl.DataFrame,
        ...     config: dict
        ... ) -> dict[str, pl.DataFrame]:
        ...     # Process trips
        ...     linked_trips = ...
        ...     return {"linked_trips": linked_trips}

        >>> @step(validate_output=True)
        ... def load_data(input_paths: dict) -> dict[str, pl.DataFrame]:
        ...     return {
        ...         "households": households_df,
        ...         "persons": persons_df,
        ...         ...
        ...     }

    Returns:
        Decorated function with validation
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
            # Extract and remove canonical_data from kwargs
            canonical_data = kwargs.pop("canonical_data", None)

            if validate_input:
                _validate_inputs(func, args, kwargs, canonical_data)

            result = func(*args, **kwargs)

            # Update canonical_data with results if available
            if canonical_data and isinstance(result, dict):
                for key, value in result.items():
                    if _is_canonical_dataframe(key, value):
                        setattr(canonical_data, key, value)

            # Validate outputs if requested
            if validate_output and isinstance(result, dict):
                _validate_dict_outputs(result, func.__name__, canonical_data)
            elif validate_output and isinstance(result, tuple):
                logger.warning(
                    "Step '%s' returns tuple - cannot auto-validate. "
                    "Consider returning dict with table names as keys.",
                    func.__name__,
                )

            return result

        return wrapper  # type: ignore[return-value]

    return decorator


def _validate_inputs(
    func: Callable,
    args: tuple,
    kwargs: dict,
    canonical_data: CanonicalData | None = None,
) -> None:
    """Validate input parameters that are canonical DataFrames."""
    sig = inspect.signature(func)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()

    # Use provided instance or check if one exists in kwargs
    validator = canonical_data
    if validator is None and "canonical_data" in bound.arguments:
        validator = bound.arguments["canonical_data"]

    for param_name, param_value in bound.arguments.items():
        if not _is_canonical_dataframe(param_name, param_value):
            continue

        logger.info(
            "Validating input '%s' for step '%s'",
            param_name,
            func.__name__,
        )
        # Use validator instance if available, otherwise create temporary
        step_name = func.__name__
        if validator:
            setattr(validator, param_name, param_value)
            validator.validate(param_name, step=step_name)
        else:
            temp_validator = CanonicalData()
            setattr(temp_validator, param_name, param_value)
            temp_validator.validate(param_name, step=step_name)


def _validate_dict_outputs(
    result: dict,
    func_name: str,
    canonical_data: CanonicalData | None = None,
) -> None:
    """Validate outputs in dict format."""
    for key, value in result.items():
        if not _is_canonical_dataframe(key, value):
            continue

        logger.info(
            "Validating output '%s' from step '%s'",
            key,
            func_name,
        )
        # Validate using canonical_data instance or create temporary
        if canonical_data:
            # Data already updated by wrapper, just validate
            canonical_data.validate(key, step=func_name)
        else:
            # No canonical_data instance, validate with temporary
            temp_validator = CanonicalData()
            setattr(temp_validator, key, value)
            temp_validator.validate(key, step=func_name)


def _is_canonical_dataframe(name: str, value: Any) -> bool:  # noqa: ANN401
    """Check if a value is a DataFrame for a canonical table."""
    return name in CANONICAL_TABLES and isinstance(value, pl.DataFrame)

