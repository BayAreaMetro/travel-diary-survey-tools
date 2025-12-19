"""Configuration model for CT-RAMP formatting."""

from pydantic import BaseModel, Field


class CTRAMPConfig(BaseModel):
    """Configuration for CT-RAMP formatting.

    Attributes:
        income_low_threshold: Income threshold for low income bracket
        income_med_threshold: Income threshold for medium income bracket
        income_high_threshold: Income threshold for high income bracket
        drop_missing_taz: If True, remove households without valid TAZ IDs
    """

    income_low_threshold: int = Field(
        ge=0,
        description=(
            "Income threshold for low income bracket (in $2000 units). "
            "Household income below this threshold is classified as 'work_low'."
            " Default: 30 ($60k annual income)"
        ),
    )

    income_med_threshold: int = Field(
        ge=0,
        description=(
            "Income threshold for medium income bracket (in $2000 units). "
            "Household income below this threshold (but above low) is "
            "classified as 'work_med'. "
            "Default: 75 ($150k annual income)"
        ),
    )

    income_high_threshold: int = Field(
        ge=0,
        description=(
            "Income threshold for high income bracket (in $2000 units). "
            "Household income below this threshold (but above med) is "
            "classified as 'work_high'. Above this is 'work_very high'. "
            "Default: 120 ($240k annual income)"
        ),
    )

    drop_missing_taz: bool = Field(
        default=True,
        description="If True, remove households without valid TAZ IDs",
    )

    class Config:
        """Pydantic configuration."""

        extra = "forbid"  # Reject unknown fields
