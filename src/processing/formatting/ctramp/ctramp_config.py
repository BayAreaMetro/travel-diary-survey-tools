"""Configuration model for CT-RAMP formatting."""

from pydantic import BaseModel, ConfigDict, Field, model_validator


class CTRAMPConfig(BaseModel):
    """Configuration for CT-RAMP formatting.

    Attributes:
        income_low_threshold: Income threshold for low income bracket
        income_med_threshold: Income threshold for medium income bracket
        income_high_threshold: Income threshold for high income bracket
        drop_missing_taz: If True, remove households without valid TAZ IDs
        age_adult: Age threshold for adult vs child in joint tour composition
        income_base_year_dollars: Base year for income dollar units
        gender_default_for_missing: Default gender for missing/non-binary
    """

    income_low_threshold: int = Field(
        ge=0,
        description=(
            "Income threshold for low income bracket (in dollars). "
            "Income below this is classified as 'work_low'. "
            "Example: 60000 for $60k annual income"
        ),
    )

    income_med_threshold: int = Field(
        ge=0,
        description=(
            "Income threshold for medium income bracket (in dollars). "
            "Household income below this threshold (but above low) is "
            "classified as 'work_med'. "
            "Example: 150000 for $150k annual income"
        ),
    )

    income_high_threshold: int = Field(
        ge=0,
        description=(
            "Income threshold for high income bracket (in dollars). "
            "Household income below this threshold (but above med) is "
            "classified as 'work_high'. Above this is 'work_very high'. "
            "Example: 240000 for $240k annual income"
        ),
    )

    drop_missing_taz: bool = Field(
        default=True,
        description="If True, remove households without valid TAZ IDs",
    )

    # Age thresholds
    age_adult: int = Field(
        default=4,  # AgeCategory.AGE_18_TO_24.value
        ge=3,  # AgeCategory.AGE_16_TO_17.value minimum
        le=5,  # AgeCategory.AGE_25_TO_34.value maximum
        description=(
            "Age threshold for adult vs child in joint tours. "
            "Should be an AgeCategory enum value. Default: 4 (AGE_18_TO_24)"
        ),
    )

    # Economic parameters
    income_base_year_dollars: int = Field(
        ge=1000,
        description=(
            "Base year for income dollar units (e.g., 2023 for $2023). "
            "Used for converting income to CT-RAMP format."
        ),
    )

    # Gender handling
    gender_default_for_missing: str = Field(
        default="f",
        pattern="^[mf]$",
        description=(
            "Default gender ('m' or 'f') for missing/non-binary values "
            "when CT-RAMP requires binary gender. Default: 'f'"
        ),
    )

    default_joint_tour_travelers: int = Field(
        default=2,
        ge=1,
        description=(
            "Default number of travelers for joint tours when num_travelers "
            "is missing. Default: 2"
        ),
    )

    @model_validator(mode="after")
    def validate_income_ordering(self) -> "CTRAMPConfig":
        """Validate that income thresholds are in proper order."""
        if self.income_low_threshold >= self.income_med_threshold:
            msg = (
                f"income_low_threshold ({self.income_low_threshold})"
                "must be less than "
                f"income_med_threshold ({self.income_med_threshold})"
            )
            raise ValueError(msg)

        if self.income_med_threshold >= self.income_high_threshold:
            msg = (
                f"income_med_threshold ({self.income_med_threshold}) "
                "must be less than "
                f"income_high_threshold ({self.income_high_threshold})"
            )
            raise ValueError(msg)

        return self

    model_config = ConfigDict(extra="forbid")
