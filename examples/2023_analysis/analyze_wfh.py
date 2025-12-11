"""Summarize work from home rates."""
# ruff: noqa: PLR2004

import logging
from enum import IntEnum

import polars as pl

from data_canon.codebook.persons import Employment
from pipeline.decoration import step

logger = logging.getLogger(__name__)


# Custom enums for categorizing responses
class FrequencyCategory(IntEnum):
    """Ordered frequency categories for sorting."""

    FIVE_PLUS = 5
    FOUR = 4
    TWO_THREE = 3
    ONE = 2
    LESS_THAN_WEEKLY = 1
    NEVER = 0
    MISSING = -1

    def to_label(self) -> str:
        """Convert to display label."""
        return {
            5: "5+ days per week",
            4: "4 days per week",
            3: "2-3 days per week",
            2: "1 day per week",
            1: "Less than once a week",
            0: "Never",
            -1: "Missing Response",
        }[self.value]


class TeleworkRatio(IntEnum):
    """Ordered telework ratio categories for sorting."""

    ALWAYS = 3
    MORE_THAN_HALF = 2
    LESS_THAN_HALF = 1
    NEVER = 0
    MISSING = -1

    def to_label(self) -> str:
        """Convert to display label."""
        return {
            3: "Always",
            2: "More than half",
            1: "Less than half",
            0: "Never",
            -1: "Missing Response",
        }[self.value]


@step()
def summarize_wfh(
    persons: pl.DataFrame,
    person_weights: pl.DataFrame,
) -> pl.DataFrame:
    """Summarize work from home rates."""
    # Prepare person table
    persons_clean = (
        # Drop old weights, and join new weights
        persons.drop("person_weight")
        .join(
            person_weights.select("person_id", "person_weight"),
            on="person_id",
            how="left",
        )
        # Drop rows with missing weights (incomplete days etc.)
        .filter(
            pl.col("person_weight").is_not_null()
            & (pl.col("person_weight") > 0)
        )
    )

    # Check person sum for reasonableness
    person_weight_sum = persons_clean.select("person_weight").sum().item()
    msg = f"Sum of person weights is not close to 1: {person_weight_sum:.2f}"
    logger.info(msg)

    # Filter just employed persons
    employed_persons = persons_clean.filter(
        pl.col("employment").is_in(
            [
                Employment.EMPLOYED_FULLTIME.value,
                Employment.EMPLOYED_PARTTIME.value,
                Employment.EMPLOYED_SELF.value,
            ]
        )
    ).select(
        "person_id",
        # "age",
        "employment",
        "telework_freq",
        "commute_freq",
        "job_type",
        "person_weight",
    )

    # Employment code:
    # 1: Employed full-time (paid)
    # 2: Employed part-time (paid)
    # 3: Self-employed
    # 5: Not employed and not looking for work (e.g., retired, stay-at-home parent, student)  # noqa: E501
    # 6: Unemployed and looking for work
    # 7: Unpaid volunteer or intern
    # 8: Employed, but not currently working (e.g., on leave, furloughed 100%)
    # 995: Missing Response

    # Telework code:
    # 1: 6-7 days a week
    # 2: 5 days a week
    # 3: 4 days a week
    # 4: 3 days a week
    # 5: 2 days a week
    # 6: 1 day a week
    # 7: 1-3 days a month
    # 8: Less than monthly
    # 995: Missing Response
    # 996: Never

    # Job type code:
    # 1: One work location
    # 2: Multiple work locations
    # 3: WFH always
    # 4: Travel for work
    # 5: Hybrid
    # 995: Missing Response

    # Telework freq is only ask when job_type is [1, 2, 4, 5]
    # Commute freq is only ask when job_type is [1, 2, 5]

    # Sum of days - Bad combos > 7 days/week
    #             6    5    4    3    2    1    days
    #             1    2    3    4    5    6    enum
    # 6    1      12   11   10   9    8    7
    # 5    2      11   10   9    8    7    6
    # 4    3      10   9    8    7    6    5
    # 3    4      9    8    7    6    5    4
    # 2    5      8    7    6    5    4    3
    # 1    6      7    6    5    4    3    2
    # days enum

    # Sum of enums - Bad combos < 7
    #             6    5    4    3    2    1    days
    #             1    2    3    4    5    6    enum
    # 6    1      2    3    4    5    6    7
    # 5    2      3    4    5    6    7    8
    # 4    3      4    5    6    7    8    9
    # 3    4      5    6    7    8    9    10
    # 2    5      6    7    8    9    10   11
    # 1    6      7    8    9    10   11   12
    # days enum

    # Check for temporal violations:
    # Bad telework/commute freq combos where sum < 7 days/week
    # ignoring the less than weekly and never categories
    bad_freqs = employed_persons.filter(
        (pl.col("telework_freq") + pl.col("commute_freq") < 7)
        & (pl.col("telework_freq") < 8)
        & (pl.col("commute_freq") < 8)
    )

    # Simple definition of teleworking freq for only full time
    # Mirrored for commute freq using commute_freq instead of telework_freqs
    # - 5+ days per week:
    #      - telework_freq in [1, 2] OR job_type == 3 (WFH always)
    #      - commute_freq in [1, 2] OR job_type != 3
    # - 4 days per week:
    #      - telework_freq == 3
    #      - commute_freq == 3
    # - 2-3 days per week:
    #      - telework_freq in [4, 5]
    #      - commute_freq in [4, 5]
    # - 1 day per week:
    #      - telework_freq == 6
    #      - commute_freq == 6
    # - less than once a week:
    #      - telework_freq in [7, 8]
    #      - commute_freq in [7, 8]
    # - never:
    #      - telework_freq == 996
    #      - commute_freq == 996

    # Universal definition of teleworking ratio to account for part time
    # - Always:
    #      - job_type == 3 (WFH always) OR
    #        (telework_freq in [1, 2] AND commute_freq == 996 AND job_type != 3)
    # - More than half:
    #      - telework_freq < commute_freq AND job_type != 3 AND
    #        telework_freq != 995 AND commute_freq != 995
    # - Less than half:
    #      - telework_freq > commute_freq AND job_type != 3 AND
    #        telework_freq != 995 AND commute_freq != 995
    # - Never:
    #      - telework_freq == 996 AND commute_freq == 996 AND job_type != 3

    # Telework freq
    employed_persons = employed_persons.with_columns(
        # 5+ days per week
        pl.when(
            (pl.col("telework_freq").is_in([1, 2]))
            | (pl.col("job_type") == 3)  # WFH always
        )
        .then(pl.lit(FrequencyCategory.FIVE_PLUS.value))
        # 4 days per week
        .when(pl.col("telework_freq") == 3)
        .then(pl.lit(FrequencyCategory.FOUR.value))
        # 2-3 days per week
        .when(pl.col("telework_freq").is_in([4, 5]))
        .then(pl.lit(FrequencyCategory.TWO_THREE.value))
        # 1 day per week
        .when(pl.col("telework_freq") == 6)
        .then(pl.lit(FrequencyCategory.ONE.value))
        # Less than once a week
        .when(pl.col("telework_freq").is_in([7, 8]))
        .then(pl.lit(FrequencyCategory.LESS_THAN_WEEKLY.value))
        # Never
        .when(pl.col("telework_freq") == 996)
        .then(pl.lit(FrequencyCategory.NEVER.value))
        # Missing response
        .otherwise(pl.lit(FrequencyCategory.MISSING.value))
        .alias("wfh_cat")
    ).with_columns(
        pl.col("wfh_cat")
        .map_elements(
            lambda x: FrequencyCategory(x).to_label(), return_dtype=pl.String
        )
        .alias("wfh_str")
    )

    # Commute freq
    employed_persons = employed_persons.with_columns(
        # Commute 5+ days per week
        pl.when(
            (pl.col("commute_freq").is_in([1, 2])) & (pl.col("job_type") != 3)
        )
        .then(pl.lit(FrequencyCategory.FIVE_PLUS.value))
        # 4 days per week
        .when(pl.col("commute_freq") == 3)
        .then(pl.lit(FrequencyCategory.FOUR.value))
        # 2-3 days per week
        .when(pl.col("commute_freq").is_in([4, 5]))
        .then(pl.lit(FrequencyCategory.TWO_THREE.value))
        # 1 day per week
        .when(pl.col("commute_freq") == 6)
        .then(pl.lit(FrequencyCategory.ONE.value))
        # Less than once a week
        .when(pl.col("commute_freq").is_in([7, 8]))
        .then(pl.lit(FrequencyCategory.LESS_THAN_WEEKLY.value))
        # Never
        .when((pl.col("commute_freq") == 996) | (pl.col("job_type") == 3))
        .then(pl.lit(FrequencyCategory.NEVER.value))
        # Missing response
        .otherwise(pl.lit(FrequencyCategory.MISSING.value))
        .alias("commute_freq_cat")
    ).with_columns(
        pl.col("commute_freq_cat")
        .map_elements(
            lambda x: FrequencyCategory(x).to_label(), return_dtype=pl.String
        )
        .alias("commute_freq_str")
    )

    # Telework ratio
    employed_persons = employed_persons.with_columns(
        # Always WFH
        pl.when(
            (
                (pl.col("telework_freq").is_in([1, 2]))
                & (pl.col("commute_freq") == 996)
            )
            | (pl.col("job_type") == 3)
        )
        .then(pl.lit(TeleworkRatio.ALWAYS.value))
        # WFH at least half the time
        .when(
            (pl.col("telework_freq") <= pl.col("commute_freq"))
            & (pl.col("job_type") != 3)
            & (pl.col("telework_freq") != 995)
            & (pl.col("commute_freq") != 995)
        )
        .then(pl.lit(TeleworkRatio.MORE_THAN_HALF.value))
        # WFH less than half the time
        .when(
            (pl.col("telework_freq") > pl.col("commute_freq"))
            & (pl.col("job_type") != 3)
            & (pl.col("telework_freq") != 995)
            & (pl.col("commute_freq") != 995)
        )
        .then(pl.lit(TeleworkRatio.LESS_THAN_HALF.value))
        # Never WFH
        .when(
            (pl.col("telework_freq") == 996)
            & (pl.col("commute_freq") == 996)
            & (pl.col("job_type") != 3)
        )
        .then(pl.lit(TeleworkRatio.NEVER.value))
        # Missing response
        .otherwise(pl.lit(TeleworkRatio.MISSING.value))
        .alias("telework_ratio_cat")
    ).with_columns(
        pl.col("telework_ratio_cat")
        .map_elements(
            lambda x: TeleworkRatio(x).to_label(), return_dtype=pl.String
        )
        .alias("telework_ratio_str")
    )

    # Tally up the counts
    telework_freq_summary = (
        employed_persons.group_by("wfh_str", "wfh_cat")
        .agg(
            pl.len().alias("count"),
            pl.col("person_weight").sum().alias("weighted_count"),
        )
        # Calculate the percentage of each category
        .with_columns(
            (
                100 * pl.col("weighted_count") / pl.col("weighted_count").sum()
            ).alias("% (wtd)"),
            (100 * pl.col("count") / pl.col("count").sum()).alias("% (unwtd)"),
        )
        .sort("wfh_cat")
    )
    commute_freq_summary = (
        employed_persons.group_by("commute_freq_str", "commute_freq_cat")
        .agg(
            pl.len().alias("count"),
            pl.col("person_weight").sum().alias("weighted_count"),
        )
        # Calculate the percentage of each category
        .with_columns(
            (
                100 * pl.col("weighted_count") / pl.col("weighted_count").sum()
            ).alias("% (wtd)"),
            (100 * pl.col("count") / pl.col("count").sum()).alias("% (unwtd)"),
        )
        .sort("commute_freq_cat")
    )
    telework_ratio_summary = (
        employed_persons.group_by("telework_ratio_str", "telework_ratio_cat")
        .agg(
            pl.len().alias("count"),
            pl.col("person_weight").sum().alias("weighted_count"),
        )
        # Calculate the percentage of each category
        .with_columns(
            (
                100 * pl.col("weighted_count") / pl.col("weighted_count").sum()
            ).alias("% (wtd)"),
            (100 * pl.col("count") / pl.col("count").sum()).alias("% (unwtd)"),
        )
        .sort("telework_ratio_cat")
    )
    bad_freqs_summary = pl.DataFrame(
        {
            "Measure": ["Count", "Weighted count", "% (wtd)", "% (unwtd)"],
            "Value": [
                bad_freqs.shape[0],
                bad_freqs["person_weight"].sum(),
                round(
                    100
                    * bad_freqs["person_weight"].sum()
                    / employed_persons["person_weight"].sum(),
                    2,
                ),
                round(100 * bad_freqs.shape[0] / employed_persons.shape[0], 2),
            ],
        },
        strict=False,
    )

    # Round all floats to 2 decimal places
    telework_freq_summary = telework_freq_summary.with_columns(
        pl.col("weighted_count").round(2).alias("weighted_count"),
        pl.col("% (wtd)").round(2).alias("% (wtd)"),
        pl.col("% (unwtd)").round(2).alias("% (unwtd)"),
    ).drop("wfh_cat")
    commute_freq_summary = commute_freq_summary.with_columns(
        pl.col("weighted_count").round(2).alias("weighted_count"),
        pl.col("% (wtd)").round(2).alias("% (wtd)"),
        pl.col("% (unwtd)").round(2).alias("% (unwtd)"),
    ).drop("commute_freq_cat")
    telework_ratio_summary = telework_ratio_summary.with_columns(
        pl.col("weighted_count").round(2).alias("weighted_count"),
        pl.col("% (wtd)").round(2).alias("% (wtd)"),
        pl.col("% (unwtd)").round(2).alias("% (unwtd)"),
    ).drop("telework_ratio_cat")

    # Print out the summaries
    logger.info("Telework frequency summary:")
    logger.info(telework_freq_summary)
    logger.info("Commute frequency summary:")
    logger.info(commute_freq_summary)
    logger.info("Telework ratio summary:")
    logger.info(telework_ratio_summary)
    logger.info("Temporal violations summary:")
    logger.info(bad_freqs_summary)

    return persons
