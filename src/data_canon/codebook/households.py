"""Codebook enumerations for hh table."""

from data_canon.core.labeled_enum import LabeledEnum


class BicycleType(LabeledEnum):
    """bicycle_type value labels."""

    canonical_field_name = "bicycle_type"

    STANDARD = (1, "Standard")
    ELECTRIC = (2, "Electric")
    OTHER = (3, "Other")
    MISSING = (995, "Missing Response")


class HomeInRegion(LabeledEnum):
    """home_in_region value labels."""

    canonical_field_name = "home_in_region"

    NO = (0, "No")
    YES = (1, "Yes")


class IncomeDetailed(LabeledEnum):
    """income_detailed value labels."""

    canonical_field_name = "income_detailed"

    INCOME_UNDER15 = (1, "Under $15,000")
    INCOME_15TO25 = (2, "$15,000-$24,999")
    INCOME_25TO35 = (3, "$25,000-$34,999")
    INCOME_35TO50 = (4, "$35,000-$49,999")
    INCOME_50TO75 = (5, "$50,000-$74,999")
    INCOME_75TO100 = (6, "$75,000-$99,999")
    INCOME_100TO150 = (7, "$100,000-$149,999")
    INCOME_150TO200 = (8, "$150,000-$199,999")
    INCOME_200TO250 = (9, "$200,000-$249,999")
    INCOME_250_OR_MORE = (10, "$250,000 or more")
    PNTA = (999, "Prefer not to answer")


class IncomeFollowup(LabeledEnum):
    """income_followup value labels."""

    canonical_field_name = "income_followup"

    INCOME_UNDER25 = (1, "Under $25,000")
    INCOME_25TO50 = (2, "$25,000-$49,999")
    INCOME_50TO75 = (3, "$50,000-$74,999")
    INCOME_75TO100 = (4, "$75,000-$99,999")
    INCOME_100TO200 = (5, "$100,000-$199,999")
    INCOME_200_OR_MORE = (6, "$200,000 or more")
    MISSING = (995, "Missing Response")
    PNTA = (999, "Prefer not to answer")


class IncomeBroad(LabeledEnum):
    """income_broad value labels."""

    canonical_field_name = "income_broad"

    INCOME_UNDER25 = (1, "Under $25,000")
    INCOME_25TO50 = (2, "$25,000-$49,999")
    INCOME_50TO75 = (3, "$50,000-$74,999")
    INCOME_75TO100 = (4, "$75,000-$99,999")
    INCOME_100TO200 = (5, "$100,000-$199,999")
    INCOME_200_OR_MORE = (6, "$200,000 or more")
    MISSING = (995, "Missing Response")
    PNTA = (999, "Prefer not to answer")


class ParticipationGroup(LabeledEnum):
    """participation_group value labels."""

    canonical_field_name = "participation_group"
    field_description = (
"Indicates the survey mode used for signup and diary completion"
    )

    SIGNUP_BMOVE_DIARY_BMOVE = (1, "Signup via browserMove, Diary via browserMove")  # noqa: E501
    SIGNUP_BMOVE_DIARY_CALL_CENTER = (2, "Signup via browserMove, Diary via call center")  # noqa: E501
    SIGNUP_BMOVE_DIARY_RMOVE = (3, "Signup via browserMove, Diary via rMove")
    SIGNUP_CALL_DIARY_BMOVE = (4, "Signup via call center, Diary via browserMove")  # noqa: E501
    SIGNUP_CALL_DIARY_CALL_CENTER = (5, "Signup via call center, Diary via call center")  # noqa: E501
    SIGNUP_CALL_DIARY_RMOVE = (6, "Signup via call center, Diary via rMove")
    SIGNUP_RMOVE_DIARY_BMOVE = (7, "Signup via rMove, Diary via browserMove")
    SIGNUP_RMOVE_DIARY_CALL_CENTER = (8, "Signup via rMove, Diary via call center")  # noqa: E501
    SIGNUP_RMOVE_DIARY_RMOVE = (9, "Signup via rMove, Diary via rMove")
