"""Diagnostics sub-package — HTML report generation for weighting results."""

from .charts import crosswalk_figure
from .report import generate_report

__all__ = ["crosswalk_figure", "generate_report"]
