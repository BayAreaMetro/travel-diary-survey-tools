"""Pipeline execution module for running data processing steps."""

import inspect
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml

from data_canon.core.dataclass import CanonicalData

logger = logging.getLogger(__name__)


class Pipeline:
    """Class to run a data processing pipeline based on a configuration file."""

    data: CanonicalData
    steps: dict[str, Callable]

    def __init__(
        self, config_path: str, processing_steps: list[Callable] | None = None
    ) -> None:
        """Initialize the Pipeline with configuration and custom steps.

        Args:
            config_path: Path to the YAML configuration.
            processing_steps: Optional list of processing step functions.
                steps.
            custom_steps: Mapping of step names to custom functions
                (overrides default steps), optional.
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.data = CanonicalData()
        self.steps = {func.__name__: func for func in processing_steps or []}

    def _load_config(self) -> dict[str, Any]:
        """Load the pipeline configuration from a YAML file.

        Replaces template variables in the format {{ variable_name }} with
        their corresponding values defined in the config.

        Returns:
            The configuration dictionary.
        """
        with Path(self.config_path).open() as f:
            config = yaml.safe_load(f)

        # Extract top-level variables for substitution
        variables = {
            key: value
            for key, value in config.items()
            if isinstance(value, str)
        }

        # Recursively replace template variables
        def replace_templates(obj: Any) -> Any:  # noqa: ANN401
            if isinstance(obj, str):
                # Replace {{ variable_name }} with actual values
                for var_name, var_value in variables.items():
                    obj = obj.replace(f"{{{{ {var_name} }}}}", str(var_value))
                return obj

            if isinstance(obj, dict):
                return {k: replace_templates(v) for k, v in obj.items()}

            if isinstance(obj, list):
                return [replace_templates(item) for item in obj]

            return obj

        return replace_templates(config)

    def parse_step_args(
        self, step_name: str, step_obj: Callable
    ) -> dict[str, Any]:
        """Separate the canonical data and parameters.

        If argument name matches a canonical table, it is passed from self.data.
        Else, it is taken from the step configuration "parameters".

        Args:
            step_name: Name of the step.
            step_obj: The step function or class.

        """
        step_args = inspect.signature(step_obj).parameters

        # if the arg name is a canonical table, pass it from self.data
        data_kwargs = {}
        config_kwargs = {}

        reserved = {
            "canonical_data",
            "validate_input",
            "validate_output",
            "kwargs",
        }
        expected_kwargs = [x for x in step_args if x not in reserved]

        for arg_name, param in step_args.items():
            if arg_name == "canonical_data":
                # Pass the entire CanonicalData instance if requested
                data_kwargs[arg_name] = self.data
            elif hasattr(self.data, arg_name):
                data_kwargs[arg_name] = getattr(self.data, arg_name)
            else:
                step_cfg = self.config["steps"]
                params = next(
                    (
                        s.get("params", {})
                        for s in step_cfg
                        if s["name"] == step_name
                    ),
                    {},
                )
                # Only add if parameter exists in config or has default
                if arg_name in params:
                    config_kwargs[arg_name] = params[arg_name]
                elif (
                    param.default is not inspect.Parameter.empty
                    or arg_name in reserved
                ):
                    # Has default value, don't need to provide it
                    pass
                else:
                    # If no default and not in config, omit it
                    # This will cause TypeError if it's required
                    msg = (
                        f"Missing required parameter '{arg_name}' "
                        f"for step '{step_name}'. Function expects "
                        f""""{'", "'.join(expected_kwargs)}"."""
                    )
                    raise ValueError(msg)

        return {**data_kwargs, **config_kwargs}

    def run(self) -> CanonicalData:
        """Run a data processing pipeline based on a configuration file."""
        for step_cfg in self.config["steps"]:
            step_name = step_cfg["name"]

            if step_name not in self.steps:
                msg = f"Step '{step_name}' not found in pipeline steps."
                raise ValueError(msg)

            step_obj = self.steps.get(step_name)

            logger.info("▶ Running step: %s", step_name)

            kwargs = self.parse_step_args(step_name, step_obj)
            kwargs["validate_input"] = step_cfg.get("validate_input", True)
            kwargs["validate_output"] = step_cfg.get("validate_output", False)
            kwargs["canonical_data"] = self.data

            # Execute step
            step_obj(**kwargs)

        logger.info("Pipeline completed.")
        return self.data
