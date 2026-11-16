"""Pipeline execution module for running data processing steps."""

import importlib.util
import inspect
import logging
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ValidationError

import processing
from data_canon.core.dataclass import CanonicalData
from pipeline.config import (
    check_input_paths,
    check_roots_resolved,
    resolve_templates,
    shorthands_used_by_steps,
    template_namespace,
)
from pipeline.decoration import PIPELINE_ARGS, field_owners, settings_models
from pipeline.logger import setup_logging

logger = logging.getLogger(__name__)

# The keys a step entry in the config may carry.
_STEP_KEYS = frozenset({"name", "file", "params", "validate_input", "validate_output"})


def _check_config_keys(
    step_name: str, params: dict[str, Any], tables: set[str], accepted: set[str]
) -> None:
    """Raise on a config key that names a table, or that nothing in the step takes."""
    table_keys = sorted(k for k in params if k in tables)
    if table_keys:
        msg = (
            f"Config for step '{step_name}' sets {table_keys}, which name "
            "canonical tables. Tables are passed by the pipeline and cannot "
            "be set in config."
        )
        raise ValueError(msg)
    unknown = sorted(k for k in params if k not in accepted)
    if unknown:
        msg = (
            f"Config for step '{step_name}' sets {unknown}, which the step "
            f"does not take. It takes: {sorted(accepted)}."
        )
        raise ValueError(msg)


def _model_settings(
    step_name: str, arg_name: str, params: dict[str, Any], flat: dict[str, Any]
) -> object | None:
    """A model argument's keys, as a block or flat; None when the config gives neither.

    Raises:
        ValueError: If the config gives both, which leaves two places to read.
    """
    block = params.get(arg_name)
    if block is not None and flat:
        msg = (
            f"Config for step '{step_name}' sets {sorted(flat)} flat and also a "
            f"'{arg_name}:' block; write those settings one way."
        )
        raise ValueError(msg)
    if block is not None:
        return block
    return flat or None


def _build_settings(
    step_name: str, arg_name: str, model: type[BaseModel], settings: object
) -> BaseModel:
    """Build a step's settings model from its keys, naming the step on failure.

    Raises:
        TypeError: If the model allows extra fields, which would let a misspelt
            key in a block through unread.
        ValueError: If a block is not a mapping, or the model rejects the keys.
    """
    if model.model_config.get("extra") != "forbid":
        msg = (
            f"{model.__name__} (step '{step_name}', argument '{arg_name}') must set "
            'model_config = ConfigDict(extra="forbid"), or a misspelt key is ignored.'
        )
        raise TypeError(msg)
    if not isinstance(settings, dict):
        msg = (
            f"Config for step '{step_name}' sets '{arg_name}' to {settings!r}; it must be "
            f"a block of {model.__name__} settings."
        )
        raise ValueError(msg)  # noqa: TRY004 - a config error, reported as one
    try:
        return model.model_validate(settings)
    except ValidationError as e:
        msg = (
            f"Config for step '{step_name}', {model.__name__} settings (flat, or under "
            f"'{arg_name}:'):\n{e}"
        )
        raise ValueError(msg) from e


def _steps_from_files(step_entries: list[dict], config_dir: Path) -> list[Callable]:
    """The project steps the config names a ``file`` for, each file imported once.

    Args:
        step_entries: The config's ``steps`` list.
        config_dir: The folder of the config file, which a relative ``file`` is read from.

    Raises:
        ValueError: If a ``file`` is not a ``.py`` file or doesn't exist, or the
            step isn't a ``@step()`` function in it.
    """
    modules: dict[Path, object] = {}
    found = []
    for entry in step_entries:
        if "file" not in entry:
            continue
        name, path = entry["name"], (config_dir / entry["file"]).resolve()
        if path.suffix != ".py":
            msg = f"Step '{name}': file must be a .py file, got '{entry['file']}'."
            raise ValueError(msg)
        if not path.is_file():
            msg = f"Step '{name}': file '{entry['file']}' not found at {path}."
            raise ValueError(msg)
        if path not in modules:
            modules[path] = _import_file(path)
        func = getattr(modules[path], name, None)
        if not hasattr(func, "contract"):
            msg = (
                f"Step '{name}': {path.name} has no @step() function named '{name}'. "
                "The step's name is its function's name, which may differ from the file's."
            )
            raise ValueError(msg)
        found.append(func)
    return found


def _import_file(path: Path) -> object:
    """Import the module at *path*, named after its file."""
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        msg = f"Cannot import {path}."
        raise ValueError(msg)
    module = importlib.util.module_from_spec(spec)
    sys.modules[path.stem] = module  # the module's own classes resolve their imports through it
    spec.loader.exec_module(module)
    return module


def _with_builtins(custom: list[Callable]) -> dict[str, Callable]:
    """Every built-in step, plus *custom*, by name.

    Raises:
        ValueError: If a custom step has a built-in step's name.
    """
    names = [f.__name__ for f in custom]
    repeated = sorted({n for n in names if names.count(n) > 1})
    if repeated:
        msg = f"Project step(s) {repeated} are given more than once."
        raise ValueError(msg)
    clashes = sorted(n for n in names if n in processing.__all__)
    if clashes:
        msg = (
            f"Project step(s) {clashes} have the name of a built-in step. Rename "
            "them: a project step adds to the built-in steps, it never replaces one."
        )
        raise ValueError(msg)
    builtins = {name: getattr(processing, name) for name in processing.__all__}
    return {**builtins, **{f.__name__: f for f in custom}}


class Pipeline:
    """Class to run a data processing pipeline based on a configuration file."""

    data: CanonicalData
    steps: dict[str, Callable]

    def __init__(
        self,
        config_path: str | Path,
        steps: list[Callable] | None = None,
        log_file_mode: str = "a",
    ) -> None:
        """Initialize the Pipeline with configuration and custom steps.

        Every built-in step (``processing.__all__``) is available without being
        passed. A project's own step names its file on its step entry
        (``file: clean_bats_2023.py``, relative to the config's folder); *steps*
        adds steps directly, for callers without a config file of their own. A
        formatter step brings the models of the files it writes, so none are
        passed either.

        Args:
            config_path: Path to the YAML configuration.
            steps: Project step functions passed directly.
            log_file_mode: File open mode for the log file. Use "a" to append
                (default) or "w" to overwrite at the start of each run.

        Raises:
            ValueError: If a project step has a built-in step's name, or two
                project steps share a name.
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.data = CanonicalData()
        from_files = _steps_from_files(self.config.get("steps", []), Path(config_path).parent)
        self.steps = _with_builtins([*(steps or []), *from_files])

        # A log file gets console + file logging; none gets console only
        log_filename = self.config.get("log_file") or None
        setup_logging(log_file=log_filename, log_file_mode=log_file_mode)
        if log_filename:
            logger.info("Log file: %s", log_filename)
        else:
            logger.info("Console-only logging enabled")

    def _load_config(self) -> dict[str, Any]:
        """Load the pipeline configuration from a YAML file.

        Replaces template variables in the format {{ variable_name }} with
        their corresponding values defined in the config.

        Returns:
            The configuration dictionary.
        """
        with Path(self.config_path).open() as f:
            config = yaml.safe_load(f)

        check_roots_resolved(config, template_namespace(config))
        resolved = resolve_templates(config)
        check_input_paths(resolved, shorthands_used_by_steps(config))
        return resolved

    def parse_step_args(self, step_name: str, step_obj: Callable) -> dict[str, Any]:
        """Separate the canonical data and parameters.

        The tables the step's contract declares are passed from self.data, by
        name; the decorator checks them. Other arguments come from the step's
        ``params`` in the config.

        A config key the step does not take raises, as does one naming a
        canonical table: either would otherwise be dropped without a word, and
        the step would run on its own default instead of the configured value.

        An argument annotated with a pydantic model is built from its fields'
        keys. They sit flat under ``params:``, each routed to the model that
        owns the name, or in a block named after the argument; not both. The
        model decides what may be left out: a field with a default may, one
        without may not, the same rule as a plain argument.

        Args:
            step_name: Name of the step.
            step_obj: The step function or class.

        """
        step_args = inspect.signature(step_obj).parameters
        models = settings_models(step_obj)
        owners = field_owners(step_obj)
        tables = {"canonical_data", *self.data.TABLE_NAMES}
        plain = {a for a in step_args if a not in PIPELINE_ARGS | tables | set(models)}

        params = next(
            (s.get("params") or {} for s in self.config["steps"] if s["name"] == step_name),
            {},
        )
        _check_config_keys(step_name, params, tables, plain | set(models) | set(owners))

        flat: dict[str, dict[str, Any]] = {arg: {} for arg in models}
        for key, value in params.items():
            if key in owners:
                flat[owners[key]][key] = value

        contract = getattr(step_obj, "contract", None)
        taken = contract.taken if contract else {}
        data_kwargs = {name: getattr(self.data, name) for name in taken}
        config_kwargs = {}
        for arg_name, param in step_args.items():
            if arg_name == "canonical_data":
                # Pass the entire CanonicalData instance if requested
                data_kwargs[arg_name] = self.data
            elif arg_name in models:
                settings = _model_settings(step_name, arg_name, params, flat[arg_name])
                if settings is None and param.default is not param.empty:
                    continue
                config_kwargs[arg_name] = _build_settings(
                    step_name, arg_name, models[arg_name], {} if settings is None else settings
                )
            elif arg_name in params:
                config_kwargs[arg_name] = params[arg_name]
            elif param.default is not inspect.Parameter.empty or arg_name in PIPELINE_ARGS:
                # Has default value, don't need to provide it
                pass
            else:
                msg = (
                    f"Missing required parameter '{arg_name}' for step '{step_name}'. "
                    f"It takes: {sorted(plain | set(owners))}."
                )
                raise ValueError(msg)

        return {**data_kwargs, **config_kwargs}

    def _log_git_version(self) -> None:
        """Log the git version of the codebase, including diffs if dirty."""
        git = shutil.which("git")
        if git is None:
            logger.warning("Could not determine git version (git not found on PATH)")
            return
        try:
            version = subprocess.check_output(  # noqa: S603
                [git, "describe", "--always", "--dirty", "--tags"],
                stderr=subprocess.DEVNULL,
                text=True,
            ).strip()
            logger.info("Code version: %s", version)

            if version.endswith("-dirty"):
                diff = subprocess.check_output(  # noqa: S603
                    [git, "diff"],
                    stderr=subprocess.DEVNULL,
                    # git writes UTF-8; the locale codec (cp1252 on Windows) fails on it
                    encoding="utf-8",
                    errors="replace",
                ).strip()
                if diff:
                    logger.debug("Uncommitted changes (git diff):\n%s", diff)
        except subprocess.CalledProcessError:
            logger.warning("Could not determine git version (not a git repo)")

    def _validate_steps(self) -> None:
        """Check every configured step resolves to a registered function.

        Runs before any step executes, so a config naming a step the runner never
        registered fails immediately instead of part-way through a long pipeline.
        All unresolved names are reported at once.

        Raises:
            ValueError: If any configured step has no registered function, or
                carries a key other than ``name``, ``params``, ``validate_input``
                and ``validate_output``.
        """
        for step_cfg in self.config.get("steps", []):
            unknown = sorted(set(step_cfg) - _STEP_KEYS)
            if unknown:
                msg = (
                    f"Step '{step_cfg['name']}' has unknown keys {unknown}. A step takes "
                    f"{sorted(_STEP_KEYS)}; its settings go under 'params'."
                )
                raise ValueError(msg)
        missing = [
            step_cfg["name"]
            for step_cfg in self.config.get("steps", [])
            if step_cfg["name"] not in self.steps
        ]
        if not missing:
            return

        named = ", ".join(f"'{name}'" for name in missing)
        custom = ", ".join(sorted(set(self.steps) - set(processing.__all__))) or "(none)"
        label = "Step" if len(missing) == 1 else "Steps"
        msg = (
            f"{label} {named} not found in pipeline steps: not a built-in step, and "
            f"not one of the project's own steps passed to Pipeline(steps=[...]) "
            f"({custom}). Built-in steps: {', '.join(sorted(processing.__all__))}."
        )
        raise ValueError(msg)

    def run(self) -> CanonicalData:
        """Run a data processing pipeline based on a configuration file."""
        self._validate_steps()
        self._log_git_version()
        n_steps = len(self.config["steps"])
        for i, step_cfg in enumerate(self.config["steps"], start=1):
            step_name = step_cfg["name"]
            step_obj = self.steps[step_name]

            logger.info("")
            logger.info("=" * 70)
            logger.info("Step %d/%d: %s", i, n_steps, step_name)
            logger.info("=" * 70)

            kwargs = self.parse_step_args(step_name, step_obj)
            # Pass a flag only when the config sets it, so the step's @step() default stands.
            for flag in ("validate_input", "validate_output"):
                if flag in step_cfg:
                    kwargs[flag] = step_cfg[flag]
            kwargs["canonical_data"] = self.data

            # Execute step
            try:
                step_obj(**kwargs)
            except BaseException:
                logger.exception("Fatal error in step '%s' — pipeline aborted", step_name)
                # Flush all handlers so the error is guaranteed on disk before propagating
                for handler in logging.getLogger().handlers:
                    handler.flush()
                raise

        logger.info("Pipeline completed.")
        return self.data
