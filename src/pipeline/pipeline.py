"""Pipeline execution module for running data processing steps."""

import inspect
import json
import logging
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ValidationError

from data_canon.core.dataclass import CanonicalData
from pipeline.cache import PipelineCache
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


class Pipeline:
    """Class to run a data processing pipeline based on a configuration file."""

    data: CanonicalData
    steps: dict[str, Callable]
    cache: PipelineCache | None

    def __init__(
        self,
        config_path: str | Path,
        steps: list[Callable] | None = None,
        caching: bool | Path | str = False,
        data_models: dict[str, Any] | None = None,
        log_file_mode: str = "a",
    ) -> None:
        """Initialize the Pipeline with configuration and custom steps.

        Args:
            config_path: Path to the YAML configuration.
            steps: Optional list of processing step functions.
            caching: If False, disable caching.
                If True, use default cache directory ".cache".
                If str or Path, use specified directory for caching.
            data_models: Optional dictionary of extra data models for validation.
                These will be added to the default data models in CanonicalData object.
            log_file_mode: File open mode for the log file. Use "a" to append
                (default) or "w" to overwrite at the start of each run.
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.data = CanonicalData()
        self.steps = {func.__name__: func for func in steps or []}

        # Setup logging
        log_filename = self.config.get("log_file", None)

        # If not abs path, place log file in .cache dir
        if log_filename and not Path(log_filename).is_absolute():
            # Create cache if it doesn't exist
            Path(".cache").mkdir(parents=True, exist_ok=True)
            log_filename = Path(".cache") / log_filename

        # filename for file+console, or None for console only
        if log_filename:
            setup_logging(log_file=log_filename, log_file_mode=log_file_mode)
            logger.info("Log file: %s", log_filename)
        else:
            # Console-only logging
            setup_logging(log_file=None)
            logger.info("Console-only logging enabled")

        # Initialize cache based on caching parameter
        if caching is False:
            self.cache = None
            logger.info("Pipeline caching disabled")
        elif caching is True:
            self.cache = PipelineCache(cache_dir=Path(".cache"))
        else:
            self.cache = PipelineCache(cache_dir=Path(caching))

        # Initialize step status tracking
        self._step_status: dict[str, dict[str, Any]] = {}

        # Scan cache and report status
        self._scan_cache()
        self.report_status()

        # Add extra data models if provided
        if data_models:
            self.data.add_models(data_models)

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

    def _scan_cache(self) -> None:
        """Scan cache directory to determine which steps have cached data.

        For each step in the config, checks if cache exists and reads
        metadata from the newest cache key directory.
        """
        for step_cfg in self.config.get("steps", []):
            step_name = step_cfg["name"]
            cache_enabled = step_cfg.get("cache", False)

            # Default status
            status = {
                "has_cache": False,
                "cache_key": None,
                "tables": [],
                "cache_enabled": cache_enabled,
            }

            if not self.cache:
                self._step_status[step_name] = status
                self._step_status[step_name].update({"cache_enabled": False})
                continue

            step_cache_dir = self.cache.cache_dir / step_name
            if not step_cache_dir.exists() or not cache_enabled:
                self._step_status[step_name] = status
                continue

            # Find newest cache key directory by modification time
            cache_key_dirs = [d for d in step_cache_dir.iterdir() if d.is_dir()]
            if not cache_key_dirs:
                self._step_status[step_name] = status
                continue

            # Get newest cache directory
            newest_cache_dir = max(cache_key_dirs, key=lambda p: p.stat().st_mtime)
            cache_key = newest_cache_dir.name

            # Read metadata
            metadata_path = newest_cache_dir / "metadata.json"
            tables = []
            if metadata_path.exists():
                try:
                    with metadata_path.open() as f:
                        metadata = json.load(f)
                    tables = metadata.get("tables", [])
                except Exception:
                    logger.exception(
                        "Failed to read metadata for %s/%s",
                        step_name,
                        cache_key,
                    )

            # Update status with cache information
            status.update(
                {
                    "has_cache": True,
                    "cache_key": cache_key,
                    "tables": tables,
                }
            )

            self._step_status[step_name] = status

    def report_status(self) -> None:
        """Report the current pipeline status with ASCII flow diagram.

        Shows which steps have cached data available:
        - ✓ CACHED: Step has valid cache
        - ✗ NO CACHE: Step caching enabled but no cache exists
        - ∅ NO CACHE (disabled): Step caching disabled
        """
        # Build entire status report as a single string
        lines = []
        lines.append("")
        lines.append("=" * 70)
        lines.append("Pipeline Status")
        lines.append("=" * 70)

        # Find max step name length for alignment
        max_step_len = max(
            (len(step_cfg["name"]) for step_cfg in self.config.get("steps", [])),
            default=0,
        )

        # Build flow diagram with tables inline
        for i, step_cfg in enumerate(self.config.get("steps", [])):
            step_name = step_cfg["name"]
            status_info = self._step_status.get(step_name, {})

            has_cache = status_info.get("has_cache", False)
            cache_enabled = status_info.get("cache_enabled", False)
            tables = status_info.get("tables", [])

            if has_cache:
                symbol = "✓"
                status = "CACHED"
                tables_str = f" ({', '.join(tables)})" if tables else ""
            elif cache_enabled:
                symbol = "✗"
                status = "NO CACHE"
                tables_str = ""
            else:
                symbol = "∅"
                status = "NO CACHE (disabled)"
                tables_str = ""

            # Pad step name for alignment
            padded_step = step_name.ljust(max_step_len)
            lines.append(f"[{padded_step}] {symbol} {status}{tables_str}")

            # Add arrow if not last step
            if i < len(self.config.get("steps", [])) - 1:
                lines.append("     ↓")

        lines.append("=" * 70)
        lines.append("")

        # Log as single message
        logger.info("\n".join(lines))

    def parse_step_args(self, step_name: str, step_obj: Callable) -> dict[str, Any]:
        """Separate the canonical data and parameters.

        If argument name matches a canonical table, it is passed from self.data.
        Else, it is taken from the step configuration "parameters".

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

        data_kwargs = {}
        config_kwargs = {}
        for arg_name, param in step_args.items():
            if arg_name == "canonical_data":
                # Pass the entire CanonicalData instance if requested
                data_kwargs[arg_name] = self.data
            elif hasattr(self.data, arg_name):
                data_kwargs[arg_name] = getattr(self.data, arg_name)
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
                    text=True,
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
            ValueError: If any configured step has no registered function.
        """
        missing = [
            step_cfg["name"]
            for step_cfg in self.config.get("steps", [])
            if step_cfg["name"] not in self.steps
        ]
        if not missing:
            return

        named = ", ".join(f"'{name}'" for name in missing)
        registered = ", ".join(sorted(self.steps)) or "(none)"
        label = "Step" if len(missing) == 1 else "Steps"
        msg = (
            f"{label} {named} not found in pipeline steps. Every step named in "
            f"the config must be passed to Pipeline(steps=[...]); add the "
            f"function to the runner's step list. Registered steps: {registered}."
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
            for flag in ("validate_input", "validate_output", "cache"):
                if flag in step_cfg:
                    kwargs[flag] = step_cfg[flag]
            kwargs["canonical_data"] = self.data

            if self.cache:
                kwargs["pipeline_cache"] = self.cache

            # Execute step
            try:
                step_obj(**kwargs)
            except BaseException:
                logger.exception("Fatal error in step '%s' — pipeline aborted", step_name)
                # Flush all handlers so the error is guaranteed on disk before propagating
                for handler in logging.getLogger().handlers:
                    handler.flush()
                raise

        # Log cache statistics if caching was enabled
        self._log_cache_stats()

        # Refresh cache status after run
        self._scan_cache()

        logger.info("Pipeline completed.")
        return self.data

    def _log_cache_stats(self) -> None:
        """Log cache hit/miss statistics after a run."""
        if not self.cache:
            return
        stats = self.cache.get_stats()
        if stats["total"] == 0:
            return
        parts = []
        if stats["loaded"] > 0:
            parts.append(f"{stats['loaded']} loaded from cache")
        if stats["missing"] > 0:
            parts.append(f"{stats['missing']} re-run (no cache)")
        if stats["stale"] > 0:
            parts.append(f"{stats['stale']} re-run (stale/corrupted)")
        summary = ", ".join(parts)
        logger.info(
            "Cache summary: %s (%.1f%% cache hit rate)",
            summary,
            stats["load_rate"] * 100,
        )

    def _get_available_tables(self) -> dict[str, list[str]]:
        """Get all available tables across cached steps.

        Returns:
            Dictionary mapping table names to list of steps containing them.
        """
        table_locations = {}
        for step_cfg in self.config.get("steps", []):
            step_name = step_cfg["name"]
            status_info = self._step_status.get(step_name, {})
            if status_info.get("has_cache"):
                for tbl in status_info.get("tables", []):
                    if tbl not in table_locations:
                        table_locations[tbl] = []
                    table_locations[tbl].append(step_name)
        return table_locations

    def _find_step_with_table(self, table_name: str) -> str | None:
        """Find the latest step that has cached data for a table.

        Args:
            table_name: Name of the table to find.

        Returns:
            Step name containing the table, or None if not found.
        """
        for step_cfg in reversed(self.config.get("steps", [])):
            step_name = step_cfg["name"]
            status_info = self._step_status.get(step_name, {})

            if not status_info.get("has_cache"):
                continue

            if table_name in status_info.get("tables", []):
                return step_name

        return None

    def _load_from_step(self, table_name: str, step_name: str) -> Any:  # noqa: ANN401
        """Load a specific table from a specific step's cache.

        Args:
            table_name: Name of the table to load.
            step_name: Name of the step to load from.

        Returns:
            The loaded table data.

        Raises:
            ValueError: If step has no cache or table not in step.
        """
        if not self.cache:
            msg = "Caching is disabled. Cannot load data from cache."
            raise ValueError(msg)

        status_info = self._step_status.get(step_name)
        if not status_info or not status_info.get("has_cache"):
            msg = f"Step '{step_name}' has no cached data."
            raise ValueError(msg)

        cache_key = status_info["cache_key"]
        tables = status_info["tables"]

        if table_name not in tables:
            msg = (
                f"Table '{table_name}' not found in step '{step_name}'. "
                f"Available tables: {', '.join(tables)}"
            )
            raise ValueError(msg)

        # Load cached data
        cached_data = self.cache.load(step_name, cache_key) if self.cache else None
        if not cached_data or table_name not in cached_data:
            msg = f"Failed to load '{table_name}' from cache for step '{step_name}'."
            raise ValueError(msg)

        # Update canonical data and return
        table_data = cached_data[table_name]
        setattr(self.data, table_name, table_data)
        logger.info(
            "Loaded '%s' from step '%s' (cache key: %s)",
            table_name,
            step_name,
            cache_key[:8] + "...",
        )
        return table_data

    def get_data(
        self,
        table_name: str,
        step: str | None = None,
    ) -> Any:  # noqa: ANN401
        """Fetch a table from cached pipeline data. If no cache, just return latest.

        Args:
            table_name: Name of the table to fetch (e.g., 'households', 'trips')
            step: Optional step name to fetch from. If None, uses the last
                step that has a cache containing this table.

        Returns:
            The requested DataFrame or data object

        Raises:
            ValueError: If table not found in any cached steps, or if
                specified step doesn't have cache or doesn't contain table.

        Example:
            >>> pipeline = Pipeline(config_path, steps, caching=True)
            >>> # Fetch from latest cached step
            >>> households = pipeline.get_data("households")
            >>> # Fetch from specific step
            >>> trips = pipeline.get_data("linked_trips", step="link_trips")
        """
        if not self.cache:
            msg = "Caching is disabled. Just returning latest data."
            data = getattr(self.data, table_name, None)
            if data is None:
                msg = f"Table '{table_name}' not found in canonical data."
                raise ValueError(msg)
            logger.info(msg)
            return data

        # If step specified, load from that specific step
        if step:
            return self._load_from_step(table_name, step)

        # No step specified - find latest step with this table
        step_name = self._find_step_with_table(table_name)
        if step_name:
            return self._load_from_step(table_name, step_name)

        # Table not found - build helpful error message
        table_locations = self._get_available_tables()

        if not table_locations:
            msg = "No cached data found. Run the pipeline first."
            raise ValueError(msg)

        available_tables = ", ".join(sorted(table_locations.keys()))
        msg = (
            f"Table '{table_name}' not found in any cached step. "
            f"Available tables: {available_tables}"
        )
        raise ValueError(msg)
