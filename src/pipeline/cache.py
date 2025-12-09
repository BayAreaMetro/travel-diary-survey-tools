"""Pipeline caching system using Parquet for fast checkpointing.

Provides hash-based cache invalidation and parquet storage for
pipeline step outputs, enabling fast debugging and iteration.
"""

import hashlib
import json
import logging
import shutil
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


class PipelineCache:
    """Manages parquet-based caching for pipeline steps.

    Cache structure:
        .cache/
            {step_name}/
                {cache_key}/
                    metadata.json
                    {table_name}.parquet
                    ...

    The cache key is a hash of:
    - Step name
    - Input data (schema + row count + sample hash)
    - Step parameters

    This ensures cache invalidation when inputs or configuration change.
    """

    def __init__(self, cache_dir: Path | str = Path(".cache")) -> None:
        """Initialize pipeline cache.

        Args:
            cache_dir: Root directory for cache storage
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._stats = {"hits": 0, "misses": 0}

    def get_cache_key(
        self,
        step_name: str,
        inputs: dict[str, pl.DataFrame] | None,
        params: dict[str, Any] | None,
    ) -> str:
        """Generate cache key from step name, inputs, and parameters.

        Args:
            step_name: Name of the pipeline step
            inputs: Input DataFrames (or None for first step)
            params: Step parameters from config

        Returns:
            16-character hex hash string
        """
        # Start with step name
        hash_parts = [step_name]

        # Hash input data characteristics (not full data for performance)
        if inputs:
            for table_name in sorted(inputs.keys()):
                df = inputs[table_name]
                if df is not None:
                    # Hash the entire DataFrame for robust cache invalidation
                    # Polars hash_rows() is efficient even for large DataFrames
                    schema_str = str(df.schema)
                    row_count = len(df)
                    data_hash = ""
                    if row_count > 0:
                        # Hash all rows - Polars does this efficiently
                        data_hash = str(df.hash_rows().sum())

                    hash_parts.append(
                        f"{table_name}:{schema_str}:{row_count}:{data_hash}"
                    )

        # Hash parameters
        if params:
            # Sort keys for deterministic hashing
            params_str = json.dumps(params, sort_keys=True)
            hash_parts.append(params_str)

        # Generate hash
        combined = "|".join(hash_parts)
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    def load(
        self,
        step_name: str,
        cache_key: str,
    ) -> dict[str, pl.DataFrame] | None:
        """Load cached step outputs from parquet.

        Args:
            step_name: Name of the pipeline step
            cache_key: Cache key from get_cache_key()

        Returns:
            Dictionary of table name -> DataFrame, or None if cache miss
        """
        cache_path = self.cache_dir / step_name / cache_key

        if not cache_path.exists():
            self._stats["misses"] += 1
            logger.debug("Cache miss for %s (key: %s)", step_name, cache_key)
            return None

        # Check for metadata file
        metadata_path = cache_path / "metadata.json"
        if not metadata_path.exists():
            logger.warning(
                "Cache corrupted: missing metadata for %s", step_name
            )
            return None

        try:
            # Load metadata
            with metadata_path.open() as f:
                metadata = json.load(f)

            # Load all parquet files
            outputs = {}
            for table_name in metadata.get("tables", []):
                parquet_path = cache_path / f"{table_name}.parquet"
                if parquet_path.exists():
                    outputs[table_name] = pl.read_parquet(parquet_path)
                else:
                    logger.warning(
                        "Cache corrupted: missing %s.parquet", table_name
                    )
                    return None

            self._stats["hits"] += 1
            logger.info(
                "Cache hit for %s (key: %s, tables: %s)",
                step_name,
                cache_key,
                list(outputs.keys()),
            )
        except (OSError, json.JSONDecodeError) as e:
            logger.warning("Failed to load cache for %s: %s", step_name, e)
            return None
        else:
            return outputs

    def save(
        self,
        step_name: str,
        cache_key: str,
        outputs: dict[str, pl.DataFrame],
    ) -> None:
        """Save step outputs to parquet cache.

        Args:
            step_name: Name of the pipeline step
            cache_key: Cache key from get_cache_key()
            outputs: Dictionary of table name -> DataFrame
        """
        cache_path = self.cache_dir / step_name / cache_key
        cache_path.mkdir(parents=True, exist_ok=True)

        try:
            # Save each DataFrame as parquet
            for table_name, df in outputs.items():
                if df is not None:
                    parquet_path = cache_path / f"{table_name}.parquet"
                    df.write_parquet(parquet_path)

            # Save metadata
            metadata = {
                "step_name": step_name,
                "cache_key": cache_key,
                "tables": list(outputs.keys()),
                "row_counts": {
                    name: len(df) if df is not None else 0
                    for name, df in outputs.items()
                },
            }
            metadata_path = cache_path / "metadata.json"
            with metadata_path.open("w") as f:
                json.dump(metadata, f, indent=2)

            logger.info(
                "Cached %s (key: %s, tables: %s)",
                step_name,
                cache_key,
                list(outputs.keys()),
            )

        except Exception:
            logger.exception("Failed to save cache for %s", step_name)
            # Clean up partial cache
            if cache_path.exists():
                shutil.rmtree(cache_path)

    def invalidate(self, step_name: str | None = None) -> None:
        """Invalidate cache for a step or all steps.

        Args:
            step_name: Name of step to invalidate, or None for all steps
        """
        if step_name:
            step_path = self.cache_dir / step_name
            if step_path.exists():
                shutil.rmtree(step_path)
                logger.info("Invalidated cache for %s", step_name)
        elif self.cache_dir.exists():
            shutil.rmtree(self.cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            logger.info("Invalidated all caches")

    def list_cached_steps(self) -> list[dict[str, Any]]:
        """List all cached steps with metadata.

        Returns:
            List of dicts with step info (name, cache_key, tables, sizes)
        """
        cached_steps = []

        if not self.cache_dir.exists():
            return cached_steps

        for step_dir in self.cache_dir.iterdir():
            if not step_dir.is_dir():
                continue

            step_name = step_dir.name

            for cache_dir in step_dir.iterdir():
                if not cache_dir.is_dir():
                    continue

                cache_key = cache_dir.name
                metadata_path = cache_dir / "metadata.json"

                if metadata_path.exists():
                    try:
                        with metadata_path.open() as f:
                            metadata = json.load(f)

                        # Calculate total size
                        total_size = sum(
                            p.stat().st_size
                            for p in cache_dir.glob("*.parquet")
                        )

                        cached_steps.append(
                            {
                                "step_name": step_name,
                                "cache_key": cache_key,
                                "tables": metadata.get("tables", []),
                                "row_counts": metadata.get("row_counts", {}),
                                "size_mb": total_size / (1024 * 1024),
                                "path": str(cache_dir),
                            }
                        )
                    except (OSError, json.JSONDecodeError) as e:
                        logger.warning("Failed to read cache metadata: %s", e)

        return cached_steps

    def get_stats(self) -> dict[str, int]:
        """Get cache hit/miss statistics.

        Returns:
            Dict with 'hits', 'misses', and 'hit_rate' keys
        """
        total = self._stats["hits"] + self._stats["misses"]
        hit_rate = self._stats["hits"] / total if total > 0 else 0.0

        return {
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "total": total,
            "hit_rate": hit_rate,
        }

    def reset_stats(self) -> None:
        """Reset cache statistics."""
        self._stats = {"hits": 0, "misses": 0}
