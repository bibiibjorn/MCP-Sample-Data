"""
Subset Engine - Creates representative data subsets with referential integrity.

Supports multiple sampling strategies:
1. Random sampling
2. Stratified sampling (maintain category distributions)
3. Time-window sampling
4. Referential sampling (maintain FK relationships)
"""

import random
from enum import Enum
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple
from pathlib import Path
from datetime import datetime, date

import polars as pl


class SamplingStrategy(Enum):
    """Available sampling strategies"""
    RANDOM = "random"
    STRATIFIED = "stratified"
    TIME_WINDOW = "time_window"
    REFERENTIAL = "referential"
    TOP_N = "top_n"
    SYSTEMATIC = "systematic"


@dataclass
class SubsetConfig:
    """Configuration for creating a subset"""
    source_path: str
    output_path: Optional[str] = None
    strategy: SamplingStrategy = SamplingStrategy.RANDOM
    target_rows: Optional[int] = None
    target_percentage: Optional[float] = None
    stratify_columns: Optional[List[str]] = None
    time_column: Optional[str] = None
    time_start: Optional[str] = None
    time_end: Optional[str] = None
    related_files: Optional[Dict[str, str]] = None  # {file_path: key_column}
    key_column: Optional[str] = None
    seed: Optional[int] = None
    preserve_proportions: bool = True


@dataclass
class SubsetResult:
    """Result of subset operation"""
    success: bool
    source_path: str
    output_path: Optional[str] = None
    source_rows: int = 0
    subset_rows: int = 0
    reduction_percentage: float = 0.0
    strategy_used: str = ""
    related_files_created: Dict[str, str] = field(default_factory=dict)
    distribution_preserved: Dict[str, float] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class SubsetEngine:
    """
    Creates representative subsets of data.

    Supports multiple strategies and can maintain referential
    integrity across related tables.
    """

    def __init__(self, seed: Optional[int] = None):
        """
        Initialize the subset engine.

        Args:
            seed: Random seed for reproducibility
        """
        self.seed = seed or random.randint(0, 2**32 - 1)
        self.rng = random.Random(self.seed)

    def create_subset(self, config: SubsetConfig) -> SubsetResult:
        """
        Create a subset based on configuration.

        Args:
            config: Subset configuration

        Returns:
            SubsetResult with details
        """
        result = SubsetResult(
            success=True,
            source_path=config.source_path,
            strategy_used=config.strategy.value
        )

        try:
            # Load source file
            path = Path(config.source_path)
            if not path.exists():
                return SubsetResult(
                    success=False,
                    source_path=config.source_path,
                    errors=[f"File not found: {config.source_path}"]
                )

            if path.suffix.lower() == '.parquet':
                df = pl.read_parquet(path)
            else:
                df = pl.read_csv(path, infer_schema_length=10000)

            result.source_rows = len(df)

            # Calculate target size
            target_rows = self._calculate_target_rows(config, len(df))

            # Apply sampling strategy
            if config.strategy == SamplingStrategy.RANDOM:
                subset_df = self._random_sample(df, target_rows, config.seed)
            elif config.strategy == SamplingStrategy.STRATIFIED:
                subset_df = self._stratified_sample(df, target_rows, config)
            elif config.strategy == SamplingStrategy.TIME_WINDOW:
                subset_df = self._time_window_sample(df, config)
            elif config.strategy == SamplingStrategy.REFERENTIAL:
                subset_df, result.related_files_created = self._referential_sample(
                    df, target_rows, config
                )
            elif config.strategy == SamplingStrategy.TOP_N:
                subset_df = self._top_n_sample(df, target_rows, config)
            elif config.strategy == SamplingStrategy.SYSTEMATIC:
                subset_df = self._systematic_sample(df, target_rows, config.seed)
            else:
                subset_df = self._random_sample(df, target_rows, config.seed)

            result.subset_rows = len(subset_df)
            result.reduction_percentage = round(
                (1 - result.subset_rows / result.source_rows) * 100, 2
            )

            # Calculate distribution preservation scores
            if config.stratify_columns:
                for col in config.stratify_columns:
                    if col in df.columns:
                        score = self._calculate_distribution_similarity(
                            df[col], subset_df[col]
                        )
                        result.distribution_preserved[col] = round(score, 3)

            # Write output
            if config.output_path:
                out_path = Path(config.output_path)
            else:
                out_path = path.parent / f"{path.stem}_subset{path.suffix}"

            if out_path.suffix.lower() == '.parquet':
                subset_df.write_parquet(out_path)
            else:
                subset_df.write_csv(out_path)

            result.output_path = str(out_path)

        except Exception as e:
            result.success = False
            result.errors.append(str(e))

        return result

    def _calculate_target_rows(self, config: SubsetConfig, total_rows: int) -> int:
        """Calculate target number of rows"""
        if config.target_rows:
            return min(config.target_rows, total_rows)
        elif config.target_percentage:
            return max(1, int(total_rows * config.target_percentage / 100))
        else:
            # Default to 10% or 10000 rows, whichever is smaller
            return min(total_rows, max(1, int(total_rows * 0.1), 10000))

    def _random_sample(
        self,
        df: pl.DataFrame,
        n: int,
        seed: Optional[int] = None
    ) -> pl.DataFrame:
        """Simple random sampling"""
        if n >= len(df):
            return df
        return df.sample(n=n, seed=seed or self.seed)

    def _stratified_sample(
        self,
        df: pl.DataFrame,
        target_rows: int,
        config: SubsetConfig
    ) -> pl.DataFrame:
        """Stratified sampling to preserve category distributions"""
        if not config.stratify_columns:
            return self._random_sample(df, target_rows, config.seed)

        # Use first stratify column (or combine multiple)
        strat_col = config.stratify_columns[0]
        if strat_col not in df.columns:
            return self._random_sample(df, target_rows, config.seed)

        # Calculate proportions
        total_rows = len(df)
        sample_fraction = target_rows / total_rows

        # Group and sample proportionally
        sampled_groups = []
        for group_name, group_df in df.group_by(strat_col):
            group_size = len(group_df)

            if config.preserve_proportions:
                # Proportional sampling
                n_sample = max(1, int(group_size * sample_fraction))
            else:
                # Equal sampling per group
                n_groups = df[strat_col].n_unique()
                n_sample = max(1, target_rows // n_groups)

            n_sample = min(n_sample, group_size)
            sampled = group_df.sample(n=n_sample, seed=self.seed)
            sampled_groups.append(sampled)

        if sampled_groups:
            result = pl.concat(sampled_groups)
            # Shuffle to avoid ordering by strata
            return result.sample(fraction=1.0, seed=self.seed)
        return df.head(target_rows)

    def _time_window_sample(
        self,
        df: pl.DataFrame,
        config: SubsetConfig
    ) -> pl.DataFrame:
        """Sample based on time window"""
        if not config.time_column or config.time_column not in df.columns:
            return df

        # Parse dates
        time_col = config.time_column
        df_with_dates = df.with_columns(
            pl.col(time_col).cast(pl.Date).alias("_subset_date")
        )

        # Apply filters
        if config.time_start:
            start_date = datetime.strptime(config.time_start, "%Y-%m-%d").date()
            df_with_dates = df_with_dates.filter(pl.col("_subset_date") >= start_date)

        if config.time_end:
            end_date = datetime.strptime(config.time_end, "%Y-%m-%d").date()
            df_with_dates = df_with_dates.filter(pl.col("_subset_date") <= end_date)

        # Remove helper column
        return df_with_dates.drop("_subset_date")

    def _referential_sample(
        self,
        df: pl.DataFrame,
        target_rows: int,
        config: SubsetConfig
    ) -> Tuple[pl.DataFrame, Dict[str, str]]:
        """
        Sample while maintaining referential integrity.

        First samples from the main table, then filters related tables
        to only include referenced records.
        """
        related_outputs = {}

        if not config.related_files or not config.key_column:
            return self._random_sample(df, target_rows, config.seed), related_outputs

        # Sample main table
        main_subset = self._random_sample(df, target_rows, config.seed)

        # Get unique keys from main subset
        key_col = config.key_column
        if key_col not in df.columns:
            return main_subset, related_outputs

        selected_keys = set(main_subset[key_col].unique().to_list())

        # Filter related tables
        for related_path, related_key in config.related_files.items():
            try:
                related_path_obj = Path(related_path)
                if not related_path_obj.exists():
                    continue

                if related_path_obj.suffix.lower() == '.parquet':
                    related_df = pl.read_parquet(related_path_obj)
                else:
                    related_df = pl.read_csv(related_path_obj, infer_schema_length=10000)

                if related_key not in related_df.columns:
                    continue

                # Filter to only related records
                related_subset = related_df.filter(
                    pl.col(related_key).is_in(list(selected_keys))
                )

                # Write output
                out_path = related_path_obj.parent / f"{related_path_obj.stem}_subset{related_path_obj.suffix}"
                if out_path.suffix.lower() == '.parquet':
                    related_subset.write_parquet(out_path)
                else:
                    related_subset.write_csv(out_path)

                related_outputs[related_path] = str(out_path)

            except Exception as e:
                # Log but continue with other files
                pass

        return main_subset, related_outputs

    def _top_n_sample(
        self,
        df: pl.DataFrame,
        n: int,
        config: SubsetConfig
    ) -> pl.DataFrame:
        """Take top N rows (with optional sorting)"""
        return df.head(n)

    def _systematic_sample(
        self,
        df: pl.DataFrame,
        n: int,
        seed: Optional[int] = None
    ) -> pl.DataFrame:
        """Systematic sampling (every k-th row)"""
        if n >= len(df):
            return df

        k = len(df) // n
        rng = random.Random(seed or self.seed)
        start = rng.randint(0, k - 1)

        indices = list(range(start, len(df), k))[:n]
        return df[indices]

    def _calculate_distribution_similarity(
        self,
        original: pl.Series,
        subset: pl.Series
    ) -> float:
        """
        Calculate how well the subset preserves the original distribution.

        Returns a score from 0 to 1 where 1 means perfect preservation.
        """
        try:
            # Get value counts
            orig_counts = original.value_counts()
            subset_counts = subset.value_counts()

            # Normalize to proportions
            orig_props = {
                row[0]: row[1] / len(original)
                for row in orig_counts.iter_rows()
            }
            subset_props = {
                row[0]: row[1] / len(subset)
                for row in subset_counts.iter_rows()
            }

            # Calculate similarity (1 - mean absolute difference)
            all_values = set(orig_props.keys()) | set(subset_props.keys())
            if not all_values:
                return 1.0

            total_diff = sum(
                abs(orig_props.get(v, 0) - subset_props.get(v, 0))
                for v in all_values
            )

            return max(0, 1 - total_diff / 2)

        except Exception:
            return 0.0

    def create_multi_table_subset(
        self,
        fact_config: SubsetConfig,
        dimension_configs: List[SubsetConfig]
    ) -> Dict[str, SubsetResult]:
        """
        Create subsets of a star schema maintaining referential integrity.

        Args:
            fact_config: Configuration for the fact table
            dimension_configs: Configurations for dimension tables

        Returns:
            Dict of file paths to SubsetResults
        """
        results = {}

        # First, create fact table subset
        fact_result = self.create_subset(fact_config)
        results[fact_config.source_path] = fact_result

        if not fact_result.success:
            return results

        # Load fact subset to get dimension keys
        fact_path = Path(fact_result.output_path)
        if fact_path.suffix.lower() == '.parquet':
            fact_df = pl.read_parquet(fact_path)
        else:
            fact_df = pl.read_csv(fact_path, infer_schema_length=10000)

        # For each dimension, filter to only referenced keys
        for dim_config in dimension_configs:
            if not dim_config.key_column:
                continue

            # Find matching column in fact table
            fact_key = dim_config.key_column
            if fact_key not in fact_df.columns:
                # Try to find by suffix
                for col in fact_df.columns:
                    if col.endswith('_key') or col.endswith('_id'):
                        if dim_config.key_column.lower() in col.lower():
                            fact_key = col
                            break

            if fact_key in fact_df.columns:
                # Get unique keys from fact
                used_keys = set(fact_df[fact_key].unique().drop_nulls().to_list())

                # Load and filter dimension
                dim_path = Path(dim_config.source_path)
                if dim_path.suffix.lower() == '.parquet':
                    dim_df = pl.read_parquet(dim_path)
                else:
                    dim_df = pl.read_csv(dim_path, infer_schema_length=10000)

                if dim_config.key_column in dim_df.columns:
                    dim_subset = dim_df.filter(
                        pl.col(dim_config.key_column).is_in(list(used_keys))
                    )

                    # Write output
                    if dim_config.output_path:
                        out_path = Path(dim_config.output_path)
                    else:
                        out_path = dim_path.parent / f"{dim_path.stem}_subset{dim_path.suffix}"

                    if out_path.suffix.lower() == '.parquet':
                        dim_subset.write_parquet(out_path)
                    else:
                        dim_subset.write_csv(out_path)

                    results[dim_config.source_path] = SubsetResult(
                        success=True,
                        source_path=dim_config.source_path,
                        output_path=str(out_path),
                        source_rows=len(dim_df),
                        subset_rows=len(dim_subset),
                        reduction_percentage=round(
                            (1 - len(dim_subset) / len(dim_df)) * 100, 2
                        ),
                        strategy_used="referential"
                    )

        return results
