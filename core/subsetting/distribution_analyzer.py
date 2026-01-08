"""
Distribution Analyzer - Compares distributions between original and subset data.

Provides statistical analysis to verify that subsets are representative
of the original data across various dimensions.
"""

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

import polars as pl


@dataclass
class ColumnComparison:
    """Comparison results for a single column"""
    column_name: str
    column_type: str
    similarity_score: float
    original_stats: Dict[str, Any] = field(default_factory=dict)
    subset_stats: Dict[str, Any] = field(default_factory=dict)
    differences: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class DistributionComparison:
    """Complete distribution comparison results"""
    source_file: str
    subset_file: str
    source_rows: int
    subset_rows: int
    overall_similarity: float
    column_comparisons: List[ColumnComparison] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)


class DistributionAnalyzer:
    """
    Analyzes and compares distributions between datasets.

    Used to verify that subsets maintain the statistical properties
    of the original data.
    """

    def __init__(self):
        """Initialize the distribution analyzer"""
        pass

    def compare_files(
        self,
        source_path: str,
        subset_path: str,
        columns: Optional[List[str]] = None,
        detailed: bool = True
    ) -> DistributionComparison:
        """
        Compare distributions between source and subset files.

        Args:
            source_path: Path to original file
            subset_path: Path to subset file
            columns: Specific columns to compare (all if None)
            detailed: Include detailed statistics

        Returns:
            DistributionComparison with analysis results
        """
        # Load files
        source_p = Path(source_path)
        subset_p = Path(subset_path)

        if source_p.suffix.lower() == '.parquet':
            source_df = pl.read_parquet(source_p)
        else:
            source_df = pl.read_csv(source_p, infer_schema_length=10000)

        if subset_p.suffix.lower() == '.parquet':
            subset_df = pl.read_parquet(subset_p)
        else:
            subset_df = pl.read_csv(subset_p, infer_schema_length=10000)

        return self.compare_dataframes(
            source_df, subset_df,
            source_path, subset_path,
            columns, detailed
        )

    def compare_dataframes(
        self,
        source_df: pl.DataFrame,
        subset_df: pl.DataFrame,
        source_name: str = "source",
        subset_name: str = "subset",
        columns: Optional[List[str]] = None,
        detailed: bool = True
    ) -> DistributionComparison:
        """
        Compare distributions between two DataFrames.

        Args:
            source_df: Original DataFrame
            subset_df: Subset DataFrame
            source_name: Name for source file
            subset_name: Name for subset file
            columns: Specific columns to compare
            detailed: Include detailed statistics

        Returns:
            DistributionComparison with analysis results
        """
        result = DistributionComparison(
            source_file=source_name,
            subset_file=subset_name,
            source_rows=len(source_df),
            subset_rows=len(subset_df)
        )

        # Determine columns to compare
        cols_to_compare = columns if columns else source_df.columns
        cols_to_compare = [c for c in cols_to_compare if c in subset_df.columns]

        similarity_scores = []

        for col in cols_to_compare:
            comparison = self._compare_column(
                source_df[col],
                subset_df[col],
                col,
                detailed
            )
            result.column_comparisons.append(comparison)
            similarity_scores.append(comparison.similarity_score)

        # Calculate overall similarity
        if similarity_scores:
            result.overall_similarity = round(
                sum(similarity_scores) / len(similarity_scores), 3
            )
        else:
            result.overall_similarity = 0.0

        # Generate summary
        result.summary = self._generate_summary(result)

        # Generate recommendations
        result.recommendations = self._generate_recommendations(result)

        return result

    def _compare_column(
        self,
        source_col: pl.Series,
        subset_col: pl.Series,
        col_name: str,
        detailed: bool
    ) -> ColumnComparison:
        """Compare a single column between source and subset"""
        comparison = ColumnComparison(
            column_name=col_name,
            column_type=str(source_col.dtype)
        )

        try:
            if source_col.dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                                     pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
                                     pl.Float32, pl.Float64]:
                comparison = self._compare_numeric(
                    source_col, subset_col, comparison, detailed
                )
            elif source_col.dtype in [pl.Utf8, pl.String, pl.Categorical]:
                comparison = self._compare_categorical(
                    source_col, subset_col, comparison, detailed
                )
            elif source_col.dtype in [pl.Date, pl.Datetime]:
                comparison = self._compare_temporal(
                    source_col, subset_col, comparison, detailed
                )
            elif source_col.dtype == pl.Boolean:
                comparison = self._compare_boolean(
                    source_col, subset_col, comparison, detailed
                )
            else:
                # Default to categorical comparison
                comparison = self._compare_categorical(
                    source_col.cast(pl.Utf8),
                    subset_col.cast(pl.Utf8),
                    comparison, detailed
                )
        except Exception as e:
            comparison.similarity_score = 0.0
            comparison.warnings.append(f"Error comparing column: {str(e)}")

        return comparison

    def _compare_numeric(
        self,
        source: pl.Series,
        subset: pl.Series,
        comparison: ColumnComparison,
        detailed: bool
    ) -> ColumnComparison:
        """Compare numeric columns"""
        # Calculate statistics
        source_stats = self._numeric_stats(source)
        subset_stats = self._numeric_stats(subset)

        comparison.original_stats = source_stats
        comparison.subset_stats = subset_stats

        # Calculate similarity based on key statistics
        scores = []

        # Mean comparison
        if source_stats['mean'] != 0:
            mean_diff = abs(source_stats['mean'] - subset_stats['mean']) / abs(source_stats['mean'])
            scores.append(max(0, 1 - mean_diff))
        else:
            scores.append(1.0 if subset_stats['mean'] == 0 else 0.0)

        # Std comparison
        if source_stats['std'] != 0:
            std_diff = abs(source_stats['std'] - subset_stats['std']) / abs(source_stats['std'])
            scores.append(max(0, 1 - std_diff))
        else:
            scores.append(1.0 if subset_stats['std'] == 0 else 0.0)

        # Median comparison
        if source_stats['median'] != 0:
            median_diff = abs(source_stats['median'] - subset_stats['median']) / abs(source_stats['median'])
            scores.append(max(0, 1 - median_diff))
        else:
            scores.append(1.0 if subset_stats['median'] == 0 else 0.0)

        # Null rate comparison
        null_diff = abs(source_stats['null_rate'] - subset_stats['null_rate'])
        scores.append(max(0, 1 - null_diff * 2))  # Penalize null differences more

        comparison.similarity_score = round(sum(scores) / len(scores), 3)

        # Add differences
        if detailed:
            mean_diff_pct = self._safe_pct_diff(source_stats['mean'], subset_stats['mean'])
            if abs(mean_diff_pct) > 5:
                comparison.differences.append(
                    f"Mean differs by {mean_diff_pct:+.1f}%"
                )

            std_diff_pct = self._safe_pct_diff(source_stats['std'], subset_stats['std'])
            if abs(std_diff_pct) > 10:
                comparison.differences.append(
                    f"Std deviation differs by {std_diff_pct:+.1f}%"
                )

        return comparison

    def _compare_categorical(
        self,
        source: pl.Series,
        subset: pl.Series,
        comparison: ColumnComparison,
        detailed: bool
    ) -> ColumnComparison:
        """Compare categorical columns"""
        # Get value distributions
        source_dist = self._get_distribution(source)
        subset_dist = self._get_distribution(subset)

        comparison.original_stats = {
            'unique_count': source.n_unique(),
            'null_rate': round(source.null_count() / len(source), 4),
            'top_values': dict(list(source_dist.items())[:5])
        }

        comparison.subset_stats = {
            'unique_count': subset.n_unique(),
            'null_rate': round(subset.null_count() / len(subset), 4),
            'top_values': dict(list(subset_dist.items())[:5])
        }

        # Calculate distribution similarity using Jensen-Shannon divergence
        all_values = set(source_dist.keys()) | set(subset_dist.keys())

        if not all_values:
            comparison.similarity_score = 1.0
            return comparison

        # Calculate JS divergence
        p = [source_dist.get(v, 0) for v in all_values]
        q = [subset_dist.get(v, 0) for v in all_values]

        # Add small epsilon to avoid division by zero
        eps = 1e-10
        p = [max(x, eps) for x in p]
        q = [max(x, eps) for x in q]

        # Normalize
        p_sum = sum(p)
        q_sum = sum(q)
        p = [x / p_sum for x in p]
        q = [x / q_sum for x in q]

        # Calculate midpoint
        m = [(pi + qi) / 2 for pi, qi in zip(p, q)]

        # KL divergences
        def kl_div(a, b):
            return sum(ai * math.log(ai / bi) for ai, bi in zip(a, b) if ai > 0)

        js_div = (kl_div(p, m) + kl_div(q, m)) / 2
        js_distance = math.sqrt(js_div)

        comparison.similarity_score = round(max(0, 1 - js_distance), 3)

        # Add differences
        if detailed:
            # Check for missing categories
            missing_in_subset = set(source_dist.keys()) - set(subset_dist.keys())
            if missing_in_subset:
                top_missing = list(missing_in_subset)[:3]
                comparison.differences.append(
                    f"Missing {len(missing_in_subset)} categories in subset: {top_missing}"
                )

            # Check for proportion differences
            for value in list(source_dist.keys())[:5]:
                if value in subset_dist:
                    diff = subset_dist[value] - source_dist[value]
                    if abs(diff) > 0.05:  # 5% difference
                        comparison.differences.append(
                            f"'{value}': {source_dist[value]:.1%} → {subset_dist[value]:.1%}"
                        )

        return comparison

    def _compare_temporal(
        self,
        source: pl.Series,
        subset: pl.Series,
        comparison: ColumnComparison,
        detailed: bool
    ) -> ColumnComparison:
        """Compare temporal columns"""
        source_stats = {
            'min': str(source.min()),
            'max': str(source.max()),
            'null_rate': round(source.null_count() / len(source), 4)
        }

        subset_stats = {
            'min': str(subset.min()),
            'max': str(subset.max()),
            'null_rate': round(subset.null_count() / len(subset), 4)
        }

        comparison.original_stats = source_stats
        comparison.subset_stats = subset_stats

        # Similarity based on date range coverage
        try:
            source_min = source.min()
            source_max = source.max()
            subset_min = subset.min()
            subset_max = subset.max()

            if source_min and source_max and subset_min and subset_max:
                source_range = (source_max - source_min).days
                subset_range = (subset_max - subset_min).days

                if source_range > 0:
                    coverage = subset_range / source_range
                    comparison.similarity_score = round(min(1.0, coverage), 3)
                else:
                    comparison.similarity_score = 1.0 if subset_range == 0 else 0.5
            else:
                comparison.similarity_score = 0.5
        except Exception:
            comparison.similarity_score = 0.5

        if detailed:
            if source_stats['min'] != subset_stats['min']:
                comparison.differences.append(
                    f"Date range starts: {source_stats['min']} → {subset_stats['min']}"
                )
            if source_stats['max'] != subset_stats['max']:
                comparison.differences.append(
                    f"Date range ends: {source_stats['max']} → {subset_stats['max']}"
                )

        return comparison

    def _compare_boolean(
        self,
        source: pl.Series,
        subset: pl.Series,
        comparison: ColumnComparison,
        detailed: bool
    ) -> ColumnComparison:
        """Compare boolean columns"""
        source_true_rate = source.sum() / len(source) if len(source) > 0 else 0
        subset_true_rate = subset.sum() / len(subset) if len(subset) > 0 else 0

        comparison.original_stats = {
            'true_rate': round(source_true_rate, 4),
            'null_rate': round(source.null_count() / len(source), 4)
        }

        comparison.subset_stats = {
            'true_rate': round(subset_true_rate, 4),
            'null_rate': round(subset.null_count() / len(subset), 4)
        }

        # Similarity based on true rate difference
        diff = abs(source_true_rate - subset_true_rate)
        comparison.similarity_score = round(max(0, 1 - diff * 2), 3)

        if detailed and diff > 0.05:
            comparison.differences.append(
                f"True rate: {source_true_rate:.1%} → {subset_true_rate:.1%}"
            )

        return comparison

    def _numeric_stats(self, series: pl.Series) -> Dict[str, float]:
        """Calculate numeric statistics"""
        return {
            'min': float(series.min()) if series.min() is not None else 0,
            'max': float(series.max()) if series.max() is not None else 0,
            'mean': float(series.mean()) if series.mean() is not None else 0,
            'median': float(series.median()) if series.median() is not None else 0,
            'std': float(series.std()) if series.std() is not None else 0,
            'null_rate': series.null_count() / len(series) if len(series) > 0 else 0
        }

    def _get_distribution(self, series: pl.Series) -> Dict[Any, float]:
        """Get value distribution as proportions"""
        counts = series.value_counts()
        total = len(series)
        return {
            row[0]: row[1] / total
            for row in counts.iter_rows()
            if row[0] is not None
        }

    def _safe_pct_diff(self, original: float, subset: float) -> float:
        """Calculate percentage difference safely"""
        if original == 0:
            return 0 if subset == 0 else 100
        return ((subset - original) / abs(original)) * 100

    def _generate_summary(self, result: DistributionComparison) -> Dict[str, Any]:
        """Generate summary statistics"""
        scores = [c.similarity_score for c in result.column_comparisons]

        return {
            'total_columns_compared': len(result.column_comparisons),
            'average_similarity': round(sum(scores) / len(scores), 3) if scores else 0,
            'min_similarity': round(min(scores), 3) if scores else 0,
            'max_similarity': round(max(scores), 3) if scores else 0,
            'columns_below_threshold': [
                c.column_name for c in result.column_comparisons
                if c.similarity_score < 0.8
            ],
            'sample_size_ratio': round(result.subset_rows / result.source_rows, 4),
            'quality_grade': self._get_quality_grade(result.overall_similarity)
        }

    def _get_quality_grade(self, similarity: float) -> str:
        """Convert similarity score to quality grade"""
        if similarity >= 0.95:
            return "A"
        elif similarity >= 0.90:
            return "B"
        elif similarity >= 0.80:
            return "C"
        elif similarity >= 0.70:
            return "D"
        else:
            return "F"

    def _generate_recommendations(
        self,
        result: DistributionComparison
    ) -> List[str]:
        """Generate recommendations based on analysis"""
        recommendations = []

        # Check for low overall similarity
        if result.overall_similarity < 0.8:
            recommendations.append(
                "Consider using stratified sampling to better preserve distributions"
            )

        # Check for columns with low similarity
        low_sim_cols = [
            c for c in result.column_comparisons
            if c.similarity_score < 0.7
        ]
        if low_sim_cols:
            col_names = [c.column_name for c in low_sim_cols[:3]]
            recommendations.append(
                f"Columns with poor representation: {', '.join(col_names)}. "
                "Consider stratifying on these columns."
            )

        # Check sample size
        ratio = result.subset_rows / result.source_rows
        if ratio < 0.01:
            recommendations.append(
                f"Sample size is very small ({ratio:.1%}). Consider increasing "
                "to at least 1% for better representation."
            )
        elif ratio > 0.5:
            recommendations.append(
                f"Sample size is {ratio:.0%} of original. If storage is a concern, "
                "a smaller sample may be sufficient."
            )

        # Check for missing categories
        for col in result.column_comparisons:
            if any("Missing" in d for d in col.differences):
                recommendations.append(
                    f"Column '{col.column_name}' has missing categories in subset. "
                    "Use stratified sampling to ensure all categories are represented."
                )
                break

        return recommendations
