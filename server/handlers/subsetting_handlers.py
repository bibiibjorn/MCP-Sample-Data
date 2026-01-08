"""
MCP Handlers for data subsetting tools.

Tools:
- 10_create_subset: Create representative data subsets
- 10_analyze_subset: Compare subset distributions with original
"""

import os
from typing import Any, Dict, List, Optional
from pathlib import Path

import polars as pl

from core.subsetting import (
    SubsetEngine,
    SubsetConfig,
    SamplingStrategy,
    DistributionAnalyzer
)
from server.tool_schemas import TOOL_SCHEMAS


def register_subsetting_handlers(registry):
    """Register all subsetting-related tool handlers"""

    # =========================================================================
    # 10_create_subset
    # =========================================================================
    def create_subset(
        file_path: str,
        output_path: Optional[str] = None,
        strategy: str = "random",
        target_rows: Optional[int] = None,
        target_percentage: Optional[float] = None,
        stratify_columns: Optional[List[str]] = None,
        time_column: Optional[str] = None,
        time_start: Optional[str] = None,
        time_end: Optional[str] = None,
        related_files: Optional[Dict[str, str]] = None,
        key_column: Optional[str] = None,
        preserve_proportions: bool = True,
        seed: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Create a representative subset of a data file.

        Args:
            file_path: Path to source file
            output_path: Path for subset output (default: adds _subset suffix)
            strategy: Sampling strategy - "random", "stratified", "time_window",
                     "referential", "top_n", "systematic"
            target_rows: Target number of rows (use this OR target_percentage)
            target_percentage: Target percentage of rows (e.g., 10 for 10%)
            stratify_columns: Columns to stratify by (for stratified sampling)
            time_column: Date column for time-based sampling
            time_start: Start date (YYYY-MM-DD) for time window
            time_end: End date (YYYY-MM-DD) for time window
            related_files: Dict of {file_path: key_column} for referential sampling
            key_column: Primary key column for referential integrity
            preserve_proportions: Maintain category proportions in stratified sampling
            seed: Random seed for reproducibility

        Returns:
            Subset creation results
        """
        try:
            # Parse strategy
            try:
                sampling_strategy = SamplingStrategy(strategy.lower())
            except ValueError:
                return {
                    'success': False,
                    'error': f"Unknown strategy: {strategy}. Valid: {[s.value for s in SamplingStrategy]}"
                }

            # Build config
            config = SubsetConfig(
                source_path=file_path,
                output_path=output_path,
                strategy=sampling_strategy,
                target_rows=target_rows,
                target_percentage=target_percentage,
                stratify_columns=stratify_columns,
                time_column=time_column,
                time_start=time_start,
                time_end=time_end,
                related_files=related_files,
                key_column=key_column,
                preserve_proportions=preserve_proportions,
                seed=seed
            )

            # Create subset
            engine = SubsetEngine(seed=seed)
            result = engine.create_subset(config)

            return {
                'success': result.success,
                'source_file': result.source_path,
                'output_file': result.output_path,
                'source_rows': result.source_rows,
                'subset_rows': result.subset_rows,
                'reduction_percentage': result.reduction_percentage,
                'strategy_used': result.strategy_used,
                'related_files_created': result.related_files_created,
                'distribution_preserved': result.distribution_preserved,
                'seed_used': seed or engine.seed,
                'errors': result.errors,
                'warnings': result.warnings
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    schema = TOOL_SCHEMAS['10_create_subset']
    registry.register(
        '10_create_subset',
        create_subset,
        'subsetting',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # =========================================================================
    # 10_analyze_subset
    # =========================================================================
    def analyze_subset(
        source_file: str,
        subset_file: str,
        columns: Optional[List[str]] = None,
        detailed: bool = True
    ) -> Dict[str, Any]:
        """
        Analyze how well a subset represents the original data.

        Args:
            source_file: Path to original file
            subset_file: Path to subset file
            columns: Specific columns to compare (default: all)
            detailed: Include detailed statistics and differences

        Returns:
            Distribution comparison results
        """
        try:
            # Verify files exist
            source_path = Path(source_file)
            subset_path = Path(subset_file)

            if not source_path.exists():
                return {
                    'success': False,
                    'error': f"Source file not found: {source_file}"
                }

            if not subset_path.exists():
                return {
                    'success': False,
                    'error': f"Subset file not found: {subset_file}"
                }

            # Run analysis
            analyzer = DistributionAnalyzer()
            result = analyzer.compare_files(
                str(source_path),
                str(subset_path),
                columns=columns,
                detailed=detailed
            )

            # Format column comparisons
            column_details = []
            for col in result.column_comparisons:
                col_data = {
                    'column': col.column_name,
                    'type': col.column_type,
                    'similarity': col.similarity_score,
                    'status': _get_status(col.similarity_score)
                }

                if detailed:
                    col_data['original_stats'] = col.original_stats
                    col_data['subset_stats'] = col.subset_stats
                    col_data['differences'] = col.differences
                    col_data['warnings'] = col.warnings

                column_details.append(col_data)

            return {
                'success': True,
                'source_file': result.source_file,
                'subset_file': result.subset_file,
                'source_rows': result.source_rows,
                'subset_rows': result.subset_rows,
                'sample_ratio': round(result.subset_rows / result.source_rows, 4),
                'overall_similarity': result.overall_similarity,
                'quality_grade': result.summary.get('quality_grade', 'N/A'),
                'columns_compared': len(result.column_comparisons),
                'columns_below_threshold': result.summary.get('columns_below_threshold', []),
                'column_details': column_details,
                'recommendations': result.recommendations
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    schema = TOOL_SCHEMAS['10_analyze_subset']
    registry.register(
        '10_analyze_subset',
        analyze_subset,
        'subsetting',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )


# =============================================================================
# Helper Functions
# =============================================================================

def _get_status(similarity: float) -> str:
    """Convert similarity score to status"""
    if similarity >= 0.9:
        return "excellent"
    elif similarity >= 0.8:
        return "good"
    elif similarity >= 0.7:
        return "acceptable"
    elif similarity >= 0.5:
        return "poor"
    else:
        return "very_poor"
