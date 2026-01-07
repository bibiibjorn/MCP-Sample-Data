"""
Statistical Validator Module
Validates data using statistical checks
"""
import polars as pl
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class StatisticalValidator:
    """Performs statistical validation on data"""

    def check_totals(
        self,
        df: pl.DataFrame,
        measure_column: str,
        group_columns: Optional[List[str]] = None,
        expected_total: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Validate that measures sum correctly across groups.

        Args:
            df: DataFrame to check
            measure_column: Column to sum
            group_columns: Hierarchy columns (e.g., ['year', 'quarter', 'month'])
            expected_total: Optional expected grand total

        Returns:
            Total validation results
        """
        try:
            if measure_column not in df.columns:
                return {'success': False, 'error': f'Measure column not found: {measure_column}'}

            grand_total = df[measure_column].sum()

            result = {
                'success': True,
                'grand_total': grand_total,
                'row_count': len(df)
            }

            # Check against expected total
            if expected_total is not None:
                difference = grand_total - expected_total
                result['expected_total'] = expected_total
                result['difference'] = difference
                result['matches_expected'] = abs(difference) < 0.01

            # Generate breakdown by groups
            if group_columns:
                breakdowns = []
                for i, col in enumerate(group_columns):
                    if col not in df.columns:
                        continue

                    group_totals = df.group_by(col).agg([
                        pl.col(measure_column).sum().alias('total'),
                        pl.count().alias('count')
                    ]).sort(col)

                    breakdowns.append({
                        'group_column': col,
                        'level': i + 1,
                        'unique_values': group_totals.height,
                        'totals': group_totals.to_dicts()
                    })

                result['breakdowns'] = breakdowns

                # Check hierarchy consistency
                if len(group_columns) > 1:
                    consistency_check = self._check_hierarchy_consistency(
                        df, measure_column, group_columns
                    )
                    result['hierarchy_consistent'] = consistency_check['consistent']
                    if not consistency_check['consistent']:
                        result['hierarchy_issues'] = consistency_check['issues']

            return result

        except Exception as e:
            logger.error(f"Error checking totals: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _check_hierarchy_consistency(
        self,
        df: pl.DataFrame,
        measure_column: str,
        group_columns: List[str]
    ) -> Dict[str, Any]:
        """Check that totals are consistent across hierarchy levels"""
        issues = []

        # Check that each level sums to the same grand total
        grand_total = df[measure_column].sum()

        for col in group_columns:
            if col not in df.columns:
                continue

            level_total = df.group_by(col).agg(
                pl.col(measure_column).sum()
            )[measure_column].sum()

            if abs(level_total - grand_total) > 0.01:
                issues.append({
                    'level': col,
                    'level_total': level_total,
                    'grand_total': grand_total,
                    'difference': level_total - grand_total
                })

        return {
            'consistent': len(issues) == 0,
            'issues': issues
        }

    def detect_outliers(
        self,
        df: pl.DataFrame,
        column: str,
        method: str = 'iqr',
        threshold: float = 1.5
    ) -> Dict[str, Any]:
        """
        Detect outliers in a numeric column.

        Args:
            df: DataFrame to check
            column: Column to analyze
            method: Detection method ('iqr', 'zscore')
            threshold: Threshold for outlier detection

        Returns:
            Outlier detection results
        """
        try:
            if column not in df.columns:
                return {'success': False, 'error': f'Column not found: {column}'}

            col_data = df[column].drop_nulls()

            if method == 'iqr':
                q1 = col_data.quantile(0.25)
                q3 = col_data.quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - threshold * iqr
                upper_bound = q3 + threshold * iqr

                outliers = df.filter(
                    (pl.col(column) < lower_bound) | (pl.col(column) > upper_bound)
                )

                return {
                    'success': True,
                    'method': 'iqr',
                    'threshold': threshold,
                    'statistics': {
                        'q1': q1,
                        'q3': q3,
                        'iqr': iqr,
                        'lower_bound': lower_bound,
                        'upper_bound': upper_bound
                    },
                    'outlier_count': len(outliers),
                    'outlier_percentage': round(len(outliers) / len(df) * 100, 2),
                    'sample_outliers': outliers[column].head(10).to_list()
                }

            elif method == 'zscore':
                mean = col_data.mean()
                std = col_data.std()

                if std == 0:
                    return {
                        'success': True,
                        'method': 'zscore',
                        'outlier_count': 0,
                        'message': 'Standard deviation is 0, no outliers detected'
                    }

                # Calculate z-scores
                z_scores = (col_data - mean) / std
                outlier_mask = z_scores.abs() > threshold

                outlier_count = outlier_mask.sum()

                return {
                    'success': True,
                    'method': 'zscore',
                    'threshold': threshold,
                    'statistics': {
                        'mean': mean,
                        'std': std
                    },
                    'outlier_count': outlier_count,
                    'outlier_percentage': round(outlier_count / len(df) * 100, 2)
                }

            else:
                return {'success': False, 'error': f'Unknown method: {method}'}

        except Exception as e:
            logger.error(f"Error detecting outliers: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
