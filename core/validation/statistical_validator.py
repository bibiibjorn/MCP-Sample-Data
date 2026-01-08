"""
Statistical Validator Module
Validates data using statistical checks
"""
import polars as pl
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)

# Token optimization constants
MAX_BREAKDOWN_ROWS = 20         # Max rows per breakdown level
SAMPLE_OUTLIER_LIMIT = 10       # Max outlier samples to return


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

            # Generate breakdown by groups (token-optimized)
            if group_columns:
                breakdowns = []
                for i, col in enumerate(group_columns):
                    if col not in df.columns:
                        continue

                    group_totals = df.group_by(col).agg([
                        pl.col(measure_column).sum().alias('total'),
                        pl.count().alias('count')
                    ]).sort('total', descending=True)  # Sort by total for relevance

                    total_groups = group_totals.height
                    # Limit output for token efficiency
                    limited_totals = group_totals.head(MAX_BREAKDOWN_ROWS)

                    breakdown = {
                        'column': col,
                        'level': i + 1,
                        'groups': total_groups,
                        'totals': limited_totals.to_dicts()
                    }

                    if total_groups > MAX_BREAKDOWN_ROWS:
                        breakdown['truncated'] = True
                        breakdown['note'] = f'Showing top {MAX_BREAKDOWN_ROWS} of {total_groups}'

                    breakdowns.append(breakdown)

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
                    'bounds': {
                        'lower': round(lower_bound, 2),
                        'upper': round(upper_bound, 2)
                    },
                    'outlier_count': len(outliers),
                    'outlier_pct': round(len(outliers) / len(df) * 100, 1),
                    'samples': outliers[column].head(SAMPLE_OUTLIER_LIMIT).to_list()
                }

            elif method == 'zscore':
                mean = col_data.mean()
                std = col_data.std()

                if std == 0:
                    return {
                        'success': True,
                        'method': 'zscore',
                        'outlier_count': 0,
                        'note': 'Std=0, no outliers'
                    }

                # Calculate z-scores
                z_scores = (col_data - mean) / std
                outlier_mask = z_scores.abs() > threshold
                outlier_count = outlier_mask.sum()

                return {
                    'success': True,
                    'method': 'zscore',
                    'threshold': threshold,
                    'stats': {
                        'mean': round(mean, 2),
                        'std': round(std, 2)
                    },
                    'outlier_count': outlier_count,
                    'outlier_pct': round(outlier_count / len(df) * 100, 1)
                }

            else:
                return {'success': False, 'error': f'Unknown method: {method}'}

        except Exception as e:
            logger.error(f"Error detecting outliers: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def detect_anomalies(
        self,
        df: pl.DataFrame,
        columns: Optional[List[str]] = None,
        method: str = 'zscore',
        threshold: float = 3.0
    ) -> Dict[str, Any]:
        """
        Detect statistical anomalies across multiple columns.

        Args:
            df: DataFrame to analyze
            columns: Specific columns to analyze (None = all numeric)
            method: Detection method ('zscore', 'iqr')
            threshold: Threshold for outlier detection

        Returns:
            Anomaly detection results
        """
        try:
            # Determine columns to analyze
            if columns:
                target_columns = [c for c in columns if c in df.columns]
            else:
                # Analyze all numeric columns
                numeric_types = {pl.Int64, pl.Int32, pl.Int16, pl.Int8,
                               pl.UInt64, pl.UInt32, pl.UInt16, pl.UInt8,
                               pl.Float64, pl.Float32}
                target_columns = [c for c in df.columns if df[c].dtype in numeric_types]

            if not target_columns:
                return {
                    'success': True,
                    'anomalies_detected': False,
                    'note': 'No numeric columns to analyze'
                }

            results = {
                'success': True,
                'method': method,
                'threshold': threshold,
                'columns_analyzed': len(target_columns),
                'total_rows': len(df),
                'column_results': []
            }

            total_anomalies = 0

            for col in target_columns:
                col_result = self.detect_outliers(df, col, method, threshold)

                if col_result.get('success'):
                    outlier_count = col_result.get('outlier_count', 0)
                    total_anomalies += outlier_count

                    column_summary = {
                        'column': col,
                        'outlier_count': outlier_count,
                        'outlier_pct': col_result.get('outlier_pct', 0)
                    }

                    if 'bounds' in col_result:
                        column_summary['bounds'] = col_result['bounds']
                    if 'stats' in col_result:
                        column_summary['stats'] = col_result['stats']
                    if 'samples' in col_result:
                        column_summary['sample_outliers'] = col_result['samples'][:SAMPLE_OUTLIER_LIMIT]

                    results['column_results'].append(column_summary)

            results['total_anomalies'] = total_anomalies
            results['anomalies_detected'] = total_anomalies > 0

            # Sort by outlier count descending
            results['column_results'].sort(key=lambda x: -x['outlier_count'])

            return results

        except Exception as e:
            logger.error(f"Error detecting anomalies: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
