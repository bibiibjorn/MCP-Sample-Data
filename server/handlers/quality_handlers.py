"""
Quality Handlers - MCP tool handlers for data quality scoring

Provides handlers for:
- 04_score_data_quality: Calculate comprehensive quality scores
- 04_compare_quality: Compare quality between files
- 04_generate_quality_report: Generate detailed reports
"""

from typing import Dict, Any, Optional, List
from datetime import datetime, date
from pathlib import Path
import time

from ..tool_schemas import TOOL_SCHEMAS
from .file_utils import read_data_file, truncate_row_data


def register_quality_handlers(registry):
    """Register quality scoring handlers with the registry"""

    from core.quality import QualityScorer, QualityRules, QualityReportGenerator
    from core.quality.quality_scorer import QualityReport, compare_quality

    scorer = QualityScorer()
    report_generator = QualityReportGenerator()

    def score_data_quality(
        file_path: str,
        dimensions: Optional[List[str]] = None,
        rules: Optional[Dict[str, Any]] = None,
        date_column: Optional[str] = None,
        key_columns: Optional[List[str]] = None,
        rules_file: Optional[str] = None
    ) -> Dict[str, Any]:
        """Calculate comprehensive data quality score"""
        try:
            start_time = time.time()

            # Load data
            df = read_data_file(file_path)
            if df is None:
                return {'success': False, 'error': f'Unable to read file: {file_path}'}

            # Load rules from file if provided
            if rules_file:
                try:
                    quality_rules = QualityRules.from_yaml(rules_file)
                    rules = quality_rules.to_scorer_format()
                except Exception as e:
                    return {'success': False, 'error': f'Error loading rules file: {e}'}

            rules = rules or {}

            # Calculate score
            quality_score = scorer.score(
                df=df,
                rules=rules,
                dimensions=dimensions,
                key_columns=key_columns,
                date_column=date_column,
                reference_date=date.today()
            )

            scan_duration = time.time() - start_time

            # Format response
            dimension_breakdown = {}
            for dim_name, dim_score in quality_score.dimension_scores.items():
                dimension_breakdown[dim_name] = {
                    'score': dim_score.score,
                    'weight': f"{dim_score.weight:.0%}",
                    'contribution': dim_score.weighted_contribution,
                    'issues': len(dim_score.issues),
                    'details': dim_score.details
                }

            # Get worst columns
            worst_columns = sorted(
                [(name, cs.overall_score) for name, cs in quality_score.column_scores.items()],
                key=lambda x: x[1]
            )[:5]

            # Format issues for response
            issues_summary = []
            for issue in quality_score.issues[:10]:
                issues_summary.append({
                    'severity': issue.severity,
                    'dimension': issue.dimension,
                    'column': issue.column,
                    'description': issue.description,
                    'affected_rows': issue.affected_rows
                })

            return {
                'success': True,
                'overall_score': quality_score.overall_score,
                'grade': quality_score.grade,
                'dimensions': dimension_breakdown,
                'issue_summary': {
                    'critical': quality_score.critical_count,
                    'warning': quality_score.warning_count,
                    'info': quality_score.info_count
                },
                'issues': issues_summary,
                'worst_columns': [
                    {'column': col, 'score': score}
                    for col, score in worst_columns
                ],
                'recommendations': quality_score.recommendations,
                'metadata': {
                    'file': file_path,
                    'rows': len(df),
                    'columns': len(df.columns),
                    'scan_duration_seconds': round(scan_duration, 2)
                }
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    def compare_quality_scores(
        file_path_a: str,
        file_path_b: str,
        dimensions: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Compare quality scores between two files"""
        try:
            # Score first file
            df_a = read_data_file(file_path_a)
            if df_a is None:
                return {'success': False, 'error': f'Unable to read file: {file_path_a}'}

            score_a = scorer.score(df=df_a, dimensions=dimensions)

            # Score second file
            df_b = read_data_file(file_path_b)
            if df_b is None:
                return {'success': False, 'error': f'Unable to read file: {file_path_b}'}

            score_b = scorer.score(df=df_b, dimensions=dimensions)

            # Compare
            comparison = compare_quality(score_a, score_b)

            return {
                'success': True,
                'file_a': {
                    'path': file_path_a,
                    'score': score_a.overall_score,
                    'grade': score_a.grade,
                    'issues': score_a.critical_count + score_a.warning_count
                },
                'file_b': {
                    'path': file_path_b,
                    'score': score_b.overall_score,
                    'grade': score_b.grade,
                    'issues': score_b.critical_count + score_b.warning_count
                },
                'comparison': {
                    'overall_delta': comparison['overall_delta'],
                    'improved': comparison['overall_improved'],
                    'grade_change': comparison['grade_change'],
                    'improved_dimensions': comparison['improved_dimensions'],
                    'degraded_dimensions': comparison['degraded_dimensions'],
                    'dimension_deltas': comparison['dimension_deltas'],
                    'new_issues': comparison['new_issues'],
                    'issues_resolved': comparison['issues_resolved']
                },
                'verdict': _get_comparison_verdict(comparison)
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    def generate_quality_report(
        file_path: str,
        output_path: str,
        format: str = 'markdown',
        include_recommendations: bool = True,
        include_column_details: bool = True,
        rules: Optional[Dict[str, Any]] = None,
        dimensions: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Generate detailed quality report"""
        try:
            start_time = time.time()

            # Load and score data
            df = read_data_file(file_path)
            if df is None:
                return {'success': False, 'error': f'Unable to read file: {file_path}'}

            quality_score = scorer.score(
                df=df,
                rules=rules or {},
                dimensions=dimensions
            )

            scan_duration = time.time() - start_time

            # Create report object
            report = QualityReport(
                score=quality_score,
                file_path=file_path,
                row_count=len(df),
                column_count=len(df.columns),
                scan_timestamp=datetime.now(),
                scan_duration_seconds=scan_duration,
                rules_applied=list((rules or {}).keys())
            )

            # Validate format
            format = format.lower()
            if format not in ['markdown', 'md', 'html', 'json']:
                return {'success': False, 'error': f'Unsupported format: {format}'}

            # Generate report
            report_generator.save_report(
                report=report,
                output_path=output_path,
                format=format,
                include_recommendations=include_recommendations,
                include_column_details=include_column_details
            )

            return {
                'success': True,
                'report_path': output_path,
                'format': format,
                'overall_score': quality_score.overall_score,
                'grade': quality_score.grade,
                'issues_found': quality_score.critical_count + quality_score.warning_count + quality_score.info_count,
                'recommendations_count': len(quality_score.recommendations)
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _get_comparison_verdict(comparison: Dict[str, Any]) -> str:
        """Generate human-readable verdict from comparison"""
        delta = comparison['overall_delta']

        if delta > 5:
            return f"Significant improvement (+{delta:.1f} points)"
        elif delta > 0:
            return f"Slight improvement (+{delta:.1f} points)"
        elif delta > -5:
            return f"Slight degradation ({delta:.1f} points)"
        else:
            return f"Significant degradation ({delta:.1f} points)"

    # Register handlers
    schema = TOOL_SCHEMAS.get('04_score_data_quality', {})
    registry.register(
        '04_score_data_quality',
        score_data_quality,
        'validation',
        schema.get('description', 'Calculate comprehensive data quality score'),
        schema.get('parameters', {}),
        schema.get('required', ['file_path'])
    )

    schema = TOOL_SCHEMAS.get('04_compare_quality', {})
    registry.register(
        '04_compare_quality',
        compare_quality_scores,
        'validation',
        schema.get('description', 'Compare quality scores between files'),
        schema.get('parameters', {}),
        schema.get('required', ['file_path_a', 'file_path_b'])
    )

    schema = TOOL_SCHEMAS.get('04_generate_quality_report', {})
    registry.register(
        '04_generate_quality_report',
        generate_quality_report,
        'validation',
        schema.get('description', 'Generate detailed quality report'),
        schema.get('parameters', {}),
        schema.get('required', ['file_path', 'output_path'])
    )
