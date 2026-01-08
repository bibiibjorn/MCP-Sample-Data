"""
Quality Scorer - Main orchestration for data quality scoring

Calculates comprehensive quality scores across multiple dimensions
following DAMA data quality framework.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date
import polars as pl
import numpy as np
from enum import Enum


class QualityGrade(Enum):
    """Quality grade based on overall score"""
    A_PLUS = ("A+", 97, 100)
    A = ("A", 93, 96)
    A_MINUS = ("A-", 90, 92)
    B_PLUS = ("B+", 87, 89)
    B = ("B", 83, 86)
    B_MINUS = ("B-", 80, 82)
    C_PLUS = ("C+", 77, 79)
    C = ("C", 73, 76)
    C_MINUS = ("C-", 70, 72)
    D_PLUS = ("D+", 67, 69)
    D = ("D", 63, 66)
    D_MINUS = ("D-", 60, 62)
    F = ("F", 0, 59)

    @classmethod
    def from_score(cls, score: float) -> 'QualityGrade':
        """Get grade from numeric score"""
        for grade in cls:
            if grade.value[1] <= score <= grade.value[2]:
                return grade
        return cls.F


@dataclass
class DimensionScore:
    """Score for a single quality dimension"""
    dimension: str
    score: float  # 0-100
    weight: float
    weighted_contribution: float
    column_scores: Dict[str, float] = field(default_factory=dict)
    issues: List[Dict[str, Any]] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ColumnQualityScore:
    """Quality score for a single column"""
    column_name: str
    data_type: str
    overall_score: float
    dimension_scores: Dict[str, float] = field(default_factory=dict)
    issues: List[str] = field(default_factory=list)


@dataclass
class QualityIssue:
    """A quality issue found in the data"""
    severity: str  # critical, warning, info
    dimension: str
    column: Optional[str]
    issue_type: str
    description: str
    affected_rows: int
    affected_percentage: float
    sample_values: List[Any] = field(default_factory=list)
    suggested_fix: Optional[str] = None


@dataclass
class QualityScore:
    """Overall quality score result"""
    overall_score: float  # 0-100
    grade: str
    dimension_scores: Dict[str, DimensionScore]
    column_scores: Dict[str, ColumnQualityScore]
    issues: List[QualityIssue]
    critical_count: int
    warning_count: int
    info_count: int
    recommendations: List[str]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityReport:
    """Full quality report with all details"""
    score: QualityScore
    file_path: str
    row_count: int
    column_count: int
    scan_timestamp: datetime
    scan_duration_seconds: float
    rules_applied: List[str] = field(default_factory=list)


class QualityScorer:
    """
    Comprehensive data quality scoring engine.

    Evaluates data across six dimensions:
    - Completeness (25%): Missing/null values
    - Validity (20%): Format and range compliance
    - Uniqueness (15%): Duplicate detection
    - Accuracy (15%): Outlier and anomaly detection
    - Consistency (15%): Cross-column logic
    - Timeliness (10%): Data freshness
    """

    DEFAULT_WEIGHTS = {
        'completeness': 0.25,
        'validity': 0.20,
        'uniqueness': 0.15,
        'accuracy': 0.15,
        'consistency': 0.15,
        'timeliness': 0.10
    }

    def __init__(self, weights: Optional[Dict[str, float]] = None):
        """
        Initialize quality scorer.

        Args:
            weights: Custom weights for each dimension (must sum to 1.0)
        """
        self.weights = weights or self.DEFAULT_WEIGHTS.copy()
        self._validate_weights()

    def _validate_weights(self):
        """Validate that weights sum to 1.0"""
        total = sum(self.weights.values())
        if abs(total - 1.0) > 0.001:
            # Normalize weights
            self.weights = {k: v / total for k, v in self.weights.items()}

    def score(
        self,
        df: pl.DataFrame,
        rules: Optional[Dict[str, Any]] = None,
        dimensions: Optional[List[str]] = None,
        key_columns: Optional[List[str]] = None,
        date_column: Optional[str] = None,
        reference_date: Optional[date] = None
    ) -> QualityScore:
        """
        Calculate comprehensive quality score.

        Args:
            df: DataFrame to score
            rules: Custom validation rules per column
            dimensions: Specific dimensions to score (default: all)
            key_columns: Columns that should be unique
            date_column: Column for timeliness scoring
            reference_date: Reference date for timeliness (default: today)

        Returns:
            QualityScore with overall score and dimension breakdown
        """
        dimensions = dimensions or list(self.weights.keys())
        rules = rules or {}
        reference_date = reference_date or date.today()

        dimension_scores: Dict[str, DimensionScore] = {}
        all_issues: List[QualityIssue] = []
        column_scores: Dict[str, ColumnQualityScore] = {}

        # Initialize column scores
        for col in df.columns:
            dtype = str(df[col].dtype)
            column_scores[col] = ColumnQualityScore(
                column_name=col,
                data_type=dtype,
                overall_score=100.0,
                dimension_scores={},
                issues=[]
            )

        # Score each dimension
        if 'completeness' in dimensions:
            dim_score, issues = self._score_completeness(df, rules)
            dimension_scores['completeness'] = dim_score
            all_issues.extend(issues)
            self._update_column_scores(column_scores, 'completeness', dim_score)

        if 'uniqueness' in dimensions:
            dim_score, issues = self._score_uniqueness(df, key_columns, rules)
            dimension_scores['uniqueness'] = dim_score
            all_issues.extend(issues)
            self._update_column_scores(column_scores, 'uniqueness', dim_score)

        if 'validity' in dimensions:
            dim_score, issues = self._score_validity(df, rules)
            dimension_scores['validity'] = dim_score
            all_issues.extend(issues)
            self._update_column_scores(column_scores, 'validity', dim_score)

        if 'accuracy' in dimensions:
            dim_score, issues = self._score_accuracy(df, rules)
            dimension_scores['accuracy'] = dim_score
            all_issues.extend(issues)
            self._update_column_scores(column_scores, 'accuracy', dim_score)

        if 'consistency' in dimensions:
            dim_score, issues = self._score_consistency(df, rules)
            dimension_scores['consistency'] = dim_score
            all_issues.extend(issues)
            self._update_column_scores(column_scores, 'consistency', dim_score)

        if 'timeliness' in dimensions:
            dim_score, issues = self._score_timeliness(df, date_column, reference_date, rules)
            dimension_scores['timeliness'] = dim_score
            all_issues.extend(issues)
            self._update_column_scores(column_scores, 'timeliness', dim_score)

        # Calculate overall score
        overall_score = sum(
            dimension_scores[dim].weighted_contribution
            for dim in dimension_scores
        )

        # Finalize column scores
        for col in column_scores:
            if column_scores[col].dimension_scores:
                column_scores[col].overall_score = np.mean(
                    list(column_scores[col].dimension_scores.values())
                )

        # Get grade
        grade = QualityGrade.from_score(overall_score).value[0]

        # Count issues by severity
        critical_count = sum(1 for i in all_issues if i.severity == 'critical')
        warning_count = sum(1 for i in all_issues if i.severity == 'warning')
        info_count = sum(1 for i in all_issues if i.severity == 'info')

        # Generate recommendations
        recommendations = self._generate_recommendations(
            dimension_scores, all_issues, df
        )

        return QualityScore(
            overall_score=round(overall_score, 2),
            grade=grade,
            dimension_scores=dimension_scores,
            column_scores=column_scores,
            issues=all_issues,
            critical_count=critical_count,
            warning_count=warning_count,
            info_count=info_count,
            recommendations=recommendations,
            metadata={
                'row_count': len(df),
                'column_count': len(df.columns),
                'dimensions_scored': dimensions
            }
        )

    def _update_column_scores(
        self,
        column_scores: Dict[str, ColumnQualityScore],
        dimension: str,
        dim_score: DimensionScore
    ):
        """Update column-level scores from dimension score"""
        for col, score in dim_score.column_scores.items():
            if col in column_scores:
                column_scores[col].dimension_scores[dimension] = score
                for issue in dim_score.issues:
                    if issue.get('column') == col:
                        column_scores[col].issues.append(issue.get('description', ''))

    def _score_completeness(
        self,
        df: pl.DataFrame,
        rules: Dict[str, Any]
    ) -> Tuple[DimensionScore, List[QualityIssue]]:
        """Score completeness - missing/null values"""
        column_scores = {}
        issues = []
        total_cells = len(df) * len(df.columns)
        total_null = 0

        for col in df.columns:
            null_count = df[col].null_count()
            total_null += null_count
            null_pct = (null_count / len(df)) * 100 if len(df) > 0 else 0
            col_score = 100 - null_pct
            column_scores[col] = col_score

            # Check against rules
            max_null_pct = rules.get(col, {}).get('max_null_pct', 100)
            if null_pct > max_null_pct:
                severity = 'critical' if null_pct > 20 else 'warning' if null_pct > 5 else 'info'
                issues.append(QualityIssue(
                    severity=severity,
                    dimension='completeness',
                    column=col,
                    issue_type='missing_values',
                    description=f'{null_pct:.1f}% null values (threshold: {max_null_pct}%)',
                    affected_rows=null_count,
                    affected_percentage=null_pct,
                    suggested_fix=f'Review and fill missing values in {col}'
                ))
            elif null_pct > 0:
                issues.append(QualityIssue(
                    severity='info',
                    dimension='completeness',
                    column=col,
                    issue_type='missing_values',
                    description=f'{null_pct:.1f}% null values',
                    affected_rows=null_count,
                    affected_percentage=null_pct
                ))

        # Calculate dimension score
        if column_scores:
            score = np.mean(list(column_scores.values()))
        else:
            score = 100.0

        weight = self.weights.get('completeness', 0.25)

        return DimensionScore(
            dimension='completeness',
            score=round(score, 2),
            weight=weight,
            weighted_contribution=round(score * weight, 2),
            column_scores=column_scores,
            issues=[{
                'column': i.column,
                'description': i.description,
                'severity': i.severity
            } for i in issues],
            details={
                'total_cells': total_cells,
                'null_cells': total_null,
                'null_percentage': round((total_null / total_cells) * 100, 2) if total_cells > 0 else 0
            }
        ), issues

    def _score_uniqueness(
        self,
        df: pl.DataFrame,
        key_columns: Optional[List[str]],
        rules: Dict[str, Any]
    ) -> Tuple[DimensionScore, List[QualityIssue]]:
        """Score uniqueness - duplicate detection"""
        column_scores = {}
        issues = []
        total_rows = len(df)

        # Check for duplicate rows
        unique_rows = df.unique().shape[0]
        duplicate_rows = total_rows - unique_rows
        dup_pct = (duplicate_rows / total_rows) * 100 if total_rows > 0 else 0

        if duplicate_rows > 0:
            severity = 'critical' if dup_pct > 10 else 'warning' if dup_pct > 1 else 'info'
            issues.append(QualityIssue(
                severity=severity,
                dimension='uniqueness',
                column=None,
                issue_type='duplicate_rows',
                description=f'{duplicate_rows:,} duplicate rows ({dup_pct:.2f}%)',
                affected_rows=duplicate_rows,
                affected_percentage=dup_pct,
                suggested_fix='Review and deduplicate records'
            ))

        row_uniqueness_score = 100 - dup_pct

        # Check key columns for uniqueness
        key_columns = key_columns or []
        for col in df.columns:
            unique_values = df[col].n_unique()
            uniqueness_ratio = (unique_values / total_rows) * 100 if total_rows > 0 else 100
            column_scores[col] = uniqueness_ratio

            # Key columns should be fully unique
            if col in key_columns and unique_values < total_rows:
                dup_count = total_rows - unique_values
                issues.append(QualityIssue(
                    severity='critical',
                    dimension='uniqueness',
                    column=col,
                    issue_type='key_not_unique',
                    description=f'Key column has {dup_count:,} duplicate values',
                    affected_rows=dup_count,
                    affected_percentage=(dup_count / total_rows) * 100,
                    suggested_fix=f'Ensure {col} contains unique values'
                ))

        # Combine row and column uniqueness
        score = (row_uniqueness_score + np.mean(list(column_scores.values()))) / 2 if column_scores else row_uniqueness_score
        weight = self.weights.get('uniqueness', 0.15)

        return DimensionScore(
            dimension='uniqueness',
            score=round(score, 2),
            weight=weight,
            weighted_contribution=round(score * weight, 2),
            column_scores=column_scores,
            issues=[{
                'column': i.column,
                'description': i.description,
                'severity': i.severity
            } for i in issues],
            details={
                'total_rows': total_rows,
                'unique_rows': unique_rows,
                'duplicate_rows': duplicate_rows,
                'duplicate_percentage': round(dup_pct, 2)
            }
        ), issues

    def _score_validity(
        self,
        df: pl.DataFrame,
        rules: Dict[str, Any]
    ) -> Tuple[DimensionScore, List[QualityIssue]]:
        """Score validity - format and range compliance"""
        column_scores = {}
        issues = []
        total_rows = len(df)

        for col in df.columns:
            valid_count = total_rows
            col_rules = rules.get(col, {})
            col_issues = []

            # Range validation for numeric columns
            if df[col].dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                                  pl.Float32, pl.Float64]:
                non_null = df[col].drop_nulls()
                if len(non_null) > 0:
                    min_val = col_rules.get('min')
                    max_val = col_rules.get('max')

                    if min_val is not None:
                        below_min = (non_null < min_val).sum()
                        if below_min > 0:
                            valid_count -= below_min
                            col_issues.append(f'{below_min} values below min ({min_val})')

                    if max_val is not None:
                        above_max = (non_null > max_val).sum()
                        if above_max > 0:
                            valid_count -= above_max
                            col_issues.append(f'{above_max} values above max ({max_val})')

            # Enum validation
            allowed_values = col_rules.get('enum') or col_rules.get('allowed_values')
            if allowed_values:
                invalid = df.filter(~pl.col(col).is_in(allowed_values)).shape[0]
                if invalid > 0:
                    valid_count -= invalid
                    col_issues.append(f'{invalid} values not in allowed list')

            # Regex validation for string columns
            pattern = col_rules.get('pattern') or col_rules.get('regex')
            if pattern and df[col].dtype == pl.Utf8:
                try:
                    matches = df[col].str.contains(pattern).sum()
                    non_matches = total_rows - matches - df[col].null_count()
                    if non_matches > 0:
                        valid_count -= non_matches
                        col_issues.append(f'{non_matches} values don\'t match pattern')
                except Exception:
                    pass  # Skip invalid regex

            # Calculate column validity score
            col_score = (valid_count / total_rows) * 100 if total_rows > 0 else 100
            column_scores[col] = col_score

            # Create issues
            for issue_desc in col_issues:
                issues.append(QualityIssue(
                    severity='warning' if col_score >= 90 else 'critical',
                    dimension='validity',
                    column=col,
                    issue_type='invalid_values',
                    description=issue_desc,
                    affected_rows=total_rows - valid_count,
                    affected_percentage=100 - col_score,
                    suggested_fix=f'Review invalid values in {col}'
                ))

        score = np.mean(list(column_scores.values())) if column_scores else 100.0
        weight = self.weights.get('validity', 0.20)

        return DimensionScore(
            dimension='validity',
            score=round(score, 2),
            weight=weight,
            weighted_contribution=round(score * weight, 2),
            column_scores=column_scores,
            issues=[{
                'column': i.column,
                'description': i.description,
                'severity': i.severity
            } for i in issues],
            details={}
        ), issues

    def _score_accuracy(
        self,
        df: pl.DataFrame,
        rules: Dict[str, Any]
    ) -> Tuple[DimensionScore, List[QualityIssue]]:
        """Score accuracy - outlier and anomaly detection"""
        column_scores = {}
        issues = []
        total_rows = len(df)

        for col in df.columns:
            col_score = 100.0

            # Only check numeric columns for outliers
            if df[col].dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                                  pl.Float32, pl.Float64]:
                non_null = df[col].drop_nulls()
                if len(non_null) > 10:  # Need enough data for statistics
                    values = non_null.to_numpy()

                    # Z-score based outlier detection
                    mean = np.mean(values)
                    std = np.std(values)

                    if std > 0:
                        z_scores = np.abs((values - mean) / std)
                        outlier_count = np.sum(z_scores > 3)  # 3 sigma rule

                        if outlier_count > 0:
                            outlier_pct = (outlier_count / len(values)) * 100
                            col_score = 100 - min(outlier_pct * 2, 30)  # Cap impact at 30%

                            if outlier_pct > 5:
                                issues.append(QualityIssue(
                                    severity='warning',
                                    dimension='accuracy',
                                    column=col,
                                    issue_type='outliers',
                                    description=f'{outlier_count} outliers detected ({outlier_pct:.1f}%)',
                                    affected_rows=int(outlier_count),
                                    affected_percentage=outlier_pct,
                                    suggested_fix=f'Review outliers in {col}'
                                ))

            column_scores[col] = col_score

        score = np.mean(list(column_scores.values())) if column_scores else 100.0
        weight = self.weights.get('accuracy', 0.15)

        return DimensionScore(
            dimension='accuracy',
            score=round(score, 2),
            weight=weight,
            weighted_contribution=round(score * weight, 2),
            column_scores=column_scores,
            issues=[{
                'column': i.column,
                'description': i.description,
                'severity': i.severity
            } for i in issues],
            details={}
        ), issues

    def _score_consistency(
        self,
        df: pl.DataFrame,
        rules: Dict[str, Any]
    ) -> Tuple[DimensionScore, List[QualityIssue]]:
        """Score consistency - cross-column logic"""
        issues = []
        checks_passed = 0
        total_checks = 0

        # Check consistency rules
        consistency_rules = rules.get('_consistency', [])

        for rule in consistency_rules:
            total_checks += 1
            rule_type = rule.get('type')

            if rule_type == 'date_order':
                # Check that date1 <= date2
                col1, col2 = rule.get('columns', [None, None])
                if col1 in df.columns and col2 in df.columns:
                    invalid = df.filter(pl.col(col1) > pl.col(col2)).shape[0]
                    if invalid == 0:
                        checks_passed += 1
                    else:
                        issues.append(QualityIssue(
                            severity='warning',
                            dimension='consistency',
                            column=f'{col1}, {col2}',
                            issue_type='date_order',
                            description=f'{invalid} rows where {col1} > {col2}',
                            affected_rows=invalid,
                            affected_percentage=(invalid / len(df)) * 100
                        ))

            elif rule_type == 'sum_equals':
                # Check that sum of columns equals total column
                columns = rule.get('columns', [])
                total_col = rule.get('total_column')
                tolerance = rule.get('tolerance', 0.01)

                if all(c in df.columns for c in columns) and total_col in df.columns:
                    sum_expr = sum(pl.col(c) for c in columns)
                    invalid = df.filter(
                        (pl.col(total_col) - sum_expr).abs() > tolerance
                    ).shape[0]

                    if invalid == 0:
                        checks_passed += 1
                    else:
                        issues.append(QualityIssue(
                            severity='warning',
                            dimension='consistency',
                            column=total_col,
                            issue_type='sum_mismatch',
                            description=f'{invalid} rows where sum != {total_col}',
                            affected_rows=invalid,
                            affected_percentage=(invalid / len(df)) * 100
                        ))

        # Auto-detect common consistency issues if no explicit rules
        if total_checks == 0:
            # Look for date pairs
            date_cols = [c for c in df.columns if df[c].dtype == pl.Date or
                        any(x in c.lower() for x in ['date', 'time', 'start', 'end'])]

            for i, col1 in enumerate(date_cols):
                for col2 in date_cols[i+1:]:
                    if 'start' in col1.lower() and 'end' in col2.lower():
                        total_checks += 1
                        try:
                            invalid = df.filter(pl.col(col1) > pl.col(col2)).shape[0]
                            if invalid == 0:
                                checks_passed += 1
                            else:
                                issues.append(QualityIssue(
                                    severity='warning',
                                    dimension='consistency',
                                    column=f'{col1}, {col2}',
                                    issue_type='date_order',
                                    description=f'{invalid} rows where {col1} > {col2}',
                                    affected_rows=invalid,
                                    affected_percentage=(invalid / len(df)) * 100
                                ))
                        except Exception:
                            pass

        # Calculate score
        if total_checks > 0:
            score = (checks_passed / total_checks) * 100
        else:
            score = 100.0  # No checks = assume consistent

        weight = self.weights.get('consistency', 0.15)

        return DimensionScore(
            dimension='consistency',
            score=round(score, 2),
            weight=weight,
            weighted_contribution=round(score * weight, 2),
            column_scores={},
            issues=[{
                'column': i.column,
                'description': i.description,
                'severity': i.severity
            } for i in issues],
            details={
                'checks_performed': total_checks,
                'checks_passed': checks_passed
            }
        ), issues

    def _score_timeliness(
        self,
        df: pl.DataFrame,
        date_column: Optional[str],
        reference_date: date,
        rules: Dict[str, Any]
    ) -> Tuple[DimensionScore, List[QualityIssue]]:
        """Score timeliness - data freshness"""
        issues = []
        score = 100.0

        if date_column and date_column in df.columns:
            try:
                col = df[date_column].drop_nulls()
                if len(col) > 0:
                    # Convert to date if datetime
                    if col.dtype == pl.Datetime:
                        col = col.dt.date()

                    max_date = col.max()
                    if max_date:
                        days_old = (reference_date - max_date).days

                        # Timeliness rules
                        max_age = rules.get(date_column, {}).get('max_age_days', 365)

                        if days_old > max_age:
                            score = max(0, 100 - ((days_old - max_age) / max_age) * 100)
                            issues.append(QualityIssue(
                                severity='warning' if days_old < max_age * 2 else 'critical',
                                dimension='timeliness',
                                column=date_column,
                                issue_type='stale_data',
                                description=f'Data is {days_old} days old (max: {max_age})',
                                affected_rows=len(df),
                                affected_percentage=100,
                                suggested_fix='Update data to include more recent records'
                            ))
                        else:
                            # Good timeliness - slight deduction based on age
                            score = max(80, 100 - (days_old / max_age) * 20)
            except Exception:
                pass  # Unable to parse dates

        weight = self.weights.get('timeliness', 0.10)

        return DimensionScore(
            dimension='timeliness',
            score=round(score, 2),
            weight=weight,
            weighted_contribution=round(score * weight, 2),
            column_scores={date_column: score} if date_column else {},
            issues=[{
                'column': i.column,
                'description': i.description,
                'severity': i.severity
            } for i in issues],
            details={
                'date_column': date_column,
                'reference_date': str(reference_date)
            }
        ), issues

    def _generate_recommendations(
        self,
        dimension_scores: Dict[str, DimensionScore],
        issues: List[QualityIssue],
        df: pl.DataFrame
    ) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []

        # Priority: critical issues first
        critical_issues = [i for i in issues if i.severity == 'critical']
        for issue in critical_issues[:3]:  # Top 3 critical
            if issue.suggested_fix:
                recommendations.append(f"[CRITICAL] {issue.suggested_fix}")

        # Dimension-specific recommendations
        for dim, score in dimension_scores.items():
            if score.score < 80:
                if dim == 'completeness':
                    null_cols = [c for c, s in score.column_scores.items() if s < 90]
                    if null_cols:
                        recommendations.append(
                            f"Address missing values in: {', '.join(null_cols[:3])}"
                        )
                elif dim == 'uniqueness':
                    if score.details.get('duplicate_rows', 0) > 0:
                        recommendations.append(
                            f"Remove {score.details['duplicate_rows']:,} duplicate rows"
                        )
                elif dim == 'validity':
                    recommendations.append(
                        "Review data validation rules for invalid values"
                    )
                elif dim == 'accuracy':
                    recommendations.append(
                        "Investigate statistical outliers for potential data errors"
                    )
                elif dim == 'consistency':
                    recommendations.append(
                        "Review cross-column consistency rules"
                    )
                elif dim == 'timeliness':
                    recommendations.append(
                        "Update data with more recent records"
                    )

        return recommendations[:10]  # Limit to 10 recommendations


def compare_quality(
    score_a: QualityScore,
    score_b: QualityScore
) -> Dict[str, Any]:
    """
    Compare two quality scores.

    Args:
        score_a: Baseline score
        score_b: Comparison score

    Returns:
        Comparison results with deltas
    """
    overall_delta = score_b.overall_score - score_a.overall_score

    dimension_deltas = {}
    improved = []
    degraded = []

    for dim in score_a.dimension_scores:
        if dim in score_b.dimension_scores:
            delta = score_b.dimension_scores[dim].score - score_a.dimension_scores[dim].score
            dimension_deltas[dim] = delta
            if delta > 1:
                improved.append(dim)
            elif delta < -1:
                degraded.append(dim)

    return {
        'overall_delta': round(overall_delta, 2),
        'overall_improved': overall_delta > 0,
        'grade_change': f"{score_a.grade} -> {score_b.grade}",
        'dimension_deltas': dimension_deltas,
        'improved_dimensions': improved,
        'degraded_dimensions': degraded,
        'new_issues': score_b.critical_count - score_a.critical_count,
        'issues_resolved': max(0, score_a.critical_count - score_b.critical_count)
    }
