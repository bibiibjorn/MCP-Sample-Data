"""
Data Quality Scoring Module

Provides comprehensive data quality assessment across multiple dimensions:
- Completeness: Missing/null value analysis
- Uniqueness: Duplicate detection
- Validity: Format and range compliance
- Accuracy: Statistical accuracy and outlier detection
- Consistency: Cross-column and cross-file consistency
- Timeliness: Data freshness analysis

Based on DAMA and ISO 8000 data quality standards.
"""

from .quality_scorer import QualityScorer, QualityScore, QualityReport
from .quality_rules import QualityRules, QualityRule
from .quality_report import QualityReportGenerator

__all__ = [
    'QualityScorer',
    'QualityScore',
    'QualityReport',
    'QualityRules',
    'QualityRule',
    'QualityReportGenerator'
]
