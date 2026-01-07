"""
Pattern Analyzer Module
Detects patterns in data columns (dates, codes, currencies, etc.)
"""
import polars as pl
from typing import Dict, Any, List, Optional
import re
import logging

logger = logging.getLogger(__name__)


class PatternAnalyzer:
    """Analyzes patterns in data columns"""

    def __init__(self):
        self.patterns = {
            'email': r'^[\w\.-]+@[\w\.-]+\.\w+$',
            'phone': r'^[\+\d\s\-\(\)]{7,20}$',
            'date_iso': r'^\d{4}-\d{2}-\d{2}$',
            'date_us': r'^\d{2}/\d{2}/\d{4}$',
            'date_eu': r'^\d{2}\.\d{2}\.\d{4}$',
            'uuid': r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$',
            'ip_address': r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$',
            'url': r'^https?://[\w\.-]+',
            'currency_symbol': r'^[\$\€\£\¥][\d,\.]+$',
            'percentage': r'^[\d\.]+%$',
            'code_numeric': r'^\d{4,10}$',
            'code_alpha': r'^[A-Z]{2,5}$',
            'code_alphanumeric': r'^[A-Z0-9]{3,10}$'
        }

    def analyze(self, col_data: pl.Series, col_name: str) -> Dict[str, Any]:
        """Analyze patterns in a column"""
        result = {
            'patterns_detected': [],
            'likely_type': 'unknown',
            'sample_values': col_data.head(5).to_list()
        }

        # Skip null-only columns
        non_null = col_data.drop_nulls()
        if len(non_null) == 0:
            result['likely_type'] = 'all_null'
            return result

        # Get sample values as strings
        try:
            sample_values = non_null.cast(pl.Utf8).head(100).to_list()
        except:
            return result

        # Check each pattern
        pattern_matches = {}
        for pattern_name, pattern_regex in self.patterns.items():
            match_count = sum(1 for v in sample_values if v and re.match(pattern_regex, str(v), re.IGNORECASE))
            match_pct = match_count / len(sample_values) * 100 if sample_values else 0
            if match_pct > 70:
                pattern_matches[pattern_name] = match_pct

        if pattern_matches:
            result['patterns_detected'] = [
                {'pattern': k, 'match_percentage': round(v, 1)}
                for k, v in sorted(pattern_matches.items(), key=lambda x: -x[1])
            ]
            result['likely_type'] = max(pattern_matches, key=pattern_matches.get)

        # Additional analysis for numeric columns
        if col_data.dtype in [pl.Int64, pl.Int32, pl.Float64, pl.Float32]:
            result['value_characteristics'] = self._analyze_numeric(col_data)

        # Additional analysis for string columns
        if col_data.dtype in [pl.Utf8, pl.String]:
            result['string_characteristics'] = self._analyze_string(col_data)

        return result

    def _analyze_numeric(self, col_data: pl.Series) -> Dict[str, Any]:
        """Analyze characteristics of numeric column"""
        non_null = col_data.drop_nulls()
        if len(non_null) == 0:
            return {}

        return {
            'min': non_null.min(),
            'max': non_null.max(),
            'mean': round(non_null.mean(), 2) if non_null.mean() else None,
            'has_negatives': (non_null < 0).any(),
            'all_integers': (non_null == non_null.cast(pl.Int64)).all() if col_data.dtype in [pl.Float64, pl.Float32] else True,
            'likely_id': non_null.n_unique() == len(non_null) and non_null.min() > 0
        }

    def _analyze_string(self, col_data: pl.Series) -> Dict[str, Any]:
        """Analyze characteristics of string column"""
        non_null = col_data.drop_nulls()
        if len(non_null) == 0:
            return {}

        lengths = non_null.str.len_chars()

        return {
            'min_length': lengths.min(),
            'max_length': lengths.max(),
            'avg_length': round(lengths.mean(), 1) if lengths.mean() else None,
            'all_same_length': lengths.n_unique() == 1,
            'contains_digits': non_null.str.contains(r'\d').any(),
            'contains_letters': non_null.str.contains(r'[a-zA-Z]').any(),
            'unique_ratio': round(non_null.n_unique() / len(non_null), 3)
        }


def detect_patterns(col_data: pl.Series) -> List[Dict[str, Any]]:
    """Convenience function to detect patterns in a column"""
    analyzer = PatternAnalyzer()
    result = analyzer.analyze(col_data, 'column')
    return result.get('patterns_detected', [])
