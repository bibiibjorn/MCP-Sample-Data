"""
Rule Engine Module
Applies custom validation rules to data
"""
import polars as pl
import re
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)

# Token optimization constants
SAMPLE_VIOLATION_LIMIT = 5      # Max sample violations per rule
INVALID_VALUES_LIMIT = 10       # Max invalid values to show
MAX_VALUE_LENGTH = 50           # Truncate long values in output


def _truncate_value(val: Any, max_len: int = MAX_VALUE_LENGTH) -> Any:
    """Truncate long string values for token efficiency."""
    if isinstance(val, str) and len(val) > max_len:
        return val[:max_len - 3] + '...'
    return val


def _truncate_list(items: List[Any], limit: int = SAMPLE_VIOLATION_LIMIT) -> List[Any]:
    """Truncate list and truncate individual string values."""
    return [_truncate_value(v) for v in items[:limit]]


class RuleEngine:
    """Engine for applying validation rules to data"""

    def __init__(self):
        self.rule_handlers = {
            'not_null': self._check_not_null,
            'unique': self._check_unique,
            'range': self._check_range,
            'regex': self._check_regex,
            'custom_sql': self._check_custom_sql,
            'referential': self._check_referential,
            'enum': self._check_enum,
            'length': self._check_length
        }

    def validate(
        self,
        df: pl.DataFrame,
        rules: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Apply validation rules to a DataFrame.

        Args:
            df: DataFrame to validate
            rules: List of rule definitions

        Returns:
            Validation results
        """
        results = []
        all_passed = True

        for rule in rules:
            rule_name = rule.get('name', 'unnamed_rule')
            rule_type = rule.get('type')
            columns = rule.get('columns', [])
            config = rule.get('config', {})

            if rule_type not in self.rule_handlers:
                results.append({
                    'rule_name': rule_name,
                    'rule_type': rule_type,
                    'passed': False,
                    'error': f'Unknown rule type: {rule_type}'
                })
                all_passed = False
                continue

            try:
                handler = self.rule_handlers[rule_type]
                rule_result = handler(df, columns, config)
                rule_result['rule_name'] = rule_name
                rule_result['rule_type'] = rule_type

                if not rule_result.get('passed', False):
                    all_passed = False

                results.append(rule_result)

            except Exception as e:
                logger.error(f"Error executing rule {rule_name}: {e}", exc_info=True)
                results.append({
                    'rule_name': rule_name,
                    'rule_type': rule_type,
                    'passed': False,
                    'error': str(e)
                })
                all_passed = False

        return {
            'success': True,
            'all_passed': all_passed,
            'total_rules': len(rules),
            'passed_rules': sum(1 for r in results if r.get('passed', False)),
            'failed_rules': sum(1 for r in results if not r.get('passed', False)),
            'results': results
        }

    def _check_not_null(self, df: pl.DataFrame, columns: List[str], config: Dict) -> Dict[str, Any]:
        """Check for null values in columns"""
        violations = {}
        total_violations = 0

        for col in columns:
            if col not in df.columns:
                violations[col] = {'error': 'Column not found'}
                continue

            null_count = df[col].null_count()
            if null_count > 0:
                violations[col] = {
                    'null_count': null_count,
                    'null_percentage': round(null_count / len(df) * 100, 2)
                }
                total_violations += null_count

        return {
            'passed': len(violations) == 0,
            'columns_checked': columns,
            'violations': violations,
            'total_violations': total_violations
        }

    def _check_unique(self, df: pl.DataFrame, columns: List[str], config: Dict) -> Dict[str, Any]:
        """Check for unique values in columns"""
        if len(columns) == 1:
            col = columns[0]
            if col not in df.columns:
                return {'passed': False, 'error': f'Column not found: {col}'}

            unique_count = df[col].n_unique()
            total_count = len(df)
            duplicates = total_count - unique_count

            return {
                'passed': duplicates == 0,
                'column': col,
                'unique_count': unique_count,
                'total_count': total_count,
                'duplicate_count': duplicates
            }
        else:
            # Check uniqueness of column combination
            grouped = df.group_by(columns).agg(pl.count().alias('_count'))
            duplicates = grouped.filter(pl.col('_count') > 1)

            return {
                'passed': len(duplicates) == 0,
                'columns': columns,
                'duplicate_combinations': len(duplicates),
                'sample_duplicates': duplicates.head(5).to_dicts() if len(duplicates) > 0 else []
            }

    def _check_range(self, df: pl.DataFrame, columns: List[str], config: Dict) -> Dict[str, Any]:
        """Check if values are within a range"""
        min_val = config.get('min')
        max_val = config.get('max')
        violations = {}

        for col in columns:
            if col not in df.columns:
                violations[col] = {'error': 'Column not found'}
                continue

            col_violations = []
            if min_val is not None:
                below_min = df.filter(pl.col(col) < min_val).height
                if below_min > 0:
                    col_violations.append(f'{below_min} values below {min_val}')

            if max_val is not None:
                above_max = df.filter(pl.col(col) > max_val).height
                if above_max > 0:
                    col_violations.append(f'{above_max} values above {max_val}')

            if col_violations:
                violations[col] = col_violations

        return {
            'passed': len(violations) == 0,
            'columns_checked': columns,
            'range': {'min': min_val, 'max': max_val},
            'violations': violations
        }

    def _check_regex(self, df: pl.DataFrame, columns: List[str], config: Dict) -> Dict[str, Any]:
        """Check if values match a regex pattern"""
        pattern = config.get('pattern')
        if not pattern:
            return {'passed': False, 'error': 'No pattern specified'}

        violations = {}
        for col in columns:
            if col not in df.columns:
                violations[col] = {'error': 'Column not found'}
                continue

            # Filter non-matching values
            non_matching = df.filter(~pl.col(col).cast(pl.Utf8).str.contains(pattern))
            if len(non_matching) > 0:
                # Token-efficient: limit samples and truncate long values
                sample_values = non_matching[col].head(SAMPLE_VIOLATION_LIMIT).to_list()
                violations[col] = {
                    'non_matching_count': len(non_matching),
                    'samples': _truncate_list(sample_values)
                }

        return {
            'passed': len(violations) == 0,
            'pattern': pattern,
            'columns_checked': columns,
            'violations': violations
        }

    def _check_custom_sql(self, df: pl.DataFrame, columns: List[str], config: Dict) -> Dict[str, Any]:
        """Check using a custom SQL expression"""
        import duckdb

        expression = config.get('expression')
        if not expression:
            return {'passed': False, 'error': 'No SQL expression specified'}

        try:
            conn = duckdb.connect(':memory:')
            conn.register('data', df.to_pandas())

            # Execute the check
            result = conn.execute(f"SELECT COUNT(*) FROM data WHERE NOT ({expression})").fetchone()[0]

            return {
                'passed': result == 0,
                'expression': expression,
                'violations_count': result
            }

        except Exception as e:
            return {'passed': False, 'error': f'SQL error: {str(e)}'}

    def _check_referential(self, df: pl.DataFrame, columns: List[str], config: Dict) -> Dict[str, Any]:
        """Check referential integrity with another table"""
        # This is a placeholder - actual implementation would need access to other files
        return {
            'passed': True,
            'message': 'Use 04_Check_Referential_Integrity for cross-table checks'
        }

    def _check_enum(self, df: pl.DataFrame, columns: List[str], config: Dict) -> Dict[str, Any]:
        """Check if values are from a list of allowed values"""
        allowed_values = config.get('values', [])
        if not allowed_values:
            return {'passed': False, 'error': 'No allowed values specified'}

        violations = {}
        for col in columns:
            if col not in df.columns:
                violations[col] = {'error': 'Column not found'}
                continue

            unique_values = set(df[col].unique().to_list())
            invalid_values = unique_values - set(allowed_values)

            if invalid_values:
                # Token-efficient: limit and truncate invalid values
                violations[col] = {
                    'invalid_count': len(invalid_values),
                    'samples': _truncate_list(list(invalid_values), INVALID_VALUES_LIMIT)
                }

        return {
            'passed': len(violations) == 0,
            'allowed_count': len(allowed_values),
            'columns_checked': columns,
            'violations': violations
        }

    def _check_length(self, df: pl.DataFrame, columns: List[str], config: Dict) -> Dict[str, Any]:
        """Check string length constraints"""
        min_len = config.get('min')
        max_len = config.get('max')
        violations = {}

        for col in columns:
            if col not in df.columns:
                violations[col] = {'error': 'Column not found'}
                continue

            col_violations = []
            str_lengths = df[col].cast(pl.Utf8).str.len_chars()

            if min_len is not None:
                too_short = df.filter(str_lengths < min_len).height
                if too_short > 0:
                    col_violations.append(f'{too_short} values shorter than {min_len}')

            if max_len is not None:
                too_long = df.filter(str_lengths > max_len).height
                if too_long > 0:
                    col_violations.append(f'{too_long} values longer than {max_len}')

            if col_violations:
                violations[col] = col_violations

        return {
            'passed': len(violations) == 0,
            'length_constraints': {'min': min_len, 'max': max_len},
            'columns_checked': columns,
            'violations': violations
        }
