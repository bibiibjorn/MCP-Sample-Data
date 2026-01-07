"""
Validation Handlers
Handlers for data validation tools
"""
import polars as pl
from typing import Dict, Any, List, Optional
import os

from core.validation import RuleEngine, BalanceChecker, ReferentialChecker, StatisticalValidator
from server.tool_schemas import TOOL_SCHEMAS


def register_validation_handlers(registry):
    """Register all validation handlers"""

    rule_engine = RuleEngine()
    balance_checker = BalanceChecker()
    referential_checker = ReferentialChecker()
    statistical_validator = StatisticalValidator()

    # 04_validate_data
    def validate_data(
        file_path: str,
        rules: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Validate data against rules"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.csv':
                df = pl.read_csv(file_path)
            elif ext == '.parquet':
                df = pl.read_parquet(file_path)
            else:
                return {'success': False, 'error': f'Unsupported format: {ext}'}

            result = rule_engine.validate(df, rules)
            result['file_path'] = file_path

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['04_validate_data']
    registry.register(
        '04_validate_data',
        validate_data,
        'validation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 04_check_referential_integrity
    def check_referential_integrity(
        fact_file: str,
        dimension_files: Dict[str, str],
        key_mappings: Dict[str, str]
    ) -> Dict[str, Any]:
        """Check referential integrity"""
        if not os.path.exists(fact_file):
            return {'success': False, 'error': f'Fact file not found: {fact_file}'}

        try:
            # Load fact table
            ext = os.path.splitext(fact_file)[1].lower()
            if ext == '.csv':
                fact_df = pl.read_csv(fact_file)
            elif ext == '.parquet':
                fact_df = pl.read_parquet(fact_file)
            else:
                return {'success': False, 'error': f'Unsupported format: {ext}'}

            # Load dimension tables
            dimensions = {}
            for name, path in dimension_files.items():
                if not os.path.exists(path):
                    return {'success': False, 'error': f'Dimension file not found: {path}'}

                ext = os.path.splitext(path)[1].lower()
                if ext == '.csv':
                    dimensions[name] = pl.read_csv(path)
                elif ext == '.parquet':
                    dimensions[name] = pl.read_parquet(path)

            result = referential_checker.check(
                fact_df=fact_df,
                dimensions=dimensions,
                key_mappings=key_mappings
            )

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['04_check_referential_integrity']
    registry.register(
        '04_check_referential_integrity',
        check_referential_integrity,
        'validation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 04_validate_balance
    def validate_balance(
        file_path: str,
        debit_column: str,
        credit_column: str,
        group_by: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Validate financial balances"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.csv':
                df = pl.read_csv(file_path)
            elif ext == '.parquet':
                df = pl.read_parquet(file_path)
            else:
                return {'success': False, 'error': f'Unsupported format: {ext}'}

            result = balance_checker.check(
                df=df,
                debit_column=debit_column,
                credit_column=credit_column,
                group_by=group_by
            )

            result['file_path'] = file_path

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['04_validate_balance']
    registry.register(
        '04_validate_balance',
        validate_balance,
        'validation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 04_detect_anomalies
    def detect_anomalies(
        file_path: str,
        columns: Optional[List[str]] = None,
        method: str = 'zscore'
    ) -> Dict[str, Any]:
        """Detect statistical anomalies"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.csv':
                df = pl.read_csv(file_path)
            elif ext == '.parquet':
                df = pl.read_parquet(file_path)
            else:
                return {'success': False, 'error': f'Unsupported format: {ext}'}

            result = statistical_validator.detect_anomalies(
                df=df,
                columns=columns,
                method=method
            )

            result['file_path'] = file_path

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['04_detect_anomalies']
    registry.register(
        '04_detect_anomalies',
        detect_anomalies,
        'validation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )
