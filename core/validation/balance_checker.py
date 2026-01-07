"""
Balance Checker Module
Validates financial balance rules (debit = credit)
"""
import polars as pl
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class BalanceChecker:
    """Checks financial balance rules"""

    def __init__(self, tolerance: float = 0.01):
        self.tolerance = tolerance

    def check_balance(
        self,
        df: pl.DataFrame,
        debit_column: str,
        credit_column: str,
        group_by: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Check that debits equal credits.

        Args:
            df: DataFrame to check
            debit_column: Column containing debit amounts
            credit_column: Column containing credit amounts
            group_by: Optional columns to group by (e.g., period, entity)

        Returns:
            Balance check results
        """
        try:
            # Ensure columns exist
            if debit_column not in df.columns:
                return {'success': False, 'error': f'Debit column not found: {debit_column}'}
            if credit_column not in df.columns:
                return {'success': False, 'error': f'Credit column not found: {credit_column}'}

            if group_by:
                # Group by and check each group
                grouped = df.group_by(group_by).agg([
                    pl.col(debit_column).sum().alias('total_debit'),
                    pl.col(credit_column).sum().alias('total_credit')
                ]).with_columns([
                    (pl.col('total_debit') - pl.col('total_credit')).alias('difference'),
                    (pl.col('total_debit') - pl.col('total_credit')).abs().alias('abs_difference')
                ])

                # Find imbalanced groups
                imbalanced = grouped.filter(pl.col('abs_difference') > self.tolerance)

                result = {
                    'success': True,
                    'balanced': len(imbalanced) == 0,
                    'total_groups': len(grouped),
                    'balanced_groups': len(grouped) - len(imbalanced),
                    'imbalanced_groups': len(imbalanced),
                    'tolerance': self.tolerance
                }

                if len(imbalanced) > 0:
                    result['imbalanced_details'] = imbalanced.to_dicts()

                # Grand totals
                result['grand_totals'] = {
                    'total_debit': df[debit_column].sum(),
                    'total_credit': df[credit_column].sum(),
                    'difference': df[debit_column].sum() - df[credit_column].sum()
                }

            else:
                # Check overall balance
                total_debit = df[debit_column].sum()
                total_credit = df[credit_column].sum()
                difference = total_debit - total_credit

                result = {
                    'success': True,
                    'balanced': abs(difference) <= self.tolerance,
                    'total_debit': total_debit,
                    'total_credit': total_credit,
                    'difference': difference,
                    'tolerance': self.tolerance
                }

            return result

        except Exception as e:
            logger.error(f"Error checking balance: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def check_trial_balance(
        self,
        df: pl.DataFrame,
        period_column: str,
        debit_column: str,
        credit_column: str
    ) -> Dict[str, Any]:
        """
        Check trial balance per period.

        Args:
            df: Trial balance DataFrame
            period_column: Column containing period identifier
            debit_column: Column containing debit amounts
            credit_column: Column containing credit amounts

        Returns:
            Trial balance validation results
        """
        return self.check_balance(df, debit_column, credit_column, group_by=[period_column])
