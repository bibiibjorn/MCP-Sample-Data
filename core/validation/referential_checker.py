"""
Referential Integrity Checker Module
Validates foreign key relationships between tables
"""
import polars as pl
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class ReferentialChecker:
    """Checks referential integrity between tables"""

    def __init__(self, sample_size: int = 10000):
        self.sample_size = sample_size

    def check_integrity(
        self,
        fact_df: pl.DataFrame,
        dimension_df: pl.DataFrame,
        fact_key: str,
        dimension_key: str
    ) -> Dict[str, Any]:
        """
        Check referential integrity between a fact and dimension table.

        Args:
            fact_df: Fact table DataFrame
            dimension_df: Dimension table DataFrame
            fact_key: Foreign key column in fact table
            dimension_key: Primary key column in dimension table

        Returns:
            Integrity check results
        """
        try:
            # Ensure columns exist
            if fact_key not in fact_df.columns:
                return {'success': False, 'error': f'Fact key column not found: {fact_key}'}
            if dimension_key not in dimension_df.columns:
                return {'success': False, 'error': f'Dimension key column not found: {dimension_key}'}

            # Get unique values from both tables
            fact_values = set(fact_df[fact_key].unique().to_list())
            dim_values = set(dimension_df[dimension_key].unique().to_list())

            # Find orphans (fact values not in dimension)
            orphan_values = fact_values - dim_values

            # Find unused dimension values
            unused_dim_values = dim_values - fact_values

            # Count orphan records
            orphan_count = 0
            if orphan_values:
                orphan_count = fact_df.filter(pl.col(fact_key).is_in(list(orphan_values))).height

            result = {
                'success': True,
                'integrity_valid': len(orphan_values) == 0,
                'fact_unique_keys': len(fact_values),
                'dimension_unique_keys': len(dim_values),
                'orphan_values_count': len(orphan_values),
                'orphan_records_count': orphan_count,
                'unused_dimension_values_count': len(unused_dim_values),
                'coverage': {
                    'fact_to_dimension': round(len(fact_values & dim_values) / len(fact_values) * 100, 2) if fact_values else 100,
                    'dimension_used': round(len(fact_values & dim_values) / len(dim_values) * 100, 2) if dim_values else 100
                }
            }

            # Add sample of orphan values
            if orphan_values:
                sample_orphans = list(orphan_values)[:10]
                result['sample_orphan_values'] = sample_orphans

            # Add sample of unused dimension values
            if unused_dim_values:
                sample_unused = list(unused_dim_values)[:10]
                result['sample_unused_dimension_values'] = sample_unused

            return result

        except Exception as e:
            logger.error(f"Error checking referential integrity: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def check_multi_key_integrity(
        self,
        fact_df: pl.DataFrame,
        dimension_df: pl.DataFrame,
        fact_keys: list,
        dimension_keys: list
    ) -> Dict[str, Any]:
        """
        Check referential integrity with composite keys.

        Args:
            fact_df: Fact table DataFrame
            dimension_df: Dimension table DataFrame
            fact_keys: List of foreign key columns in fact table
            dimension_keys: List of primary key columns in dimension table

        Returns:
            Integrity check results
        """
        try:
            if len(fact_keys) != len(dimension_keys):
                return {'success': False, 'error': 'Number of fact and dimension keys must match'}

            # Create composite key by concatenating columns
            fact_composite = fact_df.select([
                pl.concat_str([pl.col(k).cast(pl.Utf8) for k in fact_keys], separator='|||').alias('_composite_key')
            ])
            dim_composite = dimension_df.select([
                pl.concat_str([pl.col(k).cast(pl.Utf8) for k in dimension_keys], separator='|||').alias('_composite_key')
            ])

            fact_values = set(fact_composite['_composite_key'].unique().to_list())
            dim_values = set(dim_composite['_composite_key'].unique().to_list())

            orphan_values = fact_values - dim_values

            result = {
                'success': True,
                'integrity_valid': len(orphan_values) == 0,
                'fact_unique_composite_keys': len(fact_values),
                'dimension_unique_composite_keys': len(dim_values),
                'orphan_composite_keys_count': len(orphan_values),
                'fact_keys': fact_keys,
                'dimension_keys': dimension_keys
            }

            if orphan_values:
                result['sample_orphan_keys'] = list(orphan_values)[:5]

            return result

        except Exception as e:
            logger.error(f"Error checking multi-key integrity: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
