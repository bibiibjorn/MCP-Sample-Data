"""
Referential Integrity Checker Module
Validates foreign key relationships between tables
"""
import polars as pl
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)

# Token optimization constants
SAMPLE_ORPHAN_LIMIT = 10        # Max orphan values to show
SAMPLE_UNUSED_LIMIT = 10        # Max unused dimension values to show
MAX_UNIQUE_COMPARE = 100000     # Max unique values to compare (use sampling above)
MAX_VALUE_LENGTH = 50           # Truncate long values


def _truncate_values(values: List[Any], limit: int, max_len: int = MAX_VALUE_LENGTH) -> List[Any]:
    """Truncate list and individual string values for token efficiency."""
    result = []
    for v in values[:limit]:
        if isinstance(v, str) and len(v) > max_len:
            result.append(v[:max_len - 3] + '...')
        else:
            result.append(v)
    return result


class ReferentialChecker:
    """Checks referential integrity between tables"""

    def __init__(self, sample_size: int = 10000):
        self.sample_size = sample_size

    def check(
        self,
        fact_df: pl.DataFrame,
        dimensions: Dict[str, pl.DataFrame],
        key_mappings: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Check referential integrity between a fact table and multiple dimensions.

        Args:
            fact_df: Fact table DataFrame
            dimensions: Dict mapping dimension name to DataFrame
            key_mappings: Dict mapping fact column to dimension name

        Returns:
            Integrity check results for all relationships
        """
        try:
            results = []
            all_valid = True

            for fact_key, dim_name in key_mappings.items():
                if dim_name not in dimensions:
                    results.append({
                        'fact_key': fact_key,
                        'dimension': dim_name,
                        'success': False,
                        'error': f'Dimension not found: {dim_name}'
                    })
                    all_valid = False
                    continue

                dim_df = dimensions[dim_name]
                # Assume dimension key has same name as fact key, or use first column
                dim_key = fact_key if fact_key in dim_df.columns else dim_df.columns[0]

                check_result = self.check_integrity(fact_df, dim_df, fact_key, dim_key)
                check_result['fact_key'] = fact_key
                check_result['dimension'] = dim_name
                check_result['dimension_key'] = dim_key
                results.append(check_result)

                if check_result.get('success') and not check_result.get('integrity_valid', True):
                    all_valid = False

            return {
                'success': True,
                'all_valid': all_valid,
                'checks': results
            }

        except Exception as e:
            logger.error(f"Error in check: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

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

            # Add sample of orphan values (truncated for token efficiency)
            if orphan_values:
                result['sample_orphans'] = _truncate_values(
                    list(orphan_values), SAMPLE_ORPHAN_LIMIT
                )

            # Add sample of unused dimension values (truncated)
            if unused_dim_values:
                result['sample_unused'] = _truncate_values(
                    list(unused_dim_values), SAMPLE_UNUSED_LIMIT
                )

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
                # Truncate composite keys for token efficiency
                result['sample_orphan_keys'] = _truncate_values(
                    list(orphan_values), 5
                )

            return result

        except Exception as e:
            logger.error(f"Error checking multi-key integrity: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
