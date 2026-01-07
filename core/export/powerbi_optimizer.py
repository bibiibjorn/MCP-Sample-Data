"""
Power BI Optimizer Module
Optimizes data for Power BI consumption
"""
import polars as pl
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)


class PowerBIOptimizer:
    """Optimizes data for Power BI"""

    def __init__(self):
        pass

    def optimize_for_powerbi(
        self,
        df: pl.DataFrame,
        table_type: str = 'dimension'
    ) -> Dict[str, Any]:
        """
        Optimize DataFrame for Power BI.

        Args:
            df: DataFrame to optimize
            table_type: Type of table (dimension, fact, bridge)

        Returns:
            Optimized DataFrame and optimization report
        """
        try:
            optimizations = []
            optimized_df = df.clone()

            # 1. Convert high-cardinality strings to categoricals
            for col in optimized_df.columns:
                if optimized_df[col].dtype == pl.Utf8:
                    unique_count = optimized_df[col].n_unique()
                    total_count = len(optimized_df)

                    if unique_count < total_count * 0.5:
                        optimized_df = optimized_df.with_columns(
                            pl.col(col).cast(pl.Categorical)
                        )
                        optimizations.append({
                            'column': col,
                            'optimization': 'string_to_categorical',
                            'unique_values': unique_count
                        })

            # 2. Downcast numeric types
            for col in optimized_df.columns:
                dtype = optimized_df[col].dtype

                if dtype in [pl.Int64, pl.Int32]:
                    min_val = optimized_df[col].min()
                    max_val = optimized_df[col].max()

                    if min_val is not None and max_val is not None:
                        if min_val >= 0 and max_val <= 255:
                            optimized_df = optimized_df.with_columns(
                                pl.col(col).cast(pl.UInt8)
                            )
                            optimizations.append({
                                'column': col,
                                'optimization': 'downcast_to_uint8',
                                'range': f"{min_val}-{max_val}"
                            })
                        elif min_val >= -128 and max_val <= 127:
                            optimized_df = optimized_df.with_columns(
                                pl.col(col).cast(pl.Int8)
                            )
                            optimizations.append({
                                'column': col,
                                'optimization': 'downcast_to_int8',
                                'range': f"{min_val}-{max_val}"
                            })
                        elif min_val >= 0 and max_val <= 65535:
                            optimized_df = optimized_df.with_columns(
                                pl.col(col).cast(pl.UInt16)
                            )
                            optimizations.append({
                                'column': col,
                                'optimization': 'downcast_to_uint16',
                                'range': f"{min_val}-{max_val}"
                            })
                        elif min_val >= -32768 and max_val <= 32767:
                            optimized_df = optimized_df.with_columns(
                                pl.col(col).cast(pl.Int16)
                            )
                            optimizations.append({
                                'column': col,
                                'optimization': 'downcast_to_int16',
                                'range': f"{min_val}-{max_val}"
                            })

                elif dtype == pl.Float64:
                    # Check if can be Float32
                    optimized_df = optimized_df.with_columns(
                        pl.col(col).cast(pl.Float32)
                    )
                    optimizations.append({
                        'column': col,
                        'optimization': 'float64_to_float32'
                    })

            # 3. Table-specific optimizations
            if table_type == 'fact':
                # Ensure fact tables have integer keys
                key_columns = [c for c in optimized_df.columns if c.endswith('_key') or c.endswith('_id')]
                for col in key_columns:
                    if optimized_df[col].dtype not in [pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                                                        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64]:
                        try:
                            optimized_df = optimized_df.with_columns(
                                pl.col(col).cast(pl.Int32)
                            )
                            optimizations.append({
                                'column': col,
                                'optimization': 'key_to_integer'
                            })
                        except:
                            pass

            elif table_type == 'dimension':
                # Ensure dimension tables have a proper key column
                pass

            # Calculate size reduction
            original_size = df.estimated_size()
            optimized_size = optimized_df.estimated_size()
            reduction_pct = (1 - optimized_size / original_size) * 100 if original_size > 0 else 0

            return {
                'success': True,
                'optimized_df': optimized_df,
                'optimizations_applied': optimizations,
                'original_size_bytes': original_size,
                'optimized_size_bytes': optimized_size,
                'size_reduction_pct': round(reduction_pct, 1),
                'column_count': len(optimized_df.columns),
                'row_count': len(optimized_df)
            }

        except Exception as e:
            logger.error(f"Error optimizing for Power BI: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def suggest_relationships(
        self,
        dataframes: Dict[str, pl.DataFrame]
    ) -> Dict[str, Any]:
        """Suggest relationships between tables for Power BI"""
        try:
            suggestions = []

            # Find potential key columns
            table_keys = {}
            for name, df in dataframes.items():
                keys = []
                for col in df.columns:
                    col_lower = col.lower()
                    if col_lower.endswith('_key') or col_lower.endswith('_id') or col_lower == 'id':
                        keys.append({
                            'column': col,
                            'unique_count': df[col].n_unique(),
                            'is_unique': df[col].n_unique() == len(df)
                        })
                table_keys[name] = keys

            # Find matching keys between tables
            table_names = list(dataframes.keys())
            for i, table1 in enumerate(table_names):
                for table2 in table_names[i+1:]:
                    for key1 in table_keys[table1]:
                        for key2 in table_keys[table2]:
                            # Check if column names suggest a relationship
                            if self._columns_match(key1['column'], key2['column']):
                                # Determine relationship type
                                if key1['is_unique'] and key2['is_unique']:
                                    rel_type = '1:1'
                                elif key1['is_unique']:
                                    rel_type = '1:*'
                                elif key2['is_unique']:
                                    rel_type = '*:1'
                                else:
                                    rel_type = '*:*'

                                suggestions.append({
                                    'from_table': table1,
                                    'from_column': key1['column'],
                                    'to_table': table2,
                                    'to_column': key2['column'],
                                    'relationship_type': rel_type,
                                    'confidence': 'high' if key1['column'] == key2['column'] else 'medium'
                                })

            return {
                'success': True,
                'suggested_relationships': suggestions,
                'table_keys': {
                    name: [k['column'] for k in keys]
                    for name, keys in table_keys.items()
                }
            }

        except Exception as e:
            logger.error(f"Error suggesting relationships: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _columns_match(self, col1: str, col2: str) -> bool:
        """Check if two column names suggest a relationship"""
        col1_lower = col1.lower()
        col2_lower = col2.lower()

        # Exact match
        if col1_lower == col2_lower:
            return True

        # Common patterns
        patterns = [
            ('_key', '_key'),
            ('_id', '_id'),
            ('id', 'key')
        ]

        for p1, p2 in patterns:
            base1 = col1_lower.replace(p1, '').replace(p2, '')
            base2 = col2_lower.replace(p1, '').replace(p2, '')
            if base1 == base2 and base1:
                return True

        return False

    def generate_dax_measures(
        self,
        df: pl.DataFrame,
        table_name: str
    ) -> Dict[str, Any]:
        """Generate common DAX measures for a table"""
        try:
            measures = []

            # Find numeric columns
            numeric_cols = [
                col for col in df.columns
                if df[col].dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                                      pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
                                      pl.Float32, pl.Float64]
            ]

            # Generate SUM measures for numeric columns
            for col in numeric_cols:
                if not col.lower().endswith('_key') and not col.lower().endswith('_id'):
                    measures.append({
                        'name': f"Total {col.replace('_', ' ').title()}",
                        'expression': f"SUM('{table_name}'[{col}])",
                        'format': '#,##0.00'
                    })

            # Generate COUNT measures
            measures.append({
                'name': f"{table_name} Count",
                'expression': f"COUNTROWS('{table_name}')",
                'format': '#,##0'
            })

            # Generate DISTINCTCOUNT for key columns
            key_cols = [c for c in df.columns if c.lower().endswith('_key') or c.lower().endswith('_id')]
            for col in key_cols:
                measures.append({
                    'name': f"Distinct {col.replace('_', ' ').title()}",
                    'expression': f"DISTINCTCOUNT('{table_name}'[{col}])",
                    'format': '#,##0'
                })

            return {
                'success': True,
                'table_name': table_name,
                'measures': measures
            }

        except Exception as e:
            logger.error(f"Error generating DAX measures: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
