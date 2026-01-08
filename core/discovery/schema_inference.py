"""
Schema Inference Module
Automatically infer data types and semantic types from data
"""
import polars as pl
from typing import Dict, Any, List
import re
import logging

logger = logging.getLogger(__name__)


class SchemaInferrer:
    """Infers optimal schema for data"""

    def __init__(self, target: str = 'powerbi'):
        self.target = target
        self.type_mappings = {
            'powerbi': {
                pl.Int64: 'Whole Number',
                pl.Int32: 'Whole Number',
                pl.Int16: 'Whole Number',
                pl.Int8: 'Whole Number',
                pl.UInt64: 'Whole Number',
                pl.UInt32: 'Whole Number',
                pl.UInt16: 'Whole Number',
                pl.UInt8: 'Whole Number',
                pl.Float64: 'Decimal Number',
                pl.Float32: 'Decimal Number',
                pl.Utf8: 'Text',
                pl.String: 'Text',
                pl.Boolean: 'True/False',
                pl.Date: 'Date',
                pl.Datetime: 'Date/Time',
                pl.Time: 'Time'
            },
            'generic': {
                pl.Int64: 'integer',
                pl.Int32: 'integer',
                pl.Float64: 'decimal',
                pl.Float32: 'decimal',
                pl.Utf8: 'string',
                pl.String: 'string',
                pl.Boolean: 'boolean',
                pl.Date: 'date',
                pl.Datetime: 'datetime'
            }
        }

    def infer(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Infer schema from DataFrame"""
        columns = []
        target_mappings = self.type_mappings.get(self.target, self.type_mappings['generic'])

        for col in df.columns:
            col_data = df[col]
            dtype = col_data.dtype

            col_schema = {
                'name': col,
                'source_type': str(dtype),
                'target_type': target_mappings.get(dtype, 'Text'),
                'nullable': col_data.null_count() > 0,
                'unique': col_data.n_unique() == len(df),
                'semantic_type': self._infer_semantic_type(col_data, col)
            }

            # Suggest optimizations
            if dtype in [pl.Int64, pl.Int32]:
                try:
                    max_val = col_data.max()
                    min_val = col_data.min()
                    if max_val is not None and min_val is not None:
                        if min_val >= 0 and max_val < 256:
                            col_schema['optimization'] = 'Could use smaller integer type (UInt8)'
                        elif min_val >= -128 and max_val < 128:
                            col_schema['optimization'] = 'Could use smaller integer type (Int8)'
                except:
                    pass

            columns.append(col_schema)

        return {
            'columns': columns,
            'row_count': len(df),
            'recommendations': self._get_recommendations(df)
        }

    def _infer_semantic_type(self, col_data: pl.Series, col_name: str) -> str:
        """Infer semantic type from column name and data"""
        name_lower = col_name.lower()

        # Pattern matching on column name
        patterns = {
            'ID': r'(^id$|_id$|^id_|_key$|^key_)',
            'Date': r'(date|time|created|updated|timestamp)',
            'Amount': r'(amount|total|sum|price|cost|revenue|sales|value)',
            'Quantity': r'(qty|quantity|count|number)',
            'Percentage': r'(pct|percent|rate|ratio)',
            'Name': r'(name|description|title|label)',
            'Code': r'(code|sku)',
            'Boolean': r'(flag|is_|has_|can_)',
            'Email': r'(email|e_mail)',
            'Phone': r'(phone|tel|mobile)',
            'Address': r'(address|street|city|state|zip|postal|country)'
        }

        for semantic_type, pattern in patterns.items():
            if re.search(pattern, name_lower):
                return semantic_type

        return 'Unknown'

    def _get_recommendations(self, df: pl.DataFrame) -> List[str]:
        """Get schema optimization recommendations"""
        recommendations = []

        # Check for potential issues
        for col in df.columns:
            null_count = df[col].null_count()
            null_pct = null_count / len(df) * 100 if len(df) > 0 else 0

            if null_pct > 50:
                recommendations.append(f"Column '{col}' has {null_pct:.1f}% null values - consider removing or handling")

            if df[col].n_unique() == 1:
                recommendations.append(f"Column '{col}' has only one value - may not be useful for analysis")

            if df[col].n_unique() == len(df) and len(df) > 10:
                recommendations.append(f"Column '{col}' has all unique values - potential primary key candidate")

        return recommendations

    # Alias for handler compatibility
    def infer_schema(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Alias for infer() method - for handler compatibility"""
        result = self.infer(df)
        result['success'] = True
        return result


def infer_schema(df: pl.DataFrame, target: str = 'powerbi') -> Dict[str, Any]:
    """Convenience function to infer schema"""
    inferrer = SchemaInferrer(target=target)
    return inferrer.infer(df)
