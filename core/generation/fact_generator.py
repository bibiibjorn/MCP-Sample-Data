"""
Fact Generator Module
Generates fact tables linked to dimension tables
"""
import polars as pl
from typing import Dict, Any, List, Optional
import random
import numpy as np
import logging

logger = logging.getLogger(__name__)


class FactGenerator:
    """Generates fact tables with referential integrity"""

    def __init__(self):
        pass

    def generate(
        self,
        name: str,
        grain: List[str],
        measures: List[Dict[str, Any]],
        dimensions: List[Dict[str, Any]],
        row_count: int,
        output_path: Optional[str] = None,
        output_format: str = 'csv'
    ) -> Dict[str, Any]:
        """
        Generate a fact table linked to dimensions.

        Args:
            name: Table name
            grain: Grain columns (foreign keys)
            measures: Measure column definitions
            dimensions: Dimension references with paths and keys
            row_count: Number of rows to generate
            output_path: Optional output file path
            output_format: 'csv' or 'parquet'

        Returns:
            Generation result with data
        """
        try:
            data = {}

            # Load dimension data and create foreign keys
            dim_values = {}
            dim_weights = {}

            for dim_def in dimensions:
                path = dim_def['path']
                key_column = dim_def['key_column']
                weight_column = dim_def.get('weight_column')

                # Load dimension
                if path.endswith('.csv'):
                    dim_df = pl.read_csv(path)
                elif path.endswith('.parquet'):
                    dim_df = pl.read_parquet(path)
                else:
                    raise ValueError(f"Unsupported file format: {path}")

                # Get key values
                key_values = dim_df[key_column].to_list()
                dim_values[key_column] = key_values

                # Get weights if specified
                if weight_column and weight_column in dim_df.columns:
                    weights = dim_df[weight_column].to_list()
                    # Normalize weights
                    total = sum(w for w in weights if w is not None)
                    dim_weights[key_column] = [w / total if w else 0 for w in weights]

            # Generate foreign key columns
            for key_col in grain:
                if key_col in dim_values:
                    values = dim_values[key_col]
                    weights = dim_weights.get(key_col)

                    if weights:
                        # Weighted random selection
                        data[key_col] = random.choices(values, weights=weights, k=row_count)
                    else:
                        # Uniform random selection
                        data[key_col] = random.choices(values, k=row_count)
                else:
                    # Generate sequential IDs if no dimension provided
                    data[key_col] = list(range(1, row_count + 1))

            # Generate measure columns
            for measure_def in measures:
                measure_name = measure_def['name']
                measure_type = measure_def.get('type', 'decimal')
                distribution = measure_def.get('distribution', 'uniform')
                min_val = measure_def.get('min', 0)
                max_val = measure_def.get('max', 1000)
                mean_val = measure_def.get('mean')
                std_val = measure_def.get('std')

                # Generate measure values
                measure_data = self._generate_measure(
                    distribution, row_count, min_val, max_val, mean_val, std_val, measure_type
                )
                data[measure_name] = measure_data

            # Create DataFrame
            df = pl.DataFrame(data)

            # Save if output path provided
            if output_path:
                if output_format == 'parquet':
                    df.write_parquet(output_path)
                else:
                    df.write_csv(output_path)

            return {
                'success': True,
                'table_name': name,
                'row_count': len(df),
                'columns': df.columns,
                'grain_columns': grain,
                'measure_columns': [m['name'] for m in measures],
                'output_path': output_path,
                'sample_data': df.head(5).to_dicts()
            }

        except Exception as e:
            logger.error(f"Error generating fact table: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _generate_measure(
        self,
        distribution: str,
        count: int,
        min_val: float,
        max_val: float,
        mean_val: Optional[float],
        std_val: Optional[float],
        measure_type: str
    ) -> List:
        """Generate measure values based on distribution"""
        if distribution == 'uniform':
            values = np.random.uniform(min_val, max_val, count)

        elif distribution == 'normal':
            if mean_val is None:
                mean_val = (min_val + max_val) / 2
            if std_val is None:
                std_val = (max_val - min_val) / 6
            values = np.random.normal(mean_val, std_val, count)
            # Clip to range
            values = np.clip(values, min_val, max_val)

        elif distribution == 'lognormal':
            # Lognormal for amounts (many small, few large)
            if mean_val is None:
                mean_val = np.log((min_val + max_val) / 2)
            if std_val is None:
                std_val = 1.0
            values = np.random.lognormal(mean_val, std_val, count)
            # Scale to range
            values = (values - values.min()) / (values.max() - values.min()) * (max_val - min_val) + min_val

        elif distribution == 'exponential':
            scale = (max_val - min_val) / 3
            values = np.random.exponential(scale, count) + min_val
            values = np.clip(values, min_val, max_val)

        else:
            values = np.random.uniform(min_val, max_val, count)

        # Convert to appropriate type
        if measure_type == 'integer':
            return [int(round(v)) for v in values]
        else:
            return [round(float(v), 2) for v in values]
