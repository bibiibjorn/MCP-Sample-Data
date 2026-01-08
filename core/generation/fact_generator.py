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

    # Predefined fact type templates
    FACT_TEMPLATES = {
        'sales': {
            'measures': [
                {'name': 'quantity', 'type': 'integer', 'min': 1, 'max': 100, 'distribution': 'normal'},
                {'name': 'unit_price', 'type': 'decimal', 'min': 5, 'max': 500, 'distribution': 'lognormal'},
                {'name': 'discount_pct', 'type': 'decimal', 'min': 0, 'max': 0.3, 'distribution': 'exponential'},
                {'name': 'total_amount', 'type': 'decimal', 'min': 10, 'max': 10000, 'distribution': 'lognormal'}
            ],
            'attributes': [
                {'name': 'order_status', 'type': 'category', 'values': ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled'], 'weights': [0.05, 0.10, 0.15, 0.65, 0.05]},
                {'name': 'payment_method', 'type': 'category', 'values': ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer', 'Cash'], 'weights': [0.40, 0.25, 0.20, 0.10, 0.05]},
                {'name': 'sales_channel', 'type': 'category', 'values': ['Online', 'In-Store', 'Phone', 'Partner'], 'weights': [0.50, 0.35, 0.10, 0.05]}
            ],
        },
        'finance': {
            'measures': [
                {'name': 'actual_amount', 'type': 'decimal', 'min': -500000, 'max': 500000, 'distribution': 'normal'},
                {'name': 'budget_amount', 'type': 'decimal', 'min': 0, 'max': 600000, 'distribution': 'normal'},
                {'name': 'forecast_amount', 'type': 'decimal', 'min': 0, 'max': 550000, 'distribution': 'normal'},
                {'name': 'prior_year_amount', 'type': 'decimal', 'min': -450000, 'max': 450000, 'distribution': 'normal'},
                {'name': 'committed_amount', 'type': 'decimal', 'min': 0, 'max': 300000, 'distribution': 'lognormal'},
                {'name': 'encumbered_amount', 'type': 'decimal', 'min': 0, 'max': 200000, 'distribution': 'exponential'},
                {'name': 'quantity', 'type': 'decimal', 'min': 0, 'max': 10000, 'distribution': 'lognormal'},
                {'name': 'unit_cost', 'type': 'decimal', 'min': 0.01, 'max': 5000, 'distribution': 'lognormal'},
                {'name': 'exchange_rate', 'type': 'decimal', 'min': 0.5, 'max': 2.0, 'distribution': 'uniform'}
            ],
            'attributes': [
                {'name': 'scenario', 'type': 'category', 'values': ['Actual', 'Budget', 'Forecast', 'Plan'], 'weights': [0.50, 0.25, 0.15, 0.10]},
                {'name': 'transaction_type', 'type': 'category', 'values': ['Revenue', 'Expense', 'Transfer', 'Adjustment', 'Accrual', 'Deferral'], 'weights': [0.30, 0.40, 0.10, 0.08, 0.07, 0.05]},
                {'name': 'entry_type', 'type': 'category', 'values': ['Journal Entry', 'Invoice', 'Payment', 'Receipt', 'Allocation', 'Reclass'], 'weights': [0.25, 0.25, 0.20, 0.15, 0.10, 0.05]},
                {'name': 'posting_status', 'type': 'category', 'values': ['Posted', 'Pending', 'Reversed', 'Draft', 'Approved'], 'weights': [0.65, 0.12, 0.05, 0.08, 0.10]},
                {'name': 'currency_code', 'type': 'category', 'values': ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'CHF', 'AUD'], 'weights': [0.45, 0.20, 0.10, 0.08, 0.07, 0.05, 0.05]},
                {'name': 'fiscal_period', 'type': 'category', 'values': ['P01', 'P02', 'P03', 'P04', 'P05', 'P06', 'P07', 'P08', 'P09', 'P10', 'P11', 'P12', 'ADJ'], 'weights': [0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.04]},
                {'name': 'fiscal_year', 'type': 'category', 'values': ['FY2022', 'FY2023', 'FY2024', 'FY2025'], 'weights': [0.15, 0.25, 0.35, 0.25]},
                {'name': 'cost_center', 'type': 'category', 'values': ['CC100', 'CC200', 'CC300', 'CC400', 'CC500', 'CC600'], 'weights': [0.20, 0.18, 0.17, 0.16, 0.15, 0.14]},
                {'name': 'ledger_type', 'type': 'category', 'values': ['Primary', 'Secondary', 'Reporting', 'Elimination'], 'weights': [0.70, 0.15, 0.10, 0.05]},
                {'name': 'intercompany_flag', 'type': 'category', 'values': ['Y', 'N'], 'weights': [0.15, 0.85]},
                {'name': 'source_system', 'type': 'category', 'values': ['SAP', 'Oracle', 'Manual', 'Interface', 'Consolidation'], 'weights': [0.40, 0.30, 0.10, 0.12, 0.08]}
            ]
        },
        'inventory': {
            'measures': [
                {'name': 'quantity_on_hand', 'type': 'integer', 'min': 0, 'max': 5000, 'distribution': 'normal'},
                {'name': 'quantity_ordered', 'type': 'integer', 'min': 0, 'max': 1000, 'distribution': 'exponential'},
                {'name': 'reorder_point', 'type': 'integer', 'min': 10, 'max': 500, 'distribution': 'uniform'}
            ],
            'attributes': [
                {'name': 'stock_status', 'type': 'category', 'values': ['In Stock', 'Low Stock', 'Out of Stock', 'Backordered'], 'weights': [0.60, 0.20, 0.10, 0.10]},
                {'name': 'movement_type', 'type': 'category', 'values': ['Receipt', 'Issue', 'Transfer', 'Adjustment', 'Return'], 'weights': [0.30, 0.35, 0.15, 0.10, 0.10]},
                {'name': 'storage_location', 'type': 'category', 'values': ['Warehouse A', 'Warehouse B', 'Distribution Center', 'Store Floor'], 'weights': [0.35, 0.30, 0.25, 0.10]}
            ]
        },
        'hr': {
            'measures': [
                {'name': 'hours_worked', 'type': 'decimal', 'min': 0, 'max': 60, 'distribution': 'normal'},
                {'name': 'overtime_hours', 'type': 'decimal', 'min': 0, 'max': 20, 'distribution': 'exponential'},
                {'name': 'salary_amount', 'type': 'decimal', 'min': 2000, 'max': 15000, 'distribution': 'normal'}
            ],
            'attributes': [
                {'name': 'attendance_status', 'type': 'category', 'values': ['Present', 'Absent', 'Leave', 'Work From Home', 'Holiday'], 'weights': [0.70, 0.05, 0.10, 0.10, 0.05]},
                {'name': 'pay_type', 'type': 'category', 'values': ['Regular', 'Overtime', 'Bonus', 'Commission', 'Adjustment'], 'weights': [0.60, 0.15, 0.10, 0.10, 0.05]},
                {'name': 'shift', 'type': 'category', 'values': ['Day', 'Evening', 'Night', 'Flexible'], 'weights': [0.50, 0.20, 0.15, 0.15]}
            ]
        },
        'transactions': {
            'measures': [
                {'name': 'amount', 'type': 'decimal', 'min': 1, 'max': 5000, 'distribution': 'lognormal'},
                {'name': 'fee', 'type': 'decimal', 'min': 0, 'max': 50, 'distribution': 'exponential'},
                {'name': 'net_amount', 'type': 'decimal', 'min': 1, 'max': 5000, 'distribution': 'lognormal'}
            ],
            'attributes': [
                {'name': 'transaction_status', 'type': 'category', 'values': ['Completed', 'Pending', 'Failed', 'Refunded', 'Disputed'], 'weights': [0.75, 0.10, 0.05, 0.07, 0.03]},
                {'name': 'transaction_type', 'type': 'category', 'values': ['Purchase', 'Refund', 'Authorization', 'Capture', 'Void'], 'weights': [0.70, 0.15, 0.05, 0.05, 0.05]},
                {'name': 'payment_processor', 'type': 'category', 'values': ['Stripe', 'PayPal', 'Square', 'Adyen', 'Internal'], 'weights': [0.35, 0.30, 0.15, 0.15, 0.05]}
            ]
        }
    }

    def __init__(self):
        pass

    def generate_from_type(
        self,
        fact_type: str,
        dimensions: Dict[str, pl.DataFrame],
        row_count: int,
        date_range: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Generate a fact table from a predefined type.

        Args:
            fact_type: Type of fact table (sales, finance, inventory, hr)
            dimensions: Dict mapping dimension name to DataFrame
            row_count: Number of rows to generate
            date_range: Optional date range filter

        Returns:
            Generation result with 'success' and 'df' keys
        """
        try:
            template = self.FACT_TEMPLATES.get(fact_type.lower())
            if not template:
                return {
                    'success': False,
                    'error': f"Unknown fact type: {fact_type}. "
                             f"Available: {', '.join(self.FACT_TEMPLATES.keys())}"
                }

            data = {}

            # Generate foreign keys from whatever dimensions the user provides
            for dim_name, dim_df in dimensions.items():
                if dim_df is None or len(dim_df.columns) == 0:
                    continue

                # Auto-detect key column: prefer columns ending with _id or _key, else use first column
                key_col = None
                for col in dim_df.columns:
                    if col.endswith('_id') or col.endswith('_key'):
                        key_col = col
                        break
                if key_col is None:
                    key_col = dim_df.columns[0]

                key_values = dim_df[key_col].to_list()
                if key_values:
                    data[key_col] = random.choices(key_values, k=row_count)

            # Generate measures
            for measure_def in template['measures']:
                measure_name = measure_def['name']
                measure_type = measure_def.get('type', 'decimal')
                distribution = measure_def.get('distribution', 'uniform')
                min_val = measure_def.get('min', 0)
                max_val = measure_def.get('max', 1000)

                measure_data = self._generate_measure(
                    distribution, row_count, min_val, max_val, None, None, measure_type
                )
                data[measure_name] = measure_data

            # Generate categorical attributes (non-numeric columns)
            for attr_def in template.get('attributes', []):
                attr_name = attr_def['name']
                attr_values = attr_def.get('values', [])
                attr_weights = attr_def.get('weights')

                if attr_values:
                    attr_data = self._generate_categorical(
                        attr_values, row_count, attr_weights
                    )
                    data[attr_name] = attr_data

            # Create DataFrame
            df = pl.DataFrame(data)

            return {
                'success': True,
                'df': df,
                'table_name': f'fact_{fact_type}',
                'row_count': len(df),
                'columns': df.columns,
                'fact_type': fact_type,
                'sample_data': df.head(5).to_dicts()
            }

        except Exception as e:
            logger.error(f"Error generating fact from type: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

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

            # Generate measure/attribute columns
            for measure_def in measures:
                measure_name = measure_def['name']
                measure_type = measure_def.get('type', 'decimal')

                # Handle categorical/string types
                if measure_type in ('category', 'string', 'text'):
                    attr_values = measure_def.get('values', [])
                    attr_weights = measure_def.get('weights')
                    if attr_values:
                        data[measure_name] = self._generate_categorical(
                            attr_values, row_count, attr_weights
                        )
                    else:
                        # Default empty string if no values provided
                        data[measure_name] = [''] * row_count
                else:
                    # Numeric types
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
                'df': df,
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

    def _generate_categorical(
        self,
        values: List[str],
        count: int,
        weights: Optional[List[float]] = None
    ) -> List[str]:
        """
        Generate categorical values with optional weighted distribution.

        Args:
            values: List of possible categorical values
            count: Number of values to generate
            weights: Optional weights for each value (must sum to ~1.0)

        Returns:
            List of randomly selected categorical values
        """
        if weights:
            return random.choices(values, weights=weights, k=count)
        else:
            return random.choices(values, k=count)
