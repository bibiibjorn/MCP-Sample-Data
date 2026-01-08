"""
Correlation Engine - Generate correlated data columns

Supports:
- Statistical correlations (Pearson/Spearman coefficients)
- Categorical correlations (conditional distributions)
- Formula-based derivations (computed columns)
- Tiered relationships (quantity ranges → discount tiers)
- Copula-based multivariate correlations
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
from enum import Enum
import math
import random
from datetime import date, datetime

import polars as pl
import numpy as np


class CorrelationType(Enum):
    """Types of correlation relationships"""
    STATISTICAL = "statistical"       # Numeric correlation (e.g., quantity vs discount)
    CATEGORICAL = "categorical"       # Conditional distribution (e.g., region → shipping cost)
    FORMULA = "formula"               # Computed column (e.g., total = qty * price)
    TIERED = "tiered"                 # Range-based mapping (e.g., quantity → discount tier)
    CONDITIONAL = "conditional"       # If-then rules
    COPULA = "copula"                 # Multivariate statistical correlation


@dataclass
class CorrelationRule:
    """Definition of a correlation relationship"""
    name: str
    correlation_type: CorrelationType
    source_columns: List[str]
    target_column: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    description: str = ""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CorrelationRule':
        """Create rule from dictionary"""
        return cls(
            name=data.get('name', 'unnamed'),
            correlation_type=CorrelationType(data.get('type', 'formula')),
            source_columns=data.get('source_columns', []),
            target_column=data.get('target_column', ''),
            parameters=data.get('parameters', {}),
            description=data.get('description', '')
        )


@dataclass
class TierDefinition:
    """Definition of a tier for tiered correlations"""
    min_value: float
    max_value: float
    output_value: Any
    output_distribution: Optional[Dict[Any, float]] = None  # For probabilistic outputs


class CorrelationEngine:
    """Engine for generating correlated data"""

    def __init__(self, seed: Optional[int] = None):
        """Initialize engine with optional seed"""
        self.seed = seed
        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)

    def apply_correlations(
        self,
        df: pl.DataFrame,
        rules: List[CorrelationRule]
    ) -> pl.DataFrame:
        """Apply multiple correlation rules to a DataFrame"""
        result_df = df.clone()

        for rule in rules:
            try:
                result_df = self._apply_rule(result_df, rule)
            except Exception as e:
                print(f"Warning: Failed to apply rule '{rule.name}': {e}")

        return result_df

    def _apply_rule(self, df: pl.DataFrame, rule: CorrelationRule) -> pl.DataFrame:
        """Apply a single correlation rule"""
        if rule.correlation_type == CorrelationType.STATISTICAL:
            return self._apply_statistical(df, rule)
        elif rule.correlation_type == CorrelationType.CATEGORICAL:
            return self._apply_categorical(df, rule)
        elif rule.correlation_type == CorrelationType.FORMULA:
            return self._apply_formula(df, rule)
        elif rule.correlation_type == CorrelationType.TIERED:
            return self._apply_tiered(df, rule)
        elif rule.correlation_type == CorrelationType.CONDITIONAL:
            return self._apply_conditional(df, rule)
        elif rule.correlation_type == CorrelationType.COPULA:
            return self._apply_copula(df, rule)
        else:
            return df

    def _apply_statistical(self, df: pl.DataFrame, rule: CorrelationRule) -> pl.DataFrame:
        """Apply statistical correlation (generate correlated random values)"""
        source_col = rule.source_columns[0]
        target_col = rule.target_column
        params = rule.parameters

        # Get correlation coefficient
        correlation = params.get('correlation', 0.5)
        target_mean = params.get('mean', 100)
        target_std = params.get('std', 20)
        min_value = params.get('min_value')
        max_value = params.get('max_value')

        # Normalize source column
        source_values = np.array(df[source_col].to_list())
        source_mean = np.mean(source_values)
        source_std = np.std(source_values)

        if source_std == 0:
            source_std = 1

        normalized_source = (source_values - source_mean) / source_std

        # Generate correlated values using Cholesky decomposition
        n = len(df)
        independent_noise = np.random.normal(0, 1, n)

        # Correlated component
        correlated = correlation * normalized_source + math.sqrt(1 - correlation**2) * independent_noise

        # Scale to target distribution
        target_values = target_mean + target_std * correlated

        # Apply bounds
        if min_value is not None:
            target_values = np.maximum(target_values, min_value)
        if max_value is not None:
            target_values = np.minimum(target_values, max_value)

        # Round if needed
        if params.get('round_to') is not None:
            target_values = np.round(target_values, params['round_to'])

        return df.with_columns(pl.Series(name=target_col, values=target_values.tolist()))

    def _apply_categorical(self, df: pl.DataFrame, rule: CorrelationRule) -> pl.DataFrame:
        """Apply categorical correlation (conditional distributions)"""
        source_col = rule.source_columns[0]
        target_col = rule.target_column
        params = rule.parameters

        # Get conditional distributions
        # Format: {source_value: {target_value: probability, ...}, ...}
        distributions = params.get('distributions', {})
        default_distribution = params.get('default', {})

        source_values = df[source_col].to_list()
        target_values = []

        for src_val in source_values:
            # Get distribution for this source value
            dist = distributions.get(str(src_val), distributions.get(src_val, default_distribution))

            if dist:
                options = list(dist.keys())
                weights = list(dist.values())
                # Normalize weights
                total = sum(weights)
                weights = [w / total for w in weights]
                target_values.append(random.choices(options, weights=weights, k=1)[0])
            else:
                target_values.append(None)

        return df.with_columns(pl.Series(name=target_col, values=target_values))

    def _apply_formula(self, df: pl.DataFrame, rule: CorrelationRule) -> pl.DataFrame:
        """Apply formula-based calculation"""
        target_col = rule.target_column
        params = rule.parameters

        formula = params.get('formula', '')
        round_to = params.get('round_to')

        # Simple formula parser supporting basic operations
        # Supports: +, -, *, /, **, (), column references, constants
        try:
            # Build expression using Polars
            expr = self._parse_formula(formula, df.columns)
            result_df = df.with_columns(expr.alias(target_col))

            if round_to is not None:
                result_df = result_df.with_columns(
                    pl.col(target_col).round(round_to)
                )

            return result_df

        except Exception as e:
            # Fallback to row-by-row calculation
            return self._apply_formula_fallback(df, rule)

    def _parse_formula(self, formula: str, columns: List[str]) -> pl.Expr:
        """Parse formula into Polars expression"""
        # Replace column names with pl.col() references
        expr_str = formula

        # Sort columns by length (longest first) to avoid partial replacements
        sorted_cols = sorted(columns, key=len, reverse=True)

        for col in sorted_cols:
            # Use word boundaries to avoid partial matches
            import re
            expr_str = re.sub(rf'\b{re.escape(col)}\b', f'pl.col("{col}")', expr_str)

        # Evaluate the expression
        expr = eval(expr_str)
        return expr

    def _apply_formula_fallback(self, df: pl.DataFrame, rule: CorrelationRule) -> pl.DataFrame:
        """Fallback formula application using row-by-row calculation"""
        target_col = rule.target_column
        params = rule.parameters
        formula = params.get('formula', '')
        round_to = params.get('round_to')

        # Convert to list of dicts for row-by-row processing
        rows = df.to_dicts()
        results = []

        for row in rows:
            try:
                # Replace column names with values
                expr = formula
                for col, val in row.items():
                    if val is not None:
                        expr = expr.replace(col, str(val))
                result = eval(expr)
                if round_to is not None:
                    result = round(result, round_to)
                results.append(result)
            except Exception:
                results.append(None)

        return df.with_columns(pl.Series(name=target_col, values=results))

    def _apply_tiered(self, df: pl.DataFrame, rule: CorrelationRule) -> pl.DataFrame:
        """Apply tiered correlation (range-based mapping)"""
        source_col = rule.source_columns[0]
        target_col = rule.target_column
        params = rule.parameters

        # Get tier definitions
        # Format: [{min: 0, max: 10, value: 0.0}, {min: 10, max: 50, value: 0.05}, ...]
        tiers = params.get('tiers', [])
        default_value = params.get('default')

        source_values = df[source_col].to_list()
        target_values = []

        for src_val in source_values:
            if src_val is None:
                target_values.append(default_value)
                continue

            matched = False
            for tier in tiers:
                min_val = tier.get('min', float('-inf'))
                max_val = tier.get('max', float('inf'))

                if min_val <= src_val < max_val:
                    # Check for probabilistic output
                    if 'distribution' in tier:
                        dist = tier['distribution']
                        options = list(dist.keys())
                        weights = list(dist.values())
                        target_values.append(random.choices(options, weights=weights, k=1)[0])
                    else:
                        target_values.append(tier.get('value', default_value))
                    matched = True
                    break

            if not matched:
                target_values.append(default_value)

        return df.with_columns(pl.Series(name=target_col, values=target_values))

    def _apply_conditional(self, df: pl.DataFrame, rule: CorrelationRule) -> pl.DataFrame:
        """Apply conditional rules (if-then logic)"""
        target_col = rule.target_column
        params = rule.parameters

        # Get conditions
        # Format: [{condition: "column1 > 100 and column2 == 'A'", value: 10}, ...]
        conditions = params.get('conditions', [])
        default_value = params.get('default')

        rows = df.to_dicts()
        results = []

        for row in rows:
            matched = False
            for cond in conditions:
                condition_str = cond.get('condition', '')

                # Replace column names with values
                eval_condition = condition_str
                for col, val in row.items():
                    if isinstance(val, str):
                        eval_condition = eval_condition.replace(col, f'"{val}"')
                    elif val is None:
                        eval_condition = eval_condition.replace(col, 'None')
                    else:
                        eval_condition = eval_condition.replace(col, str(val))

                try:
                    if eval(eval_condition):
                        # Check for probabilistic output
                        if 'distribution' in cond:
                            dist = cond['distribution']
                            options = list(dist.keys())
                            weights = list(dist.values())
                            results.append(random.choices(options, weights=weights, k=1)[0])
                        else:
                            results.append(cond.get('value', default_value))
                        matched = True
                        break
                except Exception:
                    continue

            if not matched:
                results.append(default_value)

        return df.with_columns(pl.Series(name=target_col, values=results))

    def _apply_copula(self, df: pl.DataFrame, rule: CorrelationRule) -> pl.DataFrame:
        """Apply copula-based multivariate correlation"""
        target_col = rule.target_column
        params = rule.parameters

        # Correlation matrix with source columns
        correlations = params.get('correlations', {})
        target_mean = params.get('mean', 100)
        target_std = params.get('std', 20)
        distribution = params.get('distribution', 'normal')
        min_value = params.get('min_value')
        max_value = params.get('max_value')

        n = len(df)

        if not correlations:
            # No correlations specified, generate independent
            if distribution == 'normal':
                values = np.random.normal(target_mean, target_std, n)
            elif distribution == 'lognormal':
                values = np.random.lognormal(np.log(target_mean), target_std / target_mean, n)
            else:
                values = np.random.normal(target_mean, target_std, n)
        else:
            # Build correlation-based generation
            source_cols = list(correlations.keys())
            corr_values = list(correlations.values())

            # Normalize source columns
            normalized_sources = []
            for col in source_cols:
                source_values = np.array(df[col].to_list())
                mean = np.mean(source_values)
                std = np.std(source_values) or 1
                normalized_sources.append((source_values - mean) / std)

            # Weighted combination of sources
            total_corr = sum(abs(c) for c in corr_values)
            if total_corr > 1:
                # Normalize correlations
                corr_values = [c / total_corr for c in corr_values]
                total_corr = 1

            combined = np.zeros(n)
            for norm_src, corr in zip(normalized_sources, corr_values):
                combined += corr * norm_src

            # Add independent noise
            remaining_variance = max(0, 1 - sum(c**2 for c in corr_values))
            if remaining_variance > 0:
                combined += math.sqrt(remaining_variance) * np.random.normal(0, 1, n)

            # Scale to target distribution
            values = target_mean + target_std * combined

        # Apply bounds
        if min_value is not None:
            values = np.maximum(values, min_value)
        if max_value is not None:
            values = np.minimum(values, max_value)

        # Round if needed
        if params.get('round_to') is not None:
            values = np.round(values, params['round_to'])

        return df.with_columns(pl.Series(name=target_col, values=values.tolist()))

    def generate_correlated_fact(
        self,
        base_columns: Dict[str, Any],
        correlation_rules: List[Dict[str, Any]],
        row_count: int = 10000,
        seed: Optional[int] = None
    ) -> pl.DataFrame:
        """
        Generate a fact table with correlated columns.

        Args:
            base_columns: Base column configurations {name: config}
            correlation_rules: List of correlation rule definitions
            row_count: Number of rows to generate
            seed: Random seed

        Returns:
            DataFrame with correlated columns
        """
        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)

        # Generate base columns first
        data = {}
        for col_name, config in base_columns.items():
            if isinstance(config, dict):
                col_type = config.get('type', 'random')
                if col_type == 'random':
                    mean = config.get('mean', 100)
                    std = config.get('std', 20)
                    data[col_name] = np.random.normal(mean, std, row_count).tolist()
                elif col_type == 'uniform':
                    min_val = config.get('min', 0)
                    max_val = config.get('max', 100)
                    data[col_name] = np.random.uniform(min_val, max_val, row_count).tolist()
                elif col_type == 'choice':
                    options = config.get('options', ['A', 'B', 'C'])
                    weights = config.get('weights')
                    if weights:
                        data[col_name] = random.choices(options, weights=weights, k=row_count)
                    else:
                        data[col_name] = [random.choice(options) for _ in range(row_count)]
                elif col_type == 'sequence':
                    start = config.get('start', 1)
                    data[col_name] = list(range(start, start + row_count))
            elif isinstance(config, list):
                # List of choices
                data[col_name] = [random.choice(config) for _ in range(row_count)]
            else:
                # Constant value
                data[col_name] = [config] * row_count

        df = pl.DataFrame(data)

        # Parse and apply correlation rules
        rules = [CorrelationRule.from_dict(r) for r in correlation_rules]
        df = self.apply_correlations(df, rules)

        return df


# Pre-defined correlation patterns
CORRELATION_PATTERNS = {
    'sales_quantity_discount': {
        'description': 'Higher quantities get higher discounts',
        'type': 'tiered',
        'source_columns': ['quantity'],
        'target_column': 'discount_rate',
        'parameters': {
            'tiers': [
                {'min': 0, 'max': 10, 'value': 0.0},
                {'min': 10, 'max': 25, 'value': 0.05},
                {'min': 25, 'max': 50, 'value': 0.10},
                {'min': 50, 'max': 100, 'value': 0.15},
                {'min': 100, 'max': float('inf'), 'value': 0.20}
            ],
            'default': 0.0
        }
    },
    'sales_total_calculation': {
        'description': 'Calculate total from quantity, price, and discount',
        'type': 'formula',
        'source_columns': ['quantity', 'unit_price', 'discount_rate'],
        'target_column': 'total_amount',
        'parameters': {
            'formula': 'quantity * unit_price * (1 - discount_rate)',
            'round_to': 2
        }
    },
    'region_shipping_cost': {
        'description': 'Shipping cost varies by region',
        'type': 'categorical',
        'source_columns': ['region'],
        'target_column': 'shipping_cost',
        'parameters': {
            'distributions': {
                'North': {5.99: 0.3, 7.99: 0.5, 12.99: 0.2},
                'South': {4.99: 0.4, 6.99: 0.4, 9.99: 0.2},
                'East': {6.99: 0.3, 8.99: 0.4, 14.99: 0.3},
                'West': {7.99: 0.3, 9.99: 0.4, 19.99: 0.3}
            },
            'default': {6.99: 0.5, 9.99: 0.5}
        }
    },
    'customer_segment_discount': {
        'description': 'Discount varies by customer segment',
        'type': 'categorical',
        'source_columns': ['customer_segment'],
        'target_column': 'loyalty_discount',
        'parameters': {
            'distributions': {
                'Consumer': {0.0: 0.6, 0.05: 0.3, 0.10: 0.1},
                'Corporate': {0.05: 0.3, 0.10: 0.4, 0.15: 0.3},
                'Home Office': {0.0: 0.4, 0.05: 0.4, 0.10: 0.2}
            }
        }
    },
    'price_cost_correlation': {
        'description': 'Unit cost correlated with unit price',
        'type': 'statistical',
        'source_columns': ['unit_price'],
        'target_column': 'unit_cost',
        'parameters': {
            'correlation': 0.85,
            'mean': 50,
            'std': 15,
            'min_value': 5,
            'round_to': 2
        }
    }
}


def get_correlation_pattern(pattern_name: str) -> Optional[Dict[str, Any]]:
    """Get a predefined correlation pattern"""
    return CORRELATION_PATTERNS.get(pattern_name)


def list_correlation_patterns() -> List[Dict[str, str]]:
    """List available correlation patterns"""
    return [
        {'name': name, 'description': pattern.get('description', '')}
        for name, pattern in CORRELATION_PATTERNS.items()
    ]
