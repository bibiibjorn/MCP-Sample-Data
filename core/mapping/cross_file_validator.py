"""
Cross File Validator Module
Validates data through mapping hierarchies
"""
import polars as pl
from typing import Dict, Any, List, Optional
from rapidfuzz import fuzz, process
import logging

logger = logging.getLogger(__name__)


class CrossFileValidator:
    """Validates data across multiple files through mappings"""

    def __init__(self):
        pass

    def validate_balance_sheet_equation(
        self,
        source_df: pl.DataFrame,
        mapping_df: pl.DataFrame,
        amount_column: str,
        category_column: str,
        mapping_source_column: str,
        mapping_target_column: str,
        mapping_category_column: str,
        filter_conditions: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Validate balance sheet equation: Assets = Liabilities + Equity.

        Args:
            source_df: Source data with amounts
            mapping_df: Mapping table with categories
            amount_column: Column with amounts in source
            category_column: Column to map through in source
            mapping_source_column: Column in mapping that matches source category
            mapping_target_column: Column in mapping with target category
            mapping_category_column: Column in mapping with Asset/Liability/Equity
            filter_conditions: Optional filters to apply to source data

        Returns:
            Validation results
        """
        try:
            # Apply filters if provided
            filtered_df = source_df
            if filter_conditions:
                for col, val in filter_conditions.items():
                    if col in filtered_df.columns:
                        filtered_df = filtered_df.filter(pl.col(col) == val)

            # Build mapping lookup
            mapping_lookup = {}
            for row in mapping_df.iter_rows(named=True):
                source_val = row.get(mapping_source_column)
                category = row.get(mapping_category_column)
                if source_val and category:
                    mapping_lookup[str(source_val).strip()] = category

            # Classify and sum amounts
            totals = {'Asset': 0.0, 'Liability': 0.0, 'Equity': 0.0, 'Unknown': 0.0}
            unmapped_values = []
            mapped_count = 0
            unmapped_count = 0

            category_breakdown = {}

            for row in filtered_df.iter_rows(named=True):
                source_category = row.get(category_column)
                amount = row.get(amount_column, 0) or 0

                if source_category:
                    source_category_str = str(source_category).strip()
                    mapped_category = mapping_lookup.get(source_category_str)

                    if not mapped_category:
                        # Try fuzzy matching
                        mapped_category = self._fuzzy_lookup(
                            source_category_str,
                            list(mapping_lookup.keys()),
                            mapping_lookup
                        )

                    if mapped_category:
                        mapped_count += 1
                        # Normalize category
                        cat_lower = mapped_category.lower()
                        if 'asset' in cat_lower:
                            totals['Asset'] += amount
                            category_breakdown[source_category_str] = {
                                'category': 'Asset',
                                'amount': category_breakdown.get(source_category_str, {}).get('amount', 0) + amount
                            }
                        elif 'liabilit' in cat_lower:
                            totals['Liability'] += amount
                            category_breakdown[source_category_str] = {
                                'category': 'Liability',
                                'amount': category_breakdown.get(source_category_str, {}).get('amount', 0) + amount
                            }
                        elif 'equity' in cat_lower or 'capital' in cat_lower:
                            totals['Equity'] += amount
                            category_breakdown[source_category_str] = {
                                'category': 'Equity',
                                'amount': category_breakdown.get(source_category_str, {}).get('amount', 0) + amount
                            }
                        else:
                            totals['Unknown'] += amount
                            unmapped_count += 1
                    else:
                        unmapped_count += 1
                        totals['Unknown'] += amount
                        if source_category_str not in [u['value'] for u in unmapped_values]:
                            unmapped_values.append({
                                'value': source_category_str,
                                'amount': amount,
                                'classification': 'unknown'
                            })

            # Check balance equation
            expected = totals['Liability'] + totals['Equity']
            difference = totals['Asset'] - expected

            total_rows = mapped_count + unmapped_count
            coverage = (mapped_count / total_rows * 100) if total_rows > 0 else 0

            return {
                'success': True,
                'validation_passed': abs(difference) < 0.01,
                'rule_applied': 'balance_sheet_equation',
                'details': {
                    'total_assets': round(totals['Asset'], 2),
                    'total_equity': round(totals['Equity'], 2),
                    'total_liabilities': round(totals['Liability'], 2),
                    'expected_balance': round(expected, 2),
                    'difference': round(difference, 2)
                },
                'unmapped_values': unmapped_values[:10],
                'mapping_coverage': {
                    'mapped_rows': mapped_count,
                    'unmapped_rows': unmapped_count,
                    'coverage_pct': round(coverage, 1)
                },
                'breakdown_by_category': [
                    {'category': 'Asset', 'total': round(totals['Asset'], 2)},
                    {'category': 'Liability', 'total': round(totals['Liability'], 2)},
                    {'category': 'Equity', 'total': round(totals['Equity'], 2)},
                    {'category': 'Unknown', 'total': round(totals['Unknown'], 2)}
                ]
            }

        except Exception as e:
            logger.error(f"Error validating balance sheet: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _fuzzy_lookup(
        self,
        value: str,
        candidates: List[str],
        lookup: Dict[str, str],
        threshold: float = 0.8
    ) -> Optional[str]:
        """Fuzzy lookup a value in candidates"""
        if not candidates:
            return None

        result = process.extractOne(value, candidates, scorer=fuzz.ratio)
        if result and result[1] >= threshold * 100:
            return lookup.get(result[0])
        return None

    def compare_structures(
        self,
        source_df: pl.DataFrame,
        report_df: pl.DataFrame,
        source_column: str,
        report_column: str
    ) -> Dict[str, Any]:
        """
        Compare structure of source data vs expected report format.

        Args:
            source_df: Source data
            report_df: Report structure definition
            source_column: Column in source with categories
            report_column: Column in report with line items

        Returns:
            Gap analysis results
        """
        try:
            source_values = set(
                str(v).strip() for v in source_df[source_column].unique().drop_nulls().to_list()
            )
            report_values = set(
                str(v).strip() for v in report_df[report_column].unique().drop_nulls().to_list()
            )

            # Exact matches
            exact_matches = source_values & report_values

            # Source values not in report
            source_only = source_values - report_values

            # Report values not in source
            report_only = report_values - source_values

            # Fuzzy matches for unmatched items
            fuzzy_matches = []
            for source_val in source_only:
                result = process.extractOne(
                    source_val,
                    list(report_only),
                    scorer=fuzz.ratio
                )
                if result and result[1] >= 70:
                    fuzzy_matches.append({
                        'source_value': source_val,
                        'potential_match': result[0],
                        'similarity': result[1]
                    })

            coverage = len(exact_matches) / len(source_values) * 100 if source_values else 100

            return {
                'success': True,
                'source_unique_values': len(source_values),
                'report_unique_values': len(report_values),
                'exact_matches': len(exact_matches),
                'coverage_percentage': round(coverage, 1),
                'source_only': list(source_only)[:20],
                'report_only': list(report_only)[:20],
                'potential_fuzzy_matches': fuzzy_matches[:10]
            }

        except Exception as e:
            logger.error(f"Error comparing structures: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def rollup_through_hierarchy(
        self,
        source_df: pl.DataFrame,
        formula_df: pl.DataFrame,
        amount_column: str,
        source_mapping_column: str,
        formula_element_column: str,
        formula_parent_column: str,
        target_rollup: str,
        show_detail: bool = True
    ) -> Dict[str, Any]:
        """
        Aggregate source data through a hierarchical formula structure.

        Args:
            source_df: Source data
            formula_df: Formula/hierarchy file
            amount_column: Column with amounts
            source_mapping_column: Column to map source to formula
            formula_element_column: Column with formula element names
            formula_parent_column: Column with parent element references
            target_rollup: Target total to calculate
            show_detail: Show contribution breakdown

        Returns:
            Rollup results
        """
        try:
            # Build element lookup from source
            element_totals = {}
            for row in source_df.iter_rows(named=True):
                element = row.get(source_mapping_column)
                amount = row.get(amount_column, 0) or 0
                if element:
                    element_str = str(element).strip()
                    element_totals[element_str] = element_totals.get(element_str, 0) + amount

            # Build hierarchy from formula
            hierarchy = {}
            for row in formula_df.iter_rows(named=True):
                element = row.get(formula_element_column)
                parent = row.get(formula_parent_column)
                if element:
                    element_str = str(element).strip()
                    hierarchy[element_str] = {
                        'parent': str(parent).strip() if parent else None,
                        'children': []
                    }

            # Build children lists
            for element, info in hierarchy.items():
                if info['parent'] and info['parent'] in hierarchy:
                    hierarchy[info['parent']]['children'].append(element)

            # Calculate rollup
            def calculate_total(element: str, visited: set = None) -> float:
                if visited is None:
                    visited = set()
                if element in visited:
                    return 0
                visited.add(element)

                # Get direct amount
                total = element_totals.get(element, 0)

                # Add children totals
                if element in hierarchy:
                    for child in hierarchy[element]['children']:
                        total += calculate_total(child, visited.copy())

                return total

            rollup_total = calculate_total(target_rollup)

            result = {
                'success': True,
                'target_rollup': target_rollup,
                'total': round(rollup_total, 2)
            }

            if show_detail and target_rollup in hierarchy:
                # Build detail breakdown
                def build_breakdown(element: str, level: int = 0) -> Dict:
                    direct = element_totals.get(element, 0)
                    children_breakdown = []

                    if element in hierarchy:
                        for child in hierarchy[element]['children']:
                            children_breakdown.append(build_breakdown(child, level + 1))

                    return {
                        'element': element,
                        'level': level,
                        'direct_amount': round(direct, 2),
                        'total_amount': round(calculate_total(element), 2),
                        'children': children_breakdown
                    }

                result['breakdown'] = build_breakdown(target_rollup)

            return result

        except Exception as e:
            logger.error(f"Error in rollup calculation: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
