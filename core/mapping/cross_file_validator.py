"""
Cross File Validator Module
Validates data through mapping hierarchies
"""
import polars as pl
from typing import Dict, Any, List, Optional
from rapidfuzz import fuzz, process
import logging

logger = logging.getLogger(__name__)

# Token optimization constants
UNMAPPED_VALUES_LIMIT = 10      # Max unmapped values to show
FUZZY_MATCHES_LIMIT = 10        # Max fuzzy matches to suggest
SOURCE_ONLY_LIMIT = 20          # Max source-only values
BREAKDOWN_LIMIT = 20            # Max category breakdown items
MAX_VALUE_LENGTH = 50           # Truncate long values
MAX_HIERARCHY_DEPTH = 50        # Max recursion depth for hierarchy traversal


def _truncate_val(val: Any) -> Any:
    """Truncate long string values for token efficiency."""
    if isinstance(val, str) and len(val) > MAX_VALUE_LENGTH:
        return val[:MAX_VALUE_LENGTH - 3] + '...'
    return val


class CrossFileValidator:
    """Validates data across multiple files through mappings"""

    def __init__(self):
        pass

    def validate_amounts(
        self,
        source_df: pl.DataFrame,
        amount_column: str,
        group_column: str,
        validation_rule: Dict[str, Any],
        mapping_df: Optional[pl.DataFrame] = None,
        mapping_source_column: Optional[str] = None,
        mapping_target_column: Optional[str] = None,
        tolerance: float = 0.01
    ) -> Dict[str, Any]:
        """
        Validate amounts using user-defined rules.

        Validation rule types:
        - sum_equals: left groups = sum of right groups (e.g., Total = A + B + C)
        - difference_equals: left - right = expected (e.g., Revenue - COGS = GrossProfit)
        - groups_balance: all groups sum to zero (e.g., debits = credits)
        - ratio_in_range: left/right within min/max range

        Args:
            source_df: Source data with amounts
            amount_column: Column with amounts
            group_column: Column to group by
            validation_rule: Rule definition with type and parameters
            mapping_df: Optional mapping to classify source groups
            mapping_source_column: Column in mapping matching source
            mapping_target_column: Column in mapping with target group names
            tolerance: Allowed difference for equality checks

        Returns:
            Validation results
        """
        try:
            rule_type = validation_rule.get('type', 'sum_equals')

            # Build mapping lookup if mapping provided
            mapping_lookup = {}
            if mapping_df is not None and mapping_source_column and mapping_target_column:
                for row in mapping_df.iter_rows(named=True):
                    source_val = row.get(mapping_source_column)
                    target_val = row.get(mapping_target_column)
                    if source_val and target_val:
                        mapping_lookup[str(source_val).strip()] = str(target_val).strip()

            # Calculate totals by group
            group_totals = {}
            unmapped_values = []
            mapped_count = 0
            unmapped_count = 0

            for row in source_df.iter_rows(named=True):
                source_group = row.get(group_column)
                amount = row.get(amount_column, 0) or 0

                if source_group:
                    source_group_str = str(source_group).strip()

                    # Apply mapping if available
                    if mapping_lookup:
                        target_group = mapping_lookup.get(source_group_str)
                        if not target_group:
                            # Try fuzzy matching
                            target_group = self._fuzzy_lookup(
                                source_group_str,
                                list(mapping_lookup.keys()),
                                mapping_lookup
                            )
                        if target_group:
                            mapped_count += 1
                            group_totals[target_group] = group_totals.get(target_group, 0) + amount
                        else:
                            unmapped_count += 1
                            group_totals['_unmapped_'] = group_totals.get('_unmapped_', 0) + amount
                            if len(unmapped_values) < UNMAPPED_VALUES_LIMIT:
                                if source_group_str not in [u['value'] for u in unmapped_values]:
                                    unmapped_values.append({
                                        'value': _truncate_val(source_group_str),
                                        'amount': amount
                                    })
                    else:
                        # No mapping - use source group directly
                        mapped_count += 1
                        group_totals[source_group_str] = group_totals.get(source_group_str, 0) + amount

            # Apply validation rule
            validation_result = self._apply_validation_rule(
                rule_type, validation_rule, group_totals, tolerance
            )

            # Build response
            total_rows = mapped_count + unmapped_count
            coverage = (mapped_count / total_rows * 100) if total_rows > 0 else 100

            result = {
                'success': True,
                'validation_passed': validation_result['passed'],
                'rule_type': rule_type,
                'rule_details': validation_result['details'],
                'group_totals': {k: round(v, 2) for k, v in group_totals.items() if k != '_unmapped_'},
                'tolerance': tolerance
            }

            if mapping_lookup:
                result['mapping_coverage'] = {
                    'mapped_rows': mapped_count,
                    'unmapped_rows': unmapped_count,
                    'coverage_pct': round(coverage, 1)
                }
                if unmapped_values:
                    result['unmapped_values'] = unmapped_values

            if group_totals.get('_unmapped_', 0) != 0:
                result['unmapped_amount'] = round(group_totals.get('_unmapped_', 0), 2)

            return result

        except Exception as e:
            logger.error(f"Error validating amounts: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _apply_validation_rule(
        self,
        rule_type: str,
        rule: Dict[str, Any],
        group_totals: Dict[str, float],
        tolerance: float
    ) -> Dict[str, Any]:
        """Apply a specific validation rule to group totals."""

        left_groups = rule.get('left', [])
        right_groups = rule.get('right', [])

        # Calculate left and right sums
        left_sum = sum(group_totals.get(g, 0) for g in left_groups)
        right_sum = sum(group_totals.get(g, 0) for g in right_groups)

        if rule_type == 'sum_equals':
            # Left should equal sum of right: Total = A + B + C
            difference = left_sum - right_sum
            passed = abs(difference) <= tolerance
            return {
                'passed': passed,
                'details': {
                    'left_groups': left_groups,
                    'left_sum': round(left_sum, 2),
                    'right_groups': right_groups,
                    'right_sum': round(right_sum, 2),
                    'difference': round(difference, 2),
                    'equation': f"{'+'.join(left_groups)} = {'+'.join(right_groups)}"
                }
            }

        elif rule_type == 'difference_equals':
            # Left - Right should equal expected: Revenue - COGS = 500000
            expected = rule.get('expected', 0)
            actual_diff = left_sum - right_sum
            difference = actual_diff - expected
            passed = abs(difference) <= tolerance
            return {
                'passed': passed,
                'details': {
                    'left_groups': left_groups,
                    'left_sum': round(left_sum, 2),
                    'right_groups': right_groups,
                    'right_sum': round(right_sum, 2),
                    'expected_difference': expected,
                    'actual_difference': round(actual_diff, 2),
                    'variance': round(difference, 2),
                    'equation': f"{'+'.join(left_groups)} - {'+'.join(right_groups)} = {expected}"
                }
            }

        elif rule_type == 'groups_balance':
            # All specified groups should sum to zero (or left = right if both specified)
            if left_groups and right_groups:
                difference = left_sum - right_sum
                passed = abs(difference) <= tolerance
                return {
                    'passed': passed,
                    'details': {
                        'left_groups': left_groups,
                        'left_sum': round(left_sum, 2),
                        'right_groups': right_groups,
                        'right_sum': round(right_sum, 2),
                        'difference': round(difference, 2)
                    }
                }
            else:
                # All groups should sum to zero
                all_groups = left_groups or list(group_totals.keys())
                total = sum(group_totals.get(g, 0) for g in all_groups if g != '_unmapped_')
                passed = abs(total) <= tolerance
                return {
                    'passed': passed,
                    'details': {
                        'groups': all_groups,
                        'total': round(total, 2),
                        'expected': 0
                    }
                }

        elif rule_type == 'ratio_in_range':
            # Left/Right should be within min/max range
            min_ratio = rule.get('min_ratio', 0)
            max_ratio = rule.get('max_ratio', float('inf'))

            if right_sum == 0:
                passed = False
                actual_ratio = None
                message = "Cannot calculate ratio: denominator is zero"
            else:
                actual_ratio = left_sum / right_sum
                passed = min_ratio <= actual_ratio <= max_ratio
                message = None

            result = {
                'passed': passed,
                'details': {
                    'left_groups': left_groups,
                    'left_sum': round(left_sum, 2),
                    'right_groups': right_groups,
                    'right_sum': round(right_sum, 2),
                    'min_ratio': min_ratio,
                    'max_ratio': max_ratio
                }
            }
            if actual_ratio is not None:
                result['details']['actual_ratio'] = round(actual_ratio, 4)
            if message:
                result['details']['message'] = message
            return result

        elif rule_type == 'custom_equation':
            # For future extension - parse custom equation strings
            return {
                'passed': False,
                'details': {'error': 'custom_equation not yet implemented, use sum_equals or difference_equals'}
            }

        else:
            return {
                'passed': False,
                'details': {'error': f'Unknown rule type: {rule_type}'}
            }

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
        Token-optimized output with limited samples and truncated values.
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

            # Fuzzy matches for unmatched items (limited)
            fuzzy_matches = []
            for source_val in list(source_only)[:FUZZY_MATCHES_LIMIT * 2]:  # Check more, return limited
                if len(fuzzy_matches) >= FUZZY_MATCHES_LIMIT:
                    break
                result = process.extractOne(
                    source_val,
                    list(report_only),
                    scorer=fuzz.ratio
                )
                if result and result[1] >= 70:
                    fuzzy_matches.append({
                        'source': _truncate_val(source_val),
                        'match': _truncate_val(result[0]),
                        'score': result[1]
                    })

            coverage = len(exact_matches) / len(source_values) * 100 if source_values else 100

            result = {
                'success': True,
                'source_unique': len(source_values),
                'report_unique': len(report_values),
                'exact_matches': len(exact_matches),
                'coverage_pct': round(coverage, 1),
                'source_only': [_truncate_val(v) for v in list(source_only)[:SOURCE_ONLY_LIMIT]],
                'report_only': [_truncate_val(v) for v in list(report_only)[:SOURCE_ONLY_LIMIT]],
            }

            if fuzzy_matches:
                result['fuzzy_matches'] = fuzzy_matches

            # Add truncation notes if needed
            if len(source_only) > SOURCE_ONLY_LIMIT:
                result['source_only_truncated'] = len(source_only)
            if len(report_only) > SOURCE_ONLY_LIMIT:
                result['report_only_truncated'] = len(report_only)

            return result

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

        Supports two hierarchy patterns:
        1. Parent-contains-children: Each row defines parent -> child relationship
           (e.g., "Formula Header" contains parent, "Formula Element" contains child)
           Self-referencing rows (parent == child) define leaf nodes.
        2. Child-references-parent: Each row defines child -> parent reference

        Args:
            source_df: Source data
            formula_df: Formula/hierarchy file
            amount_column: Column with amounts
            source_mapping_column: Column to map source to formula
            formula_element_column: Column with formula element names (child in parent-contains-children pattern)
            formula_parent_column: Column with parent element references (parent in parent-contains-children pattern)
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

            # Detect hierarchy pattern by checking for self-referencing rows
            # Self-referencing rows (parent == child) indicate "parent-contains-children" pattern
            self_ref_count = 0
            non_self_ref_count = 0
            for row in formula_df.iter_rows(named=True):
                element = row.get(formula_element_column)
                parent = row.get(formula_parent_column)
                if element and parent:
                    element_str = str(element).strip()
                    parent_str = str(parent).strip()
                    if element_str == parent_str:
                        self_ref_count += 1
                    else:
                        non_self_ref_count += 1

            # Use parent-contains-children pattern if we have self-referencing rows
            # This is typical for formula files where Formula Header = parent, Formula Element = child
            use_parent_contains_children = self_ref_count > 0

            # Build hierarchy from formula
            hierarchy = {}
            for row in formula_df.iter_rows(named=True):
                element = row.get(formula_element_column)
                parent = row.get(formula_parent_column)
                if element:
                    element_str = str(element).strip()
                    parent_str = str(parent).strip() if parent else None

                    if use_parent_contains_children:
                        # Parent-contains-children pattern:
                        # - formula_parent_column = parent node
                        # - formula_element_column = child node
                        # - Self-referencing rows (parent == child) are leaf definitions

                        # Ensure parent exists in hierarchy
                        if parent_str and parent_str not in hierarchy:
                            hierarchy[parent_str] = {'children': set()}

                        # Ensure element exists in hierarchy
                        if element_str not in hierarchy:
                            hierarchy[element_str] = {'children': set()}

                        # Add element as child of parent (skip self-references - they're leaf definitions)
                        if parent_str and element_str != parent_str:
                            hierarchy[parent_str]['children'].add(element_str)
                    else:
                        # Child-references-parent pattern (original logic):
                        # - formula_element_column = child/current node
                        # - formula_parent_column = parent reference
                        if element_str not in hierarchy:
                            hierarchy[element_str] = {
                                'parent': parent_str,
                                'children': set()
                            }

            # For child-references-parent pattern, build children lists from parent references
            if not use_parent_contains_children:
                for element, info in hierarchy.items():
                    if info.get('parent') and info['parent'] in hierarchy:
                        hierarchy[info['parent']]['children'].add(element)

            # Convert children sets to sorted lists for consistent ordering
            for element in hierarchy:
                hierarchy[element]['children'] = sorted(hierarchy[element]['children'])

            # Cache for memoizing calculated totals
            total_cache = {}

            # Calculate rollup with memoization (no cycle detection needed for parent-contains-children)
            def calculate_total(element: str, visited: set = None, depth: int = 0) -> float:
                # Check depth limit
                if depth > MAX_HIERARCHY_DEPTH:
                    logger.warning(f"Max hierarchy depth exceeded at element: {element}")
                    return element_totals.get(element, 0)

                # Return cached value if available
                if element in total_cache:
                    return total_cache[element]

                if visited is None:
                    visited = set()
                if element in visited:
                    # Cycle detected - return only direct amount (shouldn't happen with proper data)
                    logger.warning(f"Cycle detected at element: {element}")
                    return element_totals.get(element, 0)
                visited.add(element)

                # Get direct amount (for leaf nodes)
                total = element_totals.get(element, 0)

                # Add children totals
                if element in hierarchy:
                    for child in hierarchy[element]['children']:
                        total += calculate_total(child, visited.copy(), depth + 1)

                # Cache the result
                total_cache[element] = total
                return total

            rollup_total = calculate_total(target_rollup)

            result = {
                'success': True,
                'target_rollup': target_rollup,
                'total': round(rollup_total, 2),
                'hierarchy_pattern': 'parent_contains_children' if use_parent_contains_children else 'child_references_parent',
                'self_referencing_rows': self_ref_count,
                'hierarchy_rows': non_self_ref_count
            }

            if show_detail and target_rollup in hierarchy:
                # Build detail breakdown
                def build_breakdown(element: str, level: int = 0, visited: set = None) -> Dict:
                    if visited is None:
                        visited = set()

                    # Check for cycles or max depth
                    if element in visited or level > MAX_HIERARCHY_DEPTH:
                        return {
                            'element': element,
                            'level': level,
                            'direct_amount': round(element_totals.get(element, 0), 2),
                            'total_amount': round(total_cache.get(element, element_totals.get(element, 0)), 2),
                            'children': [],
                            'cycle_detected': True
                        }

                    visited.add(element)
                    direct = element_totals.get(element, 0)
                    children_breakdown = []

                    if element in hierarchy:
                        for child in hierarchy[element]['children']:
                            children_breakdown.append(build_breakdown(child, level + 1, visited.copy()))

                    return {
                        'element': element,
                        'level': level,
                        'direct_amount': round(direct, 2),
                        'total_amount': round(total_cache.get(element, calculate_total(element)), 2),
                        'children': children_breakdown
                    }

                result['breakdown'] = build_breakdown(target_rollup)

            return result

        except Exception as e:
            logger.error(f"Error in rollup calculation: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
