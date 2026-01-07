"""
Domain Detector Module
Classifies the domain/type of data (financial, sales, inventory, HR)
"""
import polars as pl
from typing import Dict, Any, List, Set
import re
import logging

logger = logging.getLogger(__name__)


class DomainDetector:
    """Detects the domain of data based on column names and content"""

    def __init__(self):
        self.domain_indicators = {
            'financial': {
                'columns': {
                    'debit', 'credit', 'balance', 'account', 'ledger', 'journal',
                    'fiscal', 'period', 'gl', 'revenue', 'expense', 'asset',
                    'liability', 'equity', 'profit', 'loss', 'budget', 'actual',
                    'variance', 'trial_balance', 'coa', 'chart_of_accounts'
                },
                'patterns': [
                    r'debit|credit', r'account.*code', r'gl.*type', r'fiscal.*year',
                    r'journal.*entry', r'balance.*sheet', r'income.*statement',
                    r'posting.*period', r'currency'
                ],
                'subtypes': {
                    'balance_sheet': ['asset', 'liability', 'equity'],
                    'income_statement': ['revenue', 'expense', 'profit', 'loss'],
                    'trial_balance': ['debit', 'credit', 'balance'],
                    'general_ledger': ['journal', 'entry', 'posting'],
                    'chart_of_accounts': ['account_code', 'account_name', 'account_type']
                }
            },
            'sales': {
                'columns': {
                    'order', 'customer', 'product', 'sale', 'quantity', 'price',
                    'discount', 'invoice', 'ship', 'payment', 'channel', 'region',
                    'territory', 'rep', 'commission', 'return', 'refund'
                },
                'patterns': [
                    r'order.*id', r'customer.*id', r'product.*id', r'unit.*price',
                    r'line.*total', r'ship.*date', r'order.*date'
                ],
                'subtypes': {
                    'orders': ['order', 'line_item', 'quantity'],
                    'customers': ['customer', 'segment', 'region'],
                    'products': ['product', 'category', 'price']
                }
            },
            'inventory': {
                'columns': {
                    'stock', 'warehouse', 'inventory', 'bin', 'location', 'sku',
                    'reorder', 'safety', 'lead_time', 'movement', 'receipt',
                    'issue', 'transfer', 'adjustment', 'lot', 'serial'
                },
                'patterns': [
                    r'stock.*level', r'warehouse.*id', r'bin.*location',
                    r'reorder.*point', r'safety.*stock'
                ],
                'subtypes': {
                    'stock_levels': ['quantity', 'location', 'sku'],
                    'movements': ['movement', 'receipt', 'issue'],
                    'warehouses': ['warehouse', 'location', 'capacity']
                }
            },
            'hr': {
                'columns': {
                    'employee', 'salary', 'department', 'position', 'hire',
                    'termination', 'performance', 'leave', 'attendance',
                    'payroll', 'benefit', 'bonus', 'manager', 'headcount'
                },
                'patterns': [
                    r'employee.*id', r'hire.*date', r'department.*id',
                    r'salary', r'bonus', r'performance.*rating'
                ],
                'subtypes': {
                    'employees': ['employee', 'name', 'hire_date'],
                    'departments': ['department', 'manager'],
                    'payroll': ['salary', 'bonus', 'deduction']
                }
            }
        }

    def detect(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Detect the domain of a DataFrame"""
        columns_lower = [col.lower() for col in df.columns]
        columns_set = set(columns_lower)

        scores = {}
        subtype_matches = {}

        for domain, indicators in self.domain_indicators.items():
            score = 0
            matched_columns = []
            matched_patterns = []

            # Check column name matches
            domain_cols = indicators['columns']
            for col in columns_lower:
                for indicator in domain_cols:
                    if indicator in col:
                        score += 1
                        matched_columns.append(col)
                        break

            # Check pattern matches
            for pattern in indicators['patterns']:
                for col in columns_lower:
                    if re.search(pattern, col):
                        score += 2  # Patterns are more specific
                        matched_patterns.append(col)

            scores[domain] = {
                'score': score,
                'matched_columns': list(set(matched_columns)),
                'matched_patterns': list(set(matched_patterns))
            }

            # Detect subtypes
            for subtype, subtype_indicators in indicators.get('subtypes', {}).items():
                subtype_matches_count = sum(
                    1 for ind in subtype_indicators
                    for col in columns_lower if ind in col
                )
                if subtype_matches_count > 0:
                    if domain not in subtype_matches:
                        subtype_matches[domain] = {}
                    subtype_matches[domain][subtype] = subtype_matches_count

        # Determine primary domain
        max_score = max(s['score'] for s in scores.values())
        if max_score == 0:
            primary_domain = 'generic'
            confidence = 0.0
        else:
            primary_domain = max(scores, key=lambda d: scores[d]['score'])
            # Normalize confidence
            total_score = sum(s['score'] for s in scores.values())
            confidence = round(scores[primary_domain]['score'] / total_score, 2) if total_score > 0 else 0

        # Determine subtype
        primary_subtype = None
        if primary_domain in subtype_matches and subtype_matches[primary_domain]:
            primary_subtype = max(subtype_matches[primary_domain],
                                  key=subtype_matches[primary_domain].get)

        result = {
            'primary_domain': primary_domain,
            'confidence': confidence,
            'subtype': primary_subtype,
            'domain_scores': scores,
            'suggested_validation_rules': self._get_validation_rules(primary_domain, primary_subtype),
            'recommended_tables': self._get_recommended_tables(primary_domain)
        }

        return result

    def _get_validation_rules(self, domain: str, subtype: str = None) -> List[Dict]:
        """Get suggested validation rules for a domain"""
        rules = {
            'financial': [
                {'type': 'balance_check', 'description': 'Debit = Credit per period'},
                {'type': 'referential', 'description': 'Account codes exist in CoA'},
                {'type': 'not_null', 'description': 'Amount columns should not be null'}
            ],
            'sales': [
                {'type': 'referential', 'description': 'Customer IDs exist in customer master'},
                {'type': 'range', 'description': 'Prices should be positive'},
                {'type': 'not_null', 'description': 'Order date should not be null'}
            ],
            'inventory': [
                {'type': 'range', 'description': 'Stock levels should be non-negative'},
                {'type': 'referential', 'description': 'SKUs exist in product master'}
            ],
            'hr': [
                {'type': 'referential', 'description': 'Department IDs exist in department master'},
                {'type': 'range', 'description': 'Salary should be positive'}
            ]
        }
        return rules.get(domain, [])

    def _get_recommended_tables(self, domain: str) -> List[str]:
        """Get recommended related tables for a domain"""
        tables = {
            'financial': ['dim_account', 'dim_period', 'dim_entity', 'fact_gl_transactions'],
            'sales': ['dim_customer', 'dim_product', 'dim_date', 'fact_sales'],
            'inventory': ['dim_product', 'dim_warehouse', 'dim_date', 'fact_inventory'],
            'hr': ['dim_employee', 'dim_department', 'dim_date', 'fact_payroll']
        }
        return tables.get(domain, [])


def detect_domain_hints(df: pl.DataFrame) -> Dict[str, Any]:
    """Convenience function to detect domain hints"""
    detector = DomainDetector()
    result = detector.detect(df)
    return {
        'domain': result['primary_domain'],
        'subtype': result['subtype'],
        'confidence': result['confidence'],
        'indicators': result['domain_scores'].get(result['primary_domain'], {}).get('matched_columns', [])
    }
