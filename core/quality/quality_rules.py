"""
Quality Rules - Rule definitions and parsing for data quality validation
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Union
from enum import Enum
import re
import yaml
from pathlib import Path


class RuleType(Enum):
    """Types of quality rules"""
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    RANGE = "range"
    REGEX = "regex"
    ENUM = "enum"
    LENGTH = "length"
    DATE_ORDER = "date_order"
    SUM_EQUALS = "sum_equals"
    CUSTOM = "custom"


class Severity(Enum):
    """Rule violation severity"""
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


@dataclass
class QualityRule:
    """Definition of a single quality rule"""
    name: str
    rule_type: RuleType
    column: Optional[str] = None
    columns: Optional[List[str]] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    severity: Severity = Severity.WARNING
    description: Optional[str] = None
    enabled: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Convert rule to dictionary"""
        return {
            'name': self.name,
            'type': self.rule_type.value,
            'column': self.column,
            'columns': self.columns,
            'parameters': self.parameters,
            'severity': self.severity.value,
            'description': self.description,
            'enabled': self.enabled
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QualityRule':
        """Create rule from dictionary"""
        return cls(
            name=data.get('name', 'unnamed_rule'),
            rule_type=RuleType(data.get('type', 'custom')),
            column=data.get('column'),
            columns=data.get('columns'),
            parameters=data.get('parameters', {}),
            severity=Severity(data.get('severity', 'warning')),
            description=data.get('description'),
            enabled=data.get('enabled', True)
        )


@dataclass
class QualityRules:
    """Collection of quality rules"""
    rules: List[QualityRule] = field(default_factory=list)
    completeness_rules: Dict[str, Any] = field(default_factory=dict)
    validity_rules: Dict[str, Any] = field(default_factory=dict)
    uniqueness_rules: Dict[str, Any] = field(default_factory=dict)
    consistency_rules: List[Dict[str, Any]] = field(default_factory=list)
    accuracy_rules: Dict[str, Any] = field(default_factory=dict)
    timeliness_rules: Dict[str, Any] = field(default_factory=dict)

    def add_rule(self, rule: QualityRule):
        """Add a rule to the collection"""
        self.rules.append(rule)
        self._categorize_rule(rule)

    def _categorize_rule(self, rule: QualityRule):
        """Categorize rule by dimension"""
        if rule.rule_type == RuleType.NOT_NULL:
            if rule.column:
                self.completeness_rules[rule.column] = {
                    'max_null_pct': rule.parameters.get('max_null_pct', 0)
                }
        elif rule.rule_type == RuleType.UNIQUE:
            if rule.column:
                self.uniqueness_rules[rule.column] = {
                    'must_be_unique': True
                }
        elif rule.rule_type in [RuleType.RANGE, RuleType.REGEX, RuleType.ENUM, RuleType.LENGTH]:
            if rule.column:
                self.validity_rules[rule.column] = {
                    **self.validity_rules.get(rule.column, {}),
                    **rule.parameters
                }
        elif rule.rule_type in [RuleType.DATE_ORDER, RuleType.SUM_EQUALS]:
            self.consistency_rules.append({
                'type': rule.rule_type.value,
                'columns': rule.columns,
                **rule.parameters
            })

    def to_scorer_format(self) -> Dict[str, Any]:
        """Convert rules to format expected by QualityScorer"""
        result = {}

        # Merge completeness and validity rules per column
        all_columns = set(self.completeness_rules.keys()) | set(self.validity_rules.keys())
        for col in all_columns:
            result[col] = {
                **self.completeness_rules.get(col, {}),
                **self.validity_rules.get(col, {}),
                **self.timeliness_rules.get(col, {})
            }

        # Add consistency rules
        if self.consistency_rules:
            result['_consistency'] = self.consistency_rules

        return result

    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'QualityRules':
        """Load rules from YAML file"""
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        rules = cls()

        # Parse completeness rules
        for col_rule in data.get('completeness', {}).get('required_columns', []):
            rules.add_rule(QualityRule(
                name=f"not_null_{col_rule['column']}",
                rule_type=RuleType.NOT_NULL,
                column=col_rule['column'],
                parameters={'max_null_pct': col_rule.get('max_null_pct', 0)},
                severity=Severity(col_rule.get('severity', 'warning'))
            ))

        # Parse validity rules
        for val_rule in data.get('validity', []):
            rule_type = RuleType(val_rule.get('type', 'custom'))
            params = {k: v for k, v in val_rule.items() if k not in ['column', 'type', 'severity']}
            rules.add_rule(QualityRule(
                name=f"{rule_type.value}_{val_rule['column']}",
                rule_type=rule_type,
                column=val_rule['column'],
                parameters=params,
                severity=Severity(val_rule.get('severity', 'warning'))
            ))

        # Parse uniqueness rules
        for key_rule in data.get('uniqueness', {}).get('primary_keys', []):
            rules.add_rule(QualityRule(
                name=f"unique_{key_rule['column']}",
                rule_type=RuleType.UNIQUE,
                column=key_rule['column'],
                severity=Severity.CRITICAL
            ))

        # Parse consistency rules
        for cons_rule in data.get('consistency', []):
            rule_type = RuleType(cons_rule.get('type', 'custom'))
            rules.add_rule(QualityRule(
                name=cons_rule.get('name', f"consistency_{len(rules.rules)}"),
                rule_type=rule_type,
                columns=cons_rule.get('columns'),
                parameters={k: v for k, v in cons_rule.items() if k not in ['name', 'type', 'columns']},
                severity=Severity(cons_rule.get('severity', 'warning'))
            ))

        return rules

    def to_yaml(self, yaml_path: str):
        """Save rules to YAML file"""
        data = {
            'completeness': {
                'required_columns': []
            },
            'validity': [],
            'uniqueness': {
                'primary_keys': []
            },
            'consistency': []
        }

        for rule in self.rules:
            if rule.rule_type == RuleType.NOT_NULL:
                data['completeness']['required_columns'].append({
                    'column': rule.column,
                    'max_null_pct': rule.parameters.get('max_null_pct', 0),
                    'severity': rule.severity.value
                })
            elif rule.rule_type == RuleType.UNIQUE:
                data['uniqueness']['primary_keys'].append({
                    'column': rule.column,
                    'must_be_unique': True
                })
            elif rule.rule_type in [RuleType.RANGE, RuleType.REGEX, RuleType.ENUM, RuleType.LENGTH]:
                data['validity'].append({
                    'column': rule.column,
                    'type': rule.rule_type.value,
                    **rule.parameters,
                    'severity': rule.severity.value
                })
            elif rule.rule_type in [RuleType.DATE_ORDER, RuleType.SUM_EQUALS]:
                data['consistency'].append({
                    'name': rule.name,
                    'type': rule.rule_type.value,
                    'columns': rule.columns,
                    **rule.parameters
                })

        with open(yaml_path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)


class RuleBuilder:
    """Builder for creating quality rules"""

    def __init__(self):
        self._rules = QualityRules()

    def require_not_null(
        self,
        column: str,
        max_null_pct: float = 0,
        severity: str = 'warning'
    ) -> 'RuleBuilder':
        """Add not-null requirement"""
        self._rules.add_rule(QualityRule(
            name=f"not_null_{column}",
            rule_type=RuleType.NOT_NULL,
            column=column,
            parameters={'max_null_pct': max_null_pct},
            severity=Severity(severity)
        ))
        return self

    def require_unique(
        self,
        column: str,
        severity: str = 'critical'
    ) -> 'RuleBuilder':
        """Add uniqueness requirement"""
        self._rules.add_rule(QualityRule(
            name=f"unique_{column}",
            rule_type=RuleType.UNIQUE,
            column=column,
            severity=Severity(severity)
        ))
        return self

    def require_range(
        self,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        severity: str = 'warning'
    ) -> 'RuleBuilder':
        """Add range requirement"""
        params = {}
        if min_val is not None:
            params['min'] = min_val
        if max_val is not None:
            params['max'] = max_val

        self._rules.add_rule(QualityRule(
            name=f"range_{column}",
            rule_type=RuleType.RANGE,
            column=column,
            parameters=params,
            severity=Severity(severity)
        ))
        return self

    def require_pattern(
        self,
        column: str,
        pattern: str,
        severity: str = 'warning'
    ) -> 'RuleBuilder':
        """Add regex pattern requirement"""
        self._rules.add_rule(QualityRule(
            name=f"pattern_{column}",
            rule_type=RuleType.REGEX,
            column=column,
            parameters={'pattern': pattern},
            severity=Severity(severity)
        ))
        return self

    def require_enum(
        self,
        column: str,
        allowed_values: List[Any],
        severity: str = 'warning'
    ) -> 'RuleBuilder':
        """Add enum requirement"""
        self._rules.add_rule(QualityRule(
            name=f"enum_{column}",
            rule_type=RuleType.ENUM,
            column=column,
            parameters={'enum': allowed_values, 'allowed_values': allowed_values},
            severity=Severity(severity)
        ))
        return self

    def require_length(
        self,
        column: str,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        severity: str = 'warning'
    ) -> 'RuleBuilder':
        """Add length requirement"""
        params = {}
        if min_length is not None:
            params['min_length'] = min_length
        if max_length is not None:
            params['max_length'] = max_length

        self._rules.add_rule(QualityRule(
            name=f"length_{column}",
            rule_type=RuleType.LENGTH,
            column=column,
            parameters=params,
            severity=Severity(severity)
        ))
        return self

    def require_date_order(
        self,
        start_column: str,
        end_column: str,
        severity: str = 'warning'
    ) -> 'RuleBuilder':
        """Add date ordering requirement (start <= end)"""
        self._rules.add_rule(QualityRule(
            name=f"date_order_{start_column}_{end_column}",
            rule_type=RuleType.DATE_ORDER,
            columns=[start_column, end_column],
            severity=Severity(severity)
        ))
        return self

    def require_sum_equals(
        self,
        columns: List[str],
        total_column: str,
        tolerance: float = 0.01,
        severity: str = 'warning'
    ) -> 'RuleBuilder':
        """Add sum validation requirement"""
        self._rules.add_rule(QualityRule(
            name=f"sum_equals_{total_column}",
            rule_type=RuleType.SUM_EQUALS,
            columns=columns,
            parameters={'total_column': total_column, 'tolerance': tolerance},
            severity=Severity(severity)
        ))
        return self

    def build(self) -> QualityRules:
        """Build and return the rules collection"""
        return self._rules


# Common validation patterns
COMMON_PATTERNS = {
    'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    'phone_us': r'^\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}$',
    'phone_intl': r'^\+[1-9]\d{1,14}$',
    'zip_us': r'^\d{5}(-\d{4})?$',
    'ssn': r'^\d{3}-?\d{2}-?\d{4}$',
    'credit_card': r'^\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}$',
    'url': r'^https?://[^\s/$.?#].[^\s]*$',
    'ip_v4': r'^(\d{1,3}\.){3}\d{1,3}$',
    'uuid': r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    'date_iso': r'^\d{4}-\d{2}-\d{2}$',
    'currency': r'^\$?\d{1,3}(,\d{3})*(\.\d{2})?$'
}


def get_default_rules_for_domain(domain: str) -> QualityRules:
    """Get default quality rules for a business domain"""
    builder = RuleBuilder()

    if domain == 'financial':
        builder.require_not_null('account_id', severity='critical')
        builder.require_not_null('amount')
        builder.require_not_null('transaction_date')
        builder.require_unique('transaction_id', severity='critical')
        builder.require_range('amount', min_val=0)

    elif domain == 'sales':
        builder.require_not_null('order_id', severity='critical')
        builder.require_not_null('customer_id')
        builder.require_not_null('order_date')
        builder.require_unique('order_id', severity='critical')
        builder.require_range('quantity', min_val=1)
        builder.require_range('unit_price', min_val=0)

    elif domain == 'healthcare':
        builder.require_not_null('patient_id', severity='critical')
        builder.require_not_null('encounter_date')
        builder.require_unique('encounter_id', severity='critical')

    elif domain == 'hr':
        builder.require_not_null('employee_id', severity='critical')
        builder.require_unique('employee_id', severity='critical')
        builder.require_range('salary', min_val=0)

    return builder.build()
