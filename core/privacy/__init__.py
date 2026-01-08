"""
Privacy module for PII detection and anonymization.

Provides tools to detect, classify, and anonymize personally identifiable
information (PII) in datasets while maintaining data utility and consistency.
"""

from .pii_patterns import (
    PIIType,
    PIIPattern,
    PII_PATTERNS,
    detect_pii_in_value
)

from .pii_detector import (
    PIIDetector,
    PIIDetectionResult,
    ColumnPIIInfo
)

from .anonymization_engine import (
    AnonymizationStrategy,
    AnonymizationEngine,
    AnonymizationResult,
    ColumnAnonymizationConfig
)

from .consistency_manager import (
    ConsistencyManager
)

__all__ = [
    # PII Patterns
    'PIIType',
    'PIIPattern',
    'PII_PATTERNS',
    'detect_pii_in_value',

    # PII Detection
    'PIIDetector',
    'PIIDetectionResult',
    'ColumnPIIInfo',

    # Anonymization
    'AnonymizationStrategy',
    'AnonymizationEngine',
    'AnonymizationResult',
    'ColumnAnonymizationConfig',

    # Consistency
    'ConsistencyManager'
]
