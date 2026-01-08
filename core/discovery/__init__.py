"""Discovery module for MCP-Sample-Data Server."""

from core.discovery.schema_inference import SchemaInferrer, infer_schema
from core.discovery.domain_detector import DomainDetector, detect_domain_hints
from core.discovery.pattern_analyzer import PatternAnalyzer, detect_patterns
from core.discovery.relationship_finder import RelationshipFinder

# Alias for backward compatibility
SchemaInference = SchemaInferrer

__all__ = [
    'SchemaInferrer', 'SchemaInference', 'infer_schema',
    'DomainDetector', 'detect_domain_hints',
    'PatternAnalyzer', 'detect_patterns',
    'RelationshipFinder'
]
