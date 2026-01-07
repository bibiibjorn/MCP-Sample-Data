"""Core modules for MCP-Sample-Data"""
from .config import ConfigManager
from .discovery import SchemaInference, DomainDetector, PatternAnalyzer, RelationshipFinder
from .generation import DimensionGenerator, FactGenerator, DateDimensionGenerator, TemplateEngine
from .editing import DuckDBEngine, PolarsEngine, QueryBuilder
from .validation import RuleEngine, BalanceChecker, ReferentialChecker, StatisticalValidator
from .mapping import MappingDiscovery, MappingManager, HierarchyAnalyzer, ContextLoader, CrossFileValidator
from .export import CSVExporter, ParquetExporter, PowerBIOptimizer
from .storage import ProjectManager, FileManager, CacheManager

__all__ = [
    # Config
    'ConfigManager',
    # Discovery
    'SchemaInference', 'DomainDetector', 'PatternAnalyzer', 'RelationshipFinder',
    # Generation
    'DimensionGenerator', 'FactGenerator', 'DateDimensionGenerator', 'TemplateEngine',
    # Editing
    'DuckDBEngine', 'PolarsEngine', 'QueryBuilder',
    # Validation
    'RuleEngine', 'BalanceChecker', 'ReferentialChecker', 'StatisticalValidator',
    # Mapping
    'MappingDiscovery', 'MappingManager', 'HierarchyAnalyzer', 'ContextLoader', 'CrossFileValidator',
    # Export
    'CSVExporter', 'ParquetExporter', 'PowerBIOptimizer',
    # Storage
    'ProjectManager', 'FileManager', 'CacheManager'
]
