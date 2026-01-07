"""
Discovery Handlers
Handlers for data discovery and analysis tools
"""
import polars as pl
from typing import Dict, Any, List, Optional
import os

from core.discovery import SchemaInference, DomainDetector, PatternAnalyzer, RelationshipFinder
from server.tool_schemas import TOOL_SCHEMAS


def register_discovery_handlers(registry):
    """Register all discovery handlers"""

    schema_inference = SchemaInference()
    domain_detector = DomainDetector()
    pattern_analyzer = PatternAnalyzer()
    relationship_finder = RelationshipFinder()

    # 01_analyze_file
    def analyze_file(file_path: str, include_statistics: bool = True, include_patterns: bool = True) -> Dict[str, Any]:
        """Analyze a data file"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            # Load file
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.csv':
                df = pl.read_csv(file_path)
            elif ext == '.parquet':
                df = pl.read_parquet(file_path)
            elif ext in ['.xlsx', '.xls']:
                df = pl.read_excel(file_path)
            else:
                return {'success': False, 'error': f'Unsupported format: {ext}'}

            result = {
                'success': True,
                'file_path': file_path,
                'row_count': len(df),
                'column_count': len(df.columns),
                'columns': []
            }

            for col in df.columns:
                col_info = {
                    'name': col,
                    'dtype': str(df[col].dtype),
                    'null_count': df[col].null_count(),
                    'unique_count': df[col].n_unique()
                }

                if include_statistics and df[col].dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]:
                    col_info['statistics'] = {
                        'min': df[col].min(),
                        'max': df[col].max(),
                        'mean': df[col].mean(),
                        'std': df[col].std()
                    }

                if include_patterns:
                    patterns = pattern_analyzer.analyze_column(df, col)
                    if patterns.get('patterns'):
                        col_info['detected_patterns'] = patterns['patterns']

                result['columns'].append(col_info)

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['01_analyze_file']
    registry.register(
        '01_analyze_file',
        analyze_file,
        'discovery',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 01_detect_domain
    def detect_domain(file_path: str, confidence_threshold: float = 0.7) -> Dict[str, Any]:
        """Detect business domain"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.csv':
                df = pl.read_csv(file_path)
            elif ext == '.parquet':
                df = pl.read_parquet(file_path)
            else:
                return {'success': False, 'error': f'Unsupported format: {ext}'}

            result = domain_detector.detect(df)
            result['file_path'] = file_path

            # Filter by confidence
            if result.get('confidence', 0) < confidence_threshold:
                result['warning'] = f'Confidence below threshold ({confidence_threshold})'

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['01_detect_domain']
    registry.register(
        '01_detect_domain',
        detect_domain,
        'discovery',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 01_find_relationships
    def find_relationships(file_paths: List[str], primary_file: Optional[str] = None) -> Dict[str, Any]:
        """Find relationships between files"""
        try:
            dataframes = {}
            for path in file_paths:
                if not os.path.exists(path):
                    return {'success': False, 'error': f'File not found: {path}'}

                ext = os.path.splitext(path)[1].lower()
                if ext == '.csv':
                    dataframes[path] = pl.read_csv(path)
                elif ext == '.parquet':
                    dataframes[path] = pl.read_parquet(path)

            result = relationship_finder.find_relationships(dataframes, primary_file)
            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['01_find_relationships']
    registry.register(
        '01_find_relationships',
        find_relationships,
        'discovery',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 01_profile_column
    def profile_column(file_path: str, column_name: str) -> Dict[str, Any]:
        """Profile a specific column"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.csv':
                df = pl.read_csv(file_path)
            elif ext == '.parquet':
                df = pl.read_parquet(file_path)
            else:
                return {'success': False, 'error': f'Unsupported format: {ext}'}

            if column_name not in df.columns:
                return {'success': False, 'error': f'Column not found: {column_name}'}

            col = df[column_name]

            profile = {
                'success': True,
                'column_name': column_name,
                'dtype': str(col.dtype),
                'row_count': len(col),
                'null_count': col.null_count(),
                'null_percentage': round(col.null_count() / len(col) * 100, 2),
                'unique_count': col.n_unique(),
                'unique_percentage': round(col.n_unique() / len(col) * 100, 2)
            }

            # Statistics for numeric columns
            if col.dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8]:
                profile['statistics'] = {
                    'min': col.min(),
                    'max': col.max(),
                    'mean': col.mean(),
                    'median': col.median(),
                    'std': col.std(),
                    'sum': col.sum()
                }

            # Value distribution
            value_counts = col.value_counts().sort('count', descending=True)
            profile['top_values'] = value_counts.head(10).to_dicts()

            # Pattern analysis
            patterns = pattern_analyzer.analyze_column(df, column_name)
            profile['patterns'] = patterns

            return profile

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['01_profile_column']
    registry.register(
        '01_profile_column',
        profile_column,
        'discovery',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 01_suggest_schema
    def suggest_schema(file_path: str, optimize_for: str = 'balanced') -> Dict[str, Any]:
        """Suggest optimal schema"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.csv':
                df = pl.read_csv(file_path)
            elif ext == '.parquet':
                df = pl.read_parquet(file_path)
            else:
                return {'success': False, 'error': f'Unsupported format: {ext}'}

            result = schema_inference.infer_schema(df)
            result['file_path'] = file_path
            result['optimization_target'] = optimize_for

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['01_suggest_schema']
    registry.register(
        '01_suggest_schema',
        suggest_schema,
        'discovery',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )
