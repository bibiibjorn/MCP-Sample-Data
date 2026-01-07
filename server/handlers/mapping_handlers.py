"""
Mapping Handlers
Handlers for cross-file mapping and validation tools
"""
import polars as pl
from typing import Dict, Any, List, Optional
import os

from core.mapping import MappingDiscovery, MappingManager, HierarchyAnalyzer, ContextLoader, CrossFileValidator
from server.tool_schemas import TOOL_SCHEMAS


def register_mapping_handlers(registry):
    """Register all mapping handlers"""

    mapping_discovery = MappingDiscovery()
    mapping_manager = MappingManager()
    hierarchy_analyzer = HierarchyAnalyzer()
    context_loader = ContextLoader()
    cross_file_validator = CrossFileValidator()

    # 08_discover_mappings
    def discover_mappings(
        file_paths: List[str],
        source_file: str
    ) -> Dict[str, Any]:
        """Discover mappings between files"""
        try:
            # Validate files exist
            for path in file_paths:
                if not os.path.exists(path):
                    return {'success': False, 'error': f'File not found: {path}'}

            result = mapping_discovery.discover(
                files=file_paths,
                source_file=source_file
            )

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['08_discover_mappings']
    registry.register(
        '08_discover_mappings',
        discover_mappings,
        'mapping',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 08_define_mapping
    def define_mapping(
        mapping_name: str,
        source_file: str,
        source_column: str,
        target_file: str,
        target_column: str
    ) -> Dict[str, Any]:
        """Define a mapping between files"""
        try:
            result = mapping_manager.define_mapping(
                mapping_name=mapping_name,
                source_file=source_file,
                source_column=source_column,
                target_file=target_file,
                target_column=target_column
            )

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['08_define_mapping']
    registry.register(
        '08_define_mapping',
        define_mapping,
        'mapping',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 08_validate_balance_sheet
    def validate_balance_sheet(
        source_file: str,
        mapping_file: str,
        amount_column: str,
        category_column: str
    ) -> Dict[str, Any]:
        """Validate balance sheet equation"""
        if not os.path.exists(source_file):
            return {'success': False, 'error': f'Source file not found: {source_file}'}
        if not os.path.exists(mapping_file):
            return {'success': False, 'error': f'Mapping file not found: {mapping_file}'}

        try:
            # Load files
            ext = os.path.splitext(source_file)[1].lower()
            if ext == '.csv':
                source_df = pl.read_csv(source_file)
            elif ext == '.parquet':
                source_df = pl.read_parquet(source_file)
            else:
                return {'success': False, 'error': f'Unsupported format: {ext}'}

            ext = os.path.splitext(mapping_file)[1].lower()
            if ext == '.csv':
                mapping_df = pl.read_csv(mapping_file)
            elif ext == '.parquet':
                mapping_df = pl.read_parquet(mapping_file)
            else:
                return {'success': False, 'error': f'Unsupported format: {ext}'}

            # Auto-detect mapping columns
            mapping_source_col = None
            mapping_category_col = None

            for col in mapping_df.columns:
                col_lower = col.lower()
                if 'source' in col_lower or 'from' in col_lower or 'code' in col_lower:
                    mapping_source_col = col
                if 'category' in col_lower or 'type' in col_lower or 'class' in col_lower:
                    mapping_category_col = col

            if not mapping_source_col:
                mapping_source_col = mapping_df.columns[0]
            if not mapping_category_col:
                mapping_category_col = mapping_df.columns[-1]

            result = cross_file_validator.validate_balance_sheet_equation(
                source_df=source_df,
                mapping_df=mapping_df,
                amount_column=amount_column,
                category_column=category_column,
                mapping_source_column=mapping_source_col,
                mapping_target_column=mapping_source_col,
                mapping_category_column=mapping_category_col
            )

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['08_validate_balance_sheet']
    registry.register(
        '08_validate_balance_sheet',
        validate_balance_sheet,
        'mapping',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 08_load_context
    def load_context(
        files: List[Dict[str, Any]],
        context_name: str
    ) -> Dict[str, Any]:
        """Load multiple files as a context"""
        try:
            # Validate files exist
            for file_def in files:
                if not os.path.exists(file_def['path']):
                    return {'success': False, 'error': f"File not found: {file_def['path']}"}

            result = context_loader.load_context(
                files=files,
                context_name=context_name
            )

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['08_load_context']
    registry.register(
        '08_load_context',
        load_context,
        'mapping',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 08_query_context
    def query_context(
        context_name: str,
        query: str
    ) -> Dict[str, Any]:
        """Query a loaded context"""
        try:
            result = context_loader.query_context(
                context_name=context_name,
                query=query
            )

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['08_query_context']
    registry.register(
        '08_query_context',
        query_context,
        'mapping',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 08_analyze_hierarchy
    def analyze_hierarchy(
        file_path: str,
        parent_column: Optional[str] = None,
        child_column: Optional[str] = None
    ) -> Dict[str, Any]:
        """Analyze hierarchical structure"""
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

            result = hierarchy_analyzer.analyze_hierarchy(
                df=df,
                parent_column=parent_column,
                child_column=child_column
            )

            result['file_path'] = file_path

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['08_analyze_hierarchy']
    registry.register(
        '08_analyze_hierarchy',
        analyze_hierarchy,
        'mapping',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 08_rollup_through_hierarchy
    def rollup_through_hierarchy(
        source_file: str,
        formula_file: str,
        amount_column: str,
        target_rollup: str
    ) -> Dict[str, Any]:
        """Roll up data through hierarchy"""
        if not os.path.exists(source_file):
            return {'success': False, 'error': f'Source file not found: {source_file}'}
        if not os.path.exists(formula_file):
            return {'success': False, 'error': f'Formula file not found: {formula_file}'}

        try:
            # Load files
            ext = os.path.splitext(source_file)[1].lower()
            if ext == '.csv':
                source_df = pl.read_csv(source_file)
            elif ext == '.parquet':
                source_df = pl.read_parquet(source_file)
            else:
                return {'success': False, 'error': f'Unsupported format: {ext}'}

            ext = os.path.splitext(formula_file)[1].lower()
            if ext == '.csv':
                formula_df = pl.read_csv(formula_file)
            elif ext == '.parquet':
                formula_df = pl.read_parquet(formula_file)
            else:
                return {'success': False, 'error': f'Unsupported format: {ext}'}

            # Auto-detect columns
            source_mapping_col = None
            for col in source_df.columns:
                if col != amount_column:
                    source_mapping_col = col
                    break

            element_col = None
            parent_col = None
            for col in formula_df.columns:
                col_lower = col.lower()
                if 'element' in col_lower or 'name' in col_lower or 'item' in col_lower:
                    element_col = col
                if 'parent' in col_lower:
                    parent_col = col

            if not element_col:
                element_col = formula_df.columns[0]
            if not parent_col:
                parent_col = formula_df.columns[1] if len(formula_df.columns) > 1 else formula_df.columns[0]

            result = cross_file_validator.rollup_through_hierarchy(
                source_df=source_df,
                formula_df=formula_df,
                amount_column=amount_column,
                source_mapping_column=source_mapping_col,
                formula_element_column=element_col,
                formula_parent_column=parent_col,
                target_rollup=target_rollup
            )

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['08_rollup_through_hierarchy']
    registry.register(
        '08_rollup_through_hierarchy',
        rollup_through_hierarchy,
        'mapping',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )
