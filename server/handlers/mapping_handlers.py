"""
Mapping Handlers
Handlers for cross-file mapping and validation tools
"""
import polars as pl
from typing import Dict, Any, List, Optional
import os

from core.mapping import MappingDiscovery, MappingManager, HierarchyAnalyzer, ContextLoader, CrossFileValidator
from server.tool_schemas import TOOL_SCHEMAS
from server.handlers.file_utils import read_data_file


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

    # 08_validate_amounts
    def validate_amounts(
        source_file: str,
        amount_column: str,
        group_column: str,
        validation_rule: Dict[str, Any],
        mapping_file: Optional[str] = None,
        mapping_source_column: Optional[str] = None,
        mapping_target_column: Optional[str] = None,
        tolerance: float = 0.01
    ) -> Dict[str, Any]:
        """Validate amounts using user-defined rules (supports CSV, Excel, and Parquet)"""
        if not os.path.exists(source_file):
            return {'success': False, 'error': f'Source file not found: {source_file}'}

        try:
            # Load source file
            source_df = read_data_file(source_file)

            # Load mapping file if provided
            mapping_df = None
            if mapping_file:
                if not os.path.exists(mapping_file):
                    return {'success': False, 'error': f'Mapping file not found: {mapping_file}'}
                mapping_df = read_data_file(mapping_file)

                # Auto-detect mapping columns if not specified
                if not mapping_source_column:
                    for col in mapping_df.columns:
                        col_lower = col.lower()
                        if 'source' in col_lower or 'from' in col_lower or 'code' in col_lower:
                            mapping_source_column = col
                            break
                    if not mapping_source_column:
                        mapping_source_column = mapping_df.columns[0]

                if not mapping_target_column:
                    for col in mapping_df.columns:
                        col_lower = col.lower()
                        if 'target' in col_lower or 'to' in col_lower or 'category' in col_lower or 'group' in col_lower:
                            mapping_target_column = col
                            break
                    if not mapping_target_column:
                        mapping_target_column = mapping_df.columns[-1] if len(mapping_df.columns) > 1 else mapping_df.columns[0]

            result = cross_file_validator.validate_amounts(
                source_df=source_df,
                amount_column=amount_column,
                group_column=group_column,
                validation_rule=validation_rule,
                mapping_df=mapping_df,
                mapping_source_column=mapping_source_column,
                mapping_target_column=mapping_target_column,
                tolerance=tolerance
            )

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['08_validate_amounts']
    registry.register(
        '08_validate_amounts',
        validate_amounts,
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
        query: str,
        limit: Optional[int] = None,
        include_data: bool = True
    ) -> Dict[str, Any]:
        """Query a loaded context with automatic row limiting for token efficiency"""
        try:
            result = context_loader.query_context(
                context_name=context_name,
                query=query,
                limit=limit,
                include_data=include_data
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
        """Analyze hierarchical structure (supports CSV, Excel, and Parquet)"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            df = read_data_file(file_path)

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
        """Roll up data through hierarchy (supports CSV, Excel, and Parquet)"""
        if not os.path.exists(source_file):
            return {'success': False, 'error': f'Source file not found: {source_file}'}
        if not os.path.exists(formula_file):
            return {'success': False, 'error': f'Formula file not found: {formula_file}'}

        try:
            # Load files
            source_df = read_data_file(source_file)
            formula_df = read_data_file(formula_file)

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
                # Detect element/child column
                if 'element' in col_lower or 'child' in col_lower:
                    element_col = col
                # Detect parent/header column (common patterns: "Formula Header", "Parent", "Header")
                if 'parent' in col_lower or 'header' in col_lower:
                    parent_col = col

            # Fallback: if we have columns like "Formula Header" and "Formula Element"
            # the first is typically parent, second is element/child
            if not element_col and not parent_col:
                # Look for column pairs that suggest parent-child relationship
                for i, col in enumerate(formula_df.columns):
                    col_lower = col.lower()
                    if 'formula' in col_lower and 'header' in col_lower:
                        parent_col = col
                    elif 'formula' in col_lower and 'element' in col_lower:
                        element_col = col

            if not element_col:
                # Default: second column (index 1) is typically the element/child
                element_col = formula_df.columns[1] if len(formula_df.columns) > 1 else formula_df.columns[0]
            if not parent_col:
                # Default: first column (index 0) is typically the parent/header
                parent_col = formula_df.columns[0]

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
