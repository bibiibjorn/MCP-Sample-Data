"""
Hierarchy Analyzer Module
Detects and navigates hierarchies in data
"""
import polars as pl
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class HierarchyAnalyzer:
    """Analyzes hierarchical structures in data"""

    def __init__(self):
        pass

    def analyze_hierarchy(
        self,
        df: pl.DataFrame,
        level_columns: Optional[List[str]] = None,
        parent_column: Optional[str] = None,
        child_column: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Analyze hierarchical structure in a DataFrame.

        Args:
            df: DataFrame to analyze
            level_columns: Columns representing hierarchy levels
            parent_column: Column containing parent references
            child_column: Column containing child references

        Returns:
            Hierarchy analysis results
        """
        try:
            result = {
                'success': True,
                'hierarchy_type': None,
                'levels': [],
                'root_nodes': [],
                'leaf_nodes': [],
                'max_depth': 0
            }

            # If level columns provided, analyze as level-based hierarchy
            if level_columns:
                result['hierarchy_type'] = 'level_based'
                result['levels'] = self._analyze_level_hierarchy(df, level_columns)
                result['max_depth'] = len(level_columns)

            # If parent/child columns provided, analyze as parent-child hierarchy
            elif parent_column and child_column:
                result['hierarchy_type'] = 'parent_child'
                pc_analysis = self._analyze_parent_child_hierarchy(
                    df, parent_column, child_column
                )
                result.update(pc_analysis)

            # Auto-detect hierarchy
            else:
                detected = self._auto_detect_hierarchy(df)
                result.update(detected)

            return result

        except Exception as e:
            logger.error(f"Error analyzing hierarchy: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _analyze_level_hierarchy(
        self,
        df: pl.DataFrame,
        level_columns: List[str]
    ) -> List[Dict[str, Any]]:
        """Analyze a level-based hierarchy"""
        levels = []

        for i, col in enumerate(level_columns):
            if col not in df.columns:
                continue

            unique_values = df[col].drop_nulls().unique()

            levels.append({
                'level': i + 1,
                'column': col,
                'unique_values': len(unique_values),
                'sample_values': unique_values.head(5).to_list()
            })

        return levels

    def _analyze_parent_child_hierarchy(
        self,
        df: pl.DataFrame,
        parent_column: str,
        child_column: str
    ) -> Dict[str, Any]:
        """Analyze a parent-child hierarchy"""
        parents = set(df[parent_column].drop_nulls().unique().to_list())
        children = set(df[child_column].drop_nulls().unique().to_list())

        # Root nodes have no parent (or parent is null/self)
        root_nodes = children - parents

        # Leaf nodes are not parents of anything
        leaf_nodes = parents - children

        # Calculate depth
        max_depth = self._calculate_depth(df, parent_column, child_column)

        return {
            'parent_column': parent_column,
            'child_column': child_column,
            'root_nodes': list(root_nodes)[:10],
            'root_count': len(root_nodes),
            'leaf_nodes': list(leaf_nodes)[:10],
            'leaf_count': len(leaf_nodes),
            'max_depth': max_depth
        }

    def _calculate_depth(
        self,
        df: pl.DataFrame,
        parent_column: str,
        child_column: str,
        max_iterations: int = 20
    ) -> int:
        """Calculate maximum depth of parent-child hierarchy"""
        # Build parent lookup
        parent_lookup = dict(zip(
            df[child_column].to_list(),
            df[parent_column].to_list()
        ))

        max_depth = 0

        for child in df[child_column].unique().to_list():
            depth = 0
            current = child
            visited = set()

            while current in parent_lookup and current not in visited and depth < max_iterations:
                visited.add(current)
                current = parent_lookup[current]
                depth += 1

            max_depth = max(max_depth, depth)

        return max_depth

    def _auto_detect_hierarchy(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Auto-detect hierarchy structure"""
        # Look for common hierarchy column patterns
        level_patterns = ['level', 'lvl', 'tier', 'depth']
        parent_patterns = ['parent', 'parent_id', 'parent_code']
        child_patterns = ['child', 'child_id', 'id', 'code', 'key']

        level_cols = []
        parent_col = None
        child_col = None

        for col in df.columns:
            col_lower = col.lower()

            if any(p in col_lower for p in level_patterns):
                level_cols.append(col)
            elif any(p in col_lower for p in parent_patterns) and not parent_col:
                parent_col = col
            elif any(p in col_lower for p in child_patterns) and not child_col:
                child_col = col

        if level_cols:
            return {
                'hierarchy_type': 'level_based',
                'detected_level_columns': level_cols,
                'levels': self._analyze_level_hierarchy(df, level_cols),
                'max_depth': len(level_cols)
            }
        elif parent_col and child_col:
            result = self._analyze_parent_child_hierarchy(df, parent_col, child_col)
            result['hierarchy_type'] = 'parent_child'
            return result
        else:
            return {
                'hierarchy_type': 'none_detected',
                'message': 'No clear hierarchy structure detected'
            }

    def get_path_to_root(
        self,
        df: pl.DataFrame,
        node: Any,
        parent_column: str,
        child_column: str,
        name_column: Optional[str] = None
    ) -> List[Any]:
        """Get the path from a node to the root"""
        # Build parent lookup
        parent_lookup = dict(zip(
            df[child_column].to_list(),
            df[parent_column].to_list()
        ))

        name_lookup = {}
        if name_column and name_column in df.columns:
            name_lookup = dict(zip(
                df[child_column].to_list(),
                df[name_column].to_list()
            ))

        path = []
        current = node
        visited = set()

        while current is not None and current not in visited:
            visited.add(current)
            if name_lookup:
                path.append({'id': current, 'name': name_lookup.get(current)})
            else:
                path.append(current)

            current = parent_lookup.get(current)

        return path

    def get_children(
        self,
        df: pl.DataFrame,
        node: Any,
        parent_column: str,
        child_column: str
    ) -> List[Any]:
        """Get immediate children of a node"""
        children = df.filter(pl.col(parent_column) == node)[child_column].to_list()
        return children

    def get_descendants(
        self,
        df: pl.DataFrame,
        node: Any,
        parent_column: str,
        child_column: str,
        max_depth: int = 10
    ) -> List[Any]:
        """Get all descendants of a node"""
        descendants = []
        to_process = [node]
        depth = 0

        while to_process and depth < max_depth:
            current_level = []
            for n in to_process:
                children = self.get_children(df, n, parent_column, child_column)
                current_level.extend(children)
                descendants.extend(children)

            to_process = current_level
            depth += 1

        return descendants
