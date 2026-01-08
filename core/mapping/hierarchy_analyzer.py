"""
Hierarchy Analyzer Module
Detects and navigates hierarchies in data
"""
import polars as pl
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)

# Token optimization constants
SAMPLE_VALUES_LIMIT = 5         # Sample values per level
ROOT_NODES_LIMIT = 10           # Max root/leaf nodes to show
DESCENDANTS_LIMIT = 50          # Max descendants to return
PATH_MAX_DEPTH = 20             # Max depth for path traversal


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
        """Analyze a level-based hierarchy (token-optimized)"""
        levels = []

        for i, col in enumerate(level_columns):
            if col not in df.columns:
                continue

            unique_values = df[col].drop_nulls().unique()

            levels.append({
                'level': i + 1,
                'column': col,
                'unique': len(unique_values),
                'samples': unique_values.head(SAMPLE_VALUES_LIMIT).to_list()
            })

        return levels

    def _analyze_parent_child_hierarchy(
        self,
        df: pl.DataFrame,
        parent_column: str,
        child_column: str
    ) -> Dict[str, Any]:
        """
        Analyze a parent-child hierarchy (token-optimized).

        Supports two patterns:
        1. Parent-contains-children: Each row defines parent -> child
           Self-referencing rows (parent == child) define leaf nodes
        2. Child-references-parent: Each row defines child -> parent reference
        """
        parents = set(df[parent_column].drop_nulls().unique().to_list())
        children = set(df[child_column].drop_nulls().unique().to_list())

        # Detect self-referencing rows (parent == child pattern)
        self_ref_rows = []
        non_self_ref_children = set()
        non_self_ref_parents = set()

        for row in df.iter_rows(named=True):
            parent_val = row.get(parent_column)
            child_val = row.get(child_column)
            if parent_val is not None and child_val is not None:
                parent_str = str(parent_val).strip()
                child_str = str(child_val).strip()
                if parent_str == child_str:
                    self_ref_rows.append(parent_str)
                else:
                    non_self_ref_parents.add(parent_str)
                    non_self_ref_children.add(child_str)

        # Determine hierarchy pattern
        has_self_refs = len(self_ref_rows) > 0
        hierarchy_pattern = 'parent_contains_children' if has_self_refs else 'child_references_parent'

        if has_self_refs:
            # Parent-contains-children pattern:
            # - Self-referencing rows are leaf definitions
            # - Root nodes are parents that are never children in non-self-ref rows
            # - Leaf nodes are defined by self-referencing rows (or have no children)
            root_nodes = non_self_ref_parents - non_self_ref_children
            leaf_nodes = set(self_ref_rows)

            # Also add nodes that appear as children but never as parents (pure leaves)
            pure_leaves = non_self_ref_children - non_self_ref_parents
            leaf_nodes.update(pure_leaves)
        else:
            # Child-references-parent pattern (original logic):
            # Root nodes have no parent (children that are not also parents)
            root_nodes = children - parents
            # Leaf nodes are not parents of anything
            leaf_nodes = parents - children

        # Calculate depth
        max_depth = self._calculate_depth(df, parent_column, child_column, has_self_refs)

        return {
            'parent_col': parent_column,
            'child_col': child_column,
            'hierarchy_pattern': hierarchy_pattern,
            'self_referencing_rows': len(self_ref_rows),
            'roots': list(root_nodes)[:ROOT_NODES_LIMIT],
            'root_count': len(root_nodes),
            'leaves': list(leaf_nodes)[:ROOT_NODES_LIMIT],
            'leaf_count': len(leaf_nodes),
            'max_depth': max_depth
        }

    def _calculate_depth(
        self,
        df: pl.DataFrame,
        parent_column: str,
        child_column: str,
        has_self_refs: bool = False,
        max_iterations: int = 20
    ) -> int:
        """
        Calculate maximum depth of parent-child hierarchy.

        For parent-contains-children pattern (has_self_refs=True):
        - Build hierarchy from parent -> children relationships
        - Skip self-referencing rows when building hierarchy

        For child-references-parent pattern:
        - Follow parent references from each child
        """
        if has_self_refs:
            # Parent-contains-children pattern: build actual hierarchy
            hierarchy = {}
            for row in df.iter_rows(named=True):
                parent_val = row.get(parent_column)
                child_val = row.get(child_column)
                if parent_val is not None and child_val is not None:
                    parent_str = str(parent_val).strip()
                    child_str = str(child_val).strip()

                    # Skip self-referencing rows
                    if parent_str == child_str:
                        continue

                    if parent_str not in hierarchy:
                        hierarchy[parent_str] = set()
                    hierarchy[parent_str].add(child_str)

            # Calculate depth by traversing from roots
            def get_depth(node: str, visited: set) -> int:
                if node in visited:
                    return 0  # Cycle detected
                if node not in hierarchy or len(hierarchy[node]) == 0:
                    return 1  # Leaf node
                visited.add(node)
                max_child_depth = 0
                for child in hierarchy[node]:
                    max_child_depth = max(max_child_depth, get_depth(child, visited.copy()))
                return 1 + max_child_depth

            # Find roots (parents that are never children)
            all_children = set()
            for children in hierarchy.values():
                all_children.update(children)
            roots = set(hierarchy.keys()) - all_children

            max_depth = 0
            for root in roots:
                depth = get_depth(root, set())
                max_depth = max(max_depth, depth)

            return max_depth
        else:
            # Child-references-parent pattern (original logic)
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
        parent_patterns = ['parent', 'parent_id', 'parent_code', 'header']
        child_patterns = ['child', 'child_id', 'element', 'id', 'code', 'key']

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

        # Special handling for "Formula Header" / "Formula Element" pattern
        if not parent_col or not child_col:
            for col in df.columns:
                col_lower = col.lower()
                if 'formula' in col_lower and 'header' in col_lower:
                    parent_col = col
                elif 'formula' in col_lower and 'element' in col_lower:
                    child_col = col

        # Fallback: first two columns as parent/child
        if not parent_col and not child_col and len(df.columns) >= 2:
            # Check if first two columns have overlapping values (suggesting hierarchy)
            col1_vals = set(str(v) for v in df[df.columns[0]].drop_nulls().unique().to_list())
            col2_vals = set(str(v) for v in df[df.columns[1]].drop_nulls().unique().to_list())
            if col1_vals & col2_vals:  # Some overlap
                parent_col = df.columns[0]
                child_col = df.columns[1]

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
            result['detected_parent_col'] = parent_col
            result['detected_child_col'] = child_col
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
        """Get all descendants of a node (token-limited)"""
        descendants = []
        to_process = [node]
        depth = 0

        while to_process and depth < max_depth and len(descendants) < DESCENDANTS_LIMIT:
            current_level = []
            for n in to_process:
                if len(descendants) >= DESCENDANTS_LIMIT:
                    break
                children = self.get_children(df, n, parent_column, child_column)
                current_level.extend(children)
                descendants.extend(children)

            to_process = current_level
            depth += 1

        return descendants[:DESCENDANTS_LIMIT]
