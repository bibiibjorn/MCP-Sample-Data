"""
Query Builder Module
Builds SQL queries for common operations
"""
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class QueryBuilder:
    """Builds SQL queries"""

    def __init__(self):
        pass

    def select(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        group_by: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        limit: Optional[int] = None
    ) -> str:
        """Build a SELECT query"""
        # Columns
        col_str = ", ".join(columns) if columns else "*"
        query = f"SELECT {col_str} FROM {table}"

        # WHERE
        if where:
            query += f" WHERE {where}"

        # GROUP BY
        if group_by:
            query += f" GROUP BY {', '.join(group_by)}"

        # ORDER BY
        if order_by:
            query += f" ORDER BY {', '.join(order_by)}"

        # LIMIT
        if limit:
            query += f" LIMIT {limit}"

        return query

    def insert(
        self,
        table: str,
        columns: List[str],
        values: List[Any]
    ) -> str:
        """Build an INSERT query"""
        col_str = ", ".join(columns)
        val_str = ", ".join([self._format_value(v) for v in values])
        return f"INSERT INTO {table} ({col_str}) VALUES ({val_str})"

    def update(
        self,
        table: str,
        updates: Dict[str, Any],
        where: str
    ) -> str:
        """Build an UPDATE query"""
        set_clauses = [f"{col} = {self._format_value(val)}" for col, val in updates.items()]
        set_str = ", ".join(set_clauses)
        return f"UPDATE {table} SET {set_str} WHERE {where}"

    def delete(
        self,
        table: str,
        where: str
    ) -> str:
        """Build a DELETE query"""
        return f"DELETE FROM {table} WHERE {where}"

    def join(
        self,
        left_table: str,
        right_table: str,
        on: str,
        join_type: str = 'INNER'
    ) -> str:
        """Build a JOIN query"""
        return f"SELECT * FROM {left_table} {join_type} JOIN {right_table} ON {on}"

    def aggregate(
        self,
        table: str,
        group_by: List[str],
        aggregations: List[Dict[str, str]],
        where: Optional[str] = None
    ) -> str:
        """Build an aggregation query"""
        # Build SELECT clause
        select_parts = group_by.copy()

        for agg in aggregations:
            func = agg.get('function', 'SUM')
            column = agg['column']
            alias = agg.get('alias', f"{column}_{func.lower()}")
            select_parts.append(f"{func}({column}) AS {alias}")

        query = f"SELECT {', '.join(select_parts)} FROM {table}"

        if where:
            query += f" WHERE {where}"

        query += f" GROUP BY {', '.join(group_by)}"

        return query

    def union(
        self,
        queries: List[str],
        all: bool = False
    ) -> str:
        """Build a UNION query"""
        union_type = "UNION ALL" if all else "UNION"
        return f" {union_type} ".join(queries)

    def case_when(
        self,
        conditions: List[Dict[str, Any]],
        else_value: Any = None,
        alias: Optional[str] = None
    ) -> str:
        """Build a CASE WHEN expression"""
        cases = []
        for cond in conditions:
            when = cond['when']
            then = self._format_value(cond['then'])
            cases.append(f"WHEN {when} THEN {then}")

        case_str = "CASE " + " ".join(cases)

        if else_value is not None:
            case_str += f" ELSE {self._format_value(else_value)}"

        case_str += " END"

        if alias:
            case_str += f" AS {alias}"

        return case_str

    def _format_value(self, value: Any) -> str:
        """Format a value for SQL"""
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            # Escape single quotes
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        else:
            return str(value)

    def build_from_spec(self, spec: Dict[str, Any]) -> str:
        """Build a query from a specification dictionary"""
        query_type = spec.get('type', 'select').lower()

        if query_type == 'select':
            return self.select(
                table=spec['table'],
                columns=spec.get('columns'),
                where=spec.get('where'),
                group_by=spec.get('group_by'),
                order_by=spec.get('order_by'),
                limit=spec.get('limit')
            )
        elif query_type == 'aggregate':
            return self.aggregate(
                table=spec['table'],
                group_by=spec['group_by'],
                aggregations=spec['aggregations'],
                where=spec.get('where')
            )
        elif query_type == 'join':
            return self.join(
                left_table=spec['left_table'],
                right_table=spec['right_table'],
                on=spec['on'],
                join_type=spec.get('join_type', 'INNER')
            )
        else:
            raise ValueError(f"Unknown query type: {query_type}")
