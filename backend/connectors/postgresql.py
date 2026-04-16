"""
PostgreSQL connector — uses psycopg2 directly for profiling queries
and SQLAlchemy Inspector for schema discovery.
"""

import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text, inspect as sa_inspect
from sqlalchemy.pool import NullPool

from .base import (
    BaseConnector, ConnectorType,
    ColumnInfo, TableInfo, SchemaInfo,
    ConnectionTestResult, ProfileResult,
)

logger = logging.getLogger(__name__)

EXCLUDED_SCHEMAS = {"pg_catalog", "information_schema", "pg_toast", "pg_temp_1", "pg_toast_temp_1"}


class PostgreSQLConnector(BaseConnector):
    connector_type = ConnectorType.POSTGRESQL
    display_name   = "PostgreSQL"
    icon           = "🐘"

    # ── Connection string builder ────────────────────────────────────────────

    def _connection_string(self) -> str:
        c = self.config
        ssl = c.get("sslmode", "prefer")
        return (
            f"postgresql+psycopg2://{c['username']}:{c['password']}"
            f"@{c['host']}:{c.get('port', 5432)}/{c['database']}"
            f"?sslmode={ssl}"
        )

    def _engine(self):
        return create_engine(
            self._connection_string(),
            poolclass=NullPool,   # no persistent pool — short-lived connections
            connect_args={"connect_timeout": 10},
        )

    # ── Core interface ───────────────────────────────────────────────────────

    def test_connection(self) -> ConnectionTestResult:
        start = time.perf_counter()
        try:
            engine = self._engine()
            with engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.scalar()
            latency = (time.perf_counter() - start) * 1000
            return ConnectionTestResult(
                success=True,
                message="Connection successful",
                latency_ms=round(latency, 1),
                server_version=version,
            )
        except Exception as exc:
            return ConnectionTestResult(
                success=False,
                message="Connection failed",
                error=str(exc),
            )

    def get_schemas(self) -> List[str]:
        engine = self._engine()
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
            ))
            return [
                r[0] for r in rows
                if r[0] not in EXCLUDED_SCHEMAS
            ]

    def get_tables(self, schema: str = "public") -> List[TableInfo]:
        engine = self._engine()
        inspector = sa_inspect(engine)
        table_names = inspector.get_table_names(schema=schema)

        tables = []
        with engine.connect() as conn:
            for tname in table_names:
                # Quick row count via pg statistics (fast)
                try:
                    row = conn.execute(text(
                        "SELECT reltuples::bigint FROM pg_class c "
                        "JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "WHERE n.nspname = :schema AND c.relname = :table"
                    ), {"schema": schema, "table": tname}).fetchone()
                    row_count = int(row[0]) if row and row[0] >= 0 else 0
                except Exception:
                    row_count = 0

                cols = inspector.get_columns(tname, schema=schema)
                tables.append(TableInfo(
                    schema_name=schema,
                    table_name=tname,
                    full_name=f"{schema}.{tname}",
                    row_count=row_count,
                    column_count=len(cols),
                ))
        return tables

    def get_columns(self, schema: str, table: str) -> List[ColumnInfo]:
        engine = self._engine()
        inspector = sa_inspect(engine)
        pk_cols = {c for c in inspector.get_pk_constraint(table, schema=schema).get("constrained_columns", [])}
        raw_cols = inspector.get_columns(table, schema=schema)
        return [
            ColumnInfo(
                name=col["name"],
                data_type=str(col["type"]),
                nullable=col.get("nullable", True),
                primary_key=col["name"] in pk_cols,
                default=str(col["default"]) if col.get("default") is not None else None,
            )
            for col in raw_cols
        ]

    def profile_table(self, schema: str, table: str, sample_rows: int = 10) -> ProfileResult:
        """
        Full table profiling:
        - Exact row count
        - Per column: null count, distinct count, min/max/avg (for numerics/dates), sample values
        - Quality score & issue detection
        """
        engine = self._engine()
        full = f"{schema}.{table}"

        with engine.connect() as conn:
            # Row count (exact)
            row_count = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')).scalar() or 0

            # Column definitions
            inspector = sa_inspect(engine)
            pk_cols = set(inspector.get_pk_constraint(table, schema=schema).get("constrained_columns", []))
            raw_cols = inspector.get_columns(table, schema=schema)

            profiled_columns: List[ColumnInfo] = []
            for col in raw_cols:
                cname  = col["name"]
                ctype  = str(col["type"])
                cinfo  = ColumnInfo(
                    name=cname,
                    data_type=ctype,
                    nullable=col.get("nullable", True),
                    primary_key=cname in pk_cols,
                )

                if row_count == 0:
                    profiled_columns.append(cinfo)
                    continue

                # Null stats
                try:
                    null_count = conn.execute(
                        text(f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE "{cname}" IS NULL')
                    ).scalar() or 0
                    cinfo.null_count = null_count
                    cinfo.null_pct   = round((null_count / row_count) * 100, 2)
                except Exception:
                    pass

                # Distinct count
                try:
                    cinfo.distinct_count = conn.execute(
                        text(f'SELECT COUNT(DISTINCT "{cname}") FROM "{schema}"."{table}"')
                    ).scalar() or 0
                except Exception:
                    pass

                # Min / max / avg (numeric and date types)
                numeric_types = ("int", "float", "numeric", "decimal", "real", "double", "serial", "bigint", "smallint")
                date_types    = ("date", "time", "timestamp")
                lower_type    = ctype.lower()

                if any(t in lower_type for t in numeric_types + date_types):
                    try:
                        row = conn.execute(
                            text(f'SELECT MIN("{cname}"), MAX("{cname}") FROM "{schema}"."{table}"')
                        ).fetchone()
                        if row:
                            cinfo.min_value = str(row[0]) if row[0] is not None else None
                            cinfo.max_value = str(row[1]) if row[1] is not None else None
                    except Exception:
                        pass

                    if any(t in lower_type for t in numeric_types):
                        try:
                            avg = conn.execute(
                                text(f'SELECT AVG("{cname}"::numeric) FROM "{schema}"."{table}"')
                            ).scalar()
                            cinfo.avg_value = round(float(avg), 4) if avg is not None else None
                        except Exception:
                            pass

                # Sample values (non-null)
                try:
                    sample = conn.execute(
                        text(
                            f'SELECT DISTINCT "{cname}" FROM "{schema}"."{table}" '
                            f'WHERE "{cname}" IS NOT NULL LIMIT {sample_rows}'
                        )
                    ).fetchall()
                    cinfo.sample_values = [str(r[0]) for r in sample]
                except Exception:
                    pass

                # Per-column quality score
                cinfo.quality_score = round(max(0.0, 100.0 - cinfo.null_pct * 0.4), 1)
                profiled_columns.append(cinfo)

        quality_score = self._compute_quality_score(profiled_columns, row_count)
        issues        = self._classify_issues(profiled_columns, row_count)

        return ProfileResult(
            table_full_name=full,
            row_count=row_count,
            column_count=len(profiled_columns),
            columns=profiled_columns,
            quality_score=quality_score,
            issues=issues,
            profiled_at=datetime.utcnow().isoformat() + "Z",
        )

    @classmethod
    def required_fields(cls) -> List[Dict[str, Any]]:
        return [
            {"name": "host",     "label": "Host",     "type": "text",     "placeholder": "localhost",   "required": True},
            {"name": "port",     "label": "Port",     "type": "number",   "placeholder": "5432",        "required": True,  "default": 5432},
            {"name": "database", "label": "Database", "type": "text",     "placeholder": "mydb",        "required": True},
            {"name": "username", "label": "Username", "type": "text",     "placeholder": "postgres",    "required": True},
            {"name": "password", "label": "Password", "type": "password", "placeholder": "••••••••",   "required": True},
            {"name": "sslmode",  "label": "SSL Mode", "type": "select",   "options": ["disable","allow","prefer","require","verify-ca","verify-full"], "default": "prefer", "required": False},
        ]
