"""
Microsoft Fabric SQL Analytics Endpoint connector.

Microsoft Fabric exposes a T-SQL compatible SQL Analytics Endpoint
(previously called "SQL Endpoint" in Synapse Analytics).

Connection options supported:
  1. Service Principal  — client_id + client_secret + tenant_id  (recommended for SaaS)
  2. SQL Authentication — username + password
  3. Access Token       — bearer token (useful for short-lived dev access)

The endpoint URL looks like:
  <workspace-id>.datawarehouse.fabric.microsoft.com

Driver used: ODBC Driver 18 for SQL Server (must be installed on the host).
Fallback:    pymssql (pure-Python, works without ODBC).
"""

import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text, inspect as sa_inspect
from sqlalchemy.pool import NullPool

from .base import (
    BaseConnector, ConnectorType,
    ColumnInfo, TableInfo,
    ConnectionTestResult, ProfileResult,
)

logger = logging.getLogger(__name__)

# Schemas that are internal to Fabric / SQL Server — skip them
EXCLUDED_SCHEMAS = {
    "sys", "INFORMATION_SCHEMA", "guest", "db_owner", "db_accessadmin",
    "db_securityadmin", "db_ddladmin", "db_backupoperator", "db_datareader",
    "db_datawriter", "db_denydatareader", "db_denydatawriter",
}


def _get_odbc_driver() -> Optional[str]:
    """Return the first available ODBC driver for SQL Server, or None."""
    try:
        import pyodbc
        drivers = pyodbc.drivers()
        for candidate in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "FreeTDS"]:
            if candidate in drivers:
                return candidate
    except ImportError:
        pass
    return None


class FabricConnector(BaseConnector):
    connector_type = ConnectorType.FABRIC
    display_name   = "Microsoft Fabric"
    icon           = "🪟"

    # ── Auth modes ───────────────────────────────────────────────────────────

    def _auth_mode(self) -> str:
        c = self.config
        if c.get("client_id") and c.get("client_secret") and c.get("tenant_id"):
            return "service_principal"
        if c.get("access_token"):
            return "access_token"
        return "sql_auth"

    # ── Connection string / engine ───────────────────────────────────────────

    def _connection_string(self) -> str:
        c     = self.config
        host  = c["endpoint"]           # e.g. abc123.datawarehouse.fabric.microsoft.com
        db    = c.get("database", "")
        driver = _get_odbc_driver()

        auth = self._auth_mode()

        if driver:
            # ── ODBC path ────────────────────────────────────────────────────
            base = (
                f"mssql+pyodbc://@{host}/{db}"
                f"?driver={driver.replace(' ', '+')}"
                "&Encrypt=yes&TrustServerCertificate=no&Connection+Timeout=30"
            )
            if auth == "sql_auth":
                uid = c["username"]
                pwd = c["password"]
                return (
                    f"mssql+pyodbc://{uid}:{pwd}@{host}/{db}"
                    f"?driver={driver.replace(' ', '+')}"
                    "&Encrypt=yes&TrustServerCertificate=no&Connection+Timeout=30"
                )
            # Service principal & access_token handled via connect_args
            return base

        else:
            # ── pymssql fallback ─────────────────────────────────────────────
            if auth == "sql_auth":
                uid = c["username"]
                pwd = c["password"]
                return f"mssql+pymssql://{uid}:{pwd}@{host}/{db}"
            # SP / token fallback not well-supported without ODBC
            raise RuntimeError(
                "ODBC Driver 17/18 for SQL Server required for Service Principal / Access Token auth. "
                "Install it from https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server"
            )

    def _connect_args(self) -> Dict[str, Any]:
        """Extra connect_args for Service Principal and token-based auth."""
        auth = self._auth_mode()
        if auth == "service_principal":
            # Use Azure Identity to get a token
            try:
                from azure.identity import ClientSecretCredential
                c = self.config
                cred = ClientSecretCredential(
                    tenant_id=c["tenant_id"],
                    client_id=c["client_id"],
                    client_secret=c["client_secret"],
                )
                token = cred.get_token("https://database.windows.net/.default").token
                # SQL Server ODBC accepts the token via attrs_before
                import struct
                token_bytes = token.encode("utf-16-le")
                token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
                SQL_COPT_SS_ACCESS_TOKEN = 1256
                return {"attrs_before": {SQL_COPT_SS_ACCESS_TOKEN: token_struct}}
            except ImportError:
                raise RuntimeError(
                    "pip install azure-identity is required for Service Principal auth"
                )
        if auth == "access_token":
            import struct
            token = self.config["access_token"]
            token_bytes = token.encode("utf-16-le")
            token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
            SQL_COPT_SS_ACCESS_TOKEN = 1256
            return {"attrs_before": {SQL_COPT_SS_ACCESS_TOKEN: token_struct}}
        return {}

    def _engine(self):
        return create_engine(
            self._connection_string(),
            poolclass=NullPool,
            connect_args=self._connect_args(),
        )

    # ── Core interface ───────────────────────────────────────────────────────

    def test_connection(self) -> ConnectionTestResult:
        start = time.perf_counter()
        try:
            engine = self._engine()
            with engine.connect() as conn:
                version = conn.execute(text("SELECT @@VERSION")).scalar()
            latency = (time.perf_counter() - start) * 1000
            return ConnectionTestResult(
                success=True,
                message="Connection to Microsoft Fabric successful",
                latency_ms=round(latency, 1),
                server_version=version[:120] if version else None,
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
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME"
            ))
            return [r[0] for r in rows if r[0] not in EXCLUDED_SCHEMAS]

    def get_tables(self, schema: str = "dbo") -> List[TableInfo]:
        engine = self._engine()
        inspector = sa_inspect(engine)
        try:
            table_names = inspector.get_table_names(schema=schema)
        except Exception:
            table_names = []

        tables = []
        with engine.connect() as conn:
            for tname in table_names:
                try:
                    row_count = conn.execute(
                        text(f"SELECT COUNT(*) FROM [{schema}].[{tname}]")
                    ).scalar() or 0
                except Exception:
                    row_count = 0

                try:
                    col_count = conn.execute(
                        text(
                            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
                            "WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t"
                        ),
                        {"s": schema, "t": tname}
                    ).scalar() or 0
                except Exception:
                    col_count = 0

                tables.append(TableInfo(
                    schema_name=schema,
                    table_name=tname,
                    full_name=f"{schema}.{tname}",
                    row_count=row_count,
                    column_count=col_count,
                ))
        return tables

    def get_columns(self, schema: str, table: str) -> List[ColumnInfo]:
        engine = self._engine()
        inspector = sa_inspect(engine)
        pk_cols = set(
            inspector.get_pk_constraint(table, schema=schema).get("constrained_columns", [])
        )
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
        engine = self._engine()
        full   = f"{schema}.{table}"

        with engine.connect() as conn:
            row_count = conn.execute(
                text(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            ).scalar() or 0

            inspector  = sa_inspect(engine)
            pk_cols    = set(inspector.get_pk_constraint(table, schema=schema).get("constrained_columns", []))
            raw_cols   = inspector.get_columns(table, schema=schema)

            profiled_columns: List[ColumnInfo] = []
            for col in raw_cols:
                cname = col["name"]
                ctype = str(col["type"])
                cinfo = ColumnInfo(
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
                        text(f"SELECT COUNT(*) FROM [{schema}].[{table}] WHERE [{cname}] IS NULL")
                    ).scalar() or 0
                    cinfo.null_count = null_count
                    cinfo.null_pct   = round((null_count / row_count) * 100, 2)
                except Exception:
                    pass

                # Distinct count
                try:
                    cinfo.distinct_count = conn.execute(
                        text(f"SELECT COUNT(DISTINCT [{cname}]) FROM [{schema}].[{table}]")
                    ).scalar() or 0
                except Exception:
                    pass

                # Min / Max (numeric + date)
                numeric_types = ("int", "float", "numeric", "decimal", "real", "money", "bigint", "smallint")
                date_types    = ("date", "time", "datetime")
                lower_type    = ctype.lower()

                if any(t in lower_type for t in numeric_types + date_types):
                    try:
                        row = conn.execute(
                            text(f"SELECT MIN([{cname}]), MAX([{cname}]) FROM [{schema}].[{table}]")
                        ).fetchone()
                        if row:
                            cinfo.min_value = str(row[0]) if row[0] is not None else None
                            cinfo.max_value = str(row[1]) if row[1] is not None else None
                    except Exception:
                        pass

                    if any(t in lower_type for t in numeric_types):
                        try:
                            avg = conn.execute(
                                text(f"SELECT AVG(CAST([{cname}] AS FLOAT)) FROM [{schema}].[{table}]")
                            ).scalar()
                            cinfo.avg_value = round(float(avg), 4) if avg is not None else None
                        except Exception:
                            pass

                # Sample values
                try:
                    sample = conn.execute(
                        text(f"SELECT DISTINCT TOP {sample_rows} [{cname}] FROM [{schema}].[{table}] WHERE [{cname}] IS NOT NULL")
                    ).fetchall()
                    cinfo.sample_values = [str(r[0]) for r in sample]
                except Exception:
                    pass

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
            {
                "name": "endpoint",
                "label": "Fabric SQL Endpoint",
                "type": "text",
                "placeholder": "<workspace-id>.datawarehouse.fabric.microsoft.com",
                "help": "Found in Fabric workspace → SQL Analytics Endpoint → Connection string",
                "required": True,
            },
            {
                "name": "database",
                "label": "Database / Lakehouse Name",
                "type": "text",
                "placeholder": "MyLakehouse",
                "required": True,
            },
            {
                "name": "auth_mode",
                "label": "Authentication Method",
                "type": "select",
                "options": ["sql_auth", "service_principal", "access_token"],
                "default": "sql_auth",
                "required": True,
            },
            # SQL Auth fields
            {"name": "username",      "label": "Username",      "type": "text",     "placeholder": "fabric_user",     "required": False, "show_when": {"auth_mode": "sql_auth"}},
            {"name": "password",      "label": "Password",      "type": "password", "placeholder": "••••••••",        "required": False, "show_when": {"auth_mode": "sql_auth"}},
            # Service Principal fields
            {"name": "tenant_id",     "label": "Tenant ID",     "type": "text",     "placeholder": "xxxxxxxx-...",    "required": False, "show_when": {"auth_mode": "service_principal"}},
            {"name": "client_id",     "label": "Client ID",     "type": "text",     "placeholder": "xxxxxxxx-...",    "required": False, "show_when": {"auth_mode": "service_principal"}},
            {"name": "client_secret", "label": "Client Secret", "type": "password", "placeholder": "••••••••",        "required": False, "show_when": {"auth_mode": "service_principal"}},
            # Access Token
            {"name": "access_token",  "label": "Access Token",  "type": "password", "placeholder": "eyJ0eXAi...",    "required": False, "show_when": {"auth_mode": "access_token"}},
        ]
