"""
Base connector interface — every warehouse connector implements this.
All connectors return standardized data structures so the rest of the
platform never has to care which underlying database it is talking to.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from enum import Enum


class ConnectorType(str, Enum):
    POSTGRESQL  = "postgresql"
    MYSQL       = "mysql"
    MSSQL       = "mssql"
    FABRIC      = "fabric"           # Microsoft Fabric SQL Analytics Endpoint
    SNOWFLAKE   = "snowflake"
    BIGQUERY    = "bigquery"
    REDSHIFT    = "redshift"
    DATABRICKS  = "databricks"
    SQLITE      = "sqlite"           # local dev / testing


# ── Standardised data structures ────────────────────────────────────────────

@dataclass
class ColumnInfo:
    name: str
    data_type: str
    nullable: bool
    primary_key: bool = False
    default: Optional[str] = None
    # Profiling stats (populated after profiling)
    null_count: int = 0
    null_pct: float = 0.0
    distinct_count: int = 0
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    avg_value: Optional[Any] = None
    sample_values: List[Any] = field(default_factory=list)
    quality_score: float = 100.0


@dataclass
class TableInfo:
    schema_name: str
    table_name: str
    full_name: str          # schema.table
    row_count: int = 0
    column_count: int = 0
    size_bytes: Optional[int] = None
    columns: List[ColumnInfo] = field(default_factory=list)
    quality_score: float = 100.0
    last_profiled: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    description: Optional[str] = None


@dataclass
class SchemaInfo:
    schema_name: str
    tables: List[TableInfo] = field(default_factory=list)
    table_count: int = 0


@dataclass
class ConnectionTestResult:
    success: bool
    message: str
    latency_ms: Optional[float] = None
    server_version: Optional[str] = None
    error: Optional[str] = None


@dataclass
class ProfileResult:
    table_full_name: str
    row_count: int
    column_count: int
    columns: List[ColumnInfo]
    quality_score: float
    issues: List[Dict[str, Any]] = field(default_factory=list)
    profiled_at: Optional[str] = None


# ── Abstract base ────────────────────────────────────────────────────────────

class BaseConnector(ABC):
    """
    Universal interface every connector must implement.
    The platform calls these methods — connectors handle the dialect.
    """

    connector_type: ConnectorType
    display_name: str
    icon: str

    def __init__(self, config: Dict[str, Any]):
        """
        config: decrypted credential dict specific to the connector type.
        E.g. for PostgreSQL: {host, port, database, username, password, sslmode}
        """
        self.config = config

    # ── Connection lifecycle ─────────────────────────────────────────────────

    @abstractmethod
    def test_connection(self) -> ConnectionTestResult:
        """Open a short-lived connection, ping, return result and latency."""
        ...

    @abstractmethod
    def get_schemas(self) -> List[str]:
        """Return list of available schema names."""
        ...

    @abstractmethod
    def get_tables(self, schema: str) -> List[TableInfo]:
        """Return table list for a schema (without column detail)."""
        ...

    @abstractmethod
    def get_columns(self, schema: str, table: str) -> List[ColumnInfo]:
        """Return column definitions for a specific table."""
        ...

    @abstractmethod
    def profile_table(self, schema: str, table: str, sample_rows: int = 10) -> ProfileResult:
        """
        Run a full profiling pass on a table:
        - row count, column count
        - per-column: null%, distinct count, min/max/avg, sample values
        - compute quality score
        Returns a ProfileResult with all findings.
        """
        ...

    # ── Shared helpers ───────────────────────────────────────────────────────

    def _compute_quality_score(self, columns: List[ColumnInfo], row_count: int) -> float:
        """
        Simple quality scoring formula:
        - Penalise high null percentages
        - Penalise columns where distinct_count == 1 (constant column)
        Score is 0-100.
        """
        if not columns or row_count == 0:
            return 100.0

        total_penalty = 0.0
        for col in columns:
            # Null penalty: up to 40 points per column, weighted by null_pct
            null_penalty = col.null_pct * 0.4
            total_penalty += null_penalty

        avg_penalty = total_penalty / len(columns)
        return round(max(0.0, 100.0 - avg_penalty), 1)

    def _classify_issues(self, columns: List[ColumnInfo], row_count: int) -> List[Dict[str, Any]]:
        """Generate quality issue dicts from profiling results."""
        issues = []
        for col in columns:
            if col.null_pct > 20:
                issues.append({
                    "column": col.name,
                    "issue_type": "High Null Rate",
                    "severity": "high" if col.null_pct > 50 else "medium",
                    "description": f"{col.name} is {col.null_pct:.1f}% null",
                    "record_count": col.null_count,
                })
            if row_count > 10 and col.distinct_count == 1:
                issues.append({
                    "column": col.name,
                    "issue_type": "Constant Column",
                    "severity": "low",
                    "description": f"{col.name} has only one distinct value",
                    "record_count": row_count,
                })
        return issues

    # ── Metadata helpers ─────────────────────────────────────────────────────

    @classmethod
    def required_fields(cls) -> List[Dict[str, Any]]:
        """
        Return UI form field definitions so the frontend can render
        the correct connection form for each connector type dynamically.
        Override in subclasses.
        """
        return []
