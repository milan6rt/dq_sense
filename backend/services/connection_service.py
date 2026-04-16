"""
Connection service — all business logic around connections.
Handles credential encryption, connector instantiation, schema discovery,
and profiling runs. The API layer calls only this service.
"""

import os
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from cryptography.fernet import Fernet
from sqlalchemy.orm import Session

from db.models import Connection, DiscoveredTable, DiscoveredColumn, QualityIssue, ProfilingRun
from connectors.registry import build_connector, list_connector_types, get_connector_class
from connectors.base import ConnectionTestResult, ProfileResult

logger = logging.getLogger(__name__)

# ── Encryption key ────────────────────────────────────────────────────────────
# In production: load from env / secrets manager.
# We auto-generate + persist to a local key file for dev convenience.

_KEY_FILE = os.getenv("DATAIQ_KEY_FILE", "./dataiq.key")

def _load_or_create_key() -> bytes:
    if os.path.exists(_KEY_FILE):
        with open(_KEY_FILE, "rb") as f:
            return f.read().strip()
    key = Fernet.generate_key()
    with open(_KEY_FILE, "wb") as f:
        f.write(key)
    logger.info(f"Generated new encryption key at {_KEY_FILE}")
    return key

_fernet = Fernet(_load_or_create_key())


def _encrypt(data: dict) -> str:
    return _fernet.encrypt(json.dumps(data).encode()).decode()


def _decrypt(ciphertext: str) -> dict:
    return json.loads(_fernet.decrypt(ciphertext.encode()).decode())


# ── CRUD ──────────────────────────────────────────────────────────────────────

def create_connection(
    db: Session,
    name: str,
    connector_type: str,
    config: dict,
    description: str = "",
    tenant_id: Optional[str] = None,
) -> Connection:
    """Create and persist a new connection. Credentials are encrypted at rest."""
    # Validate connector type exists
    get_connector_class(connector_type)

    conn = Connection(
        name=name,
        connector_type=connector_type,
        description=description,
        encrypted_config=_encrypt(config),
        tenant_id=tenant_id,
        status="untested",
    )
    db.add(conn)
    db.commit()
    db.refresh(conn)
    logger.info(f"Created connection '{name}' ({connector_type}) id={conn.id}")
    return conn


def list_connections(db: Session, tenant_id: Optional[str] = None) -> List[Connection]:
    q = db.query(Connection)
    if tenant_id is not None:
        q = q.filter(Connection.tenant_id == tenant_id)
    return q.order_by(Connection.created_at.desc()).all()


def get_connection(db: Session, connection_id: str) -> Optional[Connection]:
    return db.query(Connection).filter(Connection.id == connection_id).first()


def update_connection(
    db: Session,
    connection_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    config: Optional[dict] = None,
) -> Optional[Connection]:
    conn = get_connection(db, connection_id)
    if not conn:
        return None
    if name is not None:
        conn.name = name
    if description is not None:
        conn.description = description
    if config is not None:
        conn.encrypted_config = _encrypt(config)
        conn.status = "untested"    # reset status when creds change
    conn.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(conn)
    return conn


def delete_connection(db: Session, connection_id: str) -> bool:
    conn = get_connection(db, connection_id)
    if not conn:
        return False
    db.delete(conn)
    db.commit()
    return True


# ── Connection testing ────────────────────────────────────────────────────────

def test_connection(db: Session, connection_id: str) -> ConnectionTestResult:
    """Decrypt credentials, instantiate connector, run test, persist result."""
    conn = get_connection(db, connection_id)
    if not conn:
        return ConnectionTestResult(success=False, message="Connection not found", error="Not found")

    try:
        config    = _decrypt(conn.encrypted_config)
        connector = build_connector(conn.connector_type, config)
        result    = connector.test_connection()
    except Exception as exc:
        result = ConnectionTestResult(success=False, message="Test failed", error=str(exc))

    conn.last_tested_at  = datetime.utcnow()
    conn.status          = "ok" if result.success else "error"
    conn.last_test_error = result.error
    conn.latency_ms      = result.latency_ms
    conn.server_version  = result.server_version
    db.commit()
    return result


# ── Schema discovery ──────────────────────────────────────────────────────────

def discover_schemas(db: Session, connection_id: str) -> List[str]:
    conn = get_connection(db, connection_id)
    if not conn:
        raise ValueError("Connection not found")
    config    = _decrypt(conn.encrypted_config)
    connector = build_connector(conn.connector_type, config)
    return connector.get_schemas()


def discover_tables(db: Session, connection_id: str, schema: str) -> List[Dict[str, Any]]:
    """
    Fetch tables from the remote warehouse, upsert into discovered_tables,
    and return the enriched list.
    """
    conn = get_connection(db, connection_id)
    if not conn:
        raise ValueError("Connection not found")

    config    = _decrypt(conn.encrypted_config)
    connector = build_connector(conn.connector_type, config)
    remote_tables = connector.get_tables(schema)

    result = []
    for tinfo in remote_tables:
        # Upsert
        existing = (
            db.query(DiscoveredTable)
            .filter_by(connection_id=connection_id, schema_name=schema, table_name=tinfo.table_name)
            .first()
        )
        if existing:
            existing.row_count    = tinfo.row_count
            existing.column_count = tinfo.column_count
            existing.updated_at   = datetime.utcnow()
            dt = existing
        else:
            dt = DiscoveredTable(
                connection_id=connection_id,
                schema_name=schema,
                table_name=tinfo.table_name,
                full_name=tinfo.full_name,
                row_count=tinfo.row_count,
                column_count=tinfo.column_count,
            )
            db.add(dt)
        db.flush()

        result.append(_table_to_dict(dt, conn))

    db.commit()
    return result


def get_all_tables(db: Session, connection_id: str) -> List[Dict[str, Any]]:
    """Return all previously discovered tables for a connection (from DB)."""
    conn = get_connection(db, connection_id)
    if not conn:
        raise ValueError("Connection not found")
    tables = (
        db.query(DiscoveredTable)
        .filter_by(connection_id=connection_id)
        .order_by(DiscoveredTable.schema_name, DiscoveredTable.table_name)
        .all()
    )
    return [_table_to_dict(t, conn) for t in tables]


# ── Profiling ──────────────────────────────────────────────────────────────────

def profile_table(
    db: Session,
    connection_id: str,
    schema: str,
    table: str,
) -> Dict[str, Any]:
    """
    Run a full profiling pass against a remote table.
    Persists column stats + quality issues in the DB.
    Returns a rich profiling summary dict.
    """
    conn = get_connection(db, connection_id)
    if not conn:
        raise ValueError("Connection not found")

    config    = _decrypt(conn.encrypted_config)
    connector = build_connector(conn.connector_type, config)

    # Create a profiling run record
    run = ProfilingRun(connection_id=connection_id, triggered_by="user", status="running")
    db.add(run)
    db.commit()

    try:
        result: ProfileResult = connector.profile_table(schema, table)

        # Upsert DiscoveredTable
        dt = (
            db.query(DiscoveredTable)
            .filter_by(connection_id=connection_id, schema_name=schema, table_name=table)
            .first()
        )
        if not dt:
            dt = DiscoveredTable(
                connection_id=connection_id,
                schema_name=schema,
                table_name=table,
                full_name=f"{schema}.{table}",
            )
            db.add(dt)

        dt.row_count     = result.row_count
        dt.column_count  = result.column_count
        dt.quality_score = result.quality_score
        dt.last_profiled = datetime.utcnow()
        db.flush()

        # Upsert columns
        for i, cinfo in enumerate(result.columns):
            dc = (
                db.query(DiscoveredColumn)
                .filter_by(table_id=dt.id, column_name=cinfo.name)
                .first()
            )
            if not dc:
                dc = DiscoveredColumn(table_id=dt.id, column_name=cinfo.name)
                db.add(dc)

            dc.data_type      = cinfo.data_type
            dc.nullable       = cinfo.nullable
            dc.primary_key    = cinfo.primary_key
            dc.ordinal        = i
            dc.null_count     = cinfo.null_count
            dc.null_pct       = cinfo.null_pct
            dc.distinct_count = cinfo.distinct_count
            dc.min_value      = str(cinfo.min_value) if cinfo.min_value is not None else None
            dc.max_value      = str(cinfo.max_value) if cinfo.max_value is not None else None
            dc.avg_value      = str(cinfo.avg_value) if cinfo.avg_value is not None else None
            dc.sample_values  = cinfo.sample_values
            dc.quality_score  = cinfo.quality_score
            dc.profiled_at    = datetime.utcnow()

        # Clear old issues for this table; replace with fresh findings
        db.query(QualityIssue).filter_by(table_id=dt.id).delete()
        for issue in result.issues:
            qi = QualityIssue(
                table_id=dt.id,
                column_name=issue.get("column"),
                issue_type=issue["issue_type"],
                severity=issue["severity"],
                description=issue["description"],
                record_count=issue.get("record_count", 0),
            )
            db.add(qi)

        run.status        = "completed"
        run.tables_scanned = 1
        run.issues_found  = len(result.issues)
        run.quality_score = result.quality_score
        run.completed_at  = datetime.utcnow()
        run.table_id      = dt.id

        db.commit()

        return {
            "table_id":     dt.id,
            "full_name":    dt.full_name,
            "row_count":    result.row_count,
            "column_count": result.column_count,
            "quality_score": result.quality_score,
            "issues":       result.issues,
            "profiled_at":  result.profiled_at,
            "columns": [
                {
                    "name":          c.name,
                    "data_type":     c.data_type,
                    "nullable":      c.nullable,
                    "primary_key":   c.primary_key,
                    "null_count":    c.null_count,
                    "null_pct":      c.null_pct,
                    "distinct_count":c.distinct_count,
                    "min_value":     c.min_value,
                    "max_value":     c.max_value,
                    "avg_value":     c.avg_value,
                    "sample_values": c.sample_values,
                    "quality_score": c.quality_score,
                }
                for c in result.columns
            ],
        }

    except Exception as exc:
        run.status       = "failed"
        run.completed_at = datetime.utcnow()
        run.error        = str(exc)
        db.commit()
        raise


# ── Serialisation helpers ─────────────────────────────────────────────────────

def _table_to_dict(dt: DiscoveredTable, conn: Connection) -> Dict[str, Any]:
    return {
        "id":            dt.id,
        "connection_id": dt.connection_id,
        "connection_name": conn.name,
        "connector_type": conn.connector_type,
        "schema_name":   dt.schema_name,
        "table_name":    dt.table_name,
        "full_name":     dt.full_name,
        "row_count":     dt.row_count,
        "column_count":  dt.column_count,
        "quality_score": dt.quality_score,
        "last_profiled": dt.last_profiled.isoformat() if dt.last_profiled else None,
        "description":   dt.description,
        "owner":         dt.owner,
        "tags":          dt.tags or [],
        "domain":        dt.domain,
        "discovered_at": dt.discovered_at.isoformat() if dt.discovered_at else None,
    }


def connection_to_dict(conn: Connection) -> Dict[str, Any]:
    """Safe serialisation — never include encrypted_config."""
    return {
        "id":             conn.id,
        "name":           conn.name,
        "connector_type": conn.connector_type,
        "description":    conn.description,
        "status":         conn.status,
        "latency_ms":     conn.latency_ms,
        "server_version": conn.server_version,
        "last_tested_at": conn.last_tested_at.isoformat() if conn.last_tested_at else None,
        "last_test_error":conn.last_test_error,
        "created_at":     conn.created_at.isoformat() if conn.created_at else None,
        "updated_at":     conn.updated_at.isoformat() if conn.updated_at else None,
        "table_count":    len(conn.discovered_tables),
    }


def get_connector_metadata() -> List[Dict[str, Any]]:
    """All registered connector types + their UI field definitions."""
    return list_connector_types()
