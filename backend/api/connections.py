"""
Connection API routes — mounted at /api/connections in main.py
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy.orm import Session

from db.database import get_db
from db.models import DiscoveredColumn, DiscoveredTable
from services.connection_service import (
    create_connection, list_connections, get_connection, update_connection,
    delete_connection, test_connection, discover_schemas, discover_tables,
    get_all_tables, profile_table, connection_to_dict, get_connector_metadata,
)

router = APIRouter(prefix="/api/connections", tags=["connections"])


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class ConnectionCreate(BaseModel):
    name:           str
    connector_type: str
    # Flat credential fields — the primary format sent by the frontend
    host:           Optional[str] = None
    port:           Optional[int] = None
    database:       Optional[str] = None
    username:       Optional[str] = None
    password:       Optional[str] = None
    sslmode:        Optional[str] = "prefer"
    # Fabric / other connector-specific fields
    server:         Optional[str] = None
    workspace:      Optional[str] = None
    auth_mode:      Optional[str] = None
    tenant_id_fabric: Optional[str] = None
    client_id:      Optional[str] = None
    client_secret:  Optional[str] = None
    access_token:   Optional[str] = None
    # Nested config dict — also accepted (takes precedence over flat fields)
    config:         Optional[Dict[str, Any]] = None
    description:    Optional[str] = ""
    tenant_id:      Optional[str] = None

class ConnectionUpdate(BaseModel):
    name:        Optional[str] = None
    description: Optional[str] = None
    config:      Optional[Dict[str, Any]] = None

class ExecuteSQL(BaseModel):
    sql: str


# ── Connector metadata ────────────────────────────────────────────────────────

@router.get("/types")
def list_connector_types():
    """Return all available connector types with their UI field definitions."""
    return get_connector_metadata()


# ── CRUD ──────────────────────────────────────────────────────────────────────

@router.get("")
def list_conns(tenant_id: Optional[str] = None, db: Session = Depends(get_db)):
    """List all connections (optionally scoped to a tenant)."""
    connections = list_connections(db, tenant_id)
    return [connection_to_dict(c) for c in connections]


@router.post("", status_code=201)
def create_conn(body: ConnectionCreate, db: Session = Depends(get_db)):
    """Create a new connection. Credentials are encrypted before storage."""
    try:
        # Build config: nested dict takes precedence; otherwise collect flat fields
        _flat_keys = ("host", "port", "database", "username", "password", "sslmode",
                      "server", "workspace", "auth_mode", "tenant_id_fabric",
                      "client_id", "client_secret", "access_token")
        config = body.config or {
            k: getattr(body, k) for k in _flat_keys
            if getattr(body, k) is not None
        }
        conn = create_connection(
            db=db,
            name=body.name,
            connector_type=body.connector_type,
            config=config,
            description=body.description or "",
            tenant_id=body.tenant_id,
        )
        return connection_to_dict(conn)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/{connection_id}")
def get_conn(connection_id: str, db: Session = Depends(get_db)):
    conn = get_connection(db, connection_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    return connection_to_dict(conn)


@router.patch("/{connection_id}")
def update_conn(connection_id: str, body: ConnectionUpdate, db: Session = Depends(get_db)):
    conn = update_connection(
        db=db,
        connection_id=connection_id,
        name=body.name,
        description=body.description,
        config=body.config,
    )
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    return connection_to_dict(conn)


@router.delete("/{connection_id}", status_code=204)
def delete_conn(connection_id: str, db: Session = Depends(get_db)):
    if not delete_connection(db, connection_id):
        raise HTTPException(status_code=404, detail="Connection not found")


# ── Testing ───────────────────────────────────────────────────────────────────

@router.post("/{connection_id}/test")
def test_conn(connection_id: str, db: Session = Depends(get_db)):
    """
    Open a real connection, measure latency, return result.
    Persists status in DB (ok | error).
    """
    result = test_connection(db, connection_id)
    return {
        "success":        result.success,
        "message":        result.message,
        "latency_ms":     result.latency_ms,
        "server_version": result.server_version,
        "error":          result.error,
    }


# ── Schema discovery ──────────────────────────────────────────────────────────

@router.get("/{connection_id}/schemas")
def get_schemas(connection_id: str, db: Session = Depends(get_db)):
    """Return list of schemas available in the warehouse."""
    try:
        schemas = discover_schemas(db, connection_id)
        return {"schemas": schemas}
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Schema discovery failed: {exc}")


@router.post("/{connection_id}/discover")
def discover(connection_id: str, schema: str = "public", db: Session = Depends(get_db)):
    """
    Discover all tables in a schema.
    Upserts into discovered_tables and returns the list.
    """
    try:
        tables = discover_tables(db, connection_id, schema)
        return {"schema": schema, "tables": tables, "count": len(tables)}
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Discovery failed: {exc}")


@router.get("/{connection_id}/tables")
def get_tables(connection_id: str, db: Session = Depends(get_db)):
    """Return all previously discovered tables for a connection."""
    try:
        return get_all_tables(db, connection_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/{connection_id}/tables/{table_id}/columns")
def get_columns(connection_id: str, table_id: str, db: Session = Depends(get_db)):
    """Return all profiled columns for a discovered table."""
    table = db.query(DiscoveredTable).filter_by(id=table_id, connection_id=connection_id).first()
    if not table:
        raise HTTPException(status_code=404, detail="Table not found")
    cols = db.query(DiscoveredColumn).filter_by(table_id=table_id).order_by(DiscoveredColumn.column_name).all()
    return [
        {
            "column_name":  c.column_name,
            "data_type":    c.data_type,
            "nullable":     c.nullable,
            "null_count":   c.null_count,
            "unique_count": c.distinct_count,  # model field is distinct_count
        }
        for c in cols
    ]


# ── Profiling ─────────────────────────────────────────────────────────────────

@router.post("/{connection_id}/profile")
def profile(
    connection_id: str,
    schema: str,
    table: str,
    db: Session = Depends(get_db),
):
    """
    Run a full profiling pass on a specific table.
    Returns column stats, quality score, and detected issues.
    This can take several seconds on large tables.
    """
    try:
        result = profile_table(db, connection_id, schema, table)
        return result
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Profiling failed: {exc}")


# ── Raw SQL execution ─────────────────────────────────────────────────────────

@router.post("/{connection_id}/execute")
def execute_sql(connection_id: str, body: ExecuteSQL, db: Session = Depends(get_db)):
    """Execute one or more SQL statements on the connection (DDL/DML)."""
    from services.connection_service import get_connection, _decrypt
    from connectors.registry import build_connector as _build
    from sqlalchemy import text as sa_text
    try:
        conn_obj = get_connection(db, connection_id)
        if not conn_obj:
            raise ValueError("Connection not found")
        config    = _decrypt(conn_obj.encrypted_config)
        connector = _build(conn_obj.connector_type, config)
        engine    = connector._engine()
        statements = [s.strip() for s in body.sql.split(";") if s.strip()]
        results = []
        with engine.begin() as conn:
            for stmt in statements:
                result = conn.execute(sa_text(stmt))
                try:
                    rows = [list(r) for r in result.fetchall()]
                    cols = list(result.keys())
                    results.append({"statement": stmt[:80], "rows": rows[:50], "columns": cols})
                except Exception:
                    results.append({"statement": stmt[:80], "rows_affected": result.rowcount})
        return {"success": True, "results": results}
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Execution failed: {exc}")
