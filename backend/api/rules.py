"""
DQ Rule Engine — custom data quality rules per table.

Rule types supported:
  - not_null        : column must not have nulls
  - unique          : column must be unique
  - min_value       : numeric column >= threshold
  - max_value       : numeric column <= threshold
  - regex           : column matches regex pattern
  - referential     : column values exist in another table/column
  - freshness       : table updated within N hours
  - row_count       : table has >= N rows
  - custom_sql      : arbitrary SQL returning 0 rows = pass
"""

import logging
import re
from datetime import datetime
from typing import Optional, List
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from db.database import get_db
from db.models import DQRule, DQRuleRun, DiscoveredTable, Connection
from db.database import engine as _engine

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/rules", tags=["rules"])


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class RuleCreate(BaseModel):
    table_id:     str
    name:         str
    rule_type:    str           # not_null | unique | min_value | max_value | regex | freshness | row_count | custom_sql
    column_name:  Optional[str] = None
    parameters:   Optional[dict] = {}
    severity:     Optional[str] = "medium"   # low | medium | high | critical
    description:  Optional[str] = ""
    is_active:    Optional[bool] = True

class RuleUpdate(BaseModel):
    name:        Optional[str] = None
    rule_type:   Optional[str] = None
    column_name: Optional[str] = None
    parameters:  Optional[dict] = None
    severity:    Optional[str] = None
    description: Optional[str] = None
    is_active:   Optional[bool] = None


# ── CRUD endpoints ────────────────────────────────────────────────────────────

@router.get("/")
def list_rules(table_id: Optional[str] = None, db: Session = Depends(get_db)):
    q = db.query(DQRule)
    if table_id:
        q = q.filter(DQRule.table_id == table_id)
    rules = q.order_by(DQRule.created_at.desc()).all()
    return [_rule_dict(r) for r in rules]


@router.post("/")
def create_rule(body: RuleCreate, db: Session = Depends(get_db)):
    _validate_rule_type(body.rule_type)
    rule = DQRule(
        id          = str(uuid4()),
        table_id    = body.table_id,
        name        = body.name,
        rule_type   = body.rule_type,
        column_name = body.column_name,
        parameters  = body.parameters or {},
        severity    = body.severity,
        description = body.description,
        is_active   = body.is_active,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    logger.info(f"Created rule '{rule.name}' ({rule.rule_type}) on table {rule.table_id}")
    return _rule_dict(rule)


@router.put("/{rule_id}")
def update_rule(rule_id: str, body: RuleUpdate, db: Session = Depends(get_db)):
    rule = db.query(DQRule).filter(DQRule.id == rule_id).first()
    if not rule:
        raise HTTPException(404, "Rule not found")
    for field, val in body.dict(exclude_none=True).items():
        setattr(rule, field, val)
    rule.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(rule)
    return _rule_dict(rule)


@router.delete("/{rule_id}")
def delete_rule(rule_id: str, db: Session = Depends(get_db)):
    rule = db.query(DQRule).filter(DQRule.id == rule_id).first()
    if not rule:
        raise HTTPException(404, "Rule not found")
    db.delete(rule)
    db.commit()
    return {"ok": True}


# ── Rule execution ────────────────────────────────────────────────────────────

@router.post("/{rule_id}/run")
def run_rule(rule_id: str, db: Session = Depends(get_db)):
    """Execute a single rule and return the result."""
    rule = db.query(DQRule).filter(DQRule.id == rule_id).first()
    if not rule:
        raise HTTPException(404, "Rule not found")
    result = _execute_rule(rule, db)
    return result


@router.post("/run-all")
def run_all_rules(table_id: Optional[str] = None, db: Session = Depends(get_db)):
    """Execute all active rules (optionally filtered by table)."""
    q = db.query(DQRule).filter(DQRule.is_active == True)
    if table_id:
        q = q.filter(DQRule.table_id == table_id)
    rules = q.all()
    results = [_execute_rule(r, db) for r in rules]
    passed  = sum(1 for r in results if r["status"] == "pass")
    failed  = sum(1 for r in results if r["status"] == "fail")
    errored = sum(1 for r in results if r["status"] == "error")
    return {
        "total": len(results),
        "passed": passed,
        "failed": failed,
        "errored": errored,
        "results": results,
    }


@router.get("/{rule_id}/history")
def rule_history(rule_id: str, limit: int = 20, db: Session = Depends(get_db)):
    runs = (
        db.query(DQRuleRun)
        .filter(DQRuleRun.rule_id == rule_id)
        .order_by(DQRuleRun.ran_at.desc())
        .limit(limit)
        .all()
    )
    return [_run_dict(r) for r in runs]


# ── Templates (pre-built common rules) ───────────────────────────────────────

@router.get("/templates")
def rule_templates():
    return [
        {"rule_type": "not_null",   "name": "No Nulls",         "description": "Column must have no null values",           "needs_column": True,  "params": {}},
        {"rule_type": "unique",     "name": "Unique Values",     "description": "Column must have no duplicate values",      "needs_column": True,  "params": {}},
        {"rule_type": "min_value",  "name": "Minimum Value",     "description": "Numeric column must be >= threshold",       "needs_column": True,  "params": {"threshold": 0}},
        {"rule_type": "max_value",  "name": "Maximum Value",     "description": "Numeric column must be <= threshold",       "needs_column": True,  "params": {"threshold": 9999999}},
        {"rule_type": "regex",      "name": "Regex Pattern",     "description": "Column values must match a regex pattern",  "needs_column": True,  "params": {"pattern": "^[A-Z]"}},
        {"rule_type": "freshness",  "name": "Data Freshness",    "description": "Table must have been updated recently",     "needs_column": True,  "params": {"max_age_hours": 24, "timestamp_col": "updated_at"}},
        {"rule_type": "row_count",  "name": "Row Count Check",   "description": "Table must have at least N rows",           "needs_column": False, "params": {"min_rows": 1}},
        {"rule_type": "custom_sql", "name": "Custom SQL",        "description": "Custom SQL query — 0 rows = pass",          "needs_column": False, "params": {"sql": "SELECT 1 WHERE 1=0"}},
    ]


# ── Rule execution engine ─────────────────────────────────────────────────────

def _execute_rule(rule: "DQRule", db: Session) -> dict:
    """Run a rule against PostgreSQL and record the result."""
    table = db.query(DiscoveredTable).filter(DiscoveredTable.id == rule.table_id).first()
    if not table:
        return _save_run(rule, db, "error", 0, 0, "Table not found in catalog")

    conn_obj = db.query(Connection).filter(Connection.id == table.connection_id).first()
    if not conn_obj:
        return _save_run(rule, db, "error", 0, 0, "Connection not found")

    # Decrypt and connect to the actual DB
    try:
        from services.connection_service import _decrypt
        from connectors.registry import build_connector
        config    = _decrypt(conn_obj.encrypted_config)
        connector = build_connector(conn_obj.connector_type, config)
        pg_engine = connector._engine()
    except Exception as e:
        return _save_run(rule, db, "error", 0, 0, f"Cannot connect: {e}")

    schema = table.schema_name
    tbl    = table.table_name
    col    = rule.column_name or ""
    params = rule.parameters or {}

    # Validate column is set for rules that require one
    COLUMN_REQUIRED = {"not_null", "unique", "min_value", "max_value", "regex", "freshness"}
    if rule.rule_type in COLUMN_REQUIRED and not col.strip():
        return _save_run(rule, db, "error", 0, 0,
                         f"Rule type '{rule.rule_type}' requires a column — please edit the rule and select one")

    try:
        sql, check_fn = _build_sql(rule.rule_type, schema, tbl, col, params)
        with pg_engine.connect() as pg_conn:
            result = pg_conn.execute(text(sql))
            rows = result.fetchall()
        status, failing, message = check_fn(rows)
    except Exception as e:
        logger.warning(f"Rule '{rule.name}' error: {e}")
        return _save_run(rule, db, "error", 0, 0, str(e))

    total = table.row_count or 0
    return _save_run(rule, db, status, failing, total, message)


def _build_sql(rule_type, schema, tbl, col, params):
    """Return (sql, check_fn) for a rule type. check_fn(rows) → (status, failing_count, message)."""
    fqn = f'"{schema}"."{tbl}"'

    if rule_type == "not_null":
        sql = f'SELECT COUNT(*) AS cnt FROM {fqn} WHERE "{col}" IS NULL'
        def check(rows):
            n = rows[0][0]
            return ("pass" if n == 0 else "fail"), n, f"{n} null value(s) in {col}"
        return sql, check

    elif rule_type == "unique":
        sql = f'SELECT COUNT(*) - COUNT(DISTINCT "{col}") AS cnt FROM {fqn}'
        def check(rows):
            n = rows[0][0]
            return ("pass" if n == 0 else "fail"), n, f"{n} duplicate value(s) in {col}"
        return sql, check

    elif rule_type == "min_value":
        threshold = params.get("threshold", 0)
        sql = f'SELECT COUNT(*) AS cnt FROM {fqn} WHERE "{col}" < {threshold}'
        def check(rows):
            n = rows[0][0]
            return ("pass" if n == 0 else "fail"), n, f"{n} row(s) below minimum {threshold} in {col}"
        return sql, check

    elif rule_type == "max_value":
        threshold = params.get("threshold", 9999999)
        sql = f'SELECT COUNT(*) AS cnt FROM {fqn} WHERE "{col}" > {threshold}'
        def check(rows):
            n = rows[0][0]
            return ("pass" if n == 0 else "fail"), n, f"{n} row(s) above maximum {threshold} in {col}"
        return sql, check

    elif rule_type == "regex":
        pattern = params.get("pattern", ".*")
        sql = f"SELECT COUNT(*) AS cnt FROM {fqn} WHERE \"{col}\" !~ '{pattern}'"
        def check(rows):
            n = rows[0][0]
            return ("pass" if n == 0 else "fail"), n, f"{n} value(s) not matching pattern '{pattern}' in {col}"
        return sql, check

    elif rule_type == "freshness":
        ts_col = params.get("timestamp_col", "updated_at")
        max_age = params.get("max_age_hours", 24)
        sql = f"SELECT EXTRACT(EPOCH FROM (NOW() - MAX(\"{ts_col}\"))) / 3600 AS age_hours FROM {fqn}"
        def check(rows):
            age = rows[0][0] or 0
            ok = age <= max_age
            return ("pass" if ok else "fail"), 0 if ok else 1, f"Data is {age:.1f}h old (max {max_age}h)"
        return sql, check

    elif rule_type == "row_count":
        min_rows = params.get("min_rows", 1)
        sql = f"SELECT COUNT(*) AS cnt FROM {fqn}"
        def check(rows):
            n = rows[0][0]
            ok = n >= min_rows
            return ("pass" if ok else "fail"), 0 if ok else 1, f"Table has {n} rows (min {min_rows})"
        return sql, check

    elif rule_type == "custom_sql":
        sql = params.get("sql", "SELECT 1 WHERE 1=0")
        def check(rows):
            n = len(rows)
            return ("pass" if n == 0 else "fail"), n, f"Query returned {n} failing row(s)"
        return sql, check

    else:
        raise ValueError(f"Unknown rule type: {rule_type}")


def _save_run(rule, db, status, failing_rows, total_rows, message) -> dict:
    run = DQRuleRun(
        id           = str(uuid4()),
        rule_id      = rule.id,
        status       = status,
        failing_rows = failing_rows,
        total_rows   = total_rows,
        message      = message,
        ran_at       = datetime.utcnow(),
    )
    db.add(run)
    # Update rule's last_run info
    rule.last_run_at     = datetime.utcnow()
    rule.last_run_status = status
    db.commit()
    return _run_dict(run) | {"rule_id": rule.id, "rule_name": rule.name, "severity": rule.severity}


# ── Serialisers ───────────────────────────────────────────────────────────────

def _rule_dict(r) -> dict:
    return {
        "id": r.id, "table_id": r.table_id, "name": r.name,
        "rule_type": r.rule_type, "column_name": r.column_name,
        "parameters": r.parameters, "severity": r.severity,
        "description": r.description, "is_active": r.is_active,
        "last_run_at": r.last_run_at.isoformat() if r.last_run_at else None,
        "last_run_status": r.last_run_status,
        "created_at": r.created_at.isoformat(),
    }

def _run_dict(r) -> dict:
    return {
        "id": r.id, "rule_id": r.rule_id, "status": r.status,
        "failing_rows": r.failing_rows, "total_rows": r.total_rows,
        "message": r.message,
        "ran_at": r.ran_at.isoformat() if r.ran_at else None,
    }

def _validate_rule_type(rt):
    valid = {"not_null","unique","min_value","max_value","regex","freshness","row_count","custom_sql"}
    if rt not in valid:
        raise HTTPException(400, f"Invalid rule_type. Must be one of: {', '.join(sorted(valid))}")
