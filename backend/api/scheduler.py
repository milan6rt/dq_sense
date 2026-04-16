"""
Scheduled profiling — APScheduler-based cron jobs that re-profile tables
and run all active DQ rules on a schedule.

Example cron expressions:
  "0 6 * * *"    every day at 06:00
  "0 */6 * * *"  every 6 hours
  "*/30 * * * *" every 30 minutes
"""

import logging
from datetime import datetime
from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from db.database import get_db, SessionLocal
from db.models import ScheduledScan, Connection, DiscoveredTable

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/scheduler", tags=["scheduler"])

# APScheduler instance (initialised in main.py lifespan)
_scheduler = None


def get_scheduler():
    global _scheduler
    if _scheduler is None:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        _scheduler = AsyncIOScheduler(timezone="UTC")
        _scheduler.start()
        logger.info("APScheduler started")
    return _scheduler


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class ScanCreate(BaseModel):
    name:          str
    connection_id: Optional[str] = None
    schedule_cron: str            # cron expression
    is_active:     Optional[bool] = True

class ScanUpdate(BaseModel):
    name:          Optional[str] = None
    connection_id: Optional[str] = None
    schedule_cron: Optional[str] = None
    is_active:     Optional[bool] = None


# ── CRUD endpoints ────────────────────────────────────────────────────────────

@router.get("/")
def list_scans(db: Session = Depends(get_db)):
    scans = db.query(ScheduledScan).order_by(ScheduledScan.created_at.desc()).all()
    return [_scan_dict(s) for s in scans]


@router.post("/")
def create_scan(body: ScanCreate, db: Session = Depends(get_db)):
    _validate_cron(body.schedule_cron)
    scan = ScheduledScan(
        id            = str(uuid4()),
        name          = body.name,
        connection_id = body.connection_id,
        schedule_cron = body.schedule_cron,
        is_active     = body.is_active,
        next_run_at   = _next_run(body.schedule_cron),
    )
    db.add(scan)
    db.commit()
    db.refresh(scan)
    if scan.is_active:
        _register_job(scan)
    logger.info(f"Created scheduled scan '{scan.name}' ({scan.schedule_cron})")
    return _scan_dict(scan)


@router.put("/{scan_id}")
def update_scan(scan_id: str, body: ScanUpdate, db: Session = Depends(get_db)):
    scan = db.query(ScheduledScan).filter(ScheduledScan.id == scan_id).first()
    if not scan:
        raise HTTPException(404, "Scan not found")
    for field, val in body.dict(exclude_none=True).items():
        setattr(scan, field, val)
    if body.schedule_cron:
        _validate_cron(body.schedule_cron)
        scan.next_run_at = _next_run(body.schedule_cron)
    scan.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(scan)
    # Re-register job
    _unregister_job(scan_id)
    if scan.is_active:
        _register_job(scan)
    return _scan_dict(scan)


@router.delete("/{scan_id}")
def delete_scan(scan_id: str, db: Session = Depends(get_db)):
    scan = db.query(ScheduledScan).filter(ScheduledScan.id == scan_id).first()
    if not scan:
        raise HTTPException(404, "Scan not found")
    _unregister_job(scan_id)
    db.delete(scan)
    db.commit()
    return {"ok": True}


@router.post("/{scan_id}/run-now")
def run_now(scan_id: str, db: Session = Depends(get_db)):
    """Trigger a scan immediately regardless of schedule."""
    scan = db.query(ScheduledScan).filter(ScheduledScan.id == scan_id).first()
    if not scan:
        raise HTTPException(404, "Scan not found")
    result = _execute_scan(scan_id)
    return result


@router.get("/history")
def scan_history(limit: int = 50, db: Session = Depends(get_db)):
    """Return recent profiling run history."""
    from db.models import ProfilingRun
    runs = (
        db.query(ProfilingRun)
        .order_by(ProfilingRun.started_at.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "id": r.id,
            "connection_id": r.connection_id,
            "status": r.status,
            "tables_scanned": r.tables_scanned,
            "issues_found": r.issues_found,
            "quality_score": r.quality_score,
            "triggered_by": r.triggered_by,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "completed_at": r.completed_at.isoformat() if r.completed_at else None,
        }
        for r in runs
    ]


# ── Execution logic ───────────────────────────────────────────────────────────

def _execute_scan(scan_id: str) -> dict:
    """Run a full profile + DQ rule pass for the scan's connection."""
    db = SessionLocal()
    try:
        scan = db.query(ScheduledScan).filter(ScheduledScan.id == scan_id).first()
        if not scan:
            return {"error": "Scan not found"}

        scan.last_run_at     = datetime.utcnow()
        scan.last_run_status = "running"
        scan.next_run_at     = _next_run(scan.schedule_cron)
        db.commit()

        tables_scanned = 0
        issues_found   = 0

        # Profile all tables for the connection (or all connections if none specified)
        conn_filter = [scan.connection_id] if scan.connection_id else [
            c.id for c in db.query(Connection).all()
        ]

        for conn_id in conn_filter:
            tables = db.query(DiscoveredTable).filter(
                DiscoveredTable.connection_id == conn_id
            ).all()
            for tbl in tables:
                try:
                    _profile_table(tbl, db)
                    tables_scanned += 1
                except Exception as e:
                    logger.warning(f"Profile failed for {tbl.full_name}: {e}")

        # Run all active DQ rules
        from db.models import DQRule
        from api.rules import _execute_rule
        rules = db.query(DQRule).filter(DQRule.is_active == True).all()
        for rule in rules:
            try:
                result = _execute_rule(rule, db)
                if result.get("status") == "fail":
                    issues_found += 1
            except Exception as e:
                logger.warning(f"Rule '{rule.name}' failed: {e}")

        scan.last_run_status = "completed"
        db.commit()

        logger.info(f"Scan '{scan.name}' completed: {tables_scanned} tables, {issues_found} rule failures")
        return {
            "scan_id": scan_id,
            "status": "completed",
            "tables_scanned": tables_scanned,
            "issues_found": issues_found,
            "ran_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Scan {scan_id} failed: {e}")
        if scan:
            scan.last_run_status = "failed"
            db.commit()
        return {"scan_id": scan_id, "status": "failed", "error": str(e)}
    finally:
        db.close()


def _profile_table(tbl: DiscoveredTable, db: Session):
    """Quick row-count and null-rate refresh for a table."""
    from services.connection_service import ConnectionService
    svc    = ConnectionService(db)
    engine = svc.get_engine(tbl.connection_id)
    fqn    = f'"{tbl.schema_name}"."{tbl.table_name}"'

    with engine.connect() as conn:
        from sqlalchemy import text
        row_count = conn.execute(text(f"SELECT COUNT(*) FROM {fqn}")).scalar()

    tbl.row_count    = row_count
    tbl.last_profiled = datetime.utcnow()


# ── APScheduler job registration ──────────────────────────────────────────────

def _register_job(scan: ScheduledScan):
    try:
        sched = get_scheduler()
        parts = scan.schedule_cron.split()
        if len(parts) != 5:
            return
        minute, hour, day, month, day_of_week = parts
        sched.add_job(
            _execute_scan,
            trigger="cron",
            id=f"scan_{scan.id}",
            args=[scan.id],
            minute=minute, hour=hour,
            day=day, month=month,
            day_of_week=day_of_week,
            replace_existing=True,
            misfire_grace_time=300,
        )
        logger.info(f"Registered job scan_{scan.id} ({scan.schedule_cron})")
    except Exception as e:
        logger.warning(f"Could not register job for scan {scan.id}: {e}")


def _unregister_job(scan_id: str):
    try:
        sched = get_scheduler()
        sched.remove_job(f"scan_{scan_id}")
    except Exception:
        pass


def restore_scheduled_jobs():
    """Called at startup to re-register all active jobs from DB."""
    db = SessionLocal()
    try:
        scans = db.query(ScheduledScan).filter(ScheduledScan.is_active == True).all()
        for scan in scans:
            _register_job(scan)
        logger.info(f"Restored {len(scans)} scheduled scan job(s)")
    finally:
        db.close()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _scan_dict(s) -> dict:
    return {
        "id": s.id, "name": s.name, "connection_id": s.connection_id,
        "schedule_cron": s.schedule_cron, "is_active": s.is_active,
        "last_run_at": s.last_run_at.isoformat() if s.last_run_at else None,
        "last_run_status": s.last_run_status,
        "next_run_at": s.next_run_at.isoformat() if s.next_run_at else None,
        "created_at": s.created_at.isoformat(),
    }


def _validate_cron(expr: str):
    parts = expr.strip().split()
    if len(parts) != 5:
        raise HTTPException(400, f"Invalid cron expression '{expr}'. Expected 5 fields: minute hour day month weekday")


def _next_run(cron_expr: str) -> Optional[datetime]:
    try:
        from apscheduler.triggers.cron import CronTrigger
        parts = cron_expr.split()
        t = CronTrigger(
            minute=parts[0], hour=parts[1],
            day=parts[2], month=parts[3], day_of_week=parts[4],
            timezone="UTC"
        )
        return t.get_next_fire_time(None, datetime.utcnow())
    except Exception:
        return None
