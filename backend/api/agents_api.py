"""
Agents API — CRUD for LLM config, agent controls (start/stop/run),
schedule management, and insight retrieval.

Routes (all prefixed /api/agents):
  GET    /api/agents/                        list agents + status
  POST   /api/agents/{id}/start              start agent
  POST   /api/agents/{id}/stop               stop agent
  POST   /api/agents/{id}/run                trigger immediate run
  GET    /api/agents/{id}/logs               recent activity log
  GET    /api/agents/{id}/schedule           get cron schedule
  PUT    /api/agents/{id}/schedule           set/update cron schedule
  DELETE /api/agents/{id}/schedule           remove schedule (disable)

  GET    /api/agents/insights                all recent insights (latest 50)
  GET    /api/agents/{id}/insights           insights for one agent

  GET    /api/llm/config                     get active LLM config (key masked)
  PUT    /api/llm/config                     save LLM config + API key
  POST   /api/llm/test                       test current LLM connectivity
  GET    /api/llm/models                     list provider/model options
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from db.database import get_db
from db.models import LLMConfig, AgentInsight, AgentSchedule
from services.connection_service import _encrypt, _decrypt
from llm_provider import PROVIDER_MODELS, build_provider, get_provider

logger = logging.getLogger(__name__)

router = APIRouter()

# ── Pydantic schemas ──────────────────────────────────────────────────────────

class LLMConfigIn(BaseModel):
    provider: str            # "anthropic" | "openai"
    model:    str
    api_key:  Optional[str] = None  # plaintext — we encrypt before storing; omit to keep existing

class ScheduleIn(BaseModel):
    cron_expression: Optional[str] = None   # e.g. "0 * * * *"  (None = disable)
    is_active:       bool = True

class RunTaskIn(BaseModel):
    connection_id: Optional[str] = None
    table_id:      Optional[str] = None
    schema:        Optional[str] = None
    table_name:    Optional[str] = None


# ── Helper: get orchestrator ───────────────────────────────────────────────────

def _orchestrator():
    from agents import agent_orchestrator
    return agent_orchestrator


# ── Agent list + control ───────────────────────────────────────────────────────

@router.get("/api/agents/")
def list_agents(db: Session = Depends(get_db)):
    orch = _orchestrator()
    agents_out = []
    for agent_id, agent in orch.agents.items():
        s = agent.get_status()
        # Fetch schedule from DB
        sched = db.query(AgentSchedule).filter(AgentSchedule.agent_id == agent_id).first()
        # Latest insight
        insight = db.query(AgentInsight).filter(AgentInsight.agent_id == agent_id).order_by(
            AgentInsight.created_at.desc()
        ).first()
        agents_out.append({
            "id":              s.id,
            "name":            s.name,
            "type":            s.type,
            "status":          s.status,
            "last_run":        s.last_run.isoformat() if s.last_run else None,
            "tasks_completed": s.tasks_completed,
            "tasks_failed":    s.tasks_failed,
            "uptime":          s.uptime,
            "error_message":   s.error_message,
            "recent_activity": [
                {"activity": a.activity, "status": a.status,
                 "timestamp": a.timestamp.isoformat()}
                for a in agent.activity_log[-5:]
            ],
            "schedule": {
                "cron_expression": sched.cron_expression if sched else None,
                "is_active":       sched.is_active if sched else False,
                "last_run_at":     sched.last_run_at.isoformat() if sched and sched.last_run_at else None,
                "next_run_at":     sched.next_run_at.isoformat() if sched and sched.next_run_at else None,
            } if sched else None,
            "latest_insight": {
                "summary":      insight.summary,
                "severity":     insight.severity,
                "insight_type": insight.insight_type,
                "created_at":   insight.created_at.isoformat(),
            } if insight else None,
        })
    return agents_out


@router.post("/api/agents/{agent_id}/start")
async def start_agent(agent_id: str):
    orch = _orchestrator()
    if agent_id not in orch.agents:
        raise HTTPException(status_code=404, detail="Agent not found")
    ok = await orch.start_agent(agent_id)
    return {"agent_id": agent_id, "status": orch.agents[agent_id].status, "started": ok}


@router.post("/api/agents/{agent_id}/stop")
async def stop_agent(agent_id: str):
    orch = _orchestrator()
    if agent_id not in orch.agents:
        raise HTTPException(status_code=404, detail="Agent not found")
    ok = await orch.stop_agent(agent_id)
    return {"agent_id": agent_id, "status": orch.agents[agent_id].status, "stopped": ok}


@router.post("/api/agents/{agent_id}/run")
async def run_agent_now(agent_id: str, body: RunTaskIn = RunTaskIn()):
    """Trigger an immediate run of an agent with optional task context."""
    orch = _orchestrator()
    if agent_id not in orch.agents:
        raise HTTPException(status_code=404, detail="Agent not found")

    agent = orch.agents[agent_id]
    if agent.status != "active":
        raise HTTPException(status_code=400, detail=f"Agent is {agent.status} — start it first")

    task_data = {
        k: v for k, v in {
            "connection_id": body.connection_id,
            "table_id":      body.table_id,
            "schema":        body.schema,
            "table_name":    body.table_name,
        }.items() if v is not None
    }

    # Fire and return task_id immediately; execution happens in background
    task_id = await orch.submit_task(agent_id, task_data)
    return {"task_id": task_id, "agent_id": agent_id, "status": "queued"}


@router.get("/api/agents/{agent_id}/logs")
def get_agent_logs(agent_id: str, limit: int = 50):
    orch = _orchestrator()
    if agent_id not in orch.agents:
        raise HTTPException(status_code=404, detail="Agent not found")
    agent = orch.agents[agent_id]
    logs  = agent.execution_log[-limit:]
    return [
        {"id": l.id, "level": l.level, "message": l.message,
         "timestamp": l.timestamp.isoformat(), "details": l.details}
        for l in reversed(logs)
    ]


# ── Schedule management ────────────────────────────────────────────────────────

@router.get("/api/agents/{agent_id}/schedule")
def get_schedule(agent_id: str, db: Session = Depends(get_db)):
    sched = db.query(AgentSchedule).filter(AgentSchedule.agent_id == agent_id).first()
    if not sched:
        return {"agent_id": agent_id, "cron_expression": None, "is_active": False}
    return {
        "agent_id":       sched.agent_id,
        "cron_expression": sched.cron_expression,
        "is_active":      sched.is_active,
        "last_run_at":    sched.last_run_at.isoformat() if sched.last_run_at else None,
        "next_run_at":    sched.next_run_at.isoformat() if sched.next_run_at else None,
    }


@router.put("/api/agents/{agent_id}/schedule")
def set_schedule(agent_id: str, body: ScheduleIn, db: Session = Depends(get_db)):
    orch = _orchestrator()
    if agent_id not in orch.agents:
        raise HTTPException(status_code=404, detail="Agent not found")

    sched = db.query(AgentSchedule).filter(AgentSchedule.agent_id == agent_id).first()
    if not sched:
        sched = AgentSchedule(agent_id=agent_id)
        db.add(sched)

    sched.cron_expression = body.cron_expression
    sched.is_active       = body.is_active and bool(body.cron_expression)
    sched.updated_at      = datetime.utcnow()
    db.commit()

    # Register / update APScheduler job
    _sync_apscheduler(agent_id, sched.cron_expression if sched.is_active else None)

    return {"agent_id": agent_id, "cron_expression": sched.cron_expression, "is_active": sched.is_active}


@router.delete("/api/agents/{agent_id}/schedule")
def delete_schedule(agent_id: str, db: Session = Depends(get_db)):
    sched = db.query(AgentSchedule).filter(AgentSchedule.agent_id == agent_id).first()
    if sched:
        sched.is_active       = False
        sched.cron_expression = None
        db.commit()
    _sync_apscheduler(agent_id, None)
    return {"agent_id": agent_id, "is_active": False}


def _sync_apscheduler(agent_id: str, cron_expression: Optional[str]):
    """Add/replace/remove an APScheduler job for a given agent."""
    try:
        from api.scheduler import _scheduler as apscheduler
        if apscheduler is None:
            logger.warning("APScheduler not yet started — schedule will be applied on next restart")
            return
        job_id = f"agent_{agent_id}"

        # Remove existing job if present
        try:
            apscheduler.remove_job(job_id)
        except Exception:
            pass

        if not cron_expression:
            return

        # Parse cron fields: minute hour day month day_of_week
        fields = cron_expression.strip().split()
        if len(fields) != 5:
            raise ValueError(f"Invalid cron expression: {cron_expression}")
        minute, hour, day, month, day_of_week = fields

        async def _agent_job():
            orch  = _orchestrator()
            agent = orch.agents.get(agent_id)
            if agent and agent.status == "active":
                await agent.execute_task({})
                from db.database import SessionLocal
                db2 = SessionLocal()
                try:
                    s = db2.query(AgentSchedule).filter(AgentSchedule.agent_id == agent_id).first()
                    if s:
                        s.last_run_at = datetime.utcnow()
                        db2.commit()
                finally:
                    db2.close()

        apscheduler.add_job(
            _agent_job, "cron", id=job_id,
            minute=minute, hour=hour, day=day, month=month, day_of_week=day_of_week,
            replace_existing=True,
        )
        logger.info(f"Scheduled agent {agent_id} with cron: {cron_expression}")
    except Exception as e:
        logger.error(f"Failed to sync APScheduler for {agent_id}: {e}")


# ── Insights ───────────────────────────────────────────────────────────────────

@router.get("/api/agents/insights")
def get_all_insights(limit: int = 50, db: Session = Depends(get_db)):
    insights = db.query(AgentInsight).order_by(
        AgentInsight.created_at.desc()
    ).limit(limit).all()
    return [_insight_to_dict(i) for i in insights]


@router.get("/api/agents/{agent_id}/insights")
def get_agent_insights(agent_id: str, limit: int = 20, db: Session = Depends(get_db)):
    insights = db.query(AgentInsight).filter(
        AgentInsight.agent_id == agent_id
    ).order_by(AgentInsight.created_at.desc()).limit(limit).all()
    return [_insight_to_dict(i) for i in insights]


def _insight_to_dict(i: AgentInsight) -> dict:
    return {
        "id":           i.id,
        "agent_id":     i.agent_id,
        "insight_type": i.insight_type,
        "summary":      i.summary,
        "full_analysis":i.full_analysis,
        "severity":     i.severity,
        "table_id":     i.table_id,
        "connection_id":i.connection_id,
        "metadata":     i.insight_meta,
        "created_at":   i.created_at.isoformat(),
    }


# ── LLM Config ────────────────────────────────────────────────────────────────

@router.get("/api/llm/config")
def get_llm_config(db: Session = Depends(get_db)):
    cfg = db.query(LLMConfig).filter(LLMConfig.is_active == True).order_by(
        LLMConfig.updated_at.desc()
    ).first()
    if not cfg:
        return {"configured": False, "provider": None, "model": None}
    return {
        "configured": True,
        "provider":   cfg.provider,
        "model":      cfg.model,
        "api_key":    "••••••••" + (cfg.encrypted_api_key[-4:] if len(cfg.encrypted_api_key) > 4 else ""),
        "updated_at": cfg.updated_at.isoformat(),
    }


@router.put("/api/llm/config")
def save_llm_config(body: LLMConfigIn, db: Session = Depends(get_db)):
    if body.provider not in PROVIDER_MODELS:
        raise HTTPException(status_code=400, detail=f"Unknown provider: {body.provider}. Choose from: {list(PROVIDER_MODELS)}")

    valid_models = [m["id"] for m in PROVIDER_MODELS[body.provider]]
    if body.model not in valid_models:
        raise HTTPException(status_code=400, detail=f"Unknown model '{body.model}' for {body.provider}. Options: {valid_models}")

    # Resolve encrypted key — keep existing if no new key provided
    if body.api_key and body.api_key.strip():
        encrypted = _encrypt({"key": body.api_key.strip()})
    else:
        existing = db.query(LLMConfig).filter(LLMConfig.is_active == True).first()
        if not existing:
            raise HTTPException(status_code=400, detail="No existing API key found — please provide one.")
        encrypted = existing.encrypted_api_key

    # Deactivate old configs and insert new one
    db.query(LLMConfig).update({"is_active": False})
    cfg = LLMConfig(
        provider          = body.provider,
        model             = body.model,
        encrypted_api_key = encrypted,
        is_active         = True,
    )
    db.add(cfg)
    db.commit()
    db.refresh(cfg)

    return {
        "configured": True,
        "provider":   cfg.provider,
        "model":      cfg.model,
        "updated_at": cfg.updated_at.isoformat(),
    }


@router.delete("/api/llm/config")
def delete_llm_config(db: Session = Depends(get_db)):
    """Remove saved LLM credentials — agents will fall back to NoOpProvider."""
    db.query(LLMConfig).update({"is_active": False})
    db.commit()
    return {"configured": False, "message": "API key removed."}


@router.post("/api/llm/test")
async def test_llm(db: Session = Depends(get_db)):
    provider = get_provider(db)
    if provider.provider_name == "none":
        return {"ok": False, "message": "No LLM configured — save an API key first"}
    result = await provider.test()
    return {
        "ok":       result["ok"],
        "provider": provider.provider_name,
        "model":    provider.model_name,
        "message":  result["message"],
    }


@router.get("/api/llm/models")
def list_models():
    from llm_provider import PROVIDER_LABELS
    return [
        {
            "provider":       provider,
            "provider_label": PROVIDER_LABELS.get(provider, provider),
            "models":         models,
            "free":           provider in ("groq", "gemini", "ollama"),
            "local":          provider == "ollama",
        }
        for provider, models in PROVIDER_MODELS.items()
    ]
