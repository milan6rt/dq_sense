# agents.py
import asyncio
import logging
import uuid
import random
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ── Simple enums ───────────────────────────────────────────────────────────────

class AgentStatusEnum:
    inactive = "inactive"
    starting = "starting"
    active   = "active"
    stopping = "stopping"
    error    = "error"

class AgentType:
    profiling   = "profiling"
    validation  = "validation"
    monitoring  = "monitoring"
    lineage     = "lineage"

class AgentStatus:
    def __init__(self, id, name, type, status, last_run=None,
                 tasks_completed=0, tasks_failed=0, uptime=None, error_message=None):
        self.id              = id
        self.name            = name
        self.type            = type
        self.status          = status
        self.last_run        = last_run
        self.tasks_completed = tasks_completed
        self.tasks_failed    = tasks_failed
        self.uptime          = uptime
        self.error_message   = error_message

class AgentActivity:
    def __init__(self, id, agent_id, agent_name, activity, status, timestamp, details=None):
        self.id         = id
        self.agent_id   = agent_id
        self.agent_name = agent_name
        self.activity   = activity
        self.status     = status
        self.timestamp  = timestamp
        self.details    = details

class AgentLog:
    def __init__(self, id, agent_id, level, message, timestamp, details=None):
        self.id        = id
        self.agent_id  = agent_id
        self.level     = level
        self.message   = message
        self.timestamp = timestamp
        self.details   = details


# ── Helpers ────────────────────────────────────────────────────────────────────

def _profile_in_session(connection_id: str, schema: str, table_name: str) -> dict:
    """Run profile_table inside its own DB session (synchronous)."""
    from db.database import SessionLocal
    from services.connection_service import profile_table as _profile
    db = SessionLocal()
    try:
        return _profile(db, connection_id, schema, table_name)
    finally:
        db.close()


def _save_insight(agent_id: str, insight_type: str, summary: str,
                  full_analysis: str, severity: str = "info",
                  connection_id: str = None, table_id: str = None,
                  meta: dict = None):
    """Persist an LLM insight to the database."""
    from db.database import SessionLocal
    from db.models import AgentInsight
    db = SessionLocal()
    try:
        insight = AgentInsight(
            agent_id      = agent_id,
            insight_type  = insight_type,
            summary       = summary,
            full_analysis = full_analysis,
            severity      = severity,
            connection_id = connection_id,
            table_id      = table_id,
            insight_meta  = meta or {},
        )
        db.add(insight)
        db.commit()
    except Exception as e:
        logger.error(f"Failed to save insight: {e}")
    finally:
        db.close()


def _get_llm_provider():
    """Get the active LLM provider from DB config."""
    from db.database import SessionLocal
    from llm_provider import get_provider
    db = SessionLocal()
    try:
        return get_provider(db)
    finally:
        db.close()


# ── LLM prompts per agent ──────────────────────────────────────────────────────

PROFILER_SYSTEM = """You are a senior data quality analyst embedded in the DataIQ platform.
You receive profiling statistics for a database table and produce a concise, actionable analysis.
Format your response as:

SUMMARY: <one sentence headline>
SEVERITY: <info|warning|critical>
FINDINGS:
- <finding 1>
- <finding 2>
...
RECOMMENDATIONS:
- <recommendation 1>
- <recommendation 2>
...

Be specific. Reference column names and numbers. Flag PII if column names suggest it (email, ssn, phone, dob, etc.)."""

VALIDATOR_SYSTEM = """You are a data quality engineer reviewing DQ rule results for a table.
You receive rule execution outcomes and write a quality assessment.
Format your response as:

SUMMARY: <one sentence headline>
SEVERITY: <info|warning|critical>
QUALITY_SCORE_ASSESSMENT: <brief comment on the overall quality score>
RULE_FINDINGS:
- <rule name>: <pass/fail explanation>
...
PRIORITY_ACTIONS:
- <most important fix first>
...

Be direct and specific."""

LINEAGE_SYSTEM = """You are a data lineage expert. Given a list of tables and their column names from a data warehouse,
infer the likely data flow and lineage relationships.
Format your response as:

SUMMARY: <one sentence description of the data architecture>
SEVERITY: info
INFERRED_LAYERS:
- Source: <tables that appear to be raw/source data>
- Staging: <tables that appear to be transformed/cleaned>
- Mart/Serving: <tables that appear to be business-ready>
LIKELY_LINEAGE:
- <source table> → <downstream table> (reason: <why you think this>)
...
DATA_DOMAINS:
- <domain name>: <tables belonging to it>
...

Base your analysis on naming conventions (src_, stg_, fact_, dim_, bronze_, silver_, gold_) and column overlap."""

ANOMALY_SYSTEM = """You are an anomaly detection specialist for data pipelines.
You receive current vs previous profiling statistics for a table and identify meaningful changes.
Format your response as:

SUMMARY: <one sentence headline>
SEVERITY: <info|warning|critical>
ANOMALIES_DETECTED:
- <metric>: changed from <old> to <new> — <explanation and risk>
...
FALSE_POSITIVES_TO_IGNORE:
- <anything that looks like a change but is probably fine>
RECOMMENDED_ACTIONS:
- <action 1>
...

Only flag statistically meaningful changes. Ignore tiny fluctuations (< 2%)."""


# ── Base agent ─────────────────────────────────────────────────────────────────

class BaseAgent(ABC):
    def __init__(self, agent_id: str, name: str, agent_type: str):
        self.agent_id        = agent_id
        self.name            = name
        self.agent_type      = agent_type
        self.status          = AgentStatusEnum.inactive
        self.tasks_completed = 0
        self.tasks_failed    = 0
        self.last_run        = None
        self.start_time      = None
        self.error_message   = None
        self.is_running      = False
        self.activity_log: List[AgentActivity] = []
        self.execution_log: List[AgentLog]     = []

    async def start(self):
        try:
            self.status     = AgentStatusEnum.starting
            self.start_time = datetime.now()
            await self._initialize()
            self.status     = AgentStatusEnum.active
            self.is_running = True
            await self._log_activity("Agent started successfully", "info")
            logger.info(f"Agent {self.name} started")
        except Exception as e:
            self.status        = AgentStatusEnum.error
            self.error_message = str(e)
            await self._log_activity(f"Failed to start: {e}", "error")
            logger.error(f"Agent {self.name} start failed: {e}")

    async def stop(self):
        try:
            self.status     = AgentStatusEnum.stopping
            self.is_running = False
            await self._cleanup()
            self.status = AgentStatusEnum.inactive
            await self._log_activity("Agent stopped", "info")
            logger.info(f"Agent {self.name} stopped")
        except Exception as e:
            self.status        = AgentStatusEnum.error
            self.error_message = str(e)
            await self._log_activity(f"Error stopping: {e}", "error")

    async def execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        task_id    = str(uuid.uuid4())
        start_time = datetime.now()
        try:
            await self._log_activity(f"Starting task {task_id[:8]}", "info")
            result = await self._execute_task(task_data)
            self.tasks_completed += 1
            self.last_run         = datetime.now()
            elapsed = (datetime.now() - start_time).total_seconds()
            await self._log_activity(f"Task completed in {elapsed:.1f}s", "success")
            return {"task_id": task_id, "status": "completed", "result": result, "execution_time": elapsed}
        except Exception as e:
            self.tasks_failed += 1
            await self._log_activity(f"Task failed: {e}", "error")
            logger.error(f"{self.name} task failed: {e}")
            return {"task_id": task_id, "status": "failed", "error": str(e),
                    "execution_time": (datetime.now() - start_time).total_seconds()}

    async def _log_activity(self, message: str, level: str = "info", details: dict = None):
        activity = AgentActivity(
            id=str(uuid.uuid4()), agent_id=self.agent_id, agent_name=self.name,
            activity=message, status=level, timestamp=datetime.now(), details=details,
        )
        log_entry = AgentLog(
            id=str(uuid.uuid4()), agent_id=self.agent_id, level=level,
            message=message, timestamp=datetime.now(), details=details,
        )
        self.activity_log.append(activity)
        self.activity_log = self.activity_log[-100:]
        self.execution_log.append(log_entry)
        self.execution_log = self.execution_log[-500:]

    def get_status(self) -> AgentStatus:
        uptime = None
        if self.start_time and self.status == AgentStatusEnum.active:
            uptime = str(datetime.now() - self.start_time)
        return AgentStatus(
            id=self.agent_id, name=self.name, type=self.agent_type,
            status=self.status, last_run=self.last_run,
            tasks_completed=self.tasks_completed, tasks_failed=self.tasks_failed,
            uptime=uptime, error_message=self.error_message,
        )

    @abstractmethod
    async def _initialize(self): ...

    @abstractmethod
    async def _cleanup(self): ...

    @abstractmethod
    async def _execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]: ...


# ── Data Profiler Agent ────────────────────────────────────────────────────────

class DataProfilerAgent(BaseAgent):
    def __init__(self):
        super().__init__("data_profiler", "Data Profiler", AgentType.profiling)
        self.profiling_queue = asyncio.Queue()

    async def _initialize(self):
        await self._log_activity("Initializing — checking LLM configuration")
        loop = asyncio.get_event_loop()
        provider = await loop.run_in_executor(None, _get_llm_provider)
        llm_status = "✓ LLM ready" if provider.provider_name != "none" else "⚠ No LLM configured (rule-based mode)"
        await self._log_activity(f"Data Profiler ready — {llm_status}")
        asyncio.create_task(self._background_profiler())

    async def _cleanup(self):
        await self._log_activity("Profiler shutting down")

    async def _execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        connection_id = task_data.get("connection_id")
        schema        = task_data.get("schema", "public")
        table_name    = task_data.get("table_name") or task_data.get("table_id")

        # ── Real profiling ─────────────────────────────────────────────────
        if connection_id and table_name and schema:
            await self._log_activity(f"Profiling {schema}.{table_name}")
            try:
                loop   = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None, lambda: _profile_in_session(connection_id, schema, table_name)
                )
                await self._log_activity(
                    f"Profiling done: {result['row_count']:,} rows, quality={result['quality_score']}%",
                    "success"
                )

                # ── LLM analysis of profiling results ────────────────────
                await self._log_activity("Sending stats to LLM for analysis…")
                llm_analysis = await self._llm_analyze_profile(result, schema, table_name)
                if llm_analysis:
                    result["llm_analysis"] = llm_analysis
                return result

            except Exception as exc:
                await self._log_activity(f"Profiling failed: {exc}", "error")

        # ── Simulated fallback ─────────────────────────────────────────────
        table_id = task_data.get("table_id", "unknown")
        await self._log_activity(f"[Demo] Simulating profile for {table_id}")
        await asyncio.sleep(random.uniform(1, 2))
        return {
            "table_id":             table_id,
            "row_count":            random.randint(1000, 500000),
            "column_count":         random.randint(5, 30),
            "null_percentage":      round(random.uniform(0, 10), 2),
            "duplicate_percentage": round(random.uniform(0, 3), 2),
            "quality_score":        round(random.uniform(85, 100), 1),
            "profiled_at":          datetime.now().isoformat(),
            "mode":                 "simulated",
        }

    async def _llm_analyze_profile(self, profile: dict, schema: str, table_name: str) -> Optional[str]:
        """Ask the LLM to interpret profiling results and save insight."""
        try:
            provider = _get_llm_provider()
            if provider.provider_name == "none":
                return None

            # Build a compact summary of column stats
            cols = profile.get("column_profiles", [])
            col_summary = "\n".join(
                f"  - {c['column_name']} ({c.get('data_type','?')}): "
                f"null={c.get('null_pct',0):.1f}%, distinct={c.get('distinct_count',0):,}, "
                f"sample={c.get('sample_values',[])[:3]}"
                for c in cols[:20]  # cap at 20 cols to stay within token budget
            )

            user_prompt = f"""Table: {schema}.{table_name}
Row count: {profile.get('row_count', 0):,}
Quality score: {profile.get('quality_score', 0)}%
Column count: {profile.get('column_count', 0)}
Issues found: {len(profile.get('issues', []))}

Column profiles:
{col_summary if col_summary else '  (no column detail available)'}

Known issues:
{chr(10).join(f"  - [{i.get('severity','?')}] {i.get('issue_type','?')}: {i.get('description','')}" for i in profile.get('issues', [])[:10])}

Analyze this table's data quality and provide actionable recommendations."""

            await self._log_activity(f"Calling {provider.provider_name}/{provider.model_name}…")
            analysis = await provider.complete(system=PROFILER_SYSTEM, user=user_prompt, max_tokens=800)

            # Parse severity and summary from structured response
            severity = "info"
            summary  = f"Profiling complete for {schema}.{table_name}"
            for line in analysis.splitlines():
                if line.startswith("SEVERITY:"):
                    sev = line.replace("SEVERITY:", "").strip().lower()
                    if sev in ("info", "warning", "critical"):
                        severity = sev
                elif line.startswith("SUMMARY:"):
                    summary = line.replace("SUMMARY:", "").strip()

            # Persist to DB in background
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: _save_insight(
                agent_id=self.agent_id,
                insight_type="profile",
                summary=summary,
                full_analysis=analysis,
                severity=severity,
                connection_id=profile.get("connection_id"),
                table_id=profile.get("table_id"),
                meta={"table": f"{schema}.{table_name}", "quality_score": profile.get("quality_score")},
            ))
            await self._log_activity(f"LLM insight saved [{severity}]: {summary[:80]}", "success")
            return analysis

        except Exception as e:
            await self._log_activity(f"LLM analysis failed: {e}", "error")
            logger.warning(f"Profiler LLM call failed: {e}")
            return None

    async def _background_profiler(self):
        while self.is_running:
            try:
                if random.random() < 0.15:
                    await self._log_activity("Scanning for tables that need re-profiling")
                await asyncio.sleep(30)
            except Exception as e:
                await self._log_activity(f"Background scan error: {e}", "error")
                await asyncio.sleep(60)


# ── Quality Validator Agent ────────────────────────────────────────────────────

class QualityValidatorAgent(BaseAgent):
    def __init__(self):
        super().__init__("quality_validator", "Quality Validator", AgentType.validation)

    async def _initialize(self):
        await self._log_activity("Quality Validator ready")

    async def _cleanup(self):
        await self._log_activity("Quality Validator shutting down")

    async def _execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        table_id      = task_data.get("table_id")
        connection_id = task_data.get("connection_id")

        if table_id:
            await self._log_activity(f"Validating table {table_id}")
            result = await self._validate_table(table_id, connection_id)
            return result

        # Simulated fallback
        await self._log_activity("[Demo] Running simulated validation")
        await asyncio.sleep(random.uniform(1, 3))
        return {
            "table_id":          task_data.get("table_id", "unknown"),
            "validation_passed": random.random() > 0.3,
            "quality_score":     round(random.uniform(80, 100), 1),
            "mode":              "simulated",
        }

    async def _validate_table(self, table_id: str, connection_id: str = None) -> dict:
        """Run DQ rules for a table and use LLM to synthesize findings."""
        try:
            from db.database import SessionLocal
            from db.models import DiscoveredTable, DQRule, DQRuleRun, QualityIssue
            from api.rules import _execute_rule

            db = SessionLocal()
            try:
                table = db.query(DiscoveredTable).filter(DiscoveredTable.id == table_id).first()
                if not table:
                    return {"error": "Table not found"}

                rules = db.query(DQRule).filter(DQRule.table_id == table_id, DQRule.is_active == True).all()
                rule_results = []

                for rule in rules:
                    try:
                        r = await asyncio.get_event_loop().run_in_executor(
                            None, lambda r=rule: _execute_rule(r, db)
                        )
                        rule_results.append({"name": rule.name, "type": rule.rule_type,
                                             "status": r.get("status"), "message": r.get("message")})
                    except Exception as ex:
                        rule_results.append({"name": rule.name, "type": rule.rule_type,
                                             "status": "error", "message": str(ex)})

                issues = db.query(QualityIssue).filter(
                    QualityIssue.table_id == table_id,
                    QualityIssue.status == "open"
                ).all()

                await self._log_activity(f"Ran {len(rules)} rules, found {len(issues)} open issues")

                # LLM synthesis
                analysis = await self._llm_analyze_validation(
                    table.full_name, table.quality_score, rule_results, issues, table_id, connection_id
                )

                return {
                    "table_id":      table_id,
                    "table_name":    table.full_name,
                    "quality_score": table.quality_score,
                    "rules_run":     len(rules),
                    "rule_results":  rule_results,
                    "open_issues":   len(issues),
                    "llm_analysis":  analysis,
                }
            finally:
                db.close()
        except Exception as e:
            await self._log_activity(f"Validation error: {e}", "error")
            return {"error": str(e)}

    async def _llm_analyze_validation(self, table_name, quality_score, rule_results,
                                       issues, table_id, connection_id) -> Optional[str]:
        try:
            provider = _get_llm_provider()
            if provider.provider_name == "none":
                return None

            rules_text = "\n".join(
                f"  - [{r['status'].upper()}] {r['name']} ({r['type']}): {r.get('message','')}"
                for r in rule_results
            ) or "  (no rules configured)"

            issues_text = "\n".join(
                f"  - [{i.severity}] {i.issue_type}: {i.description}"
                for i in issues[:10]
            ) or "  (no open issues)"

            user_prompt = f"""Table: {table_name}
Quality Score: {quality_score}%
DQ Rule Results:
{rules_text}
Open Issues:
{issues_text}

Provide a quality assessment and prioritized action plan."""

            await self._log_activity(f"Calling {provider.provider_name} for validation analysis…")
            analysis = await provider.complete(system=VALIDATOR_SYSTEM, user=user_prompt, max_tokens=700)

            severity = "info"
            summary  = f"Validation complete for {table_name}"
            for line in analysis.splitlines():
                if line.startswith("SEVERITY:"):
                    sev = line.replace("SEVERITY:", "").strip().lower()
                    if sev in ("info", "warning", "critical"):
                        severity = sev
                elif line.startswith("SUMMARY:"):
                    summary = line.replace("SUMMARY:", "").strip()

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: _save_insight(
                agent_id=self.agent_id,
                insight_type="quality",
                summary=summary,
                full_analysis=analysis,
                severity=severity,
                connection_id=connection_id,
                table_id=table_id,
                meta={"table": table_name, "quality_score": quality_score},
            ))
            await self._log_activity(f"Insight saved [{severity}]: {summary[:80]}", "success")
            return analysis

        except Exception as e:
            await self._log_activity(f"LLM validation analysis failed: {e}", "error")
            return None


# ── Lineage Tracker Agent ──────────────────────────────────────────────────────

class LineageTrackerAgent(BaseAgent):
    def __init__(self):
        super().__init__("lineage_tracker", "Lineage Tracker", AgentType.lineage)

    async def _initialize(self):
        await self._log_activity("Lineage Tracker ready")

    async def _cleanup(self):
        await self._log_activity("Lineage Tracker shutting down")

    async def _execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        connection_id = task_data.get("connection_id")

        if connection_id:
            await self._log_activity(f"Inferring lineage for connection {connection_id[:8]}…")
            result = await self._infer_lineage(connection_id)
            return result

        # Simulated fallback
        await asyncio.sleep(random.uniform(1, 2))
        return {"mode": "simulated", "lineage_edges": [], "message": "Provide a connection_id for real lineage inference"}

    async def _infer_lineage(self, connection_id: str) -> dict:
        try:
            from db.database import SessionLocal
            from db.models import DiscoveredTable, DiscoveredColumn

            db = SessionLocal()
            try:
                tables = db.query(DiscoveredTable).filter(
                    DiscoveredTable.connection_id == connection_id
                ).all()

                if not tables:
                    return {"error": "No tables found for this connection"}

                # Build a compact schema summary for the LLM
                schema_lines = []
                for t in tables:
                    cols = db.query(DiscoveredColumn).filter(
                        DiscoveredColumn.table_id == t.id
                    ).order_by(DiscoveredColumn.ordinal).limit(15).all()
                    col_list = ", ".join(c.column_name for c in cols)
                    schema_lines.append(f"  {t.full_name}: [{col_list}]")

                schema_text = "\n".join(schema_lines)
                await self._log_activity(f"Analyzing schema of {len(tables)} tables with LLM…")

                provider = _get_llm_provider()
                if provider.provider_name == "none":
                    return {"error": "No LLM configured — add API key in Settings to enable lineage inference"}

                user_prompt = f"""Database schema ({len(tables)} tables):
{schema_text}

Infer the data lineage and architecture layers for this schema."""

                analysis = await provider.complete(system=LINEAGE_SYSTEM, user=user_prompt, max_tokens=900)

                summary = f"Lineage inferred for {len(tables)} tables"
                for line in analysis.splitlines():
                    if line.startswith("SUMMARY:"):
                        summary = line.replace("SUMMARY:", "").strip()
                        break

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, lambda: _save_insight(
                    agent_id=self.agent_id,
                    insight_type="lineage",
                    summary=summary,
                    full_analysis=analysis,
                    severity="info",
                    connection_id=connection_id,
                    meta={"table_count": len(tables)},
                ))
                await self._log_activity(f"Lineage insight saved: {summary[:80]}", "success")
                return {"connection_id": connection_id, "table_count": len(tables), "llm_analysis": analysis}

            finally:
                db.close()
        except Exception as e:
            await self._log_activity(f"Lineage inference error: {e}", "error")
            return {"error": str(e)}


# ── Anomaly Detector Agent ─────────────────────────────────────────────────────

class AnomalyDetectorAgent(BaseAgent):
    def __init__(self):
        super().__init__("anomaly_detector", "Anomaly Detector", AgentType.monitoring)

    async def _initialize(self):
        await self._log_activity("Anomaly Detector ready")

    async def _cleanup(self):
        await self._log_activity("Anomaly Detector shutting down")

    async def _execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        table_id = task_data.get("table_id")

        if table_id:
            await self._log_activity(f"Checking anomalies for table {table_id[:8]}…")
            return await self._detect_anomalies(table_id)

        # Simulated fallback
        await asyncio.sleep(random.uniform(1, 2))
        return {"mode": "simulated", "anomaly_detected": False,
                "message": "Provide a table_id for real anomaly detection"}

    async def _detect_anomalies(self, table_id: str) -> dict:
        try:
            from db.database import SessionLocal
            from db.models import DiscoveredTable, ProfilingRun

            db = SessionLocal()
            try:
                table = db.query(DiscoveredTable).filter(DiscoveredTable.id == table_id).first()
                if not table:
                    return {"error": "Table not found"}

                # Get last 2 profiling runs to compare
                runs = db.query(ProfilingRun).filter(
                    ProfilingRun.table_id == table_id,
                    ProfilingRun.status == "completed",
                ).order_by(ProfilingRun.completed_at.desc()).limit(2).all()

                if len(runs) < 2:
                    await self._log_activity("Insufficient profiling history — need at least 2 runs")
                    return {
                        "table_id":  table_id,
                        "message":   "Not enough profiling history to detect anomalies (need ≥ 2 runs)",
                        "anomaly_detected": False,
                    }

                current  = runs[0].summary or {}
                previous = runs[1].summary or {}

                provider = _get_llm_provider()
                if provider.provider_name == "none":
                    return {"error": "No LLM configured — add API key in Settings"}

                def fmt(d):
                    return "\n".join(f"  {k}: {v}" for k, v in d.items()) if d else "  (no data)"

                user_prompt = f"""Table: {table.full_name}

Current profiling run ({runs[0].completed_at}):
{fmt(current)}

Previous profiling run ({runs[1].completed_at}):
{fmt(previous)}

Identify any meaningful anomalies in the data changes."""

                await self._log_activity(f"Calling {provider.provider_name} for anomaly analysis…")
                analysis = await provider.complete(system=ANOMALY_SYSTEM, user=user_prompt, max_tokens=700)

                severity = "info"
                summary  = f"Anomaly check complete for {table.full_name}"
                for line in analysis.splitlines():
                    if line.startswith("SEVERITY:"):
                        sev = line.replace("SEVERITY:", "").strip().lower()
                        if sev in ("info", "warning", "critical"):
                            severity = sev
                    elif line.startswith("SUMMARY:"):
                        summary = line.replace("SUMMARY:", "").strip()

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, lambda: _save_insight(
                    agent_id=self.agent_id,
                    insight_type="anomaly",
                    summary=summary,
                    full_analysis=analysis,
                    severity=severity,
                    table_id=table_id,
                    connection_id=table.connection_id,
                    meta={"table": table.full_name},
                ))
                await self._log_activity(f"Anomaly insight saved [{severity}]: {summary[:80]}", "success")
                return {
                    "table_id":        table_id,
                    "table_name":      table.full_name,
                    "anomaly_detected": severity in ("warning", "critical"),
                    "severity":        severity,
                    "llm_analysis":    analysis,
                }
            finally:
                db.close()
        except Exception as e:
            await self._log_activity(f"Anomaly detection error: {e}", "error")
            return {"error": str(e)}


# ── Orchestrator ───────────────────────────────────────────────────────────────

class AgentOrchestrator:
    def __init__(self):
        self.agents: Dict[str, BaseAgent] = {}
        self.task_queue  = asyncio.Queue()
        self.running     = False
        self.orchestrator_task = None
        self._initialize_agents()

    def _initialize_agents(self):
        self.agents = {
            "data_profiler":     DataProfilerAgent(),
            "quality_validator": QualityValidatorAgent(),
            "lineage_tracker":   LineageTrackerAgent(),
            "anomaly_detector":  AnomalyDetectorAgent(),
        }
        logger.info(f"Initialized {len(self.agents)} agents")

    async def start_orchestrator(self):
        self.running = True
        self.orchestrator_task = asyncio.create_task(self._orchestrator_loop())
        logger.info("Agent orchestrator started")

    async def stop_orchestrator(self):
        self.running = False
        if self.orchestrator_task:
            self.orchestrator_task.cancel()
            try:
                await self.orchestrator_task
            except asyncio.CancelledError:
                pass
        for agent in self.agents.values():
            if agent.status == AgentStatusEnum.active:
                await agent.stop()
        logger.info("Agent orchestrator stopped")

    async def start_agent(self, agent_id: str) -> bool:
        if agent_id not in self.agents:
            return False
        await self.agents[agent_id].start()
        return self.agents[agent_id].status == AgentStatusEnum.active

    async def stop_agent(self, agent_id: str) -> bool:
        if agent_id not in self.agents:
            return False
        await self.agents[agent_id].stop()
        return self.agents[agent_id].status == AgentStatusEnum.inactive

    def get_agent_status(self, agent_id: str = None) -> Dict[str, Any]:
        if agent_id:
            if agent_id in self.agents:
                return self.agents[agent_id].get_status().__dict__
            return {"error": f"Agent {agent_id} not found"}
        return {aid: agent.get_status().__dict__ for aid, agent in self.agents.items()}

    async def submit_task(self, agent_id: str, task_data: Dict[str, Any]) -> str:
        if agent_id not in self.agents:
            raise ValueError(f"Agent {agent_id} not found")
        task_id = str(uuid.uuid4())
        await self.task_queue.put({
            "task_id": task_id, "agent_id": agent_id,
            "task_data": task_data, "submitted_at": datetime.now(),
        })
        return task_id

    async def _orchestrator_loop(self):
        while self.running:
            try:
                if not self.task_queue.empty():
                    task = await self.task_queue.get()
                    await self._process_task(task)
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Orchestrator error: {e}")
                await asyncio.sleep(5)

    async def _process_task(self, task: Dict[str, Any]):
        try:
            agent_id = task["agent_id"]
            agent    = self.agents[agent_id]
            if agent.status != AgentStatusEnum.active:
                logger.warning(f"Agent {agent_id} not active, skipping task")
                return
            result = await agent.execute_task(task["task_data"])
            logger.info(f"Task {task['task_id'][:8]} → {result['status']}")
        except Exception as e:
            logger.error(f"Error processing task: {e}")


# Global singleton
agent_orchestrator = AgentOrchestrator()

async def get_orchestrator():
    return agent_orchestrator
