"""
ORM models — persisted in SQLite (dev) / PostgreSQL (prod).
Designed for SaaS: every resource has a tenant_id column (NULL = single-tenant mode).
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    Column, String, Integer, Float, Boolean, Text, DateTime, JSON,
    ForeignKey, Index,
)
from sqlalchemy.orm import relationship

from .database import Base


def _uuid() -> str:
    return str(uuid.uuid4())


# ── User ─────────────────────────────────────────────────────────────────────

class User(Base):
    __tablename__ = "users"

    id           = Column(String(36),  primary_key=True, default=_uuid)
    email        = Column(String(255), nullable=False, unique=True, index=True)
    name         = Column(String(255), nullable=True)
    picture      = Column(String(512), nullable=True)   # Google profile picture URL
    google_id    = Column(String(128), nullable=True, unique=True, index=True)
    role         = Column(String(50),  default="viewer")  # viewer | editor | admin
    is_active    = Column(Boolean,     default=True)
    last_login   = Column(DateTime,    nullable=True)
    created_at   = Column(DateTime,    default=datetime.utcnow)
    updated_at   = Column(DateTime,    default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_user_email", "email"),
    )


# ── Connection ───────────────────────────────────────────────────────────────

class Connection(Base):
    __tablename__ = "connections"

    id               = Column(String(36),  primary_key=True, default=_uuid)
    tenant_id        = Column(String(36),  nullable=True,  index=True)   # future SaaS isolation
    name             = Column(String(255), nullable=False)
    connector_type   = Column(String(50),  nullable=False)               # e.g. "postgresql", "fabric"
    description      = Column(Text,        nullable=True)

    # Encrypted credential blob (Fernet-encrypted JSON)
    encrypted_config = Column(Text, nullable=False)

    # Runtime status
    status           = Column(String(20),  default="untested")           # untested | ok | error
    last_tested_at   = Column(DateTime,    nullable=True)
    last_test_error  = Column(Text,        nullable=True)
    latency_ms       = Column(Float,       nullable=True)
    server_version   = Column(String(255), nullable=True)

    created_at       = Column(DateTime,    default=datetime.utcnow)
    updated_at       = Column(DateTime,    default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    discovered_tables = relationship("DiscoveredTable", back_populates="connection", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_conn_tenant_name", "tenant_id", "name"),
    )


# ── Discovered Table ─────────────────────────────────────────────────────────

class DiscoveredTable(Base):
    __tablename__ = "discovered_tables"

    id              = Column(String(36),  primary_key=True, default=_uuid)
    connection_id   = Column(String(36),  ForeignKey("connections.id", ondelete="CASCADE"), nullable=False, index=True)
    schema_name     = Column(String(255), nullable=False)
    table_name      = Column(String(255), nullable=False)
    full_name       = Column(String(512), nullable=False)                # schema.table
    row_count       = Column(Integer,     default=0)
    column_count    = Column(Integer,     default=0)
    size_bytes      = Column(Integer,     nullable=True)

    # Quality
    quality_score   = Column(Float,       default=100.0)
    last_profiled   = Column(DateTime,    nullable=True)

    # Catalog metadata (set by users / agents)
    description     = Column(Text,        nullable=True)
    owner           = Column(String(255), nullable=True)
    tags            = Column(JSON,        default=list)                  # ["PII", "Financial"]
    domain          = Column(String(255), nullable=True)

    discovered_at   = Column(DateTime,    default=datetime.utcnow)
    updated_at      = Column(DateTime,    default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    connection      = relationship("Connection",        back_populates="discovered_tables")
    columns         = relationship("DiscoveredColumn",  back_populates="table", cascade="all, delete-orphan")
    quality_issues  = relationship("QualityIssue",      back_populates="table", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_dt_conn_schema_table", "connection_id", "schema_name", "table_name"),
    )


# ── Discovered Column ────────────────────────────────────────────────────────

class DiscoveredColumn(Base):
    __tablename__ = "discovered_columns"

    id              = Column(String(36),  primary_key=True, default=_uuid)
    table_id        = Column(String(36),  ForeignKey("discovered_tables.id", ondelete="CASCADE"), nullable=False, index=True)
    column_name     = Column(String(255), nullable=False)
    data_type       = Column(String(100), nullable=False)
    nullable        = Column(Boolean,     default=True)
    primary_key     = Column(Boolean,     default=False)
    column_default  = Column(String(500), nullable=True)
    ordinal         = Column(Integer,     default=0)

    # Profiling stats
    null_count      = Column(Integer,     default=0)
    null_pct        = Column(Float,       default=0.0)
    distinct_count  = Column(Integer,     default=0)
    min_value       = Column(String(500), nullable=True)
    max_value       = Column(String(500), nullable=True)
    avg_value       = Column(String(100), nullable=True)
    sample_values   = Column(JSON,        default=list)
    quality_score   = Column(Float,       default=100.0)

    # Catalog metadata
    description     = Column(Text,        nullable=True)
    is_pii          = Column(Boolean,     default=False)
    pii_type        = Column(String(100), nullable=True)                 # email, ssn, phone, etc.
    classification  = Column(String(50),  nullable=True)                 # public, internal, sensitive, highly_sensitive

    profiled_at     = Column(DateTime,    nullable=True)

    # Relationships
    table           = relationship("DiscoveredTable", back_populates="columns")

    __table_args__ = (
        Index("ix_dc_table_col", "table_id", "column_name"),
    )


# ── Quality Issue ─────────────────────────────────────────────────────────────

class QualityIssue(Base):
    __tablename__ = "quality_issues"

    id              = Column(String(36),  primary_key=True, default=_uuid)
    table_id        = Column(String(36),  ForeignKey("discovered_tables.id", ondelete="CASCADE"), nullable=False, index=True)
    column_name     = Column(String(255), nullable=True)                 # null = table-level issue
    issue_type      = Column(String(100), nullable=False)                # High Null Rate, Duplicate Records, etc.
    severity        = Column(String(20),  default="medium")             # low | medium | high | critical
    description     = Column(Text,        nullable=False)
    record_count    = Column(Integer,     default=0)
    status          = Column(String(20),  default="open")               # open | acknowledged | resolved | ignored
    detected_at     = Column(DateTime,    default=datetime.utcnow)
    resolved_at     = Column(DateTime,    nullable=True)
    assigned_to     = Column(String(255), nullable=True)
    notes           = Column(Text,        nullable=True)

    # Relationships
    table           = relationship("DiscoveredTable", back_populates="quality_issues")

    __table_args__ = (
        Index("ix_qi_table_status", "table_id", "status"),
    )


# ── Profiling Run ─────────────────────────────────────────────────────────────

class ProfilingRun(Base):
    __tablename__ = "profiling_runs"

    id              = Column(String(36),  primary_key=True, default=_uuid)
    connection_id   = Column(String(36),  ForeignKey("connections.id", ondelete="CASCADE"), nullable=False, index=True)
    table_id        = Column(String(36),  ForeignKey("discovered_tables.id", ondelete="SET NULL"), nullable=True)
    triggered_by    = Column(String(50),  default="agent")              # agent | user | schedule
    status          = Column(String(20),  default="running")            # running | completed | failed
    tables_scanned  = Column(Integer,     default=0)
    issues_found    = Column(Integer,     default=0)
    quality_score   = Column(Float,       nullable=True)
    started_at      = Column(DateTime,    default=datetime.utcnow)
    completed_at    = Column(DateTime,    nullable=True)
    error           = Column(Text,        nullable=True)
    summary         = Column(JSON,        default=dict)


# ── DQ Rule ───────────────────────────────────────────────────────────────────

class DQRule(Base):
    __tablename__ = "dq_rules"

    id               = Column(String(36),  primary_key=True, default=_uuid)
    table_id         = Column(String(36),  ForeignKey("discovered_tables.id", ondelete="CASCADE"), nullable=False, index=True)
    name             = Column(String(255), nullable=False)
    rule_type        = Column(String(50),  nullable=False)   # not_null | unique | min_value | max_value | regex | freshness | row_count | custom_sql
    column_name      = Column(String(255), nullable=True)
    parameters       = Column(JSON,        default=dict)
    severity         = Column(String(20),  default="medium")
    description      = Column(Text,        nullable=True)
    is_active        = Column(Boolean,     default=True)
    last_run_at      = Column(DateTime,    nullable=True)
    last_run_status  = Column(String(20),  nullable=True)    # pass | fail | error
    created_at       = Column(DateTime,    default=datetime.utcnow)
    updated_at       = Column(DateTime,    default=datetime.utcnow, onupdate=datetime.utcnow)

    runs             = relationship("DQRuleRun", back_populates="rule", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_dqrule_table", "table_id"),
    )


# ── LLM Config ────────────────────────────────────────────────────────────────

class LLMConfig(Base):
    __tablename__ = "llm_config"

    id              = Column(String(36),  primary_key=True, default=_uuid)
    provider        = Column(String(50),  nullable=False)   # "anthropic" | "openai"
    model           = Column(String(100), nullable=False)   # e.g. "claude-sonnet-4-6"
    encrypted_api_key = Column(Text,      nullable=False)
    is_active       = Column(Boolean,     default=True)
    created_at      = Column(DateTime,    default=datetime.utcnow)
    updated_at      = Column(DateTime,    default=datetime.utcnow, onupdate=datetime.utcnow)


# ── Agent Insight ─────────────────────────────────────────────────────────────

class AgentInsight(Base):
    __tablename__ = "agent_insights"

    id              = Column(String(36),  primary_key=True, default=_uuid)
    agent_id        = Column(String(50),  nullable=False, index=True)   # "data_profiler" etc.
    connection_id   = Column(String(36),  ForeignKey("connections.id", ondelete="CASCADE"), nullable=True)
    table_id        = Column(String(36),  ForeignKey("discovered_tables.id", ondelete="CASCADE"), nullable=True)
    insight_type    = Column(String(50),  nullable=False)   # "profile","quality","lineage","anomaly"
    summary         = Column(Text,        nullable=False)   # 1-2 sentence headline
    full_analysis   = Column(Text,        nullable=False)   # full LLM response
    severity        = Column(String(20),  default="info")   # info | warning | critical
    insight_meta    = Column(JSON,        default=dict)
    created_at      = Column(DateTime,    default=datetime.utcnow)

    __table_args__ = (
        Index("ix_insight_agent", "agent_id"),
        Index("ix_insight_table", "table_id"),
    )


# ── Agent Schedule ────────────────────────────────────────────────────────────

class AgentSchedule(Base):
    __tablename__ = "agent_schedules"

    id              = Column(String(36),  primary_key=True, default=_uuid)
    agent_id        = Column(String(50),  nullable=False, unique=True)
    cron_expression = Column(String(100), nullable=True)    # null = not scheduled
    is_active       = Column(Boolean,     default=False)
    last_run_at     = Column(DateTime,    nullable=True)
    next_run_at     = Column(DateTime,    nullable=True)
    created_at      = Column(DateTime,    default=datetime.utcnow)
    updated_at      = Column(DateTime,    default=datetime.utcnow, onupdate=datetime.utcnow)


# ── DQ Rule Run ───────────────────────────────────────────────────────────────

class DQRuleRun(Base):
    __tablename__ = "dq_rule_runs"

    id           = Column(String(36),  primary_key=True, default=_uuid)
    rule_id      = Column(String(36),  ForeignKey("dq_rules.id", ondelete="CASCADE"), nullable=False, index=True)
    status       = Column(String(20),  nullable=False)    # pass | fail | error
    failing_rows = Column(Integer,     default=0)
    total_rows   = Column(Integer,     default=0)
    message      = Column(Text,        nullable=True)
    ran_at       = Column(DateTime,    default=datetime.utcnow)

    rule         = relationship("DQRule", back_populates="runs")


# ── Scheduled Scan ────────────────────────────────────────────────────────────

class ScheduledScan(Base):
    __tablename__ = "scheduled_scans"

    id              = Column(String(36),  primary_key=True, default=_uuid)
    name            = Column(String(255), nullable=False)
    connection_id   = Column(String(36),  ForeignKey("connections.id", ondelete="CASCADE"), nullable=True, index=True)
    schedule_cron   = Column(String(100), nullable=False)   # e.g. "0 6 * * *"
    is_active       = Column(Boolean,     default=True)
    last_run_at     = Column(DateTime,    nullable=True)
    last_run_status = Column(String(20),  nullable=True)    # running | completed | failed
    next_run_at     = Column(DateTime,    nullable=True)
    created_at      = Column(DateTime,    default=datetime.utcnow)
    updated_at      = Column(DateTime,    default=datetime.utcnow, onupdate=datetime.utcnow)
