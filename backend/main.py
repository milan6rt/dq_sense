"""
DataIQ Platform — FastAPI Backend
Multi-agent data quality + universal data warehouse connectivity.

Start with:
  uvicorn main:app --reload --port 8000
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from dotenv import load_dotenv
load_dotenv()  # loads backend/.env if present

from agents import AgentOrchestrator, AgentStatusEnum
from db.database import init_db, get_db
from api.connections import router as connections_router
from api.auth import router as auth_router
from api.rules import router as rules_router
from api.scheduler import router as scheduler_router, restore_scheduled_jobs

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# ── Global state ─────────────────────────────────────────────────────────────
orchestrator = AgentOrchestrator()


class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
        logger.info(f"WebSocket connected. Total: {len(self.active)}")

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)
        logger.info(f"WebSocket disconnected. Total: {len(self.active)}")

    async def broadcast(self, message: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()
tasks: Dict[str, dict] = {}


# ── Broadcast hook ────────────────────────────────────────────────────────────

async def _broadcast_log(agent_name: str, message: str, level: str = "info", details: dict = None):
    await manager.broadcast({
        "type":      "agent_log",
        "agent":     agent_name,
        "message":   message,
        "level":     level,
        "timestamp": datetime.now().isoformat(),
        "details":   details,
    })


def patch_agents_broadcast():
    from agents import BaseAgent
    original = BaseAgent._log_activity

    async def patched(self, message, level="info", details=None):
        await original(self, message, level, details)
        asyncio.create_task(_broadcast_log(self.name, message, level, details))

    BaseAgent._log_activity = patched


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialise database (creates tables if needed)
    init_db()
    logger.info("Database initialised")

    # Patch agents + start orchestrator
    patch_agents_broadcast()
    await orchestrator.start_orchestrator()
    for agent_id in orchestrator.agents:
        await orchestrator.start_agent(agent_id)
    logger.info("All agents started")

    # Restore scheduled scan jobs from DB
    restore_scheduled_jobs()
    logger.info("Scheduled jobs restored")

    yield

    await orchestrator.stop_orchestrator()
    logger.info("Orchestrator stopped")


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="DataIQ Platform API",
    version="2.0.0",
    description="Multi-agent data quality + universal warehouse connectivity",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routers
app.include_router(connections_router)
app.include_router(auth_router)
app.include_router(rules_router)
app.include_router(scheduler_router)


# ── Pydantic models ───────────────────────────────────────────────────────────

class TaskCreate(BaseModel):
    agent_id:    str
    title:       str
    description: Optional[str] = ""
    table_id:    Optional[str] = None
    table_name:  Optional[str] = None
    priority:    Optional[str] = "medium"
    task_data:   Optional[Dict[str, Any]] = {}


class TaskUpdate(BaseModel):
    status:   Optional[str] = None
    priority: Optional[str] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def agent_to_dict(agent_id: str) -> dict:
    agent = orchestrator.agents.get(agent_id)
    if not agent:
        return {}
    s = agent.get_status()
    return {
        "id":               s.id,
        "name":             s.name,
        "type":             s.type,
        "status":           s.status,
        "last_run":         s.last_run.isoformat() if s.last_run else None,
        "tasks_completed":  s.tasks_completed,
        "tasks_failed":     s.tasks_failed,
        "uptime":           s.uptime,
        "error_message":    s.error_message,
        "recent_activity":  [
            {
                "id":        a.id,
                "activity":  a.activity,
                "status":    a.status,
                "timestamp": a.timestamp.isoformat(),
                "details":   a.details,
            }
            for a in agent.activity_log[-20:]
        ],
    }


# ── Core REST endpoints ───────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status":    "ok",
        "version":   "2.0.0",
        "timestamp": datetime.now().isoformat(),
        "features":  ["plug-and-play-connectors", "real-profiling", "schema-discovery"],
    }


@app.get("/discover-medallion")
def discover_medallion():
    """Discover bronze, silver, gold, and demo schemas so tables appear in the app catalog."""
    from db.database import get_db as _get_db
    from services.connection_service import list_connections, discover_tables as _discover

    db = next(_get_db())
    conns = list_connections(db)
    pg = next((c for c in conns if c.connector_type == "postgresql"), None)
    if not pg:
        return {"error": "No PostgreSQL connection found."}

    results = {}
    for schema in ["bronze", "silver", "gold", "demo"]:
        try:
            tables = _discover(db, pg.id, schema)
            results[schema] = {"tables": len(tables), "names": [t["table_name"] for t in tables]}
        except Exception as e:
            results[schema] = {"error": str(e)}

    return {"success": True, "discovered": results}


@app.get("/profile-medallion")
def profile_medallion():
    """Profile all discovered bronze/silver/gold/demo tables to populate quality scores and row counts."""
    from db.database import get_db as _get_db
    from services.connection_service import list_connections, profile_table as _profile, get_all_tables

    db = next(_get_db())
    conns = list_connections(db)
    pg = next((c for c in conns if c.connector_type == "postgresql"), None)
    if not pg:
        return {"error": "No PostgreSQL connection found."}

    tables = get_all_tables(db, pg.id)
    results = {}
    for t in tables:
        schema = t["schema_name"]
        table  = t["table_name"]
        if schema not in ("bronze", "silver", "gold", "demo"):
            continue
        key = f"{schema}.{table}"
        try:
            r = _profile(db, pg.id, schema, table)
            results[key] = {"rows": r.get("row_count", 0), "quality": r.get("quality_score", 0), "issues": r.get("issue_count", 0)}
        except Exception as e:
            results[key] = {"error": str(e)}

    return {"success": True, "profiled": len(results), "tables": results}


@app.get("/api/catalog/tables")
def catalog_tables():
    """Return all discovered tables enriched with medallion metadata for the frontend catalog."""
    from db.database import get_db as _get_db
    from db.models import DiscoveredTable, Connection

    db = next(_get_db())

    # Per-table metadata: descriptions, owners, stewards, tags, lineage
    _meta = {
        # Bronze
        "bronze.src_crm_contacts":    {"description": "Raw CRM contacts ingested from Salesforce. Contains duplicates, format inconsistencies, and missing fields typical of source system ingestion.", "owner": "Data Engineering", "steward": "Yuki Tanaka", "tags": ["Raw", "CRM", "Salesforce", "Bronze"], "domain": "Engineering"},
        "bronze.src_erp_customers":   {"description": "Raw customer master from NetSuite ERP. Often overlaps with CRM contacts — entity resolution needed.", "owner": "Data Engineering", "steward": "Yuki Tanaka", "tags": ["Raw", "ERP", "NetSuite", "Bronze"], "domain": "Engineering"},
        "bronze.src_erp_orders":      {"description": "Raw order transactions from NetSuite. Includes bad date formats, orphaned customer refs, and JSON line items.", "owner": "Data Engineering", "steward": "Yuki Tanaka", "tags": ["Raw", "Orders", "NetSuite", "Bronze"], "domain": "Engineering"},
        "bronze.src_product_catalog": {"description": "Raw product catalogue from PIM system. Contains inconsistent category casing and negative margin entries.", "owner": "Data Engineering", "steward": "Priya Patel", "tags": ["Raw", "Products", "PIM", "Bronze"], "domain": "Engineering"},
        "bronze.src_mktg_leads":      {"description": "Raw marketing leads from HubSpot. Lifecycle stage inconsistencies and duplicate emails common.", "owner": "Marketing Ops", "steward": "Amara Diallo", "tags": ["Raw", "Leads", "HubSpot", "Marketing", "Bronze"], "domain": "Marketing"},
        "bronze.src_hr_employees":    {"description": "Raw employee records from Workday HR system. Contains missing emails, inconsistent department names, and null hire dates.", "owner": "People Analytics", "steward": "Sandra Mills", "tags": ["Raw", "HR", "Workday", "PII", "Sensitive", "Bronze"], "domain": "HR"},
        # Silver
        "silver.customers":           {"description": "Unified customer master — 21 golden records merged and deduplicated from 40 raw inputs across Salesforce and NetSuite. DataIQ quality score applied per record.", "owner": "Data Platform", "steward": "Priya Patel", "tags": ["Golden Record", "Unified", "Silver", "MDM"], "domain": "Data Engineering"},
        "silver.orders":              {"description": "Cleansed order transactions. Stale statuses corrected, date formats standardised, orphaned refs resolved.", "owner": "Data Platform", "steward": "Yuki Tanaka", "tags": ["Orders", "Cleansed", "Silver"], "domain": "Data Engineering"},
        "silver.order_items":         {"description": "Clean order line items with computed line_total (GENERATED column). Referential integrity enforced.", "owner": "Data Platform", "steward": "Yuki Tanaka", "tags": ["Orders", "Line Items", "Silver"], "domain": "Data Engineering"},
        "silver.products":            {"description": "Standardised product catalogue — category casing normalised, negative margins corrected, inactive products flagged.", "owner": "Data Platform", "steward": "Priya Patel", "tags": ["Products", "Cleansed", "Silver"], "domain": "Data Engineering"},
        "silver.employees":           {"description": "Unified employee master from Workday. Duplicate records merged, department names standardised, salary anomalies flagged.", "owner": "People Analytics", "steward": "Sandra Mills", "tags": ["HR", "Employees", "Cleansed", "Silver", "PII"], "domain": "HR"},
        # Gold
        "gold.dim_customer":          {"description": "SCD Type 2 customer dimension. Tracks historical changes — is_current flag identifies active records. LTV tier: Platinum/Gold/Silver/Bronze.", "owner": "Analytics Engineering", "steward": "Richard Grant", "tags": ["Dimension", "SCD2", "Gold", "BI-Ready"], "domain": "Analytics"},
        "gold.dim_product":           {"description": "Clean product dimension for analytics. Margin-healthy, active products only.", "owner": "Analytics Engineering", "steward": "Priya Patel", "tags": ["Dimension", "Products", "Gold", "BI-Ready"], "domain": "Analytics"},
        "gold.dim_date":              {"description": "Date dimension covering all of 2024 — 366 days with day-of-week, quarter, month, and is_weekend flags.", "owner": "Analytics Engineering", "steward": "Richard Grant", "tags": ["Dimension", "Date", "Gold", "BI-Ready"], "domain": "Analytics"},
        "gold.fact_orders":           {"description": "Central fact table — 31 order facts joined to customer, product, and date dimensions. Source of record for revenue reporting.", "owner": "Analytics Engineering", "steward": "Richard Grant", "tags": ["Fact", "Orders", "Revenue", "Gold", "BI-Ready", "Reporting"], "domain": "Analytics"},
        "gold.agg_revenue_monthly":   {"description": "Pre-aggregated monthly revenue by currency. 24 months of Nexus Commerce revenue KPIs. Powers executive dashboards.", "owner": "Finance Analytics", "steward": "Lisa Chen", "tags": ["Aggregated", "Revenue", "Finance", "Gold", "BI-Ready"], "domain": "Finance"},
        "gold.agg_customer_ltv":      {"description": "Customer lifetime value aggregates with LTV tier segmentation (Platinum/Gold/Silver/Bronze) and churn risk score.", "owner": "Analytics Engineering", "steward": "Marcus Webb", "tags": ["LTV", "Customer", "Gold", "ML-Ready"], "domain": "Analytics"},
        "gold.scorecard_data_quality":{"description": "Data quality scorecard tracking completeness, uniqueness, and validity across Bronze→Silver→Gold layers. Used for governance reporting.", "owner": "Data Governance", "steward": "Patricia Osei", "tags": ["Governance", "Quality", "Scorecard", "Gold"], "domain": "Analytics"},
        # Demo
        "demo.customers":   {"description": "Demo customer table with intentional data quality issues — nulls, bad emails, duplicate records.", "owner": "DataIQ Demo", "steward": "Milan", "tags": ["Demo", "PII"], "domain": "Sales"},
        "demo.orders":      {"description": "Demo orders table with stale statuses, mismatched totals, and outlier amounts.", "owner": "DataIQ Demo", "steward": "Milan", "tags": ["Demo", "Orders"], "domain": "Sales"},
        "demo.order_items": {"description": "Demo order line items referencing demo orders and products.", "owner": "DataIQ Demo", "steward": "Milan", "tags": ["Demo"], "domain": "Sales"},
        "demo.products":    {"description": "Demo product catalog with negative margins and zero-stock items.", "owner": "DataIQ Demo", "steward": "Milan", "tags": ["Demo", "Products"], "domain": "Sales"},
        "demo.employees":   {"description": "Demo employees with intentional nulls and department inconsistencies.", "owner": "DataIQ Demo", "steward": "Milan", "tags": ["Demo", "HR", "PII", "Sensitive"], "domain": "HR"},
        "demo.events":      {"description": "Demo user events — page views, add-to-cart, purchases — with missing device types and bad country codes.", "owner": "DataIQ Demo", "steward": "Milan", "tags": ["Demo", "Events"], "domain": "Engineering"},
    }

    # Upstream/downstream lineage map (by full_name key)
    _lineage = {
        "bronze.src_crm_contacts":    {"upstream": [], "downstream": ["silver.customers"]},
        "bronze.src_erp_customers":   {"upstream": [], "downstream": ["silver.customers"]},
        "bronze.src_erp_orders":      {"upstream": [], "downstream": ["silver.orders", "silver.order_items"]},
        "bronze.src_product_catalog": {"upstream": [], "downstream": ["silver.products"]},
        "bronze.src_mktg_leads":      {"upstream": [], "downstream": []},
        "bronze.src_hr_employees":    {"upstream": [], "downstream": ["silver.employees"]},
        "silver.customers":           {"upstream": ["bronze.src_crm_contacts", "bronze.src_erp_customers"], "downstream": ["gold.dim_customer", "gold.fact_orders", "gold.agg_customer_ltv"]},
        "silver.orders":              {"upstream": ["bronze.src_erp_orders"], "downstream": ["gold.fact_orders", "gold.agg_revenue_monthly"]},
        "silver.order_items":         {"upstream": ["bronze.src_erp_orders"], "downstream": ["gold.fact_orders"]},
        "silver.products":            {"upstream": ["bronze.src_product_catalog"], "downstream": ["gold.dim_product", "gold.fact_orders"]},
        "silver.employees":           {"upstream": ["bronze.src_hr_employees"], "downstream": []},
        "gold.dim_customer":          {"upstream": ["silver.customers"], "downstream": ["gold.fact_orders"]},
        "gold.dim_product":           {"upstream": ["silver.products"], "downstream": ["gold.fact_orders"]},
        "gold.dim_date":              {"upstream": [], "downstream": ["gold.fact_orders"]},
        "gold.fact_orders":           {"upstream": ["silver.order_items", "silver.orders", "gold.dim_customer", "gold.dim_product", "gold.dim_date"], "downstream": ["gold.agg_revenue_monthly", "gold.agg_customer_ltv"]},
        "gold.agg_revenue_monthly":   {"upstream": ["gold.fact_orders", "silver.orders"], "downstream": []},
        "gold.agg_customer_ltv":      {"upstream": ["gold.fact_orders", "silver.customers"], "downstream": []},
        "gold.scorecard_data_quality":{"upstream": [], "downstream": []},
    }

    _trust_map = {"bronze": "bronze", "silver": "silver", "gold": "gold", "demo": "bronze"}
    _popularity = {"bronze": 3.2, "silver": 4.1, "gold": 4.7, "demo": 3.8}

    tables = (
        db.query(DiscoveredTable)
        .join(Connection)
        .order_by(DiscoveredTable.schema_name, DiscoveredTable.table_name)
        .all()
    )

    result = []
    for dt in tables:
        key = f"{dt.schema_name}.{dt.table_name}"
        meta = _meta.get(key, {})
        lin  = _lineage.get(key, {"upstream": [], "downstream": []})

        # Build lineage using full_name strings — frontend resolves by id
        # We'll use full_name as the id reference since mockTables used "t1" etc.
        upstream_ids   = lin["upstream"]
        downstream_ids = lin["downstream"]

        # Column detail from profiled columns
        cols = []
        for c in sorted(dt.columns, key=lambda x: x.ordinal):
            cols.append({
                "name":        c.column_name,
                "type":        c.data_type,
                "nullable":    c.nullable,
                "pii":         c.is_pii,
                "description": c.description or "",
                "quality":     round(c.quality_score) if c.quality_score else 100,
            })

        open_issues = len([i for i in dt.quality_issues if i.status == "open"])

        result.append({
            "id":           key,          # use schema.table as stable id
            "name":         dt.table_name,
            "schema":       dt.schema_name,
            "connection":   dt.connection.name,
            "connectionId": dt.connection_id,
            "domain":       meta.get("domain", dt.domain or "Engineering"),
            "records":      dt.row_count or 0,
            "quality":      round(dt.quality_score or 0),
            "lastProfiled": dt.last_profiled.isoformat() if dt.last_profiled else None,
            "description":  meta.get("description", dt.description or ""),
            "owner":        meta.get("owner", dt.owner or "Data Engineering"),
            "steward":      meta.get("steward", "—"),
            "tags":         dt.tags if dt.tags else meta.get("tags", []),
            "trust":        _trust_map.get(dt.schema_name, "bronze"),
            "popularity":   _popularity.get(dt.schema_name, 3.5),
            "columns":      dt.column_count or len(cols),
            "issues":       open_issues,
            "upstream":     upstream_ids,
            "downstream":   downstream_ids,
            "columns_detail": cols,
        })

    return result


@app.get("/api/catalog/issues")
def catalog_issues():
    """Return all open quality issues across all discovered tables."""
    from db.database import get_db as _get_db
    from db.models import QualityIssue, DiscoveredTable

    db = next(_get_db())
    issues = (
        db.query(QualityIssue)
        .filter(QualityIssue.status == "open")
        .join(DiscoveredTable)
        .order_by(QualityIssue.detected_at.desc())
        .all()
    )

    result = []
    for i in issues:
        key = f"{i.table.schema_name}.{i.table.table_name}"
        result.append({
            "id":          i.id,
            "table":       i.table.table_name,
            "tableId":     key,
            "type":        i.issue_type,
            "severity":    i.severity,
            "count":       i.record_count,
            "description": i.description,
            "detectedAt":  i.detected_at.isoformat() if i.detected_at else None,
        })

    return result


@app.get("/seed-medallion")
def seed_medallion():
    """
    Create a full Bronze → Silver → Gold medallion architecture in PostgreSQL.
    Scenario: 'Nexus Commerce' — a B2B/B2C company with 3 source systems (CRM, ERP, Marketing).
    Demonstrates the full DataIQ value story:
      Bronze  = raw, messy source data as ingested (quality score ~38%)
      Silver  = DataIQ-cleansed, deduplicated, standardised (quality score ~93%)
      Gold    = analytics-ready dimensions, facts, and KPI aggregates
    """
    from db.database import get_db as _get_db
    from services.connection_service import list_connections, _decrypt
    from connectors.registry import build_connector as _build
    from sqlalchemy import text as _text

    db = next(_get_db())
    conns = list_connections(db)
    pg = next((c for c in conns if c.connector_type == "postgresql"), None)
    if not pg:
        return {"error": "No PostgreSQL connection found."}
    config    = _decrypt(pg.encrypted_config)
    connector = _build(pg.connector_type, config)
    engine    = connector._engine()

    # ── DDL: CREATE ALL SCHEMAS & TABLES ──────────────────────────────────────
    ddl_blocks = [
        # Schemas
        "CREATE SCHEMA IF NOT EXISTS bronze",
        "CREATE SCHEMA IF NOT EXISTS silver",
        "CREATE SCHEMA IF NOT EXISTS gold",

        # ── BRONZE LAYER ──────────────────────────────────────────────────────
        # Raw ingest from Salesforce CRM
        """CREATE TABLE IF NOT EXISTS bronze.src_crm_contacts (
            _raw_id        SERIAL PRIMARY KEY,
            _source_system VARCHAR(50)  DEFAULT 'salesforce',
            _ingested_at   TIMESTAMP    DEFAULT NOW(),
            _batch_id      VARCHAR(50),
            _is_duplicate  BOOLEAN      DEFAULT FALSE,
            sf_contact_id  VARCHAR(50),
            first_name     VARCHAR(100),
            last_name      VARCHAR(100),
            email          VARCHAR(255),
            phone          VARCHAR(50),
            company        VARCHAR(255),
            title          VARCHAR(100),
            country        VARCHAR(100),
            city           VARCHAR(100),
            lead_source    VARCHAR(100),
            annual_revenue NUMERIC(15,2),
            created_date   DATE,
            last_activity  DATE
        )""",

        # Raw ingest from NetSuite ERP
        """CREATE TABLE IF NOT EXISTS bronze.src_erp_customers (
            _raw_id        SERIAL PRIMARY KEY,
            _source_system VARCHAR(50)  DEFAULT 'netsuite',
            _ingested_at   TIMESTAMP    DEFAULT NOW(),
            _batch_id      VARCHAR(50),
            ns_customer_id VARCHAR(50),
            company_name   VARCHAR(255),
            contact_email  VARCHAR(255),
            billing_phone  VARCHAR(50),
            country_code   VARCHAR(10),
            city           VARCHAR(100),
            payment_terms  VARCHAR(50),
            credit_limit   NUMERIC(12,2),
            account_status VARCHAR(50),
            since_date     DATE
        )""",

        # Raw orders from ERP
        """CREATE TABLE IF NOT EXISTS bronze.src_erp_orders (
            _raw_id         SERIAL PRIMARY KEY,
            _source_system  VARCHAR(50) DEFAULT 'netsuite',
            _ingested_at    TIMESTAMP   DEFAULT NOW(),
            _batch_id       VARCHAR(50),
            ns_order_id     VARCHAR(50),
            ns_customer_id  VARCHAR(50),
            order_date      VARCHAR(30),
            status          VARCHAR(50),
            subtotal        NUMERIC(15,2),
            tax_amount      NUMERIC(10,2),
            total_amount    NUMERIC(15,2),
            currency        VARCHAR(10),
            payment_method  VARCHAR(50),
            ship_country    VARCHAR(100),
            line_items_json TEXT
        )""",

        # Raw product catalog
        """CREATE TABLE IF NOT EXISTS bronze.src_product_catalog (
            _raw_id        SERIAL PRIMARY KEY,
            _source_system VARCHAR(50) DEFAULT 'pim',
            _ingested_at   TIMESTAMP   DEFAULT NOW(),
            _batch_id      VARCHAR(50),
            sku            VARCHAR(100),
            product_name   VARCHAR(255),
            category_l1    VARCHAR(100),
            category_l2    VARCHAR(100),
            list_price     NUMERIC(10,2),
            cost_price     NUMERIC(10,2),
            weight_kg      NUMERIC(8,3),
            is_active      VARCHAR(10),
            vendor_id      VARCHAR(50),
            created_date   DATE
        )""",

        # Raw marketing leads from HubSpot
        """CREATE TABLE IF NOT EXISTS bronze.src_mktg_leads (
            _raw_id        SERIAL PRIMARY KEY,
            _source_system VARCHAR(50) DEFAULT 'hubspot',
            _ingested_at   TIMESTAMP   DEFAULT NOW(),
            hs_lead_id     VARCHAR(50),
            email          VARCHAR(255),
            first_name     VARCHAR(100),
            last_name      VARCHAR(100),
            company        VARCHAR(255),
            job_title      VARCHAR(100),
            lifecycle_stage VARCHAR(50),
            lead_score     INTEGER,
            campaign_source VARCHAR(100),
            utm_medium     VARCHAR(100),
            created_at     TIMESTAMP,
            last_contacted TIMESTAMP,
            country        VARCHAR(100),
            phone          VARCHAR(50)
        )""",

        # Raw HR data from Workday
        """CREATE TABLE IF NOT EXISTS bronze.src_hr_employees (
            _raw_id          SERIAL PRIMARY KEY,
            _source_system   VARCHAR(50) DEFAULT 'workday',
            _ingested_at     TIMESTAMP   DEFAULT NOW(),
            _batch_id        VARCHAR(50),
            wd_employee_id   VARCHAR(50),
            full_name        VARCHAR(200),
            work_email       VARCHAR(255),
            department       VARCHAR(100),
            job_title        VARCHAR(100),
            manager_wd_id    VARCHAR(50),
            employment_type  VARCHAR(50),
            location         VARCHAR(100),
            hire_date        VARCHAR(30),
            annual_salary    NUMERIC(12,2),
            cost_centre      VARCHAR(50),
            is_active        VARCHAR(10)
        )""",

        # ── SILVER LAYER ──────────────────────────────────────────────────────
        # Unified customer master (merged CRM + ERP, deduplicated)
        """CREATE TABLE IF NOT EXISTS silver.customers (
            customer_sk    SERIAL PRIMARY KEY,
            customer_id    VARCHAR(50)  UNIQUE NOT NULL,
            full_name      VARCHAR(200) NOT NULL,
            email          VARCHAR(255),
            phone          VARCHAR(30),
            company        VARCHAR(255),
            job_title      VARCHAR(100),
            country_iso2   CHAR(2),
            country_name   VARCHAR(100),
            city           VARCHAR(100),
            segment        VARCHAR(50),
            annual_revenue NUMERIC(15,2),
            credit_limit   NUMERIC(12,2),
            payment_terms  VARCHAR(50),
            is_active      BOOLEAN      DEFAULT TRUE,
            source_systems TEXT[],
            source_ids     TEXT[],
            quality_score  NUMERIC(4,2),
            issues_resolved INTEGER     DEFAULT 0,
            created_at     TIMESTAMP    DEFAULT NOW(),
            updated_at     TIMESTAMP    DEFAULT NOW(),
            _silver_version INTEGER     DEFAULT 1
        )""",

        # Clean product catalog
        """CREATE TABLE IF NOT EXISTS silver.products (
            product_sk   SERIAL PRIMARY KEY,
            product_id   VARCHAR(50)  UNIQUE NOT NULL,
            sku          VARCHAR(100) UNIQUE NOT NULL,
            product_name VARCHAR(255) NOT NULL,
            category     VARCHAR(100),
            subcategory  VARCHAR(100),
            list_price   NUMERIC(10,2),
            cost_price   NUMERIC(10,2),
            margin_pct   NUMERIC(6,2),
            weight_kg    NUMERIC(8,3),
            vendor_id    VARCHAR(50),
            is_active    BOOLEAN      DEFAULT TRUE,
            quality_score NUMERIC(4,2),
            created_at   TIMESTAMP    DEFAULT NOW(),
            updated_at   TIMESTAMP    DEFAULT NOW()
        )""",

        # Clean orders
        """CREATE TABLE IF NOT EXISTS silver.orders (
            order_sk       SERIAL PRIMARY KEY,
            order_id       VARCHAR(50) UNIQUE NOT NULL,
            customer_sk    INTEGER REFERENCES silver.customers(customer_sk),
            order_date     DATE        NOT NULL,
            status         VARCHAR(50),
            subtotal       NUMERIC(15,2),
            tax_amount     NUMERIC(10,2),
            total_amount   NUMERIC(15,2),
            currency       CHAR(3)     DEFAULT 'USD',
            payment_method VARCHAR(50),
            ship_country   CHAR(2),
            quality_score  NUMERIC(4,2),
            source_system  VARCHAR(50),
            source_order_id VARCHAR(50),
            created_at     TIMESTAMP   DEFAULT NOW()
        )""",

        # Clean order line items
        """CREATE TABLE IF NOT EXISTS silver.order_items (
            item_sk      SERIAL PRIMARY KEY,
            order_sk     INTEGER REFERENCES silver.orders(order_sk),
            product_sk   INTEGER REFERENCES silver.products(product_sk),
            quantity     INTEGER      NOT NULL,
            unit_price   NUMERIC(10,2) NOT NULL,
            line_total   NUMERIC(15,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
            discount_pct NUMERIC(5,2) DEFAULT 0
        )""",

        # Clean employee org chart
        """CREATE TABLE IF NOT EXISTS silver.employees (
            employee_sk   SERIAL PRIMARY KEY,
            employee_id   VARCHAR(50)  UNIQUE NOT NULL,
            full_name     VARCHAR(200) NOT NULL,
            email         VARCHAR(255),
            department    VARCHAR(100),
            job_title     VARCHAR(100),
            manager_sk    INTEGER,
            employment_type VARCHAR(50),
            location      VARCHAR(100),
            hire_date     DATE,
            annual_salary NUMERIC(12,2),
            cost_centre   VARCHAR(50),
            is_active     BOOLEAN      DEFAULT TRUE,
            quality_score NUMERIC(4,2),
            created_at    TIMESTAMP    DEFAULT NOW()
        )""",

        # ── GOLD LAYER ────────────────────────────────────────────────────────
        # Customer dimension (SCD Type 2 — tracks historical changes)
        """CREATE TABLE IF NOT EXISTS gold.dim_customer (
            customer_dwh_sk  SERIAL PRIMARY KEY,
            customer_id      VARCHAR(50) NOT NULL,
            full_name        VARCHAR(200),
            email            VARCHAR(255),
            company          VARCHAR(255),
            country_iso2     CHAR(2),
            country_name     VARCHAR(100),
            city             VARCHAR(100),
            segment          VARCHAR(50),
            annual_revenue   NUMERIC(15,2),
            revenue_band     VARCHAR(20),
            is_b2b           BOOLEAN,
            is_enterprise    BOOLEAN,
            scd_start_date   DATE    NOT NULL,
            scd_end_date     DATE,
            is_current       BOOLEAN DEFAULT TRUE,
            scd_version      INTEGER DEFAULT 1
        )""",

        # Product dimension with full hierarchy
        """CREATE TABLE IF NOT EXISTS gold.dim_product (
            product_dwh_sk SERIAL PRIMARY KEY,
            product_id     VARCHAR(50) NOT NULL,
            sku            VARCHAR(100),
            product_name   VARCHAR(255),
            category       VARCHAR(100),
            subcategory    VARCHAR(100),
            list_price     NUMERIC(10,2),
            cost_price     NUMERIC(10,2),
            margin_pct     NUMERIC(6,2),
            margin_band    VARCHAR(20),
            is_active      BOOLEAN
        )""",

        # Date dimension
        """CREATE TABLE IF NOT EXISTS gold.dim_date (
            date_sk        INTEGER PRIMARY KEY,
            full_date      DATE UNIQUE,
            year           SMALLINT,
            quarter        SMALLINT,
            month          SMALLINT,
            month_name     VARCHAR(10),
            week           SMALLINT,
            day_of_week    SMALLINT,
            day_name       VARCHAR(10),
            is_weekend     BOOLEAN,
            fiscal_year    SMALLINT,
            fiscal_quarter SMALLINT
        )""",

        # Order facts
        """CREATE TABLE IF NOT EXISTS gold.fact_orders (
            fact_sk          SERIAL PRIMARY KEY,
            order_id         VARCHAR(50),
            customer_dwh_sk  INTEGER REFERENCES gold.dim_customer(customer_dwh_sk),
            product_dwh_sk   INTEGER REFERENCES gold.dim_product(product_dwh_sk),
            date_sk          INTEGER REFERENCES gold.dim_date(date_sk),
            quantity         INTEGER,
            unit_price       NUMERIC(10,2),
            line_total       NUMERIC(15,2),
            discount_amount  NUMERIC(10,2),
            tax_amount       NUMERIC(10,2),
            gross_margin     NUMERIC(15,2),
            order_status     VARCHAR(50),
            currency         CHAR(3),
            ship_country     CHAR(2)
        )""",

        # Monthly revenue aggregation
        """CREATE TABLE IF NOT EXISTS gold.agg_revenue_monthly (
            agg_sk           SERIAL PRIMARY KEY,
            year             SMALLINT,
            month            SMALLINT,
            month_label      VARCHAR(10),
            country_iso2     CHAR(2),
            segment          VARCHAR(50),
            category         VARCHAR(100),
            orders_count     INTEGER,
            units_sold       INTEGER,
            gross_revenue    NUMERIC(15,2),
            discounts        NUMERIC(12,2),
            net_revenue      NUMERIC(15,2),
            cogs             NUMERIC(15,2),
            gross_profit     NUMERIC(15,2),
            gross_margin_pct NUMERIC(6,2),
            new_customers    INTEGER,
            returning_customers INTEGER
        )""",

        # Customer lifetime value scorecard
        """CREATE TABLE IF NOT EXISTS gold.agg_customer_ltv (
            ltv_sk             SERIAL PRIMARY KEY,
            customer_id        VARCHAR(50) UNIQUE,
            full_name          VARCHAR(200),
            company            VARCHAR(255),
            segment            VARCHAR(50),
            country_iso2       CHAR(2),
            first_order_date   DATE,
            last_order_date    DATE,
            total_orders       INTEGER,
            total_units        INTEGER,
            gross_revenue      NUMERIC(15,2),
            avg_order_value    NUMERIC(12,2),
            avg_days_between_orders INTEGER,
            predicted_ltv_12m  NUMERIC(15,2),
            churn_risk         VARCHAR(20),
            customer_tier      VARCHAR(20)
        )""",

        # Data quality scorecard — the meta-layer DataIQ writes to
        """CREATE TABLE IF NOT EXISTS gold.scorecard_data_quality (
            scorecard_sk     SERIAL PRIMARY KEY,
            run_date         DATE NOT NULL,
            layer            VARCHAR(20),
            source_system    VARCHAR(50),
            table_name       VARCHAR(100),
            total_rows       INTEGER,
            complete_rows    INTEGER,
            duplicate_rows   INTEGER,
            invalid_rows     INTEGER,
            completeness_pct NUMERIC(6,2),
            uniqueness_pct   NUMERIC(6,2),
            validity_pct     NUMERIC(6,2),
            overall_score    NUMERIC(6,2),
            issues_found     INTEGER,
            issues_resolved  INTEGER,
            dq_trend         VARCHAR(20)
        )"""
    ]

    # ── SEED DATA ─────────────────────────────────────────────────────────────

    bronze_crm_sql = """
INSERT INTO bronze.src_crm_contacts
    (_batch_id,sf_contact_id,first_name,last_name,email,phone,company,title,country,city,lead_source,annual_revenue,created_date,last_activity)
VALUES
-- Clean records
('B2024-001','SF-001','Alice','Nakamura','alice.nakamura@technova.com','+1-415-555-0101','TechNova Inc','VP Engineering','United States','San Francisco','Web','8500000','2022-03-15','2024-11-01'),
('B2024-001','SF-002','Benjamin','Okafor','b.okafor@meridian.co','+44-20-7946-0202','Meridian Group','Chief Data Officer','United Kingdom','London','Referral','12000000','2021-06-20','2024-10-28'),
('B2024-001','SF-003','Clara','Dupont','c.dupont@stratexco.fr','+33-1-555-0303','Stratex Co','Head of Analytics','France','Paris','Conference','4200000','2023-01-10','2024-11-03'),
('B2024-001','SF-004','Diego','Morales','diego.morales@nexuslabs.mx','+52-55-555-0404','Nexus Labs','CTO','Mexico','Mexico City','Partner','3100000','2022-09-05','2024-10-15'),
('B2024-001','SF-005','Elena','Voronova','elena.v@databridge.ru','+7-495-555-0505','DataBridge LLC','Data Director','Russia','Moscow','Inbound','2900000','2023-04-18','2024-09-30'),
-- ⚠️ DUPLICATE CONTACTS (same company, different reps entered them separately)
('B2024-001','SF-006','James','Patterson','james.patterson@technova.com','+1-415-555-0101','TechNova Inc','Director of Data','US','San Francisco','Web','8500000','2022-08-20','2024-10-05'),
('B2024-001','SF-007','J.','Patterson','j.patterson@technova.com','+14155550101','TechNova, Inc.','Dir. Data Engineering','USA','SF','Outbound','8500000','2023-02-14','2024-09-22'),
-- More clean records
('B2024-001','SF-008','Fatima','Al-Rashid','f.rashid@gulfstream.ae','+971-4-555-0808','Gulf Stream Technologies','Analytics Manager','UAE','Dubai','Event','6700000','2022-11-30','2024-11-02'),
('B2024-001','SF-009','George','Stefanidis','g.stefanidis@athenadata.gr','+30-21-555-0909','Athena Data Solutions','CEO','Greece','Athens','Referral','1800000','2023-07-14','2024-10-20'),
('B2024-001','SF-010','Hina','Tanaka','hina.tanaka@zenithcorp.jp','+81-3-555-1010','Zenith Corp','CDO','Japan','Tokyo','Partner','22000000','2021-03-01','2024-11-04'),
-- ⚠️ BAD EMAILS / MISSING FIELDS
('B2024-001','SF-011','Ivan','Petrov','ivan.petrov@','+ 7-812-555-1111','NordTech','Analyst','Russia','St. Petersburg','Inbound','950000','2023-10-05','2024-08-15'),
('B2024-001','SF-012','Julia','Andersen',NULL,'+45-33-555-1212','Scandi Analytics','Data Engineer','Denmark','Copenhagen','Web',NULL,'2024-01-20','2024-10-30'),
('B2024-001','SF-013','Kevin','Osei','kevin@@globaldata.gh',NULL,'Global Data GH','Manager','ghana','Accra','Referral','750000','2023-05-12','2024-07-18'),
-- ⚠️ INCONSISTENT COUNTRY FORMATS
('B2024-001','SF-014','Laura','Bianchi','l.bianchi@futuredata.it','+39-02-555-1414','Future Data SRL','Head of BI','Italy','Milan','Conference','3400000','2022-04-25','2024-11-01'),
('B2024-001','SF-015','Mohammed','Al-Farsi','m.alfarsi@omanalytics.om','+968-24-555-1515','Oman Analytics','Director','Oman','Muscat','Event','2100000','2023-08-09','2024-10-12'),
('B2024-001','SF-016','Natasha','Kowalski','n.kowalski@datapl.pl','+48-22-555-1616','DataPL Sp. z o.o.','BI Manager','PL','Warsaw','Inbound','1650000','2023-02-28','2024-09-05'),
('B2024-001','SF-017','Omar','Hassan','o.hassan@saharatech.eg','+20-2-555-1717','Sahara Tech','CTO','Egypt','Cairo','Partner','4800000','2022-07-17','2024-10-25'),
('B2024-001','SF-018','Paula','Ferreira','p.ferreira@brdataworks.br','+55-11-555-1818','BR DataWorks','Head of Data','Brazil','Sao Paulo','Web','3900000','2023-03-22','2024-11-02'),
('B2024-001','SF-019','Qi','Zhang','qi.zhang@sinoanalytics.cn','+86-21-555-1919','Sino Analytics','VP Data','China','Shanghai','Conference','18000000','2021-09-14','2024-10-30'),
('B2024-001','SF-020','Rachel','Kim','rachel.kim@seouldata.kr','+82-2-555-2020','Seoul Data Co','Analytics Lead','South Korea','Seoul','Referral','5600000','2022-12-05','2024-11-01');
"""

    bronze_erp_sql = """
INSERT INTO bronze.src_erp_customers
    (_batch_id,ns_customer_id,company_name,contact_email,billing_phone,country_code,city,payment_terms,credit_limit,account_status,since_date)
VALUES
-- Same companies as CRM but with ERP-style data (different IDs, formats)
('B2024-001','NS-C001','TechNova Inc.','billing@technova.com','+14155550101','US','San Francisco','NET-30',500000,'Active','2022-03-20'),
('B2024-001','NS-C002','Meridian Group Ltd','accounts@meridian.co','+442079460202','GB','London','NET-45',750000,'Active','2021-06-25'),
('B2024-001','NS-C003','Stratex Co SAS','finance@stratexco.fr','+33155030303','FR','Paris','NET-30',250000,'Active','2023-01-15'),
('B2024-001','NS-C004','Nexus Labs S.A. de C.V.','cfo@nexuslabs.mx','+525555040404','MX','Mexico City','NET-30',180000,'Active','2022-09-10'),
('B2024-001','NS-C005','DataBridge LLC','payments@databridge.ru','+74955050505','RU','Moscow','Prepaid',50000,'Active','2023-04-20'),
('B2024-001','NS-C006','Gulf Stream Technologies','ar@gulfstream.ae','+97145080808','AE','Dubai','NET-45',400000,'Active','2022-12-01'),
('B2024-001','NS-C007','Athena Data Solutions','billing@athenadata.gr','+302109090909','GR','Athens','NET-60',120000,'Active','2023-07-20'),
('B2024-001','NS-C008','Zenith Corp','finance@zenithcorp.jp','+81355101010','JP','Tokyo','NET-30',1200000,'Active','2021-03-05'),
-- ⚠️ ERP has companies CRM doesn''t know about
('B2024-001','NS-C009','Arctic Analytics AS','ar@arcticanalytics.no','+4723111111','NO','Oslo','NET-30',200000,'Active','2022-06-14'),
('B2024-001','NS-C010','Cape Data (Pty) Ltd','billing@capedata.za','+27215551010','ZA','Cape Town','NET-45',160000,'Active','2023-09-30'),
-- ⚠️ INCONSISTENT STATUS VALUES
('B2024-001','NS-C011','Future Data SRL','invoices@futuredata.it','+390255141414','IT','Milan','NET-30',200000,'ACTIVE','2022-04-28'),
('B2024-001','NS-C012','Oman Analytics LLC','ar@omanalytics.om','+96824151515','OM','Muscat','Prepaid',80000,'active','2023-08-12'),
-- ⚠️ MISSING / BAD DATA IN ERP
('B2024-001','NS-C013','DataPL Sp z o o',NULL,'+48225161616','PL','Warsaw','NET-60',110000,'Active','2023-03-01'),
('B2024-001','NS-C014','Sahara Tech','o.hassan@saharatech.eg',NULL,'EG','Cairo','NET-45',280000,'Active','2022-07-20'),
('B2024-001','NS-C015','Sino Analytics Co Ltd','billing@sinoanalytics.cn','+862155191919','CN','Shanghai','NET-30',900000,'Active','2021-09-18'),
('B2024-001','NS-C016','Seoul Data Co','ar@seouldata.kr','+8225552020','KR','Seoul','NET-30',320000,'Active','2022-12-08'),
('B2024-001','NS-C017','BR DataWorks Ltda','finance@brdataworks.br','+551155181818','BR','São Paulo','NET-30',230000,'Active','2023-03-25'),
-- ⚠️ DUPLICATE IN ERP (same company, two accounts)
('B2024-001','NS-C018','TechNova','billing2@technova.com','+14155550199','US','San Francisco','NET-30',100000,'Active','2023-11-01'),
('B2024-001','NS-C019','NordTech LLC',NULL,'+78125551111','RU','Saint Petersburg','Prepaid',30000,'On-Hold',NULL),
('B2024-001','NS-C020','Scandi Analytics ApS','julia.andersen@scandi.dk','+4533121212','DK','Copenhagen','NET-60',95000,'Active','2024-01-22');
"""

    bronze_orders_sql = """
INSERT INTO bronze.src_erp_orders
    (_batch_id,ns_order_id,ns_customer_id,order_date,status,subtotal,tax_amount,total_amount,currency,payment_method,ship_country,line_items_json)
VALUES
('B2024-001','ORD-10001','NS-C001','2024-01-15','Closed Won',28500.00,2850.00,31350.00,'USD','Wire Transfer','US','[{"sku":"ENT-PLAT-001","qty":1,"price":28500}]'),
('B2024-001','ORD-10002','NS-C002','2024-01-22','Closed Won',45000.00,0.00,45000.00,'GBP','Invoice','GB','[{"sku":"ENT-PLAT-001","qty":1,"price":38000},{"sku":"PRO-ADD-001","qty":5,"price":1400}]'),
('B2024-001','ORD-10003','NS-C008','2024-02-01','Closed Won',95000.00,0.00,95000.00,'JPY','Wire Transfer','JP','[{"sku":"ENT-PLAT-001","qty":1,"price":75000},{"sku":"PRO-ADD-001","qty":10,"price":2000}]'),
('B2024-001','ORD-10004','NS-C001','2024-02-14','Closed Won',14500.00,1450.00,15950.00,'USD','Credit Card','US','[{"sku":"PRO-STD-001","qty":10,"price":1450}]'),
('B2024-001','ORD-10005','NS-C006','2024-02-20','Closed Won',22000.00,1100.00,23100.00,'USD','Wire Transfer','AE','[{"sku":"ENT-PLAT-001","qty":1,"price":22000}]'),
('B2024-001','ORD-10006','NS-C003','2024-03-05','Closed Won',12000.00,2400.00,14400.00,'EUR','Invoice','FR','[{"sku":"PRO-STD-001","qty":8,"price":1500}]'),
('B2024-001','ORD-10007','NS-C015','2024-03-10','Closed Won',55000.00,0.00,55000.00,'USD','Wire Transfer','CN','[{"sku":"ENT-PLAT-001","qty":1,"price":42000},{"sku":"PRO-ADD-001","qty":13,"price":1000}]'),
('B2024-001','ORD-10008','NS-C004','2024-03-18','Closed Won',9800.00,980.00,10780.00,'MXN','Credit Card','MX','[{"sku":"PRO-STD-001","qty":7,"price":1400}]'),
('B2024-001','ORD-10009','NS-C016','2024-04-02','Closed Won',18500.00,0.00,18500.00,'KRW','Wire Transfer','KR','[{"sku":"ENT-PLAT-001","qty":1,"price":18500}]'),
('B2024-001','ORD-10010','NS-C017','2024-04-09','Closed Won',11000.00,1650.00,12650.00,'BRL','Invoice','BR','[{"sku":"PRO-STD-001","qty":8,"price":1375}]'),
('B2024-001','ORD-10011','NS-C002','2024-04-15','Closed Won',67500.00,0.00,67500.00,'GBP','Invoice','GB','[{"sku":"ENT-PLAT-001","qty":1,"price":48000},{"sku":"PRO-ADD-001","qty":7,"price":2786}]'),
('B2024-001','ORD-10012','NS-C005','2024-05-01','Closed Won',8500.00,0.00,8500.00,'USD','Wire Transfer','RU','[{"sku":"PRO-STD-001","qty":5,"price":1700}]'),
('B2024-001','ORD-10013','NS-C009','2024-05-10','Closed Won',15000.00,3000.00,18000.00,'NOK','Invoice','NO','[{"sku":"ENT-PLAT-001","qty":1,"price":15000}]'),
('B2024-001','ORD-10014','NS-C001','2024-05-22','Closed Won',35000.00,3500.00,38500.00,'USD','Wire Transfer','US','[{"sku":"ENT-PLAT-001","qty":1,"price":28000},{"sku":"PRO-ADD-001","qty":5,"price":1400}]'),
('B2024-001','ORD-10015','NS-C010','2024-06-03','Closed Won',12500.00,1875.00,14375.00,'ZAR','Invoice','ZA','[{"sku":"PRO-STD-001","qty":9,"price":1389}]'),
('B2024-001','ORD-10016','NS-C007','2024-06-15','Closed Won',18000.00,3600.00,21600.00,'EUR','Invoice','GR','[{"sku":"ENT-PLAT-001","qty":1,"price":18000}]'),
('B2024-001','ORD-10017','NS-C008','2024-07-01','Closed Won',42000.00,0.00,42000.00,'JPY','Wire Transfer','JP','[{"sku":"PRO-ADD-001","qty":21,"price":2000}]'),
('B2024-001','ORD-10018','NS-C003','2024-07-18','Closed Won',24000.00,4800.00,28800.00,'EUR','Invoice','FR','[{"sku":"ENT-PLAT-001","qty":1,"price":24000}]'),
('B2024-001','ORD-10019','NS-C015','2024-08-05','Closed Won',28000.00,0.00,28000.00,'USD','Wire Transfer','CN','[{"sku":"PRO-ADD-001","qty":28,"price":1000}]'),
('B2024-001','ORD-10020','NS-C011','2024-08-20','Closed Won',16000.00,3200.00,19200.00,'EUR','Invoice','IT','[{"sku":"ENT-PLAT-001","qty":1,"price":16000}]'),
('B2024-001','ORD-10021','NS-C016','2024-09-02','Closed Won',22000.00,0.00,22000.00,'KRW','Wire Transfer','KR','[{"sku":"ENT-PLAT-001","qty":1,"price":22000}]'),
('B2024-001','ORD-10022','NS-C001','2024-09-15','Closed Won',48000.00,4800.00,52800.00,'USD','Wire Transfer','US','[{"sku":"ENT-PLAT-001","qty":1,"price":38000},{"sku":"PRO-ADD-001","qty":5,"price":2000}]'),
('B2024-001','ORD-10023','NS-C006','2024-10-01','Closed Won',30000.00,1500.00,31500.00,'USD','Wire Transfer','AE','[{"sku":"ENT-PLAT-001","qty":1,"price":30000}]'),
('B2024-001','ORD-10024','NS-C017','2024-10-14','Closed Won',14500.00,2175.00,16675.00,'BRL','Invoice','BR','[{"sku":"PRO-STD-001","qty":10,"price":1450}]'),
('B2024-001','ORD-10025','NS-C002','2024-11-01','Processing',52000.00,0.00,52000.00,'GBP','Invoice','GB','[{"sku":"ENT-PLAT-001","qty":1,"price":52000}]'),
-- ⚠️ ORDERS WITH BAD DATA
('B2024-001','ORD-10026','NS-C099','2024-09-30','Closed Won',18000.00,1800.00,19800.00,'USD','Credit Card','US','[]'),
('B2024-001','ORD-10027','NS-C001','01/15/2024','Closed Won',5000.00,500.00,5500.00,'USD','Credit Card','US','[{"sku":"PRO-STD-001","qty":3,"price":1667}]'),
('B2024-001','ORD-10028','NS-C004','2024-11-10','Pending',7500.00,750.00,7500.00,'MXN','Credit Card','MX','[{"sku":"PRO-STD-001","qty":5,"price":1500}]'),
('B2024-001','ORD-10029','NS-C008','2024-11-12','Processing',38000.00,0.00,38000.00,'JPY','Wire Transfer','JP','[{"sku":"ENT-PLAT-001","qty":1,"price":38000}]'),
('B2024-001','ORD-10030','NS-C015','2024-11-15','Processing',25000.00,0.00,25000.00,'USD','Wire Transfer','CN','[{"sku":"PRO-ADD-001","qty":25,"price":1000}]');
"""

    bronze_products_sql = """
INSERT INTO bronze.src_product_catalog
    (_batch_id,sku,product_name,category_l1,category_l2,list_price,cost_price,weight_kg,is_active,vendor_id,created_date)
VALUES
('B2024-001','ENT-PLAT-001','DataIQ Enterprise Platform — Annual Licence','Software','Data Quality',48000.00,8200.00,NULL,'Yes','V-001','2021-01-15'),
('B2024-001','ENT-PLAT-002','DataIQ Enterprise Platform — Monthly Licence','Software','Data Quality',4500.00,820.00,NULL,'Yes','V-001','2021-01-15'),
('B2024-001','PRO-STD-001','DataIQ Professional — Annual Licence','Software','Data Quality',15000.00,2800.00,NULL,'Yes','V-001','2021-06-01'),
('B2024-001','PRO-STD-002','DataIQ Professional — Monthly Licence','Software','Analytics',1499.00,280.00,NULL,'Yes','V-001','2021-06-01'),
('B2024-001','PRO-ADD-001','DataIQ Add-on: Extra Connector Pack','Software','Connectors',2000.00,320.00,NULL,'Yes','V-001','2022-03-01'),
('B2024-001','PRO-ADD-002','DataIQ Add-on: AI Agent Suite','software','AI / ML',3500.00,600.00,NULL,'yes','V-001','2022-07-15'),
('B2024-001','SUP-PREM-001','Premium Support — Annual','Support','Professional Services',8000.00,1200.00,NULL,'Yes','V-001','2021-03-01'),
('B2024-001','SUP-STD-001','Standard Support — Annual','Support','Professional Services',3000.00,500.00,NULL,'Yes','V-001','2021-03-01'),
('B2024-001','IMP-BASIC-001','Implementation: Basic Onboarding (5 days)','Services','Implementation',7500.00,3000.00,NULL,'Yes','V-002','2021-04-01'),
('B2024-001','IMP-ADV-001','Implementation: Advanced Setup (15 days)','SERVICES','Implementation',22500.00,9000.00,NULL,'Yes','V-002','2022-01-10'),
-- ⚠️ BAD DATA IN PRODUCT CATALOG
('B2024-001','IMP-MGRT-001','Data Migration Service (per TB)','services','Migration',5000.00,5500.00,NULL,'Yes','V-002','2022-05-20'),
('B2024-001','TRN-ONLINE-001','Online Training: Data Quality Fundamentals',NULL,NULL,1200.00,180.00,NULL,'Yes','V-003','2022-09-01'),
('B2024-001','TRN-ILT-001','Instructor-Led Training (3 days)','Training',NULL,4500.00,900.00,NULL,'Yes','V-003','2022-09-01'),
('B2024-001','CONS-ARCH-001','Architecture Advisory (per day)','Consulting','Advisory',2500.00,800.00,NULL,'No','V-002','2023-02-14'),
('B2024-001','CONS-DQ-001','Data Quality Assessment','Consulting','DQ Advisory',12000.00,3800.00,NULL,'Yes','V-002','2023-04-01');
"""

    bronze_hr_sql = """
INSERT INTO bronze.src_hr_employees
    (_batch_id,wd_employee_id,full_name,work_email,department,job_title,manager_wd_id,employment_type,location,hire_date,annual_salary,cost_centre,is_active)
VALUES
('B2024-001','WD-001','Sandra Mills','sandra.mills@nexuscommerce.com','Executive','CEO',NULL,'Full-Time','New York','2015-01-05',320000,'CC-EXEC','Yes'),
('B2024-001','WD-002','Richard Grant','richard.grant@nexuscommerce.com','Finance','CFO','WD-001','Full-Time','New York','2015-03-20',280000,'CC-FIN','Yes'),
('B2024-001','WD-003','Patricia Osei','patricia.osei@nexuscommerce.com','Operations','COO','WD-001','Full-Time','London','2016-06-01',275000,'CC-OPS','Yes'),
('B2024-001','WD-004','Sarah Connor','sarah.connor@nexuscommerce.com','Engineering','VP Engineering','WD-003','Full-Time','San Francisco','2017-04-10',185000,'CC-TECH','Yes'),
('B2024-001','WD-005','Marcus Webb','marcus.webb@nexuscommerce.com','Sales','VP Sales','WD-003','Full-Time','New York','2017-09-15',195000,'CC-SALES','Yes'),
('B2024-001','WD-006','Yuki Tanaka','yuki.tanaka@nexuscommerce.com','Engineering','Principal Engineer','WD-004','Full-Time','Tokyo','2018-02-28',165000,'CC-TECH','Yes'),
('B2024-001','WD-007','Amara Diallo','amara.diallo@nexuscommerce.com','Marketing','VP Marketing','WD-003','Full-Time','Paris','2018-07-01',172000,'CC-MKT','Yes'),
('B2024-001','WD-008','Carlos Reyes','carlos.reyes@nexuscommerce.com','Sales','Sales Director LATAM','WD-005','Full-Time','Mexico City','2019-03-11',145000,'CC-SALES','Yes'),
('B2024-001','WD-009','Priya Patel',NULL,'Engineering','Senior Data Engineer','WD-004','Full-Time','Mumbai','2020-02-28',130000,'CC-TECH','Yes'),
('B2024-001','WD-010','Elena Voronova','elena.v@nexuscommerce.com','Sales','Account Executive EMEA','WD-005','Full-Time','Moscow','2020-09-01',105000,'CC-SALES','Yes'),
-- ⚠️ BAD HR DATA
('B2024-001','WD-011','Dev Nair','dev.nair@nexuscommerce.com','Eng','DevOps Lead','WD-004','Full-Time','Bangalore','2021-01-10',138000,'CC-TECH','Yes'),
('B2024-001','WD-012','Nina Rodriguez','nina.r@nexuscommerce.com','marketing','Senior Marketing Manager','WD-007','Full-Time','Madrid','2021-03-08',112000,'CC-MKT','Yes'),
('B2024-001','WD-013','Lena Müller',NULL,'Engineering','Frontend Engineer','WD-004','Full-Time','Berlin',NULL,95000,'CC-TECH','Yes'),
('B2024-001','WD-014','Test Account','test@nexuscommerce.com','IT','Temp',NULL,'Contractor','Remote','2099-01-01',0,'CC-IT','Yes'),
('B2024-001','WD-015','Sandra Mills','s.mills@nexuscommerce.com','Executive','CEO','WD-001','Full-Time','New York','2015-01-05',320000,'CC-EXEC','Yes');
"""

    # ── SILVER LAYER DATA ─────────────────────────────────────────────────────
    silver_customers_sql = """
INSERT INTO silver.customers
    (customer_id,full_name,email,phone,company,job_title,country_iso2,country_name,city,segment,annual_revenue,credit_limit,payment_terms,is_active,source_systems,source_ids,quality_score,issues_resolved)
VALUES
('CUS-001','Alice Nakamura',         'alice.nakamura@technova.com',   '+1-415-555-0101', 'TechNova Inc',             'VP Engineering',     'US','United States','San Francisco','Enterprise',  8500000,500000,'NET-30',TRUE, ARRAY['salesforce','netsuite'],ARRAY['SF-001','NS-C001'],0.96,2),
('CUS-002','Benjamin Okafor',        'b.okafor@meridian.co',          '+44-20-7946-0202','Meridian Group',           'Chief Data Officer', 'GB','United Kingdom','London',      'Enterprise', 12000000,750000,'NET-45',TRUE, ARRAY['salesforce','netsuite'],ARRAY['SF-002','NS-C002'],0.98,1),
('CUS-003','Clara Dupont',           'c.dupont@stratexco.fr',         '+33-1-555-0303',  'Stratex Co',               'Head of Analytics',  'FR','France',       'Paris',        'Mid-Market',  4200000,250000,'NET-30',TRUE, ARRAY['salesforce','netsuite'],ARRAY['SF-003','NS-C003'],0.97,0),
('CUS-004','Diego Morales',          'diego.morales@nexuslabs.mx',    '+52-55-555-0404', 'Nexus Labs',               'CTO',                'MX','Mexico',       'Mexico City',  'Mid-Market',  3100000,180000,'NET-30',TRUE, ARRAY['salesforce','netsuite'],ARRAY['SF-004','NS-C004'],0.95,1),
('CUS-005','Elena Voronova',         'elena.v@databridge.ru',         '+7-495-555-0505', 'DataBridge LLC',           'Data Director',      'RU','Russia',       'Moscow',       'SMB',          2900000, 50000,'Prepaid',TRUE,ARRAY['salesforce','netsuite'],ARRAY['SF-005','NS-C005'],0.93,1),
('CUS-006','James Patterson',        'james.patterson@technova.com',  '+1-415-555-0101', 'TechNova Inc',             'Director of Data',   'US','United States','San Francisco','Enterprise',  8500000,500000,'NET-30',TRUE, ARRAY['salesforce'],            ARRAY['SF-006','SF-007'],0.91,3),
('CUS-007','Fatima Al-Rashid',       'f.rashid@gulfstream.ae',        '+971-4-555-0808', 'Gulf Stream Technologies', 'Analytics Manager',  'AE','UAE',          'Dubai',        'Enterprise',  6700000,400000,'NET-45',TRUE, ARRAY['salesforce','netsuite'],ARRAY['SF-008','NS-C006'],0.97,0),
('CUS-008','George Stefanidis',      'g.stefanidis@athenadata.gr',    '+30-21-555-0909', 'Athena Data Solutions',    'CEO',                'GR','Greece',       'Athens',       'SMB',          1800000,120000,'NET-60',TRUE, ARRAY['salesforce','netsuite'],ARRAY['SF-009','NS-C007'],0.98,0),
('CUS-009','Hina Tanaka',            'hina.tanaka@zenithcorp.jp',     '+81-3-555-1010',  'Zenith Corp',              'CDO',                'JP','Japan',        'Tokyo',        'Enterprise', 22000000,1200000,'NET-30',TRUE,ARRAY['salesforce','netsuite'],ARRAY['SF-010','NS-C008'],0.99,0),
('CUS-010','Ivan Petrov',            'ivan.petrov@nordtech.ru',       '+7-812-555-1111', 'NordTech',                 'Analyst',            'RU','Russia',       'St. Petersburg','SMB',          950000, 30000,'Prepaid',TRUE,ARRAY['salesforce','netsuite'],ARRAY['SF-011','NS-C019'],0.78,4),
('CUS-011','Julia Andersen',         'julia.andersen@scandi.dk',      '+45-33-555-1212', 'Scandi Analytics',         'Data Engineer',      'DK','Denmark',      'Copenhagen',   'SMB',              NULL, 95000,'NET-60',TRUE, ARRAY['salesforce','netsuite'],ARRAY['SF-012','NS-C020'],0.82,3),
('CUS-012','Kevin Osei',             'kevin.osei@globaldata.gh',      NULL,              'Global Data GH',           'Manager',            'GH','Ghana',        'Accra',        'SMB',           750000,  NULL,'Referral',TRUE,ARRAY['salesforce'],            ARRAY['SF-013'],         0.71,5),
('CUS-013','Laura Bianchi',          'l.bianchi@futuredata.it',       '+39-02-555-1414', 'Future Data SRL',          'Head of BI',         'IT','Italy',        'Milan',        'Mid-Market',  3400000,200000,'NET-30',TRUE, ARRAY['salesforce','netsuite'],ARRAY['SF-014','NS-C011'],0.96,2),
('CUS-014','Mohammed Al-Farsi',      'm.alfarsi@omanalytics.om',      '+968-24-555-1515','Oman Analytics',           'Director',           'OM','Oman',         'Muscat',       'Mid-Market',  2100000, 80000,'Prepaid',TRUE,ARRAY['salesforce','netsuite'],ARRAY['SF-015','NS-C012'],0.94,2),
('CUS-015','Natasha Kowalski',       'n.kowalski@datapl.pl',          '+48-22-555-1616', 'DataPL Sp. z o.o.',        'BI Manager',         'PL','Poland',       'Warsaw',       'SMB',          1650000,110000,'NET-60',TRUE, ARRAY['salesforce','netsuite'],ARRAY['SF-016','NS-C013'],0.88,3),
('CUS-016','Omar Hassan',            'o.hassan@saharatech.eg',        '+20-2-555-1717',  'Sahara Tech',              'CTO',                'EG','Egypt',        'Cairo',        'Mid-Market',  4800000,280000,'NET-45',TRUE, ARRAY['salesforce','netsuite'],ARRAY['SF-017','NS-C014'],0.95,2),
('CUS-017','Paula Ferreira',         'p.ferreira@brdataworks.br',     '+55-11-555-1818', 'BR DataWorks',             'Head of Data',       'BR','Brazil',       'São Paulo',    'Mid-Market',  3900000,230000,'NET-30',TRUE, ARRAY['salesforce','netsuite'],ARRAY['SF-018','NS-C017'],0.97,1),
('CUS-018','Qi Zhang',               'qi.zhang@sinoanalytics.cn',     '+86-21-555-1919', 'Sino Analytics',           'VP Data',            'CN','China',        'Shanghai',     'Enterprise', 18000000,900000,'NET-30',TRUE, ARRAY['salesforce','netsuite'],ARRAY['SF-019','NS-C015'],0.98,0),
('CUS-019','Rachel Kim',             'rachel.kim@seouldata.kr',       '+82-2-555-2020',  'Seoul Data Co',            'Analytics Lead',     'KR','South Korea',  'Seoul',        'Mid-Market',  5600000,320000,'NET-30',TRUE, ARRAY['salesforce','netsuite'],ARRAY['SF-020','NS-C016'],0.97,0),
('CUS-020','Arctic Analytics Team',  'ar@arcticanalytics.no',         '+47-23-555-1111', 'Arctic Analytics AS',      'Accounts',           'NO','Norway',       'Oslo',         'SMB',              NULL,200000,'NET-30',TRUE, ARRAY['netsuite'],              ARRAY['NS-C009'],        0.85,2),
('CUS-021','Cape Data Team',         'billing@capedata.za',           '+27-21-555-1010', 'Cape Data (Pty) Ltd',      'Billing',            'ZA','South Africa', 'Cape Town',    'SMB',              NULL,160000,'NET-45',TRUE, ARRAY['netsuite'],              ARRAY['NS-C010'],        0.83,2);
"""

    silver_products_sql = """
INSERT INTO silver.products
    (product_id,sku,product_name,category,subcategory,list_price,cost_price,margin_pct,vendor_id,is_active,quality_score)
VALUES
('PROD-001','ENT-PLAT-001','DataIQ Enterprise Platform — Annual Licence',   'Software','Data Quality',   48000.00, 8200.00,82.9,'V-001',TRUE, 1.00),
('PROD-002','ENT-PLAT-002','DataIQ Enterprise Platform — Monthly Licence',  'Software','Data Quality',    4500.00,  820.00,81.8,'V-001',TRUE, 1.00),
('PROD-003','PRO-STD-001', 'DataIQ Professional — Annual Licence',          'Software','Data Quality',   15000.00, 2800.00,81.3,'V-001',TRUE, 1.00),
('PROD-004','PRO-STD-002', 'DataIQ Professional — Monthly Licence',         'Software','Analytics',       1499.00,  280.00,81.3,'V-001',TRUE, 1.00),
('PROD-005','PRO-ADD-001', 'DataIQ Add-on: Extra Connector Pack',           'Software','Connectors',      2000.00,  320.00,84.0,'V-001',TRUE, 1.00),
('PROD-006','PRO-ADD-002', 'DataIQ Add-on: AI Agent Suite',                 'Software','AI & ML',         3500.00,  600.00,82.9,'V-001',TRUE, 0.97),
('PROD-007','SUP-PREM-001','Premium Support — Annual',                      'Support', 'Professional',    8000.00, 1200.00,85.0,'V-001',TRUE, 1.00),
('PROD-008','SUP-STD-001', 'Standard Support — Annual',                     'Support', 'Professional',    3000.00,  500.00,83.3,'V-001',TRUE, 1.00),
('PROD-009','IMP-BASIC-001','Implementation: Basic Onboarding (5 days)',    'Services','Implementation',  7500.00, 3000.00,60.0,'V-002',TRUE, 1.00),
('PROD-010','IMP-ADV-001', 'Implementation: Advanced Setup (15 days)',      'Services','Implementation', 22500.00, 9000.00,60.0,'V-002',TRUE, 1.00),
('PROD-011','IMP-MGRT-001','Data Migration Service (per TB)',               'Services','Migration',       5000.00, 5500.00,-10.0,'V-002',TRUE,0.88),
('PROD-012','TRN-ONLINE-001','Online Training: Data Quality Fundamentals',  'Training','Self-Paced',      1200.00,  180.00,85.0,'V-003',TRUE, 0.95),
('PROD-013','TRN-ILT-001', 'Instructor-Led Training (3 days)',              'Training','Instructor-Led',  4500.00,  900.00,80.0,'V-003',TRUE, 0.97),
('PROD-014','CONS-ARCH-001','Architecture Advisory (per day)',              'Consulting','Advisory',      2500.00,  800.00,68.0,'V-002',FALSE,0.90),
('PROD-015','CONS-DQ-001', 'Data Quality Assessment',                       'Consulting','DQ Advisory',  12000.00, 3800.00,68.3,'V-002',TRUE, 1.00);
"""

    silver_orders_sql = """
INSERT INTO silver.orders
    (order_id,customer_sk,order_date,status,subtotal,tax_amount,total_amount,currency,payment_method,ship_country,quality_score,source_system,source_order_id)
VALUES
('SO-001', 1, '2024-01-15','Closed Won',28500.00,2850.00,31350.00,'USD','Wire Transfer','US',1.00,'netsuite','ORD-10001'),
('SO-002', 2, '2024-01-22','Closed Won',45000.00,   0.00,45000.00,'GBP','Invoice',      'GB',1.00,'netsuite','ORD-10002'),
('SO-003', 9, '2024-02-01','Closed Won',95000.00,   0.00,95000.00,'USD','Wire Transfer','JP',1.00,'netsuite','ORD-10003'),
('SO-004', 1, '2024-02-14','Closed Won',14500.00,1450.00,15950.00,'USD','Credit Card',  'US',0.98,'netsuite','ORD-10004'),
('SO-005', 7, '2024-02-20','Closed Won',22000.00,1100.00,23100.00,'USD','Wire Transfer','AE',1.00,'netsuite','ORD-10005'),
('SO-006', 3, '2024-03-05','Closed Won',12000.00,2400.00,14400.00,'EUR','Invoice',      'FR',1.00,'netsuite','ORD-10006'),
('SO-007',18, '2024-03-10','Closed Won',55000.00,   0.00,55000.00,'USD','Wire Transfer','CN',1.00,'netsuite','ORD-10007'),
('SO-008', 4, '2024-03-18','Closed Won', 9800.00, 980.00,10780.00,'USD','Credit Card',  'MX',0.96,'netsuite','ORD-10008'),
('SO-009',19, '2024-04-02','Closed Won',18500.00,   0.00,18500.00,'USD','Wire Transfer','KR',1.00,'netsuite','ORD-10009'),
('SO-010',17, '2024-04-09','Closed Won',11000.00,1650.00,12650.00,'USD','Invoice',      'BR',1.00,'netsuite','ORD-10010'),
('SO-011', 2, '2024-04-15','Closed Won',67500.00,   0.00,67500.00,'GBP','Invoice',      'GB',1.00,'netsuite','ORD-10011'),
('SO-012', 5, '2024-05-01','Closed Won', 8500.00,   0.00, 8500.00,'USD','Wire Transfer','RU',0.95,'netsuite','ORD-10012'),
('SO-013',20, '2024-05-10','Closed Won',15000.00,3000.00,18000.00,'USD','Invoice',      'NO',1.00,'netsuite','ORD-10013'),
('SO-014', 1, '2024-05-22','Closed Won',35000.00,3500.00,38500.00,'USD','Wire Transfer','US',1.00,'netsuite','ORD-10014'),
('SO-015',21, '2024-06-03','Closed Won',12500.00,1875.00,14375.00,'USD','Invoice',      'ZA',1.00,'netsuite','ORD-10015'),
('SO-016', 8, '2024-06-15','Closed Won',18000.00,3600.00,21600.00,'EUR','Invoice',      'GR',1.00,'netsuite','ORD-10016'),
('SO-017', 9, '2024-07-01','Closed Won',42000.00,   0.00,42000.00,'USD','Wire Transfer','JP',1.00,'netsuite','ORD-10017'),
('SO-018', 3, '2024-07-18','Closed Won',24000.00,4800.00,28800.00,'EUR','Invoice',      'FR',1.00,'netsuite','ORD-10018'),
('SO-019',18, '2024-08-05','Closed Won',28000.00,   0.00,28000.00,'USD','Wire Transfer','CN',1.00,'netsuite','ORD-10019'),
('SO-020',13, '2024-08-20','Closed Won',16000.00,3200.00,19200.00,'EUR','Invoice',      'IT',1.00,'netsuite','ORD-10020'),
('SO-021',19, '2024-09-02','Closed Won',22000.00,   0.00,22000.00,'USD','Wire Transfer','KR',1.00,'netsuite','ORD-10021'),
('SO-022', 1, '2024-09-15','Closed Won',48000.00,4800.00,52800.00,'USD','Wire Transfer','US',1.00,'netsuite','ORD-10022'),
('SO-023', 7, '2024-10-01','Closed Won',30000.00,1500.00,31500.00,'USD','Wire Transfer','AE',1.00,'netsuite','ORD-10023'),
('SO-024',17, '2024-10-14','Closed Won',14500.00,2175.00,16675.00,'USD','Invoice',      'BR',1.00,'netsuite','ORD-10024'),
('SO-025', 2, '2024-11-01','Processing',52000.00,   0.00,52000.00,'GBP','Invoice',      'GB',1.00,'netsuite','ORD-10025');
"""

    silver_order_items_sql = """
INSERT INTO silver.order_items (order_sk,product_sk,quantity,unit_price,discount_pct) VALUES
(1, 1,1,28500.00,0),(2, 1,1,38000.00,0),(2, 5,5,1400.00,0),
(3, 1,1,75000.00,0),(3, 5,10,2000.00,0),
(4, 3,10,1450.00,0),(5, 1,1,22000.00,0),(6, 3,8,1500.00,0),
(7, 1,1,42000.00,0),(7, 5,13,1000.00,0),
(8, 3,7,1400.00,0),(9, 1,1,18500.00,0),(10,3,8,1375.00,0),
(11,1,1,48000.00,0),(11,5,7,2786.00,0),
(12,3,5,1700.00,0),(13,1,1,15000.00,0),(14,1,1,28000.00,0),(14,5,5,1400.00,0),
(15,3,9,1389.00,0),(16,1,1,18000.00,0),(17,5,21,2000.00,0),
(18,1,1,24000.00,0),(19,5,28,1000.00,0),(20,1,1,16000.00,0),
(21,1,1,22000.00,0),(22,1,1,38000.00,0),(22,5,5,2000.00,0),
(23,1,1,30000.00,0),(24,3,10,1450.00,0),(25,1,1,52000.00,0);
"""

    silver_employees_sql = """
INSERT INTO silver.employees
    (employee_id,full_name,email,department,job_title,manager_sk,employment_type,location,hire_date,annual_salary,cost_centre,is_active,quality_score)
VALUES
('EMP-001','Sandra Mills',   'sandra.mills@nexuscommerce.com',   'Executive',   'CEO',             NULL,'Full-Time','New York',     '2015-01-05',320000,'CC-EXEC', TRUE,1.00),
('EMP-002','Richard Grant',  'richard.grant@nexuscommerce.com',  'Finance',     'CFO',             1,   'Full-Time','New York',     '2015-03-20',280000,'CC-FIN',  TRUE,1.00),
('EMP-003','Patricia Osei',  'patricia.osei@nexuscommerce.com',  'Operations',  'COO',             1,   'Full-Time','London',       '2016-06-01',275000,'CC-OPS',  TRUE,1.00),
('EMP-004','Sarah Connor',   'sarah.connor@nexuscommerce.com',   'Engineering', 'VP Engineering',  3,   'Full-Time','San Francisco','2017-04-10',185000,'CC-TECH', TRUE,1.00),
('EMP-005','Marcus Webb',    'marcus.webb@nexuscommerce.com',    'Sales',       'VP Sales',        3,   'Full-Time','New York',     '2017-09-15',195000,'CC-SALES',TRUE,1.00),
('EMP-006','Yuki Tanaka',    'yuki.tanaka@nexuscommerce.com',    'Engineering', 'Principal Engineer',4, 'Full-Time','Tokyo',        '2018-02-28',165000,'CC-TECH', TRUE,1.00),
('EMP-007','Amara Diallo',   'amara.diallo@nexuscommerce.com',   'Marketing',   'VP Marketing',    3,   'Full-Time','Paris',        '2018-07-01',172000,'CC-MKT',  TRUE,1.00),
('EMP-008','Carlos Reyes',   'carlos.reyes@nexuscommerce.com',   'Sales',       'Sales Director LATAM',5,'Full-Time','Mexico City', '2019-03-11',145000,'CC-SALES',TRUE,1.00),
('EMP-009','Priya Patel',    'priya.patel@nexuscommerce.com',    'Engineering', 'Senior Data Engineer',4,'Full-Time','Mumbai',      '2020-02-28',130000,'CC-TECH', TRUE,0.95),
('EMP-010','Elena Voronova', 'elena.v@nexuscommerce.com',        'Sales',       'Account Executive EMEA',5,'Full-Time','Moscow',    '2020-09-01',105000,'CC-SALES',TRUE,1.00),
('EMP-011','Dev Nair',       'dev.nair@nexuscommerce.com',       'Engineering', 'DevOps Lead',     4,   'Full-Time','Bangalore',   '2021-01-10',138000,'CC-TECH', TRUE,0.97),
('EMP-012','Nina Rodriguez', 'nina.r@nexuscommerce.com',         'Marketing',   'Senior Marketing Manager',7,'Full-Time','Madrid', '2021-03-08',112000,'CC-MKT',  TRUE,0.97),
('EMP-013','Lena Müller',    'lena.mueller@nexuscommerce.com',   'Engineering', 'Frontend Engineer',4,  'Full-Time','Berlin',       '2022-01-15',95000, 'CC-TECH', TRUE,0.93);
"""

    # ── GOLD LAYER DATA ───────────────────────────────────────────────────────
    gold_dim_customer_sql = """
INSERT INTO gold.dim_customer
    (customer_id,full_name,email,company,country_iso2,country_name,city,segment,annual_revenue,revenue_band,is_b2b,is_enterprise,scd_start_date,scd_end_date,is_current,scd_version)
VALUES
('CUS-001','Alice Nakamura',  'alice.nakamura@technova.com',   'TechNova Inc',             'US','United States', 'San Francisco','Enterprise',  8500000,'$5M–$25M',  TRUE,TRUE, '2022-03-15',NULL,TRUE,1),
('CUS-002','Benjamin Okafor', 'b.okafor@meridian.co',          'Meridian Group',           'GB','United Kingdom','London',       'Enterprise', 12000000,'$10M+',      TRUE,TRUE, '2021-06-20',NULL,TRUE,1),
('CUS-003','Clara Dupont',    'c.dupont@stratexco.fr',         'Stratex Co',               'FR','France',        'Paris',        'Mid-Market',  4200000,'$1M–$5M',   TRUE,FALSE,'2023-01-10',NULL,TRUE,1),
('CUS-004','Diego Morales',   'diego.morales@nexuslabs.mx',    'Nexus Labs',               'MX','Mexico',        'Mexico City',  'Mid-Market',  3100000,'$1M–$5M',   TRUE,FALSE,'2022-09-05',NULL,TRUE,1),
('CUS-005','Elena Voronova',  'elena.v@databridge.ru',         'DataBridge LLC',           'RU','Russia',        'Moscow',       'SMB',          2900000,'$1M–$5M',   TRUE,FALSE,'2023-04-18',NULL,TRUE,1),
('CUS-007','Fatima Al-Rashid','f.rashid@gulfstream.ae',        'Gulf Stream Technologies', 'AE','UAE',           'Dubai',        'Enterprise',  6700000,'$5M–$25M',  TRUE,TRUE, '2022-11-30',NULL,TRUE,1),
('CUS-008','George Stefanidis','g.stefanidis@athenadata.gr',   'Athena Data Solutions',    'GR','Greece',        'Athens',       'SMB',          1800000,'$1M–$5M',   TRUE,FALSE,'2023-07-14',NULL,TRUE,1),
('CUS-009','Hina Tanaka',     'hina.tanaka@zenithcorp.jp',     'Zenith Corp',              'JP','Japan',         'Tokyo',        'Enterprise', 22000000,'$10M+',      TRUE,TRUE, '2021-03-01',NULL,TRUE,1),
('CUS-013','Laura Bianchi',   'l.bianchi@futuredata.it',       'Future Data SRL',          'IT','Italy',         'Milan',        'Mid-Market',  3400000,'$1M–$5M',   TRUE,FALSE,'2022-04-25',NULL,TRUE,1),
('CUS-016','Omar Hassan',     'o.hassan@saharatech.eg',        'Sahara Tech',              'EG','Egypt',         'Cairo',        'Mid-Market',  4800000,'$1M–$5M',   TRUE,FALSE,'2022-07-17',NULL,TRUE,1),
('CUS-017','Paula Ferreira',  'p.ferreira@brdataworks.br',     'BR DataWorks',             'BR','Brazil',        'São Paulo',    'Mid-Market',  3900000,'$1M–$5M',   TRUE,FALSE,'2023-03-22',NULL,TRUE,1),
('CUS-018','Qi Zhang',        'qi.zhang@sinoanalytics.cn',     'Sino Analytics',           'CN','China',         'Shanghai',     'Enterprise', 18000000,'$10M+',      TRUE,TRUE, '2021-09-14',NULL,TRUE,1),
('CUS-019','Rachel Kim',      'rachel.kim@seouldata.kr',       'Seoul Data Co',            'KR','South Korea',   'Seoul',        'Mid-Market',  5600000,'$5M–$25M',  TRUE,FALSE,'2022-12-05',NULL,TRUE,1),
('CUS-020','Arctic Analytics','ar@arcticanalytics.no',         'Arctic Analytics AS',      'NO','Norway',        'Oslo',         'SMB',             NULL,'Unknown',     TRUE,FALSE,'2022-06-14',NULL,TRUE,1),
('CUS-021','Cape Data',       'billing@capedata.za',           'Cape Data (Pty) Ltd',      'ZA','South Africa',  'Cape Town',    'SMB',             NULL,'Unknown',     TRUE,FALSE,'2023-09-30',NULL,TRUE,1);
"""

    gold_dim_product_sql = """
INSERT INTO gold.dim_product
    (product_id,sku,product_name,category,subcategory,list_price,cost_price,margin_pct,margin_band,is_active)
VALUES
('PROD-001','ENT-PLAT-001','DataIQ Enterprise Platform — Annual',  'Software', 'Data Quality',  48000.00, 8200.00,82.9,'High',  TRUE),
('PROD-003','PRO-STD-001', 'DataIQ Professional — Annual',         'Software', 'Data Quality',  15000.00, 2800.00,81.3,'High',  TRUE),
('PROD-005','PRO-ADD-001', 'DataIQ Add-on: Extra Connector Pack',  'Software', 'Connectors',     2000.00,  320.00,84.0,'High',  TRUE),
('PROD-007','SUP-PREM-001','Premium Support — Annual',             'Support',  'Professional',   8000.00, 1200.00,85.0,'High',  TRUE),
('PROD-009','IMP-BASIC-001','Implementation: Basic Onboarding',    'Services', 'Implementation', 7500.00, 3000.00,60.0,'Medium',TRUE),
('PROD-010','IMP-ADV-001', 'Implementation: Advanced Setup',       'Services', 'Implementation',22500.00, 9000.00,60.0,'Medium',TRUE),
('PROD-011','IMP-MGRT-001','Data Migration Service',               'Services', 'Migration',      5000.00, 5500.00,-10.0,'Negative',TRUE),
('PROD-015','CONS-DQ-001', 'Data Quality Assessment',              'Consulting','DQ Advisory',  12000.00, 3800.00,68.3,'Medium',TRUE);
"""

    # Populate dim_date for 2024
    gold_dim_date_sql = """
INSERT INTO gold.dim_date (date_sk,full_date,year,quarter,month,month_name,week,day_of_week,day_name,is_weekend,fiscal_year,fiscal_quarter)
SELECT
    TO_CHAR(d,'YYYYMMDD')::INTEGER,
    d::DATE,
    EXTRACT(YEAR FROM d)::SMALLINT,
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(MONTH FROM d)::SMALLINT,
    TO_CHAR(d,'Mon'),
    EXTRACT(WEEK FROM d)::SMALLINT,
    EXTRACT(DOW FROM d)::SMALLINT,
    TO_CHAR(d,'Day'),
    EXTRACT(DOW FROM d) IN (0,6),
    EXTRACT(YEAR FROM d)::SMALLINT,
    EXTRACT(QUARTER FROM d)::SMALLINT
FROM generate_series('2024-01-01'::DATE, '2024-12-31'::DATE, '1 day') AS d
ON CONFLICT DO NOTHING
"""

    gold_fact_orders_sql = """
INSERT INTO gold.fact_orders
    (order_id,customer_dwh_sk,product_dwh_sk,date_sk,quantity,unit_price,line_total,discount_amount,tax_amount,gross_margin,order_status,currency,ship_country)
SELECT
    so.order_id,
    dc.customer_dwh_sk,
    dp.product_dwh_sk,
    TO_CHAR(so.order_date,'YYYYMMDD')::INTEGER,
    si.quantity,
    si.unit_price,
    si.line_total,
    ROUND(si.line_total * si.discount_pct / 100, 2),
    ROUND(so.tax_amount * (si.line_total / NULLIF(so.subtotal,0)), 2),
    ROUND(si.line_total * (1 - sp.cost_price / NULLIF(sp.list_price, 0)), 2),
    so.status,
    so.currency,
    so.ship_country
FROM silver.order_items si
JOIN silver.orders so     ON si.order_sk   = so.order_sk
JOIN silver.products sp   ON si.product_sk = sp.product_sk
JOIN silver.customers sc  ON so.customer_sk = sc.customer_sk
JOIN gold.dim_customer dc ON dc.customer_id = sc.customer_id AND dc.is_current = TRUE
JOIN gold.dim_product  dp ON dp.product_id  = sp.product_id
"""

    gold_revenue_sql = """
INSERT INTO gold.agg_revenue_monthly
    (year,month,month_label,country_iso2,segment,category,orders_count,units_sold,gross_revenue,discounts,net_revenue,cogs,gross_profit,gross_margin_pct,new_customers,returning_customers)
SELECT
    dd.year, dd.month, dd.month_name,
    sc.country_iso2, sc.segment, sp.category,
    COUNT(DISTINCT so.order_id),
    SUM(si.quantity),
    SUM(si.line_total),
    SUM(ROUND(si.line_total * si.discount_pct / 100, 2)),
    SUM(si.line_total) - SUM(ROUND(si.line_total * si.discount_pct / 100, 2)),
    SUM(ROUND(si.quantity * sp.cost_price, 2)),
    SUM(si.line_total) - SUM(ROUND(si.quantity * sp.cost_price, 2)),
    ROUND(100.0 * (SUM(si.line_total) - SUM(ROUND(si.quantity * sp.cost_price, 2))) / NULLIF(SUM(si.line_total),0), 2),
    0, 0
FROM silver.order_items si
JOIN silver.orders   so ON si.order_sk   = so.order_sk
JOIN silver.products sp ON si.product_sk = sp.product_sk
JOIN silver.customers sc ON so.customer_sk = sc.customer_sk
JOIN gold.dim_date   dd ON dd.full_date  = so.order_date
WHERE so.status = 'Closed Won'
GROUP BY dd.year, dd.month, dd.month_name, sc.country_iso2, sc.segment, sp.category
"""

    gold_ltv_sql = """
INSERT INTO gold.agg_customer_ltv
    (customer_id,full_name,company,segment,country_iso2,first_order_date,last_order_date,total_orders,total_units,gross_revenue,avg_order_value,avg_days_between_orders,predicted_ltv_12m,churn_risk,customer_tier)
SELECT
    sc.customer_id,
    sc.full_name,
    sc.company,
    sc.segment,
    sc.country_iso2,
    MIN(so.order_date),
    MAX(so.order_date),
    COUNT(DISTINCT so.order_id),
    SUM(si.quantity),
    SUM(si.line_total),
    ROUND(SUM(si.line_total) / NULLIF(COUNT(DISTINCT so.order_id),0), 2),
    CASE WHEN COUNT(DISTINCT so.order_id) > 1
         THEN ROUND((MAX(so.order_date) - MIN(so.order_date))::NUMERIC / (COUNT(DISTINCT so.order_id) - 1), 0)::INTEGER
         ELSE NULL END,
    ROUND(SUM(si.line_total) / NULLIF(COUNT(DISTINCT so.order_id),0) * 2.5, 2),
    CASE
        WHEN MAX(so.order_date) < NOW() - INTERVAL '180 days' THEN 'High'
        WHEN MAX(so.order_date) < NOW() - INTERVAL '90 days'  THEN 'Medium'
        ELSE 'Low'
    END,
    CASE
        WHEN SUM(si.line_total) > 150000 THEN 'Platinum'
        WHEN SUM(si.line_total) > 50000  THEN 'Gold'
        WHEN SUM(si.line_total) > 20000  THEN 'Silver'
        ELSE 'Bronze'
    END
FROM silver.customers sc
JOIN silver.orders so     ON so.customer_sk = sc.customer_sk AND so.status = 'Closed Won'
JOIN silver.order_items si ON si.order_sk = so.order_sk
GROUP BY sc.customer_id, sc.full_name, sc.company, sc.segment, sc.country_iso2
"""

    gold_scorecard_sql = """
INSERT INTO gold.scorecard_data_quality
    (run_date,layer,source_system,table_name,total_rows,complete_rows,duplicate_rows,invalid_rows,completeness_pct,uniqueness_pct,validity_pct,overall_score,issues_found,issues_resolved,dq_trend)
VALUES
(CURRENT_DATE,'Bronze','salesforce',  'src_crm_contacts',  20,14,3,5, 70.0,85.0,75.0,76.7,8, 0,'Baseline'),
(CURRENT_DATE,'Bronze','netsuite',    'src_erp_customers', 20,16,2,4, 80.0,90.0,80.0,83.3,6, 0,'Baseline'),
(CURRENT_DATE,'Bronze','netsuite',    'src_erp_orders',    30,26,0,4, 86.7,100.0,86.7,91.1,4, 0,'Baseline'),
(CURRENT_DATE,'Bronze','pim',         'src_product_catalog',15,12,0,3,80.0,100.0,80.0,86.7,3, 0,'Baseline'),
(CURRENT_DATE,'Bronze','workday',     'src_hr_employees',  15,12,2,3, 80.0,86.7,80.0,82.2,5, 0,'Baseline'),
(CURRENT_DATE,'Silver','unified',     'customers',         21,19,0,2, 90.5,100.0,90.5,93.7,2, 21,'Improving'),
(CURRENT_DATE,'Silver','unified',     'products',          15,15,0,1, 100.0,100.0,93.3,97.8,1, 14,'Improving'),
(CURRENT_DATE,'Silver','unified',     'orders',            25,25,0,0, 100.0,100.0,100.0,100.0,0,19,'Stable'),
(CURRENT_DATE,'Silver','unified',     'employees',         13,13,0,0, 100.0,100.0,100.0,100.0,0,17,'Stable'),
(CURRENT_DATE,'Gold',  'warehouse',   'dim_customer',      15,15,0,0, 100.0,100.0,100.0,100.0,0, 0,'Stable'),
(CURRENT_DATE,'Gold',  'warehouse',   'fact_orders',       31,31,0,0, 100.0,100.0,100.0,100.0,0, 0,'Stable'),
(CURRENT_DATE,'Gold',  'warehouse',   'agg_revenue_monthly',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,0,'Stable'),
(CURRENT_DATE,'Gold',  'warehouse',   'agg_customer_ltv',  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,0,'Stable');
"""

    all_ddl   = ddl_blocks
    all_seeds = [
        bronze_crm_sql, bronze_erp_sql, bronze_orders_sql,
        bronze_products_sql, bronze_hr_sql,
        silver_customers_sql, silver_products_sql, silver_orders_sql,
        silver_order_items_sql, silver_employees_sql,
        gold_dim_customer_sql, gold_dim_product_sql, gold_dim_date_sql,
        gold_fact_orders_sql, gold_revenue_sql, gold_ltv_sql, gold_scorecard_sql,
    ]

    try:
        with engine.begin() as conn:
            # Drop + recreate schemas
            conn.execute(_text("DROP SCHEMA IF EXISTS bronze CASCADE"))
            conn.execute(_text("DROP SCHEMA IF EXISTS silver CASCADE"))
            conn.execute(_text("DROP SCHEMA IF EXISTS gold   CASCADE"))
            for stmt in all_ddl:
                conn.execute(_text(stmt))

        for sql in all_seeds:
            raw = engine.raw_connection()
            try:
                cur = raw.cursor()
                cur.execute(sql)
                raw.commit()
            finally:
                raw.close()

        # Count tables per schema
        counts = {}
        with engine.connect() as conn:
            for schema in ['bronze','silver','gold']:
                tables = conn.execute(_text(
                    f"SELECT table_name FROM information_schema.tables WHERE table_schema='{schema}' ORDER BY table_name"
                )).fetchall()
                schema_counts = {}
                for (t,) in tables:
                    n = conn.execute(_text(f'SELECT COUNT(*) FROM {schema}."{t}"')).scalar()
                    schema_counts[t] = n
                counts[schema] = schema_counts

        return {"success": True, "architecture": "Bronze → Silver → Gold", "row_counts": counts}
    except Exception as e:
        import traceback
        return {"success": False, "error": str(e), "trace": traceback.format_exc()[-800:]}


@app.get("/seed-demo")
def seed_demo():
    """One-shot: create demo schema + tables + sample data in the first PostgreSQL connection."""
    from db.database import get_db as _get_db
    from services.connection_service import list_connections, _decrypt
    from connectors.registry import build_connector as _build
    from sqlalchemy import text as _text

    db = next(_get_db())
    conns = list_connections(db)
    pg = next((c for c in conns if c.connector_type == "postgresql"), None)
    if not pg:
        return {"error": "No PostgreSQL connection found. Add one first."}

    config    = _decrypt(pg.encrypted_config)
    connector = _build(pg.connector_type, config)
    engine    = connector._engine()
    created   = []

    ddl = """
CREATE SCHEMA IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.customers (
    customer_id SERIAL PRIMARY KEY, first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL, email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20), date_of_birth DATE, country VARCHAR(100) DEFAULT 'USA',
    city VARCHAR(100), signup_date TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE, lifetime_value NUMERIC(12,2) DEFAULT 0.00
);

CREATE TABLE IF NOT EXISTS demo.products (
    product_id SERIAL PRIMARY KEY, sku VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL, category VARCHAR(100), subcategory VARCHAR(100),
    price NUMERIC(10,2) NOT NULL, cost NUMERIC(10,2), stock_quantity INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE, created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS demo.orders (
    order_id SERIAL PRIMARY KEY, customer_id INTEGER REFERENCES demo.customers(customer_id),
    order_date TIMESTAMP DEFAULT NOW(), status VARCHAR(50) DEFAULT 'pending',
    total_amount NUMERIC(12,2), currency VARCHAR(3) DEFAULT 'USD',
    shipping_address TEXT, payment_method VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS demo.order_items (
    item_id SERIAL PRIMARY KEY, order_id INTEGER REFERENCES demo.orders(order_id),
    product_id INTEGER REFERENCES demo.products(product_id),
    quantity INTEGER NOT NULL, unit_price NUMERIC(10,2) NOT NULL,
    discount NUMERIC(5,2) DEFAULT 0.00
);

CREATE TABLE IF NOT EXISTS demo.employees (
    employee_id SERIAL PRIMARY KEY, name VARCHAR(200) NOT NULL,
    email VARCHAR(255), department VARCHAR(100), role VARCHAR(100),
    salary NUMERIC(12,2), hire_date DATE, manager_id INTEGER,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS demo.events (
    event_id SERIAL PRIMARY KEY, customer_id INTEGER REFERENCES demo.customers(customer_id),
    event_type VARCHAR(100) NOT NULL, event_timestamp TIMESTAMP DEFAULT NOW(),
    session_id VARCHAR(100), page_url TEXT, device_type VARCHAR(50),
    country_code VARCHAR(3), properties JSONB
);
"""

    seed_sql = """
INSERT INTO demo.customers (first_name,last_name,email,phone,date_of_birth,city,country,lifetime_value) VALUES
('Alice','Johnson','alice.johnson@example.com','+1-555-0101','1990-03-15','New York','USA',12450.00),
('Bob','Smith','bob.smith@example.com','+1-555-0102','1985-07-22','Chicago','USA',8920.50),
('Carmen','Garcia','carmen.garcia@example.com','+34-91-555-0103','1992-11-08','Madrid','Spain',5300.75),
('David','Lee','david.lee@example.com',NULL,'1988-01-30','Seoul','Korea',3200.00),
('Emma','Wilson','emma.wilson@example.com','+44-20-555-0105','1995-05-12','London','UK',18900.25),
('Frank','Brown','frank.brown@example.com','+1-555-0106',NULL,'Austin','USA',NULL),
('Grace','Martinez','grace.martinez@example.com','+1-555-0107','1993-09-21','Miami','USA',7650.00),
('Henry','Anderson','henry.anderson@example.com','+1-555-0108','1980-12-05','Seattle','USA',22100.00),
('Isabel','Thomas','isabel.thomas@example.com','+49-30-555-0109','1991-04-17','Berlin','Germany',9400.50),
('James','Taylor','james.taylor@example.com','+1-555-0110','1987-08-29','Boston','USA',15200.75)
ON CONFLICT (email) DO NOTHING;

INSERT INTO demo.products (sku,name,category,subcategory,price,cost,stock_quantity) VALUES
('LAPTOP-001','ProBook 15 Laptop','Electronics','Computers',1299.99,780.00,45),
('PHONE-001','SmartX Pro Smartphone','Electronics','Phones',899.99,420.00,120),
('TABLET-001','TabPad Air 10"','Electronics','Tablets',549.99,280.00,60),
('HDPHONE-001','SoundMax ANC Headphones','Electronics','Audio',249.99,110.00,200),
('CHAIR-001','ErgoWork Office Chair','Furniture','Seating',449.00,180.00,30),
('DESK-001','StandUp Pro Desk','Furniture','Desks',799.00,320.00,15),
('MONITOR-001','ViewPro 27" 4K Monitor','Electronics','Displays',599.99,280.00,55),
('KEYBOARD-001','MechaType Pro Keyboard','Electronics','Accessories',149.99,60.00,300),
('MOUSE-001','PrecisionClick Wireless','Electronics','Accessories',79.99,28.00,500),
('WEBCAM-001','ClearView 4K Webcam','Electronics','Accessories',119.99,48.00,150)
ON CONFLICT (sku) DO NOTHING;

INSERT INTO demo.orders (customer_id,order_date,status,total_amount,payment_method) VALUES
(1,NOW()-INTERVAL '30 days','completed',1549.98,'credit_card'),
(2,NOW()-INTERVAL '25 days','completed',899.99,'paypal'),
(3,NOW()-INTERVAL '20 days','completed',699.98,'credit_card'),
(5,NOW()-INTERVAL '15 days','shipped',1199.98,'credit_card'),
(1,NOW()-INTERVAL '10 days','processing',249.99,'debit_card'),
(8,NOW()-INTERVAL '7 days','completed',599.99,'credit_card'),
(4,NOW()-INTERVAL '5 days','pending',149.99,'paypal'),
(7,NOW()-INTERVAL '3 days','completed',1849.99,'credit_card'),
(9,NOW()-INTERVAL '2 days','processing',449.00,'wire_transfer'),
(10,NOW()-INTERVAL '1 day','pending',919.98,'credit_card');

INSERT INTO demo.order_items (order_id,product_id,quantity,unit_price) VALUES
(1,1,1,1299.99),(1,9,1,79.99),(2,2,1,899.99),(3,4,1,249.99),(3,8,1,149.99),
(4,7,1,599.99),(4,8,1,149.99),(5,4,1,249.99),(6,7,1,599.99),(7,8,1,149.99),
(8,1,1,1299.99),(8,9,1,79.99),(9,5,1,449.00),(10,2,1,899.99),(10,9,1,79.99);

INSERT INTO demo.employees (name,email,department,role,salary,hire_date) VALUES
('Sarah Connor','sarah.connor@company.com','Engineering','VP Engineering',185000,'2018-03-01'),
('John Reese','john.reese@company.com','Engineering','Senior Engineer',145000,'2019-06-15'),
('Maria Santos','maria.santos@company.com','Marketing','CMO',175000,'2017-11-20'),
('Tom Bradley','tom.bradley@company.com','Sales','Sales Director',140000,'2020-02-10'),
('Priya Patel',NULL,'Engineering','Data Engineer',130000,'2021-04-05'),
('Alex Kim','alex.kim@company.com','HR','HR Manager',110000,'2019-09-22'),
('Lisa Chen','lisa.chen@company.com','Finance','CFO',190000,'2016-07-14'),
('Mark Davis','mark.davis@company.com',NULL,'Product Manager',125000,NULL),
('Nina Rodriguez','nina.r@company.com','Marketing','Marketing Manager',115000,'2022-01-17'),
('Carlos Gomez','carlos.gomez@company.com','Engineering','Backend Engineer',135000,'2021-08-30')
ON CONFLICT DO NOTHING;

INSERT INTO demo.events (customer_id,event_type,event_timestamp,session_id,page_url,device_type,country_code) VALUES
(1,'page_view',NOW()-INTERVAL '2 hours','sess_001','/products','desktop','US'),
(1,'add_to_cart',NOW()-INTERVAL '1.9 hours','sess_001','/products/1','desktop','US'),
(1,'purchase',NOW()-INTERVAL '1.8 hours','sess_001','/checkout','desktop','US'),
(2,'page_view',NOW()-INTERVAL '5 hours','sess_002','/home','mobile','US'),
(2,'page_view',NOW()-INTERVAL '4.9 hours','sess_002','/products','mobile','US'),
(3,'login',NOW()-INTERVAL '1 day','sess_003','/login','tablet','ES'),
(5,'page_view',NOW()-INTERVAL '3 hours','sess_004','/home','desktop','GB'),
(5,'search',NOW()-INTERVAL '2.9 hours','sess_004','/search','desktop','GB'),
(7,'page_view',NOW()-INTERVAL '30 minutes','sess_005','/account','mobile','US'),
(8,'purchase',NOW()-INTERVAL '6 hours','sess_006','/checkout','desktop','US')
ON CONFLICT DO NOTHING;
"""

    try:
        with engine.begin() as conn:
            for stmt in ddl.split(";"):
                s = stmt.strip()
                if s:
                    conn.execute(_text(s))
            created.append("schema + 6 tables")
        with engine.begin() as conn:
            for stmt in seed_sql.split(";"):
                s = stmt.strip()
                if s:
                    conn.execute(_text(s))
            created.append("sample data seeded")
        return {"success": True, "created": created,
                "tables": ["demo.customers","demo.products","demo.orders",
                           "demo.order_items","demo.employees","demo.events"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/seed-demo-rich")
def seed_demo_rich():
    """Truncate demo tables and re-seed with rich, story-driven data showcasing real data quality issues."""
    from db.database import get_db as _get_db
    from services.connection_service import list_connections, _decrypt
    from connectors.registry import build_connector as _build
    from sqlalchemy import text as _text

    db = next(_get_db())
    conns = list_connections(db)
    pg = next((c for c in conns if c.connector_type == "postgresql"), None)
    if not pg:
        return {"error": "No PostgreSQL connection found."}

    config    = _decrypt(pg.encrypted_config)
    connector = _build(pg.connector_type, config)
    engine    = connector._engine()

    # ── PREP: relax constraints so demo issues can be inserted ────────────────
    truncate_sql = """
TRUNCATE demo.events, demo.order_items, demo.orders, demo.employees RESTART IDENTITY CASCADE;
TRUNCATE demo.customers RESTART IDENTITY CASCADE;
TRUNCATE demo.products  RESTART IDENTITY CASCADE;
ALTER TABLE demo.customers ALTER COLUMN email DROP NOT NULL;
ALTER TABLE demo.employees ALTER COLUMN email DROP NOT NULL;
"""

    # ── CUSTOMERS (65 rows) ───────────────────────────────────────────────────
    # Issues: duplicates, invalid emails, inconsistent countries, bad DOBs, nulls
    customers_sql = """
INSERT INTO demo.customers (first_name,last_name,email,phone,date_of_birth,city,country,signup_date,is_active,lifetime_value) VALUES
-- Clean, high-value customers
('Alice','Johnson','alice.johnson@acmecorp.com','+1-212-555-0101','1990-03-15','New York','USA','2021-01-15',TRUE,48250.00),
('Emma','Wilson','emma.wilson@globaltech.io','+44-20-7946-0105','1995-05-12','London','UK','2020-06-20',TRUE,72900.25),
('Henry','Anderson','henry.anderson@venture.co','+1-206-555-0108','1980-12-05','Seattle','USA','2019-11-01',TRUE,91100.00),
('Isabel','Thomas','isabel.thomas@datahaus.de','+49-30-555-0109','1991-04-17','Berlin','Germany','2021-03-10',TRUE,34400.50),
('James','Taylor','james.taylor@fintech.com','+1-617-555-0110','1987-08-29','Boston','USA','2020-08-05',TRUE,55200.75),
('Lena','Müller','lena.mueller@example.de','+49-89-555-0112','1993-02-28','Munich','Germany','2022-01-08',TRUE,19800.00),
('Carlos','Reyes','carlos.reyes@latam.mx','+52-55-555-0113','1985-07-14','Mexico City','Mexico','2021-07-22',TRUE,27600.50),
('Yuki','Tanaka','yuki.tanaka@japan.co.jp','+81-3-555-0114','1992-11-03','Tokyo','Japan','2020-12-01',TRUE,41300.00),
('Priya','Sharma','priya.sharma@infotech.in','+91-22-555-0115','1994-09-19','Mumbai','India','2022-03-14',TRUE,16750.25),
('Lucas','Dubois','lucas.dubois@paris.fr','+33-1-555-0116','1988-06-25','Paris','France','2021-05-30',TRUE,38900.00),

-- ⚠️ DUPLICATE CUSTOMERS (same person, different records — classic MDM issue)
('Robert','Smith','robert.smith@company.com','+1-312-555-0201','1982-04-10','Chicago','USA','2020-03-01',TRUE,23400.00),
('Rob','Smith','rsmith@company.com','+1-312-555-0201','1982-04-10','Chicago','US',  '2021-06-15',TRUE,8900.00),   -- duplicate: Rob vs Robert, inconsistent country
('Bob','Smith','bob.smith@company.com',  '+1-312-555-0201','1982-04-10','Chicago','United States','2022-01-20',TRUE,NULL),  -- 3rd duplicate, null LTV

('Jennifer','Martinez','j.martinez@enterprise.com','+1-415-555-0202','1991-08-22','San Francisco','USA','2020-09-10',TRUE,67200.00),
('Jen','Martinez','jenmartinez@enterprise.com','+1-415-555-0202','1991-08-22','San Francisco','U.S.A.','2021-11-05',TRUE,15300.00),  -- duplicate: Jen vs Jennifer

('Michael','Chen','m.chen@techcorp.com','+1-650-555-0203','1986-03-17','Palo Alto','USA','2019-08-20',TRUE,112500.00),
('Mike','Chen','mike.chen@techcorp.com','+1-650-555-0203','1986-03-17','Palo Alto','America','2020-02-14',TRUE,44100.00),  -- duplicate + invalid country

('Sophie','Bernard','sophie.bernard@fr.com','+33-6-555-0204','1993-12-05','Lyon','France','2021-04-18',TRUE,29800.00),
('S.','Bernard','s.bernard@fr.com','+33-6-555-0204','1993-12-05','Lyon','France','2021-09-12',TRUE,4200.00),  -- duplicate: same phone, same DOB, different email/first name

-- ⚠️ INVALID EMAIL FORMATS
('David','Park','david.park@','+ 1-718-555-0301','1989-07-11','New York','USA','2021-02-28',TRUE,8400.00),             -- missing domain
('Aisha','Okonkwo','aisha.okonkwo@@gmail.com','+234-1-555-0302','1995-01-30','Lagos','Nigeria','2022-05-10',TRUE,5600.00),  -- double @@
('Marco','Ferrari','marcoferrari','+39-02-555-0303','1987-11-08','Milan','Italy','2020-11-15',TRUE,18200.00),           -- no @ at all
('Anna','Kowalski','anna.kowalski@gmial.com','+48-22-555-0304','1993-05-25','Warsaw','Poland','2021-09-03',TRUE,12700.00), -- typo: gmial
('Tom','Wright','tom@wright@outlook.com','+1-404-555-0305','1984-02-19','Atlanta','USA','2020-07-22',TRUE,31500.00),    -- double @ in middle

-- ⚠️ NULL / MISSING CRITICAL FIELDS
('Sarah','Kim',NULL,'+1-503-555-0401','1992-06-14','Portland','USA','2021-08-19',TRUE,44200.00),       -- null email
('Ahmed','Hassan',NULL,NULL,'1988-09-02','Dubai','UAE','2022-01-05',TRUE,67800.00),                   -- null email + phone
('Maria','Gonzalez',NULL,'+34-93-555-0403','1990-12-20','Barcelona','Spain','2021-06-11',TRUE,28400.00),
('Wei','Zhang',NULL,NULL,'1985-03-08','Shanghai','China','2020-04-17',TRUE,89300.00),
('Nina','Petrov',NULL,'+7-495-555-0405',NULL,'Moscow','Russia','2022-07-30',TRUE,15600.00),            -- null email + DOB

-- ⚠️ IMPOSSIBLE / BAD BIRTH DATES
('Tyler','Brooks','tyler.brooks@email.com','+1-713-555-0501','2031-01-01','Houston','USA','2021-03-15',TRUE,9800.00),    -- future DOB
('Grace','Liu','grace.liu@startup.io','+1-415-555-0502','1874-06-12','San Francisco','USA','2022-02-28',TRUE,22100.00),  -- 150 years old
('Oliver','Brown','o.brown@corp.com','+1-303-555-0503','2025-12-31','Denver','USA','2020-10-05',TRUE,33400.00),          -- future DOB

-- ⚠️ INCONSISTENT COUNTRY FORMATS
('Fatima','Al-Rashid','fatima.rashid@gulf.ae','+971-4-555-0601','1994-08-15','Dubai','UAE','2021-05-20',TRUE,52100.00),
('Khalid','Mansour','khalid.mansour@gulf.ae','+971-4-555-0602','1989-11-25','Abu Dhabi','United Arab Emirates','2020-09-14',TRUE,38700.00), -- UAE vs United Arab Emirates
('Raj','Patel','raj.patel@uk.com','+44-161-555-0603','1991-07-30','Manchester','UK','2022-04-01',TRUE,19400.00),
('Claire','Evans','claire.evans@brit.co','+44-171-555-0604','1987-02-14','London','United Kingdom','2021-01-25',TRUE,41800.00), -- UK vs United Kingdom
('Hans','Gruber','h.gruber@german.de','+49-40-555-0605','1983-05-10','Hamburg','DE','2020-06-30',TRUE,27300.00),  -- DE vs Germany

-- ⚠️ ZERO / NEGATIVE LIFETIME VALUE (data calculation errors)
('Paul','Adams','paul.adams@retail.com','+1-214-555-0701','1990-10-05','Dallas','USA','2021-11-12',TRUE,0.00),
('Karen','White','karen.white@shop.com','+1-602-555-0702','1986-04-22','Phoenix','USA','2022-03-07',TRUE,-250.00),    -- negative LTV (impossible)
('Jason','Lee','jason.lee@store.com','+1-702-555-0703','1993-08-18','Las Vegas','USA','2020-05-16',TRUE,0.00),

-- Additional clean customers to bulk up volume
('Diana','Foster','diana.foster@health.com','+1-612-555-0801','1989-01-20','Minneapolis','USA','2021-02-14',TRUE,31200.00),
('Ethan','Brooks','ethan.brooks@media.com','+1-615-555-0802','1994-07-08','Nashville','USA','2022-06-20',TRUE,14500.00),
('Fiona','Walsh','fiona.walsh@ie.com','+353-1-555-0803','1991-03-15','Dublin','Ireland','2021-08-05',TRUE,22800.00),
('George','Nakamura','g.nakamura@tech.jp','+81-6-555-0804','1987-11-28','Osaka','Japan','2020-11-17',TRUE,58900.00),
('Hannah','Singh','hannah.singh@edu.ca','+1-416-555-0805','1993-09-04','Toronto','Canada','2022-01-28',TRUE,17300.00),
('Ivan','Petrov','ivan.petrov@ru.com','+7-812-555-0806','1985-06-12','St. Petersburg','Russia','2021-04-09',TRUE,29600.00),
('Julia','Schneider','julia.schneider@de.com','+49-211-555-0807','1992-02-22','Düsseldorf','Germany','2022-05-14',TRUE,21400.00),
('Kevin','O''Brien','kevin.obrien@au.com','+61-2-555-0808','1988-08-16','Sydney','Australia','2021-09-30',TRUE,43700.00),
('Laura','Rossi','laura.rossi@italy.it','+39-06-555-0809','1990-05-03','Rome','Italy','2022-02-11',TRUE,18900.00),
('Mohammed','Al-Farsi','m.alfarsi@om.com','+968-24-555-0810','1986-12-19','Muscat','Oman','2020-07-25',TRUE,62400.00),
('Natasha','Ivanova','n.ivanova@ru.com','+7-383-555-0811','1994-04-07','Novosibirsk','Russia','2021-12-03',TRUE,11200.00),
('Oscar','Fernandez','o.fernandez@esp.com','+34-91-555-0812','1991-10-25','Madrid','Spain','2022-04-16',TRUE,25100.00),
('Paula','Moreira','p.moreira@br.com','+55-11-555-0813','1987-07-14','São Paulo','Brazil','2021-06-08',TRUE,34600.00),
('Quinn','MacLeod','q.macleod@sc.uk','+44-131-555-0814','1989-03-29','Edinburgh','UK','2020-10-22',TRUE,47800.00),
('Rosa','Sanchez','rosa.sanchez@mx.com','+52-33-555-0815','1993-01-11','Guadalajara','Mexico','2022-07-03',TRUE,13900.00);
"""

    # ── PRODUCTS (40 rows) ────────────────────────────────────────────────────
    # Issues: price < cost (negative margin), 0 stock + active, inconsistent categories, nulls
    products_sql = """
INSERT INTO demo.products (sku,name,category,subcategory,price,cost,stock_quantity,is_active,created_at) VALUES
-- Clean products
('LAPTOP-PRO-001','ProBook 15 Laptop','Electronics','Computers',1299.99,780.00,45,TRUE,'2021-01-15'),
('PHONE-X-001','SmartX Pro Smartphone','Electronics','Phones',899.99,420.00,120,TRUE,'2021-02-01'),
('TABLET-A-001','TabPad Air 10"','Electronics','Tablets',549.99,280.00,60,TRUE,'2021-02-15'),
('HDPHONE-S-001','SoundMax ANC Headphones','Electronics','Audio',249.99,110.00,200,TRUE,'2021-03-01'),
('CHAIR-E-001','ErgoWork Pro Chair','Furniture','Seating',449.00,180.00,30,TRUE,'2021-03-15'),
('DESK-S-001','StandUp Pro Desk','Furniture','Desks',799.00,320.00,15,TRUE,'2021-04-01'),
('MON-4K-001','ViewPro 27" 4K Monitor','Electronics','Displays',599.99,280.00,55,TRUE,'2021-04-15'),
('KB-MECH-001','MechaType Pro Keyboard','Electronics','Accessories',149.99,60.00,300,TRUE,'2021-05-01'),
('MOUSE-W-001','PrecisionClick Wireless Mouse','Electronics','Accessories',79.99,28.00,500,TRUE,'2021-05-15'),
('CAM-4K-001','ClearView 4K Webcam','Electronics','Accessories',119.99,48.00,150,TRUE,'2021-06-01'),
('LAPTOP-UB-001','UltraSlim 13" Laptop','Electronics','Computers',1099.99,590.00,35,TRUE,'2021-06-15'),
('PHONE-B-001','BaseLine Phone','Electronics','Phones',299.99,140.00,220,TRUE,'2021-07-01'),
('SPEAKER-B-001','BoomPod Bluetooth Speaker','Electronics','Audio',89.99,38.00,175,TRUE,'2021-07-15'),
('DOCK-U-001','USB-C 12-Port Docking Station','Electronics','Accessories',199.99,88.00,80,TRUE,'2021-08-01'),
('CHAIR-M-001','MeshBack Task Chair','Furniture','Seating',329.00,145.00,50,TRUE,'2021-08-15'),

-- ⚠️ NEGATIVE MARGIN PRODUCTS (price < cost — losing money on every sale)
('LAPTOP-GM-001','GamerX RTX Laptop','Electronics','Computers',999.99,1250.00,22,TRUE,'2021-09-01'),       -- costs $250 more than sells for
('TV-OLED-001','55" OLED Smart TV','Electronics','Displays',799.00,1100.00,8,TRUE,'2021-09-15'),           -- costs $301 more
('PHONE-P-001','FlexiFold Phone','Electronics','Phones',1299.99,1450.00,15,TRUE,'2021-10-01'),             -- costs $150 more
('CHAIR-L-001','Luxury Leather Chair','Furniture','Seating',599.00,820.00,12,TRUE,'2021-10-15'),           -- costs $221 more
('SERVER-R-001','RackPro 2U Server','Electronics','Servers',2999.00,3400.00,5,TRUE,'2021-11-01'),          -- costs $401 more

-- ⚠️ ZERO/NEGATIVE STOCK BUT ACTIVE (ghost inventory — customers can "buy" unavailable items)
('TABLET-P-001','ProTab 12" Tablet','Electronics','Tablets',699.99,320.00,0,TRUE,'2021-11-15'),            -- 0 stock, still active
('KB-SLIM-001','SlimType Wireless Keyboard','Electronics','Accessories',69.99,28.00,0,TRUE,'2021-12-01'),  -- 0 stock
('MOUSE-G-001','GamerPro RGB Mouse','Electronics','Accessories',129.99,52.00,0,TRUE,'2021-12-15'),         -- 0 stock
('MONITOR-C-001','CurvedVue 34" Monitor','Electronics','Displays',899.00,420.00,-5,TRUE,'2022-01-01'),     -- NEGATIVE stock (impossible)
('DESK-G-001','GlassTop Executive Desk','Furniture','Desks',1199.00,550.00,-3,TRUE,'2022-01-15'),          -- NEGATIVE stock

-- ⚠️ INCONSISTENT CATEGORY NAMES (case & spelling variations — breaks filtering)
('CABLE-H-001','HDMI 2.1 Cable 2m','electronics','Accessories',19.99,5.00,1000,TRUE,'2022-02-01'),        -- lowercase category
('ADAPTER-U-001','USB-A to USB-C Adapter','ELECTRONICS','Accessories',14.99,4.00,800,TRUE,'2022-02-15'),  -- ALL CAPS category
('HUB-U-001','4-Port USB Hub','Elec.','accessories',24.99,8.00,600,TRUE,'2022-03-01'),                    -- abbreviated + lowercase subcategory
('STAND-L-001','Laptop Stand Adjustable','Furniture ','Accessories',49.99,18.00,250,TRUE,'2022-03-15'),   -- trailing space in category
('BAG-L-001','Laptop Backpack 15"','furniture','Bags',79.99,32.00,180,TRUE,'2022-04-01'),                 -- lowercase Furniture

-- ⚠️ NULL CATEGORIES (unclassified products — can''t be searched or filtered)
('CHARGER-W-001','45W USB-C Charger',NULL,NULL,49.99,18.00,400,TRUE,'2022-04-15'),
('CABLE-L-001','Lightning to USB-C Cable',NULL,'Cables',9.99,2.50,2000,TRUE,'2022-05-01'),
('STAND-P-001','Phone Stand Adjustable',NULL,NULL,24.99,9.00,350,TRUE,'2022-05-15'),

-- Additional clean products
('SSD-E-001','2TB External SSD','Electronics','Storage',179.99,82.00,90,TRUE,'2022-06-01'),
('RAM-D-001','32GB DDR5 RAM Kit','Electronics','Components',159.99,70.00,120,TRUE,'2022-06-15'),
('ROUTER-W-001','Wi-Fi 6E Mesh Router','Electronics','Networking',299.99,130.00,65,TRUE,'2022-07-01'),
('LIGHT-R-001','LED Ring Light 18"','Electronics','Accessories',89.99,35.00,210,TRUE,'2022-07-15'),
('PAD-D-001','XL Desk Pad','Furniture','Accessories',39.99,14.00,450,TRUE,'2022-08-01');
"""

    # ── ORDERS (130 rows) ─────────────────────────────────────────────────────
    # Issues: totals don't match items, stale pending orders, outlier values
    orders_sql = """
INSERT INTO demo.orders (customer_id,order_date,status,total_amount,currency,payment_method) VALUES
(1, NOW()-INTERVAL '400 days','completed', 1299.99,'USD','credit_card'),
(1, NOW()-INTERVAL '300 days','completed', 2149.98,'USD','credit_card'),
(1, NOW()-INTERVAL '180 days','completed',  899.99,'USD','credit_card'),
(1, NOW()-INTERVAL '60 days', 'completed', 1499.99,'USD','credit_card'),
(2, NOW()-INTERVAL '380 days','completed', 1349.98,'USD','paypal'),
(2, NOW()-INTERVAL '250 days','completed',  599.99,'USD','paypal'),
(2, NOW()-INTERVAL '90 days', 'completed', 2099.98,'USD','paypal'),
(3, NOW()-INTERVAL '350 days','completed',  799.00,'EUR','bank_transfer'),
(3, NOW()-INTERVAL '200 days','completed', 1449.99,'EUR','credit_card'),
(3, NOW()-INTERVAL '30 days', 'shipped',    449.00,'EUR','credit_card'),
(4, NOW()-INTERVAL '320 days','completed',  129.99,'USD','paypal'),
(4, NOW()-INTERVAL '150 days','completed',  599.99,'USD','credit_card'),
(5, NOW()-INTERVAL '280 days','completed', 3299.97,'GBP','credit_card'),
(5, NOW()-INTERVAL '100 days','shipped',   1199.99,'GBP','credit_card'),
(5, NOW()-INTERVAL '10 days', 'processing', 449.00,'GBP','credit_card'),
(6, NOW()-INTERVAL '260 days','completed', 1899.98,'EUR','credit_card'),
(6, NOW()-INTERVAL '70 days', 'completed',  719.00,'EUR','wire_transfer'),
(7, NOW()-INTERVAL '240 days','completed',  889.97,'JPY','credit_card'),
(7, NOW()-INTERVAL '50 days', 'completed', 2249.98,'JPY','credit_card'),
(8, NOW()-INTERVAL '220 days','completed', 1649.98,'EUR','credit_card'),
(8, NOW()-INTERVAL '40 days', 'shipped',    599.99,'EUR','debit_card'),
(9, NOW()-INTERVAL '200 days','completed',  799.99,'INR','upi'),
(9, NOW()-INTERVAL '20 days', 'processing', 249.99,'INR','upi'),
(10,NOW()-INTERVAL '180 days','completed', 1499.98,'EUR','credit_card'),
(10,NOW()-INTERVAL '15 days', 'pending',    799.00,'EUR','credit_card'),
(11,NOW()-INTERVAL '160 days','completed', 1199.99,'USD','credit_card'),
(11,NOW()-INTERVAL '5 days',  'processing', 679.99,'USD','credit_card'),
(12,NOW()-INTERVAL '140 days','completed',  449.00,'GBP','paypal'),
(13,NOW()-INTERVAL '130 days','completed', 2799.97,'USD','credit_card'),
(14,NOW()-INTERVAL '120 days','completed', 1049.98,'JPY','credit_card'),
(15,NOW()-INTERVAL '110 days','completed',  899.99,'JPY','credit_card'),
(16,NOW()-INTERVAL '100 days','completed', 1599.98,'EUR','credit_card'),
(17,NOW()-INTERVAL '95 days', 'completed',  599.99,'MXN','credit_card'),
(18,NOW()-INTERVAL '88 days', 'completed', 1899.99,'JPY','bank_transfer'),
(19,NOW()-INTERVAL '82 days', 'completed',  749.98,'INR','upi'),
(20,NOW()-INTERVAL '75 days', 'completed', 2299.99,'EUR','credit_card'),
(21,NOW()-INTERVAL '68 days', 'completed',  999.99,'USD','credit_card'),
(22,NOW()-INTERVAL '62 days', 'completed', 1149.98,'USD','debit_card'),
(23,NOW()-INTERVAL '55 days', 'shipped',   1399.99,'EUR','credit_card'),
(24,NOW()-INTERVAL '48 days', 'shipped',    679.99,'ILS','credit_card'),
(25,NOW()-INTERVAL '42 days', 'completed', 1799.97,'USD','credit_card'),
(26,NOW()-INTERVAL '35 days', 'processing', 549.99,'AUD','credit_card'),
(27,NOW()-INTERVAL '28 days', 'processing', 899.99,'CAD','credit_card'),
(28,NOW()-INTERVAL '22 days', 'shipped',   2099.98,'EUR','wire_transfer'),
(29,NOW()-INTERVAL '16 days', 'pending',    449.99,'BRL','credit_card'),
(30,NOW()-INTERVAL '8 days',  'pending',    799.00,'GBP','credit_card'),
-- ⚠️ HIGH-VALUE OUTLIER ORDERS (potential fraud or data entry errors)
(3, NOW()-INTERVAL '45 days', 'completed',52499.00,'EUR','wire_transfer'),   -- $52k single order
(5, NOW()-INTERVAL '25 days', 'shipped',  38750.00,'GBP','wire_transfer'),   -- $38k single order
-- ⚠️ STALE PENDING ORDERS (pending for 2+ years — forgotten? system error?)
(1, NOW()-INTERVAL '800 days','pending',    299.99,'USD','credit_card'),
(2, NOW()-INTERVAL '750 days','pending',    149.99,'USD','paypal'),
(4, NOW()-INTERVAL '720 days','pending',    899.99,'USD','credit_card'),
-- ⚠️ MISMATCHED TOTALS (total_amount doesn't match order_items — accounting issue)
(7, NOW()-INTERVAL '33 days', 'completed', 9999.99,'USD','credit_card'),   -- total will be wrong vs items
(8, NOW()-INTERVAL '18 days', 'completed',    0.01,'EUR','credit_card'),   -- suspiciously low total
(11,NOW()-INTERVAL '12 days', 'processing',  -50.00,'USD','credit_card'),  -- NEGATIVE order total
-- More regular orders
(12,NOW()-INTERVAL '44 days', 'completed', 1249.98,'GBP','credit_card'),
(13,NOW()-INTERVAL '38 days', 'completed',  799.99,'USD','paypal'),
(14,NOW()-INTERVAL '27 days', 'completed', 1699.99,'JPY','credit_card'),
(15,NOW()-INTERVAL '19 days', 'shipped',    549.99,'JPY','credit_card'),
(16,NOW()-INTERVAL '14 days', 'processing', 349.00,'EUR','debit_card'),
(17,NOW()-INTERVAL '9 days',  'pending',    129.99,'MXN','paypal'),
(18,NOW()-INTERVAL '6 days',  'pending',    449.99,'JPY','credit_card'),
(19,NOW()-INTERVAL '3 days',  'processing', 899.99,'INR','upi'),
(20,NOW()-INTERVAL '1 day',   'pending',    249.99,'EUR','credit_card');
"""

    # ── ORDER ITEMS ───────────────────────────────────────────────────────────
    order_items_sql = """
INSERT INTO demo.order_items (order_id,product_id,quantity,unit_price,discount) VALUES
-- Orders 1-30: regular purchases
(1,1,1,1299.99,0),
(2,1,1,1299.99,0),(2,9,1,79.99,0),(2,8,1,149.99,0),(2,10,1,119.99,0),
(3,2,1,899.99,0),
(4,11,1,1099.99,0),(4,9,1,79.99,0),(4,8,1,149.99,0),(4,10,1,119.99,0),
(5,1,1,1299.99,0),(5,8,1,149.99,10),
(6,2,1,899.99,0),(6,7,1,599.99,0),
(7,3,1,549.99,0),(7,7,1,599.99,0),(7,4,1,249.99,0),
(8,6,1,799.00,0),
(9,1,1,1299.99,0),(9,7,1,599.99,0),(9,4,1,249.99,0),
(10,5,1,449.00,0),
(11,1,1,1299.99,0),(11,4,1,249.99,0),(11,10,1,119.99,0),
(12,2,1,599.99,0),
(13,1,1,1299.99,0),(13,7,1,599.99,0),(13,6,1,799.00,0),
(14,3,1,549.99,0),(14,4,1,249.99,0),(14,8,1,149.99,0),
(15,2,1,899.99,0),
(16,1,1,1299.99,0),(16,7,1,599.99,0),
(17,6,1,719.00,0),
(18,1,1,1299.99,0),(18,9,1,79.99,0),(18,8,1,149.99,0),
(19,1,1,1299.99,0),(19,7,1,599.99,0),(19,4,1,249.99,0),(19,9,1,79.99,0),
(20,1,1,1299.99,0),(20,8,1,149.99,0),
(21,2,1,799.99,0),
(22,2,1,899.99,0),(22,8,1,149.99,0),
(23,1,1,1299.99,0),(23,10,1,119.99,0),
(24,6,1,679.99,0),
(25,1,1,1299.99,0),(25,7,1,599.99,0),(25,9,1,79.99,0),
(26,3,1,549.99,0),
(27,2,1,899.99,0),
(28,1,1,1299.99,0),(28,7,1,599.99,0),(28,10,1,119.99,0),
(29,5,1,449.99,0),
(30,6,1,799.00,0),
-- Orders 31-46: more regular purchases
(31,2,1,899.99,0),
(32,1,1,1299.99,0),(32,7,1,599.99,0),
(33,2,1,599.99,0),
(34,1,1,1299.99,0),(34,7,1,599.99,0),
(35,4,1,249.99,0),(35,9,1,79.99,0),(35,8,1,149.99,0),
(36,1,1,1299.99,0),(36,7,1,599.99,0),(36,4,1,249.99,0),
(37,2,1,899.99,0),
(38,1,1,1099.99,0),(38,8,1,149.99,0),
(39,1,1,1299.99,0),(39,10,1,119.99,0),
(40,6,1,679.99,0),
(41,1,1,1299.99,0),(41,7,1,599.99,0),(41,9,1,79.99,0),
(42,3,1,549.99,0),
(43,2,1,899.99,0),
(44,1,1,1299.99,0),(44,7,1,599.99,0),(44,10,1,119.99,0),
(45,5,1,449.99,0),
(46,6,1,799.00,0),
-- Orders 47-48: ⚠️ HIGH-VALUE outlier orders (bulk purchases)
(47,1,10,1299.99,0),(47,7,5,599.99,0),(47,6,3,799.00,0),(47,2,8,899.99,0),
(48,1,15,1299.99,0),(48,7,8,599.99,0),(48,3,5,549.99,0),
-- Orders 49-51: ⚠️ STALE PENDING orders (2+ years old)
(49,8,1,149.99,0),(50,9,1,79.99,0),(51,2,1,899.99,0),
-- Order 52: ⚠️ MISMATCHED TOTAL (order says $9999.99, items only $249.99)
(52,4,1,249.99,0),
-- Order 53: ⚠️ MISMATCHED TOTAL (order says $0.01, items are $1299.99)
(53,1,1,1299.99,0),
-- Order 54: ⚠️ NEGATIVE TOTAL (-$50) with real items worth $229.98
(54,9,1,79.99,0),(54,8,1,149.99,0),
-- Orders 55-63: regular tail orders
(55,1,1,1249.98,0),
(56,2,1,799.99,0),
(57,1,1,1299.99,0),(57,7,1,599.99,0),
(58,3,1,549.99,0),
(59,4,1,249.99,0),(59,2,1,299.99,0),
(60,9,1,129.99,0),
(61,4,1,249.99,0),(61,10,1,119.99,0),
(62,2,1,899.99,0),
(63,4,1,249.99,0);
"""

    # ── EMPLOYEES (28 rows) ───────────────────────────────────────────────────
    # Issues: dept inconsistencies, salary outliers, null fields, self-referencing manager
    employees_sql = """
INSERT INTO demo.employees (name,email,department,role,salary,hire_date,manager_id,is_active) VALUES
-- Leadership
('Sandra Mills',     'sandra.mills@acme.com',    'Executive',   'CEO',               320000,'2015-01-05',NULL,TRUE),
('Richard Grant',    'richard.grant@acme.com',   'Executive',   'CFO',               280000,'2015-03-20',1,   TRUE),
('Patricia Osei',    'patricia.osei@acme.com',   'Executive',   'COO',               275000,'2016-06-01',1,   TRUE),
-- Engineering
('Sarah Connor',     'sarah.connor@acme.com',    'Engineering', 'VP Engineering',    185000,'2017-04-10',3,   TRUE),
('John Reese',       'john.reese@acme.com',       'Engineering', 'Senior Engineer',   145000,'2018-09-15',4,   TRUE),
('Priya Patel',      NULL,                        'Engineering', 'Data Engineer',     130000,'2020-02-28',4,   TRUE),   -- ⚠️ null email
('Carlos Mendez',    'carlos.mendez@acme.com',   'Engineering', 'Backend Engineer',  128000,'2021-05-17',4,   TRUE),
('Zoe Harrison',     'zoe.harrison@acme.com',    'engineering', 'Frontend Engineer', 122000,'2021-08-03',4,   TRUE),   -- ⚠️ lowercase dept
('Dev Nair',         'dev.nair@acme.com',         'Eng',         'DevOps Engineer',   135000,'2022-01-10',4,   TRUE),   -- ⚠️ abbreviated dept
('Lily Chen',        'lily.chen@acme.com',        'Software Engineering','ML Engineer',140000,'2022-06-20',4,   TRUE), -- ⚠️ inconsistent dept name
-- Marketing
('Maria Santos',     'maria.santos@acme.com',    'Marketing',   'VP Marketing',      168000,'2017-11-20',3,   TRUE),
('Tom Bradley',      'tom.bradley@acme.com',      'Marketing',   'Marketing Director',135000,'2019-02-14',11,  TRUE),
('Nina Rodriguez',   'nina.r@acme.com',           'Marketing',   'Campaign Manager',  105000,'2021-03-08',11,  TRUE),
('Ryan Park',        'ryan.park@acme.com',        'marketing',   'SEO Specialist',     82000,'2022-04-25',11,  TRUE),  -- ⚠️ lowercase dept
-- Sales
('Daniel Foster',    'daniel.foster@acme.com',   'Sales',       'VP Sales',          175000,'2018-01-15',3,   TRUE),
('Amber Collins',    'amber.collins@acme.com',   'Sales',       'Sales Manager',     118000,'2019-07-22',15,  TRUE),
('Ethan Brooks',     'ethan.brooks@acme.com',    'Sales',       'Account Executive',  88000,'2021-02-11',15,  TRUE),
('Hana Yamamoto',    'hana.yamamoto@acme.com',   'SALES',       'Account Executive',  85000,'2022-09-05',15,  TRUE),  -- ⚠️ uppercase dept
-- Finance & HR
('Lisa Chen',        'lisa.chen@acme.com',        'Finance',     'Finance Manager',   132000,'2018-05-30',2,   TRUE),
('Alex Kim',         'alex.kim@acme.com',         'HR',          'HR Manager',        108000,'2019-09-22',3,   TRUE),
('Vanessa Scott',    'vanessa.scott@acme.com',   'HR',          'Recruiter',          75000,'2022-03-14',20,  TRUE),
-- ⚠️ DATA QUALITY ISSUES
('Mark Davis',       'mark.davis@acme.com',       NULL,          'Product Manager',   125000,NULL,         3,   TRUE),  -- null dept + hire date
('Owen Fletcher',    NULL,                        'Engineering', 'QA Engineer',        92000,'2021-11-30',4,   TRUE),   -- null email
('S. Mills',          's.mills@acme.com',          'Executive',   'CEO',               320000,'2015-01-05',NULL,TRUE),   -- ⚠️ duplicate of Sandra Mills (same role, diff email format)
('Test Intern',       'intern@acme.com',           'Engineering', 'Intern',                0,'2023-06-01',4,   TRUE),  -- ⚠️ zero salary
('Ghost Employee',   'ghost@acme.com',            'Engineering', 'Engineer',          110000,'2026-01-01',4,   TRUE),  -- ⚠️ future hire date
('D. Nair',          'd.nair@acme.com',           'Eng',         'DevOps Engineer',   135000,'2022-01-10',4,   TRUE),  -- ⚠️ duplicate of Dev Nair (same dept/role/salary)
('P. Osei',          'p.osei@acme.com',           'Executive',   'COO',               275000,'2016-06-01',1,   TRUE);  -- ⚠️ duplicate of Patricia Osei
"""

    # ── EVENTS (250 rows) ─────────────────────────────────────────────────────
    # Issues: unattributed (null customer_id), bot patterns, bad country codes
    events_sql = """
INSERT INTO demo.events (customer_id,event_type,event_timestamp,session_id,page_url,device_type,country_code) VALUES
-- Real customer journey: Alice (customer 1) completes purchase
(1,'page_view',   NOW()-INTERVAL '5 days 4 hours',   'sess_a001','/home',         'desktop','US'),
(1,'page_view',   NOW()-INTERVAL '5 days 3.9 hours', 'sess_a001','/products',     'desktop','US'),
(1,'search',      NOW()-INTERVAL '5 days 3.8 hours', 'sess_a001','/search?q=laptop','desktop','US'),
(1,'page_view',   NOW()-INTERVAL '5 days 3.7 hours', 'sess_a001','/products/1',   'desktop','US'),
(1,'add_to_cart', NOW()-INTERVAL '5 days 3.6 hours', 'sess_a001','/products/1',   'desktop','US'),
(1,'page_view',   NOW()-INTERVAL '5 days 3.5 hours', 'sess_a001','/cart',         'desktop','US'),
(1,'checkout',    NOW()-INTERVAL '5 days 3.4 hours', 'sess_a001','/checkout',     'desktop','US'),
(1,'purchase',    NOW()-INTERVAL '5 days 3.3 hours', 'sess_a001','/checkout/done','desktop','US'),
-- Emma (customer 2) — mobile browsing, cart abandonment
(2,'page_view',   NOW()-INTERVAL '3 days 2 hours',   'sess_b001','/home',         'mobile', 'GB'),
(2,'page_view',   NOW()-INTERVAL '3 days 1.9 hours', 'sess_b001','/products',     'mobile', 'GB'),
(2,'page_view',   NOW()-INTERVAL '3 days 1.8 hours', 'sess_b001','/products/2',   'mobile', 'GB'),
(2,'add_to_cart', NOW()-INTERVAL '3 days 1.7 hours', 'sess_b001','/products/2',   'mobile', 'GB'),
(2,'page_view',   NOW()-INTERVAL '3 days 1.6 hours', 'sess_b001','/cart',         'mobile', 'GB'),
(2,'abandon_cart',NOW()-INTERVAL '3 days 1.5 hours', 'sess_b001','/cart',         'mobile', 'GB'),  -- abandoned!
-- Henry (customer 3) — high-value customer, repeat visits
(3,'login',       NOW()-INTERVAL '10 days',          'sess_c001','/login',        'desktop','US'),
(3,'page_view',   NOW()-INTERVAL '10 days',          'sess_c001','/products',     'desktop','US'),
(3,'purchase',    NOW()-INTERVAL '10 days',          'sess_c001','/checkout/done','desktop','US'),
(3,'login',       NOW()-INTERVAL '7 days',           'sess_c002','/login',        'desktop','US'),
(3,'page_view',   NOW()-INTERVAL '7 days',           'sess_c002','/account',      'desktop','US'),
(3,'support',     NOW()-INTERVAL '7 days',           'sess_c002','/support',      'desktop','US'),
-- Various customers
(4,'page_view',   NOW()-INTERVAL '2 days',           'sess_d001','/home',         'tablet', 'DE'),
(4,'search',      NOW()-INTERVAL '2 days',           'sess_d001','/search?q=chair','tablet','DE'),
(4,'page_view',   NOW()-INTERVAL '2 days',           'sess_d001','/products/5',   'tablet', 'DE'),
(5,'login',       NOW()-INTERVAL '1 day',            'sess_e001','/login',        'desktop','GB'),
(5,'page_view',   NOW()-INTERVAL '1 day',            'sess_e001','/account',      'desktop','GB'),
(5,'page_view',   NOW()-INTERVAL '1 day',            'sess_e001','/orders',       'desktop','GB'),
(6,'page_view',   NOW()-INTERVAL '6 hours',          'sess_f001','/home',         'mobile', 'FR'),
(6,'add_to_cart', NOW()-INTERVAL '5.5 hours',        'sess_f001','/products/7',   'mobile', 'FR'),
(6,'purchase',    NOW()-INTERVAL '5 hours',          'sess_f001','/checkout/done','mobile', 'FR'),
(7,'page_view',   NOW()-INTERVAL '4 hours',          'sess_g001','/home',         'desktop','JP'),
(7,'search',      NOW()-INTERVAL '3.9 hours',        'sess_g001','/search?q=monitor','desktop','JP'),
(7,'page_view',   NOW()-INTERVAL '3.8 hours',        'sess_g001','/products/7',   'desktop','JP'),
(7,'add_to_cart', NOW()-INTERVAL '3.7 hours',        'sess_g001','/products/7',   'desktop','JP'),
(7,'purchase',    NOW()-INTERVAL '3.5 hours',        'sess_g001','/checkout/done','desktop','JP'),
(8,'login',       NOW()-INTERVAL '2 hours',          'sess_h001','/login',        'tablet', 'IN'),
(8,'page_view',   NOW()-INTERVAL '1.9 hours',        'sess_h001','/products',     'tablet', 'IN'),
(9,'page_view',   NOW()-INTERVAL '1 hour',           'sess_i001','/home',         'mobile', 'FR'),
(10,'login',      NOW()-INTERVAL '30 minutes',       'sess_j001','/login',        'desktop','MX'),
-- ⚠️ UNATTRIBUTED EVENTS — null customer_id (30% of traffic not linked to any user)
(NULL,'page_view',  NOW()-INTERVAL '12 hours',       'sess_anon01','/home',       'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '12 hours',       'sess_anon01','/products',   'desktop','US'),
(NULL,'search',     NOW()-INTERVAL '11.9 hours',     'sess_anon01','/search?q=laptop','desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '11 hours',       'sess_anon02','/home',       'mobile', 'DE'),
(NULL,'page_view',  NOW()-INTERVAL '11 hours',       'sess_anon02','/products',   'mobile', 'DE'),
(NULL,'page_view',  NOW()-INTERVAL '10 hours',       'sess_anon03','/home',       'mobile', 'BR'),
(NULL,'page_view',  NOW()-INTERVAL '9 hours',        'sess_anon04','/home',       'desktop','CA'),
(NULL,'search',     NOW()-INTERVAL '9 hours',        'sess_anon04','/search?q=chair','desktop','CA'),
(NULL,'page_view',  NOW()-INTERVAL '8 hours',        'sess_anon05','/home',       'tablet', 'AU'),
(NULL,'page_view',  NOW()-INTERVAL '8 hours',        'sess_anon06','/home',       'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '7 hours',        'sess_anon07','/home',       'mobile', 'JP'),
(NULL,'add_to_cart',NOW()-INTERVAL '7 hours',        'sess_anon07','/products/2', 'mobile', 'JP'),
(NULL,'page_view',  NOW()-INTERVAL '6 hours',        'sess_anon08','/home',       'desktop','IN'),
(NULL,'page_view',  NOW()-INTERVAL '6 hours',        'sess_anon09','/products',   'mobile', 'MX'),
(NULL,'page_view',  NOW()-INTERVAL '5 hours',        'sess_anon10','/home',       'desktop','FR'),
-- ⚠️ INVALID COUNTRY CODES (non-ISO codes — breaks geo-analytics)
(NULL,'page_view',  NOW()-INTERVAL '4 hours',        'sess_bot01', '/home',       'desktop','XX'),   -- invalid
(NULL,'page_view',  NOW()-INTERVAL '4 hours',        'sess_bot02', '/home',       'desktop','ZZ'),   -- invalid
(1,   'page_view',  NOW()-INTERVAL '3 hours',        'sess_x01',   '/home',       'desktop','USA'),  -- 3-letter instead of 2-letter
(2,   'page_view',  NOW()-INTERVAL '3 hours',        'sess_x02',   '/home',       'mobile', 'GBR'),  -- 3-letter
-- ⚠️ SUSPECTED BOT TRAFFIC — same session, 50 identical events in rapid succession
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
(NULL,'page_view',  NOW()-INTERVAL '2 hours',        'sess_bot99', '/products',   'desktop','US'),
-- ⚠️ FUTURE EVENT TIMESTAMPS
(1,'page_view',     NOW()+INTERVAL '2 days',         'sess_fut01', '/home',       'desktop','US'),  -- future
(3,'purchase',      NOW()+INTERVAL '5 days',         'sess_fut02', '/checkout',   'desktop','US'),  -- future
-- More normal events to bulk up volume
(11,'page_view',    NOW()-INTERVAL '8 days',         'sess_k001','/home',         'desktop','US'),
(11,'purchase',     NOW()-INTERVAL '8 days',         'sess_k001','/checkout/done','desktop','US'),
(12,'page_view',    NOW()-INTERVAL '9 days',         'sess_l001','/home',         'mobile', 'GB'),
(13,'login',        NOW()-INTERVAL '11 days',        'sess_m001','/login',        'desktop','US'),
(13,'purchase',     NOW()-INTERVAL '11 days',        'sess_m001','/checkout/done','desktop','US'),
(14,'page_view',    NOW()-INTERVAL '13 days',        'sess_n001','/home',         'tablet', 'JP'),
(15,'search',       NOW()-INTERVAL '14 days',        'sess_o001','/search',       'desktop','JP'),
(16,'page_view',    NOW()-INTERVAL '15 days',        'sess_p001','/home',         'desktop','ES'),
(17,'page_view',    NOW()-INTERVAL '16 days',        'sess_q001','/home',         'mobile', 'MX'),
(18,'login',        NOW()-INTERVAL '17 days',        'sess_r001','/login',        'desktop','JP'),
(19,'page_view',    NOW()-INTERVAL '18 days',        'sess_s001','/home',         'tablet', 'IN'),
(20,'search',       NOW()-INTERVAL '19 days',        'sess_t001','/search',       'desktop','FR'),
(21,'purchase',     NOW()-INTERVAL '20 days',        'sess_u001','/checkout/done','desktop','US'),
(22,'page_view',    NOW()-INTERVAL '21 days',        'sess_v001','/home',         'mobile', 'US'),
(23,'add_to_cart',  NOW()-INTERVAL '22 days',        'sess_w001','/products/1',   'desktop','FR'),
(24,'page_view',    NOW()-INTERVAL '23 days',        'sess_x01b','/home',         'desktop','IL'),
(25,'purchase',     NOW()-INTERVAL '24 days',        'sess_y001','/checkout/done','desktop','US'),
(26,'page_view',    NOW()-INTERVAL '25 days',        'sess_z001','/home',         'mobile', 'AU'),
(27,'login',        NOW()-INTERVAL '26 days',        'sess_aa01','/login',        'desktop','CA'),
(28,'page_view',    NOW()-INTERVAL '27 days',        'sess_bb01','/home',         'tablet', 'DE'),
(29,'search',       NOW()-INTERVAL '28 days',        'sess_cc01','/search',       'mobile', 'BR'),
(30,'page_view',    NOW()-INTERVAL '29 days',        'sess_dd01','/home',         'desktop','GB');
"""

    try:
        with engine.begin() as conn:
            for stmt in truncate_sql.split(";"):
                s = stmt.strip()
                if s:
                    conn.execute(_text(s))
        with engine.begin() as conn:
            conn.execute(_text(customers_sql))
        with engine.begin() as conn:
            conn.execute(_text(products_sql))
        with engine.begin() as conn:
            conn.execute(_text(orders_sql))
        with engine.begin() as conn:
            conn.execute(_text(order_items_sql))
        with engine.begin() as conn:
            conn.execute(_text(employees_sql))
        with engine.begin() as conn:
            conn.execute(_text(events_sql))

        # Count rows
        counts = {}
        with engine.connect() as conn:
            for t in ["customers","products","orders","order_items","employees","events"]:
                counts[t] = conn.execute(_text(f"SELECT COUNT(*) FROM demo.{t}")).scalar()

        return {
            "success": True,
            "row_counts": counts,
            "quality_issues_seeded": {
                "customers": ["9 duplicate records (3 sets)", "5 invalid email formats", "5 null emails", "3 impossible birth dates", "5 inconsistent country formats", "3 zero/negative lifetime values"],
                "products":  ["5 negative-margin products (price < cost)", "5 zero/negative stock but active", "5 inconsistent category names", "3 null categories"],
                "orders":    ["2 high-value outliers ($38k-$52k)", "3 stale pending orders (2+ years old)", "3 mismatched totals (incl. negative)"],
                "employees": ["4 department name inconsistencies", "3 exact duplicate records", "2 null emails", "1 null department", "1 zero salary", "1 future hire date"],
                "events":    ["15 unattributed events (null customer_id)", "4 invalid country codes", "20 suspected bot events (same session)", "2 future timestamps"]
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ── Agents ────────────────────────────────────────────────────────────────────

@app.get("/agents")
async def list_agents():
    return [agent_to_dict(aid) for aid in orchestrator.agents]


@app.get("/agents/{agent_id}")
async def get_agent(agent_id: str):
    if agent_id not in orchestrator.agents:
        raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' not found")
    return agent_to_dict(agent_id)


@app.post("/agents/{agent_id}/start")
async def start_agent(agent_id: str):
    if agent_id not in orchestrator.agents:
        raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' not found")
    success = await orchestrator.start_agent(agent_id)
    await manager.broadcast({
        "type":      "agent_status",
        "agent_id":  agent_id,
        "status":    "active" if success else "error",
        "timestamp": datetime.now().isoformat(),
    })
    return {"success": success, "agent": agent_to_dict(agent_id)}


@app.post("/agents/{agent_id}/stop")
async def stop_agent(agent_id: str):
    if agent_id not in orchestrator.agents:
        raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' not found")
    success = await orchestrator.stop_agent(agent_id)
    await manager.broadcast({
        "type":      "agent_status",
        "agent_id":  agent_id,
        "status":    "inactive" if success else "error",
        "timestamp": datetime.now().isoformat(),
    })
    return {"success": success, "agent": agent_to_dict(agent_id)}


@app.get("/agents/{agent_id}/logs")
async def get_agent_logs(agent_id: str, limit: int = 50):
    if agent_id not in orchestrator.agents:
        raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' not found")
    agent = orchestrator.agents[agent_id]
    return [
        {
            "id":        log.id,
            "level":     log.level,
            "message":   log.message,
            "timestamp": log.timestamp.isoformat(),
            "details":   log.details,
        }
        for log in agent.execution_log[-limit:]
    ]


# ── Tasks ─────────────────────────────────────────────────────────────────────

@app.get("/tasks")
async def list_tasks():
    return list(tasks.values())


@app.post("/tasks")
async def create_task(task_in: TaskCreate, background_tasks: BackgroundTasks):
    if task_in.agent_id not in orchestrator.agents:
        raise HTTPException(status_code=400, detail=f"Agent '{task_in.agent_id}' not found")

    task_id = str(uuid.uuid4())
    task = {
        "id":           task_id,
        "agent_id":     task_in.agent_id,
        "agent_name":   orchestrator.agents[task_in.agent_id].name,
        "title":        task_in.title,
        "description":  task_in.description,
        "table_id":     task_in.table_id,
        "table_name":   task_in.table_name,
        "priority":     task_in.priority,
        "task_data":    task_in.task_data or {},
        "status":       "pending",
        "created_at":   datetime.now().isoformat(),
        "started_at":   None,
        "completed_at": None,
        "result":       None,
        "error":        None,
    }
    tasks[task_id] = task
    await manager.broadcast({"type": "task_created", "task": task})
    background_tasks.add_task(_run_task, task_id, task_in.agent_id, task_in.task_data or {})
    return task


async def _run_task(task_id: str, agent_id: str, task_data: dict):
    task  = tasks.get(task_id)
    agent = orchestrator.agents.get(agent_id)
    if not task or not agent:
        return

    task["status"]     = "in_progress"
    task["started_at"] = datetime.now().isoformat()
    await manager.broadcast({"type": "task_updated", "task": task})

    try:
        result             = await agent.execute_task(task_data)
        task["status"]     = "completed" if result.get("status") == "completed" else "failed"
        task["completed_at"] = datetime.now().isoformat()
        task["result"]     = result.get("result")
        task["error"]      = result.get("error")
    except Exception as e:
        task["status"]     = "failed"
        task["completed_at"] = datetime.now().isoformat()
        task["error"]      = str(e)

    await manager.broadcast({"type": "task_updated", "task": task})


@app.get("/tasks/{task_id}")
async def get_task(task_id: str):
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@app.patch("/tasks/{task_id}")
async def update_task(task_id: str, update: TaskUpdate):
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if update.status:
        task["status"] = update.status
    if update.priority:
        task["priority"] = update.priority
    await manager.broadcast({"type": "task_updated", "task": task})
    return task


@app.delete("/tasks/{task_id}")
async def delete_task(task_id: str):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    task = tasks.pop(task_id)
    await manager.broadcast({"type": "task_deleted", "task_id": task_id})
    return {"deleted": task_id}


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws/logs")
async def websocket_logs(websocket: WebSocket):
    await manager.connect(websocket)
    await websocket.send_json({
        "type":      "init",
        "agents":    [agent_to_dict(aid) for aid in orchestrator.agents],
        "tasks":     list(tasks.values()),
        "timestamp": datetime.now().isoformat(),
    })
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong", "timestamp": datetime.now().isoformat()})
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)
