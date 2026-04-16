import { useState, useEffect, useRef, useCallback } from "react";
import {
  Search, Database, Activity, BarChart3, Network, Shield,
  Plus, Star, CheckCircle, AlertTriangle, Clock, TrendingUp,
  Zap, Bot, FileText, ChevronDown, ChevronRight, Play, Pause,
  RefreshCw, Download, Bell, User, Home, Layers, Target,
  Workflow, X, Filter, Tag, Eye, MessageCircle, ThumbsUp,
  Award, GitBranch, ArrowRight, ArrowLeft, Maximize2, ZoomIn,
  ZoomOut, RotateCcw, Info, Settings, Users, Globe, Lock,
  Bookmark, ExternalLink, AlertCircle, TrendingDown, Hash,
  Calendar, Table, Columns, Key, Link, ChevronUp, MoreHorizontal,
  Check, Circle, ListTodo, Trash2, Send, Wifi, WifiOff
} from "lucide-react";

// ─── BACKEND API CONFIG ───────────────────────────────────────────────────────
const API_BASE = "http://localhost:8000";
const WS_URL   = "ws://localhost:8000/ws/logs";

// Hardcoded fallback — wizard always works even when backend is starting up
const FALLBACK_CONNECTOR_TYPES = [
  {
    type: "postgresql",
    display_name: "PostgreSQL",
    icon: "🐘",
    fields: [
      { name: "host",     label: "Host",     type: "text",     placeholder: "localhost",  required: true  },
      { name: "port",     label: "Port",     type: "number",   placeholder: "5432",       required: true, default: 5432 },
      { name: "database", label: "Database", type: "text",     placeholder: "mydb",       required: true  },
      { name: "username", label: "Username", type: "text",     placeholder: "postgres",   required: true  },
      { name: "password", label: "Password", type: "password", placeholder: "••••••••",  required: true  },
      { name: "sslmode",  label: "SSL Mode", type: "select",   options: ["disable","allow","prefer","require","verify-ca","verify-full"], default: "prefer", required: false },
    ],
  },
  {
    type: "fabric",
    display_name: "Microsoft Fabric",
    icon: "🪟",
    fields: [
      { name: "endpoint", label: "Fabric SQL Endpoint",       type: "text",     placeholder: "<workspace-id>.datawarehouse.fabric.microsoft.com", required: true,  help: "Found in Fabric workspace → SQL Analytics Endpoint → Connection string" },
      { name: "database", label: "Database / Lakehouse Name", type: "text",     placeholder: "MyLakehouse", required: true },
      { name: "auth_mode",label: "Authentication Method",     type: "select",   options: ["sql_auth","service_principal","access_token"], default: "sql_auth", required: true },
      { name: "username", label: "Username",    type: "text",     placeholder: "fabric_user", required: false, show_when: { auth_mode: "sql_auth" } },
      { name: "password", label: "Password",    type: "password", placeholder: "••••••••",   required: false, show_when: { auth_mode: "sql_auth" } },
      { name: "tenant_id",    label: "Tenant ID",     type: "text",     placeholder: "xxxxxxxx-...", required: false, show_when: { auth_mode: "service_principal" } },
      { name: "client_id",    label: "Client ID",     type: "text",     placeholder: "xxxxxxxx-...", required: false, show_when: { auth_mode: "service_principal" } },
      { name: "client_secret",label: "Client Secret", type: "password", placeholder: "••••••••",    required: false, show_when: { auth_mode: "service_principal" } },
      { name: "access_token", label: "Access Token",  type: "password", placeholder: "eyJ0eXAi...", required: false, show_when: { auth_mode: "access_token" } },
    ],
  },
];

async function apiFetch(path, opts = {}) {
  try {
    const res = await fetch(`${API_BASE}${path}`, {
      headers: { "Content-Type": "application/json" },
      ...opts,
    });
    if (!res.ok) throw new Error(`API ${res.status}`);
    return res.json();
  } catch (e) {
    console.warn("API unavailable, using mock data:", e.message);
    return null;
  }
}

// ─── ENTERPRISE MOCK DATA ────────────────────────────────────────────────────

const DOMAINS = ["Finance", "Marketing", "Operations", "HR", "Engineering", "Sales", "Product", "Legal"];
const SCHEMAS = ["public", "finance", "marketing", "ops", "hr", "sales", "analytics", "staging"];

const mockConnections = [
  { id: "c1", name: "Enterprise DW", type: "Snowflake", status: "connected", host: "xy12345.snowflakecomputing.com", db: "PROD_DW", lastSync: "2 mins ago", tables: 847 },
  { id: "c2", name: "Analytics Cluster", type: "BigQuery", status: "connected", host: "bigquery.googleapis.com", db: "analytics-prod", lastSync: "5 mins ago", tables: 312 },
  { id: "c3", name: "Operational DB", type: "PostgreSQL", status: "connected", host: "rds.prod.internal:5432", db: "operations", lastSync: "1 min ago", tables: 203 },
  { id: "c4", name: "Marketing CDP", type: "Redshift", status: "connected", host: "redshift.marketing.internal", db: "cdp", lastSync: "8 mins ago", tables: 156 },
  { id: "c5", name: "Staging Environment", type: "PostgreSQL", status: "warning", host: "rds.staging.internal:5432", db: "staging", lastSync: "2 hours ago", tables: 198 },
  { id: "c6", name: "Data Lake", type: "S3 / Hive", status: "connected", host: "s3://company-data-lake", db: "hive_metastore", lastSync: "15 mins ago", tables: 1204 },
];

const mockTables = [
  {
    id: "t1", name: "customer_master", schema: "public", connection: "Enterprise DW", connectionId: "c1",
    domain: "Sales", records: 4200000, quality: 97, lastProfiled: "3 mins ago",
    description: "Single source of truth for all customer records across the enterprise. Contains PII data, segmentation attributes, and lifecycle stage.",
    owner: "Data Governance Council", steward: "Sarah Chen", tags: ["PII", "Golden Record", "GDPR"],
    trust: "gold", popularity: 4.9, columns: 48, issues: 0,
    upstream: ["t10", "t11"], downstream: ["t3", "t5", "t7", "t8"],
    columns_detail: [
      { name: "customer_id", type: "VARCHAR(36)", nullable: false, pii: false, description: "UUID primary key", quality: 100 },
      { name: "email", type: "VARCHAR(255)", nullable: false, pii: true, description: "Primary contact email", quality: 99 },
      { name: "full_name", type: "VARCHAR(200)", nullable: true, pii: true, description: "Customer full name", quality: 98 },
      { name: "segment", type: "VARCHAR(50)", nullable: true, pii: false, description: "Customer segment classification", quality: 95 },
      { name: "lifetime_value", type: "DECIMAL(15,2)", nullable: true, pii: false, description: "Calculated LTV", quality: 92 },
      { name: "created_at", type: "TIMESTAMP", nullable: false, pii: false, description: "Record creation timestamp", quality: 100 },
    ]
  },
  {
    id: "t2", name: "orders_fact", schema: "finance", connection: "Enterprise DW", connectionId: "c1",
    domain: "Finance", records: 28500000, quality: 91, lastProfiled: "7 mins ago",
    description: "Transactional fact table for all order events. Grain: one row per order line item. Partitioned by order_date.",
    owner: "Finance Analytics", steward: "James Park", tags: ["Financial", "Transactional", "Partitioned"],
    trust: "gold", popularity: 4.8, columns: 34, issues: 2,
    upstream: ["t1", "t4", "t12"], downstream: ["t6", "t9"],
    columns_detail: [
      { name: "order_id", type: "BIGINT", nullable: false, pii: false, description: "Order surrogate key", quality: 100 },
      { name: "customer_id", type: "VARCHAR(36)", nullable: false, pii: false, description: "FK to customer_master", quality: 100 },
      { name: "order_date", type: "DATE", nullable: false, pii: false, description: "Date of order placement", quality: 100 },
      { name: "amount_usd", type: "DECIMAL(12,2)", nullable: false, pii: false, description: "Order total in USD", quality: 97 },
      { name: "status", type: "VARCHAR(30)", nullable: false, pii: false, description: "Order status enum", quality: 99 },
    ]
  },
  {
    id: "t3", name: "customer_360", schema: "analytics", connection: "Analytics Cluster", connectionId: "c2",
    domain: "Marketing", records: 4150000, quality: 88, lastProfiled: "12 mins ago",
    description: "Unified customer view joining behavioral, transactional, and CRM data. Used by Marketing and Sales teams.",
    owner: "Marketing Analytics", steward: "Priya Sharma", tags: ["Derived", "Marketing", "BI-Ready"],
    trust: "silver", popularity: 4.6, columns: 87, issues: 3,
    upstream: ["t1", "t2"], downstream: ["t7", "t13"],
    columns_detail: [
      { name: "customer_id", type: "VARCHAR(36)", nullable: false, pii: false, description: "FK to customer_master", quality: 100 },
      { name: "total_orders", type: "INTEGER", nullable: true, pii: false, description: "Count of all orders", quality: 98 },
      { name: "last_purchase_date", type: "DATE", nullable: true, pii: false, description: "Most recent order date", quality: 95 },
      { name: "predicted_churn_score", type: "FLOAT", nullable: true, pii: false, description: "ML churn probability 0-1", quality: 88 },
    ]
  },
  {
    id: "t4", name: "product_catalog", schema: "public", connection: "Operational DB", connectionId: "c3",
    domain: "Product", records: 125000, quality: 94, lastProfiled: "18 mins ago",
    description: "Master product catalog with pricing, categorization, inventory metadata, and supplier information.",
    owner: "Product Team", steward: "Marcus Liu", tags: ["Catalog", "Inventory", "Core"],
    trust: "gold", popularity: 4.3, columns: 52, issues: 1,
    upstream: ["t14"], downstream: ["t2", "t9"],
    columns_detail: [
      { name: "product_id", type: "VARCHAR(30)", nullable: false, pii: false, description: "Product SKU", quality: 100 },
      { name: "name", type: "VARCHAR(200)", nullable: false, pii: false, description: "Product display name", quality: 99 },
      { name: "category", type: "VARCHAR(100)", nullable: true, pii: false, description: "Top-level category", quality: 96 },
      { name: "price_usd", type: "DECIMAL(10,2)", nullable: false, pii: false, description: "Current list price", quality: 100 },
    ]
  },
  {
    id: "t5", name: "email_campaigns", schema: "marketing", connection: "Marketing CDP", connectionId: "c4",
    domain: "Marketing", records: 890000, quality: 79, lastProfiled: "25 mins ago",
    description: "Email campaign performance data from all marketing platforms. Includes send, open, click, and conversion events.",
    owner: "Marketing Ops", steward: "Nina Rodriguez", tags: ["Marketing", "Campaigns", "PII"],
    trust: "silver", popularity: 3.9, columns: 28, issues: 5,
    upstream: ["t1"], downstream: ["t13"],
    columns_detail: [
      { name: "campaign_id", type: "VARCHAR(50)", nullable: false, pii: false, description: "Campaign identifier", quality: 100 },
      { name: "customer_id", type: "VARCHAR(36)", nullable: true, pii: false, description: "FK to customer_master", quality: 87 },
      { name: "sent_at", type: "TIMESTAMP", nullable: false, pii: false, description: "Send timestamp", quality: 99 },
    ]
  },
  {
    id: "t6", name: "revenue_summary", schema: "finance", connection: "Analytics Cluster", connectionId: "c2",
    domain: "Finance", records: 18250, quality: 99, lastProfiled: "45 mins ago",
    description: "Aggregated daily revenue summary by business unit, region, and product line. Source of record for financial reporting.",
    owner: "Finance Analytics", steward: "James Park", tags: ["Finance", "Aggregated", "Reporting", "SOX"],
    trust: "gold", popularity: 4.7, columns: 18, issues: 0,
    upstream: ["t2"], downstream: [],
    columns_detail: [
      { name: "report_date", type: "DATE", nullable: false, pii: false, description: "Reporting date", quality: 100 },
      { name: "business_unit", type: "VARCHAR(50)", nullable: false, pii: false, description: "Business unit name", quality: 100 },
      { name: "revenue_usd", type: "DECIMAL(18,2)", nullable: false, pii: false, description: "Total revenue", quality: 100 },
      { name: "margin_pct", type: "FLOAT", nullable: true, pii: false, description: "Gross margin %", quality: 99 },
    ]
  },
  {
    id: "t7", name: "sales_pipeline", schema: "sales", connection: "Operational DB", connectionId: "c3",
    domain: "Sales", records: 45600, quality: 83, lastProfiled: "32 mins ago",
    description: "Active and historical sales opportunities from CRM. Includes forecast, stage, and activity data.",
    owner: "Revenue Operations", steward: "Alex Kim", tags: ["CRM", "Pipeline", "Forecast"],
    trust: "silver", popularity: 4.1, columns: 41, issues: 4,
    upstream: ["t1", "t3"], downstream: [],
    columns_detail: []
  },
  {
    id: "t8", name: "hr_employees", schema: "hr", connection: "Operational DB", connectionId: "c3",
    domain: "HR", records: 12400, quality: 96, lastProfiled: "1 hour ago",
    description: "Employee master data including organizational hierarchy, compensation bands, and employment history.",
    owner: "People Analytics", steward: "Dana White", tags: ["PII", "HRIS", "Sensitive", "Restricted"],
    trust: "gold", popularity: 2.8, columns: 63, issues: 0,
    upstream: ["t1"], downstream: [],
    columns_detail: []
  },
  {
    id: "t9", name: "inventory_snapshot", schema: "ops", connection: "Operational DB", connectionId: "c3",
    domain: "Operations", records: 380000, quality: 86, lastProfiled: "20 mins ago",
    description: "Daily inventory snapshot by warehouse, SKU, and location. Enables supply chain analytics.",
    owner: "Supply Chain Analytics", steward: "Tom Nguyen", tags: ["Inventory", "Supply Chain", "Daily Snapshot"],
    trust: "silver", popularity: 3.7, columns: 24, issues: 2,
    upstream: ["t2", "t4"], downstream: [],
    columns_detail: []
  },
  // Hidden/upstream source tables
  { id: "t10", name: "crm_contacts_raw", schema: "staging", connection: "Staging Environment", connectionId: "c5", domain: "Sales", records: 5100000, quality: 74, lastProfiled: "2 hours ago", description: "Raw CRM contacts ingested from Salesforce via Fivetran.", owner: "Data Engineering", steward: "Bob Singh", tags: ["Raw", "Ingestion", "CRM"], trust: "bronze", popularity: 3.1, columns: 22, issues: 8, upstream: [], downstream: ["t1"], columns_detail: [] },
  { id: "t11", name: "ecommerce_users_raw", schema: "staging", connection: "Staging Environment", connectionId: "c5", domain: "Engineering", records: 3800000, quality: 71, lastProfiled: "2.5 hours ago", description: "Raw ecommerce user events from Segment.", owner: "Data Engineering", steward: "Bob Singh", tags: ["Raw", "Ingestion", "Events"], trust: "bronze", popularity: 2.9, columns: 18, issues: 11, upstream: [], downstream: ["t1"], columns_detail: [] },
  { id: "t12", name: "payment_events", schema: "finance", connection: "Data Lake", connectionId: "c6", domain: "Finance", records: 18900000, quality: 89, lastProfiled: "30 mins ago", description: "Payment gateway events from Stripe and Adyen.", owner: "Payments Engineering", steward: "Carol Tan", tags: ["Payments", "PCI", "Events"], trust: "silver", popularity: 3.8, columns: 29, issues: 2, upstream: [], downstream: ["t2"], columns_detail: [] },
  { id: "t13", name: "marketing_attribution", schema: "marketing", connection: "Analytics Cluster", connectionId: "c2", domain: "Marketing", records: 2100000, quality: 77, lastProfiled: "40 mins ago", description: "Multi-touch attribution model outputs for marketing channels.", owner: "Marketing Analytics", steward: "Priya Sharma", tags: ["Attribution", "ML", "Derived"], trust: "bronze", popularity: 4.0, columns: 31, issues: 6, upstream: ["t3", "t5"], downstream: [], columns_detail: [] },
  { id: "t14", name: "erp_products_raw", schema: "staging", connection: "Staging Environment", connectionId: "c5", domain: "Operations", records: 128000, quality: 82, lastProfiled: "1.5 hours ago", description: "Raw product master from SAP ERP.", owner: "Data Engineering", steward: "Bob Singh", tags: ["Raw", "Ingestion", "ERP"], trust: "bronze", popularity: 2.5, columns: 15, issues: 3, upstream: [], downstream: ["t4"], columns_detail: [] },
];

const mockAgents = [
  { id: "a1", name: "Data Profiler", type: "profiling", status: "active", lastRun: "2 mins ago", tasksCompleted: 1247, tasksToday: 45, avgRuntime: "2.3s", description: "Continuously profiles table statistics, null rates, cardinality, and distribution patterns." },
  { id: "a2", name: "Quality Validator", type: "validation", status: "active", lastRun: "45 secs ago", tasksCompleted: 892, tasksToday: 23, avgRuntime: "3.1s", description: "Executes validation rules against tables and records quality issues." },
  { id: "a3", name: "Lineage Tracker", type: "lineage", status: "active", lastRun: "3 mins ago", tasksCompleted: 456, tasksToday: 12, avgRuntime: "1.8s", description: "Tracks data lineage by parsing SQL, ETL jobs, and pipeline configs." },
  { id: "a4", name: "Anomaly Detector", type: "monitoring", status: "idle", lastRun: "18 mins ago", tasksCompleted: 234, tasksToday: 8, avgRuntime: "4.2s", description: "Detects statistical anomalies in data volume, schema drift, and value distributions." },
  { id: "a5", name: "PII Scanner", type: "governance", status: "active", lastRun: "10 mins ago", tasksCompleted: 318, tasksToday: 15, avgRuntime: "5.7s", description: "Scans columns for PII and sensitive data using ML-based pattern detection." },
  { id: "a6", name: "Schema Sync", type: "ingestion", status: "active", lastRun: "1 min ago", tasksCompleted: 2103, tasksToday: 67, avgRuntime: "0.9s", description: "Keeps schema metadata in sync with source systems in real-time." },
];

const mockIssues = [
  { id: "i1", table: "email_campaigns", tableId: "t5", type: "Missing Values", severity: "high", count: 89450, description: "customer_id is null for 10.1% of records — breaks join to customer_master", detectedAt: "2 hours ago" },
  { id: "i2", table: "orders_fact", tableId: "t2", type: "Data Drift", severity: "medium", count: 124000, description: "amount_usd showing bimodal distribution not seen in historical baseline", detectedAt: "4 hours ago" },
  { id: "i3", table: "crm_contacts_raw", tableId: "t10", type: "Schema Change", severity: "high", count: 0, description: "Column 'phone_mobile' dropped in latest ingestion. Downstream tables affected.", detectedAt: "6 hours ago" },
  { id: "i4", table: "marketing_attribution", tableId: "t13", type: "Freshness", severity: "medium", count: 0, description: "Table not refreshed in 26 hours. SLA breach threshold is 24 hours.", detectedAt: "2 hours ago" },
  { id: "i5", table: "sales_pipeline", tableId: "t7", type: "Duplicate Records", severity: "low", count: 234, description: "234 duplicate opportunity_ids detected across 3 CRM sync batches", detectedAt: "1 day ago" },
  { id: "i6", table: "inventory_snapshot", tableId: "t9", type: "Referential Integrity", severity: "medium", count: 890, description: "890 product_ids in inventory_snapshot not found in product_catalog", detectedAt: "3 hours ago" },
];

const mockPolicies = [
  { id: "p1", name: "PII Data Handling Policy", domain: "All", tables: 12, status: "active", lastUpdated: "2024-01-15" },
  { id: "p2", name: "GDPR Right to Erasure", domain: "All", tables: 8, status: "active", lastUpdated: "2024-01-10" },
  { id: "p3", name: "SOX Financial Data Controls", domain: "Finance", tables: 5, status: "active", lastUpdated: "2024-01-20" },
  { id: "p4", name: "PCI DSS Compliance", domain: "Finance", tables: 3, status: "active", lastUpdated: "2024-01-08" },
  { id: "p5", name: "Data Retention — 7 Year", domain: "Legal", tables: 15, status: "review", lastUpdated: "2023-12-20" },
];

// ─── LINEAGE GRAPH ENGINE ────────────────────────────────────────────────────

function buildLineageGraph(tableId, allTables) {
  const tableMap = {};
  allTables.forEach(t => { tableMap[t.id] = t; });

  const root = tableMap[tableId];
  if (!root) return { nodes: [], edges: [] };

  const visited = new Set();
  const nodes = [];
  const edges = [];

  // BFS upstream (depth 2)
  const collectUpstream = (id, depth) => {
    if (!tableMap[id] || visited.has(id + "_up_" + depth)) return;
    visited.add(id + "_up_" + depth);
    const t = tableMap[id];
    if (depth > 0 && !nodes.find(n => n.id === id)) {
      nodes.push({ id, label: t.name, schema: t.schema, trust: t.trust, quality: t.quality, domain: t.domain, layer: -depth, type: "table", isRoot: id === tableId });
    }
    (t.upstream || []).forEach(uid => {
      if (depth < 2) {
        collectUpstream(uid, depth + 1);
        if (!edges.find(e => e.from === uid && e.to === id)) {
          edges.push({ from: uid, to: id, type: "lineage" });
        }
      }
    });
  };

  // BFS downstream (depth 2)
  const collectDownstream = (id, depth) => {
    if (!tableMap[id] || visited.has(id + "_down_" + depth)) return;
    visited.add(id + "_down_" + depth);
    const t = tableMap[id];
    if (depth > 0 && !nodes.find(n => n.id === id)) {
      nodes.push({ id, label: t.name, schema: t.schema, trust: t.trust, quality: t.quality, domain: t.domain, layer: depth, type: "table", isRoot: id === tableId });
    }
    (t.downstream || []).forEach(did => {
      if (depth < 2) {
        collectDownstream(did, depth + 1);
        if (!edges.find(e => e.from === id && e.to === did)) {
          edges.push({ from: id, to: did, type: "lineage" });
        }
      }
    });
  };

  collectUpstream(tableId, 0);
  collectDownstream(tableId, 0);

  // Add root node
  nodes.push({ id: tableId, label: root.name, schema: root.schema, trust: root.trust, quality: root.quality, domain: root.domain, layer: 0, type: "table", isRoot: true });

  // Also add ETL/transform pseudo-nodes for interesting connections
  edges.forEach((e, i) => {
    const etlId = `etl_${e.from}_${e.to}`;
    nodes.push({ id: etlId, label: "ETL Job", schema: "", trust: null, quality: null, domain: null, layer: null, type: "etl", isRoot: false, fromEdge: e.from, toEdge: e.to });
    e.viaEtl = etlId;
  });

  return { nodes, edges };
}

function computeLayout(nodes, edges, width, height) {
  const layers = {};
  nodes.forEach(n => {
    if (n.type === "etl") return;
    const l = n.layer ?? 0;
    if (!layers[l]) layers[l] = [];
    layers[l].push(n);
  });

  const layerKeys = Object.keys(layers).map(Number).sort((a, b) => a - b);
  const totalLayers = layerKeys.length;
  const colW = Math.min(220, width / (totalLayers + 1));
  const positions = {};

  layerKeys.forEach((lk, li) => {
    const col = layers[lk];
    const x = 60 + li * colW + colW / 2;
    col.forEach((n, ni) => {
      const rowH = Math.min(120, (height - 80) / Math.max(col.length, 1));
      const y = 40 + ni * rowH + rowH / 2 + (height - col.length * rowH) / 2;
      positions[n.id] = { x, y };
    });
  });

  // ETL nodes go midpoint
  nodes.forEach(n => {
    if (n.type === "etl") {
      const p1 = positions[n.fromEdge] || { x: width / 2, y: height / 2 };
      const p2 = positions[n.toEdge] || { x: width / 2, y: height / 2 };
      positions[n.id] = { x: (p1.x + p2.x) / 2, y: (p1.y + p2.y) / 2 };
    }
  });

  return positions;
}

// ─── UTILITY COMPONENTS ──────────────────────────────────────────────────────

const TrustBadge = ({ trust, size = "sm" }) => {
  const cfg = {
    gold:   { bg: "bg-yellow-100", text: "text-yellow-800", border: "border-yellow-300", icon: "★", label: "Gold" },
    silver: { bg: "bg-slate-100",  text: "text-slate-700",  border: "border-slate-300",  icon: "★", label: "Silver" },
    bronze: { bg: "bg-orange-50",  text: "text-orange-700", border: "border-orange-200", icon: "★", label: "Bronze" },
  };
  const c = cfg[trust] || cfg.bronze;
  return (
    <span className={`inline-flex items-center gap-0.5 px-2 py-0.5 rounded-full text-xs font-semibold border ${c.bg} ${c.text} ${c.border}`}>
      <span>{c.icon}</span> {c.label}
    </span>
  );
};

const QualityBar = ({ score, showLabel = true }) => {
  const color = score >= 90 ? "bg-emerald-500" : score >= 75 ? "bg-yellow-400" : "bg-red-400";
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 h-1.5 bg-slate-200 rounded-full overflow-hidden">
        <div className={`h-full rounded-full ${color} transition-all duration-500`} style={{ width: `${score}%` }} />
      </div>
      {showLabel && <span className={`text-xs font-semibold ${score >= 90 ? "text-emerald-700" : score >= 75 ? "text-yellow-700" : "text-red-700"}`}>{score}%</span>}
    </div>
  );
};

const SeverityBadge = ({ severity }) => {
  const cfg = {
    high:   "bg-red-100 text-red-700 border-red-200",
    medium: "bg-yellow-100 text-yellow-700 border-yellow-200",
    low:    "bg-blue-50 text-blue-700 border-blue-200",
  };
  return <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold border ${cfg[severity]}`}>{severity.charAt(0).toUpperCase() + severity.slice(1)}</span>;
};

const AgentTypeBadge = ({ type }) => {
  const cfg = {
    profiling:   "bg-purple-100 text-purple-700",
    validation:  "bg-blue-100 text-blue-700",
    lineage:     "bg-indigo-100 text-indigo-700",
    monitoring:  "bg-orange-100 text-orange-700",
    governance:  "bg-teal-100 text-teal-700",
    ingestion:   "bg-green-100 text-green-700",
  };
  return <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cfg[type] || "bg-slate-100 text-slate-600"}`}>{type}</span>;
};

const ConnStatusDot = ({ status }) => {
  const colors = { connected: "bg-emerald-500", warning: "bg-yellow-400", disconnected: "bg-red-400" };
  return <span className={`inline-block w-2 h-2 rounded-full ${colors[status] || "bg-slate-400"} mr-1.5`} />;
};

const Avatar = ({ name, size = 7 }) => {
  const colors = ["bg-blue-500","bg-purple-500","bg-emerald-500","bg-orange-500","bg-rose-500","bg-indigo-500"];
  const idx = name.charCodeAt(0) % colors.length;
  return (
    <div className={`w-${size} h-${size} rounded-full ${colors[idx]} flex items-center justify-center text-white text-xs font-bold flex-shrink-0`}>
      {name.split(" ").map(p => p[0]).join("").slice(0, 2)}
    </div>
  );
};

// ─── LINEAGE VISUALIZATION ───────────────────────────────────────────────────

const LineageGraph = ({ tableId, allTables, onNodeClick }) => {
  const svgRef = useRef(null);
  const containerRef = useRef(null);
  const [dims, setDims] = useState({ w: 900, h: 500 });
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [dragging, setDragging] = useState(null);
  const [hoveredNode, setHoveredNode] = useState(null);

  useEffect(() => {
    const obs = new ResizeObserver(entries => {
      const { width, height } = entries[0].contentRect;
      setDims({ w: Math.max(600, width), h: Math.max(400, height) });
    });
    if (containerRef.current) obs.observe(containerRef.current);
    return () => obs.disconnect();
  }, []);

  useEffect(() => {
    setZoom(1);
    setPan({ x: 0, y: 0 });
  }, [tableId]);

  const { nodes, edges } = buildLineageGraph(tableId, allTables);
  const tableNodes = nodes.filter(n => n.type === "table");
  const etlNodes = nodes.filter(n => n.type === "etl");
  const positions = computeLayout(nodes, edges, dims.w - 40, dims.h - 40);

  const tableMap = {};
  allTables.forEach(t => { tableMap[t.id] = t; });

  const trustColor = (trust) => {
    if (trust === "gold") return "#F59E0B";
    if (trust === "silver") return "#94A3B8";
    return "#F97316";
  };

  const qualityFill = (q) => {
    if (!q) return "#94A3B8";
    if (q >= 90) return "#10B981";
    if (q >= 75) return "#F59E0B";
    return "#EF4444";
  };

  // Drag-to-pan
  const handleMouseDown = (e) => {
    if (e.target.closest(".lineage-node")) return;
    setDragging({ x: e.clientX - pan.x, y: e.clientY - pan.y });
  };
  const handleMouseMove = (e) => {
    if (!dragging) return;
    setPan({ x: e.clientX - dragging.x, y: e.clientY - dragging.y });
  };
  const handleMouseUp = () => setDragging(null);

  const handleWheel = (e) => {
    e.preventDefault();
    setZoom(z => Math.max(0.3, Math.min(2.5, z - e.deltaY * 0.001)));
  };

  const resetView = () => { setZoom(1); setPan({ x: 0, y: 0 }); };

  const NODE_W = 160;
  const NODE_H = 64;

  const edgePath = (from, to, viaEtl) => {
    const p1 = positions[from];
    const p2 = positions[to];
    if (!p1 || !p2) return "";
    const mx = (p1.x + p2.x) / 2;
    return `M ${p1.x + NODE_W / 2} ${p1.y} C ${mx} ${p1.y}, ${mx} ${p2.y}, ${p2.x - NODE_W / 2} ${p2.y}`;
  };

  return (
    <div ref={containerRef} className="relative w-full h-full bg-slate-50 rounded-xl overflow-hidden border border-slate-200">
      {/* Controls */}
      <div className="absolute top-3 right-3 flex flex-col gap-1.5 z-10">
        <button onClick={() => setZoom(z => Math.min(2.5, z + 0.15))} className="w-8 h-8 bg-white border border-slate-200 rounded-lg flex items-center justify-center shadow-sm hover:bg-slate-50" title="Zoom in"><ZoomIn className="w-4 h-4 text-slate-600" /></button>
        <button onClick={() => setZoom(z => Math.max(0.3, z - 0.15))} className="w-8 h-8 bg-white border border-slate-200 rounded-lg flex items-center justify-center shadow-sm hover:bg-slate-50" title="Zoom out"><ZoomOut className="w-4 h-4 text-slate-600" /></button>
        <button onClick={resetView} className="w-8 h-8 bg-white border border-slate-200 rounded-lg flex items-center justify-center shadow-sm hover:bg-slate-50" title="Reset"><RotateCcw className="w-4 h-4 text-slate-600" /></button>
      </div>

      {/* Legend */}
      <div className="absolute bottom-3 left-3 flex gap-3 z-10 bg-white/90 px-3 py-2 rounded-lg border border-slate-200 text-xs text-slate-600">
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-full bg-emerald-500 inline-block" />Quality ≥90%</span>
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-full bg-yellow-400 inline-block" />75–89%</span>
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-full bg-red-400 inline-block" />&lt;75%</span>
        <span className="flex items-center gap-1"><span className="w-3 h-0.5 bg-slate-400 inline-block border-dashed border-t border-slate-400" />ETL Transform</span>
      </div>

      {/* Layer labels */}
      <div className="absolute top-3 left-3 z-10 flex gap-2">
        <span className="bg-slate-700/80 text-white text-xs px-2 py-0.5 rounded">← Upstream Sources</span>
        <span className="bg-blue-600/80 text-white text-xs px-2 py-0.5 rounded">Selected</span>
        <span className="bg-slate-700/80 text-white text-xs px-2 py-0.5 rounded">Downstream →</span>
      </div>

      <svg
        ref={svgRef}
        width="100%" height="100%"
        className={dragging ? "cursor-grabbing" : "cursor-grab"}
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseUp}
        onWheel={handleWheel}
      >
        <defs>
          <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
            <polygon points="0 0, 10 3.5, 0 7" fill="#94A3B8" />
          </marker>
          <marker id="arrowhead-active" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
            <polygon points="0 0, 10 3.5, 0 7" fill="#3B82F6" />
          </marker>
          <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
            <feDropShadow dx="0" dy="2" stdDeviation="3" floodOpacity="0.12" />
          </filter>
          <filter id="shadow-active" x="-20%" y="-20%" width="140%" height="140%">
            <feDropShadow dx="0" dy="4" stdDeviation="6" floodOpacity="0.25" />
          </filter>
        </defs>

        <g transform={`translate(${pan.x + 20},${pan.y + 20}) scale(${zoom})`}>
          {/* Draw column highlight bands for layers */}
          {[-2, -1, 0, 1, 2].map(l => {
            const nodesInLayer = tableNodes.filter(n => n.layer === l);
            if (!nodesInLayer.length) return null;
            const xs = nodesInLayer.map(n => positions[n.id]?.x ?? 0);
            const minX = Math.min(...xs) - NODE_W / 2 - 12;
            const maxX = Math.max(...xs) + NODE_W / 2 + 12;
            return (
              <rect key={l} x={minX} y={20} width={maxX - minX} height={dims.h - 80}
                fill={l === 0 ? "#EFF6FF" : "#F8FAFC"} rx="8" opacity="0.7" />
            );
          })}

          {/* Edges */}
          {edges.map((e, i) => {
            const p1 = positions[e.from];
            const p2 = positions[e.to];
            if (!p1 || !p2) return null;
            const isActive = e.from === tableId || e.to === tableId;
            const mx = (p1.x + p2.x) / 2;
            return (
              <g key={i}>
                <path
                  d={`M ${p1.x + NODE_W / 2} ${p1.y} C ${mx} ${p1.y}, ${mx} ${p2.y}, ${p2.x - NODE_W / 2} ${p2.y}`}
                  fill="none"
                  stroke={isActive ? "#3B82F6" : "#CBD5E1"}
                  strokeWidth={isActive ? 2 : 1.5}
                  strokeDasharray={e.type === "etl" ? "5,3" : undefined}
                  markerEnd={`url(#${isActive ? "arrowhead-active" : "arrowhead"})`}
                  opacity={isActive ? 1 : 0.6}
                />
                {/* ETL label on edge midpoint */}
                {isActive && (
                  <g transform={`translate(${mx}, ${(p1.y + p2.y) / 2})`}>
                    <rect x="-22" y="-9" width="44" height="18" rx="9" fill="#DBEAFE" stroke="#93C5FD" strokeWidth="1" />
                    <text x="0" y="4" textAnchor="middle" fontSize="8" fill="#1D4ED8" fontWeight="600">ETL Job</text>
                  </g>
                )}
              </g>
            );
          })}

          {/* Table Nodes */}
          {tableNodes.map(n => {
            const pos = positions[n.id];
            if (!pos) return null;
            const isRoot = n.isRoot;
            const isHovered = hoveredNode === n.id;
            const qColor = qualityFill(n.quality);
            const tColor = trustColor(n.trust);

            return (
              <g
                key={n.id}
                className="lineage-node"
                transform={`translate(${pos.x - NODE_W / 2}, ${pos.y - NODE_H / 2})`}
                onClick={() => onNodeClick && onNodeClick(n.id)}
                onMouseEnter={() => setHoveredNode(n.id)}
                onMouseLeave={() => setHoveredNode(null)}
                style={{ cursor: "pointer" }}
                filter={isRoot ? "url(#shadow-active)" : "url(#shadow)"}
              >
                {/* Node body */}
                <rect
                  width={NODE_W} height={NODE_H} rx="10"
                  fill={isRoot ? "#1E40AF" : "white"}
                  stroke={isRoot ? "#1D4ED8" : isHovered ? "#3B82F6" : "#E2E8F0"}
                  strokeWidth={isRoot ? 0 : isHovered ? 2 : 1}
                />
                {/* Quality stripe */}
                <rect x={NODE_W - 6} y={0} width={6} height={NODE_H} rx="0"
                  fill={qColor} opacity={isRoot ? 0.6 : 1}
                  style={{ borderTopRightRadius: 10, borderBottomRightRadius: 10 }} />
                <rect x={NODE_W - 6} y={0} width={6} height={NODE_H}
                  fill={qColor} rx="0" clipPath={`inset(0 0 0 0 round 0 10px 10px 0)`} opacity={isRoot ? 0.6 : 1} />

                {/* Trust star */}
                <text x={NODE_W - 18} y={14} fontSize="10" fill={tColor} fontWeight="bold">★</text>

                {/* Icon */}
                <rect x={10} y={14} width={18} height={18} rx="4"
                  fill={isRoot ? "rgba(255,255,255,0.2)" : "#EFF6FF"} />
                <text x={19} y={26} fontSize="9" textAnchor="middle" fill={isRoot ? "white" : "#3B82F6"}>⊞</text>

                {/* Table name */}
                <text x={34} y={25} fontSize="11" fontWeight="700" fill={isRoot ? "white" : "#1E293B"}
                  style={{ fontFamily: "ui-monospace, monospace" }}>
                  {n.label.length > 16 ? n.label.slice(0, 15) + "…" : n.label}
                </text>

                {/* Schema */}
                <text x={34} y={38} fontSize="9" fill={isRoot ? "rgba(255,255,255,0.7)" : "#64748B"}>
                  {n.schema}
                </text>

                {/* Quality score */}
                {n.quality && (
                  <text x={10} y={56} fontSize="9" fill={isRoot ? "rgba(255,255,255,0.7)" : "#64748B"}>
                    QS: {n.quality}%
                  </text>
                )}

                {/* Domain pill */}
                {n.domain && (
                  <>
                    <rect x={70} y={47} width={Math.min(n.domain.length * 5 + 8, 72)} height={13} rx="6"
                      fill={isRoot ? "rgba(255,255,255,0.15)" : "#F1F5F9"} />
                    <text x={74} y={57} fontSize="8" fill={isRoot ? "rgba(255,255,255,0.8)" : "#475569"}>
                      {n.domain.length > 10 ? n.domain.slice(0, 9) + "…" : n.domain}
                    </text>
                  </>
                )}

                {/* Hover tooltip */}
                {isHovered && !isRoot && (
                  <g transform={`translate(${NODE_W / 2 - 60}, ${-50})`}>
                    <rect x="0" y="0" width="120" height="40" rx="6" fill="#1E293B" />
                    <text x="10" y="14" fontSize="10" fill="white" fontWeight="600">{n.label}</text>
                    <text x="10" y="28" fontSize="9" fill="#94A3B8">Click to explore lineage</text>
                  </g>
                )}
              </g>
            );
          })}
        </g>
      </svg>
    </div>
  );
};

// ─── MAIN APP ─────────────────────────────────────────────────────────────────

export default function DataIQPlatform() {
  const [activeTab, setActiveTab] = useState("dashboard");
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedTable, setSelectedTable] = useState(null);
  const [lineageTableId, setLineageTableId] = useState("gold.fact_orders");
  const [agentLogs, setAgentLogs] = useState([]);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [searchFocused, setSearchFocused] = useState(false);
  const [agentStates, setAgentStates] = useState({});
  const [filterDomain, setFilterDomain] = useState("All");
  const [filterTrust, setFilterTrust] = useState("All");
  const [filterConn, setFilterConn] = useState("All");
  const [notifOpen, setNotifOpen] = useState(false);

  // ── Backend state ────────────────────────────────────────────────────────
  const [backendOnline, setBackendOnline] = useState(false);
  const [liveAgents, setLiveAgents] = useState({});   // agent_id → agent object from API
  const [liveTasks, setLiveTasks] = useState([]);      // tasks from backend
  const wsRef = useRef(null);

  // ── Real connections state ────────────────────────────────────────────────
  const [realConnections, setRealConnections] = useState([]);
  const [connTypes, setConnTypes] = useState([]);          // connector type metadata from API
  const [showNewConn, setShowNewConn] = useState(false);
  // Wizard state hoisted to App so it survives NewConnectionWizard re-mounts
  const [wizStep, setWizStep] = useState(1);
  const [wizSelectedType, setWizSelectedType] = useState(null);
  const [wizFormData, setWizFormData] = useState({});
  const [wizConnName, setWizConnName] = useState("");
  const [wizTesting, setWizTesting] = useState(false);
  const [wizTestRes, setWizTestRes] = useState(null);
  const [wizSaving, setWizSaving] = useState(false);
  const [testResults, setTestResults] = useState({});      // connectionId → test result
  const [discoveringSchema, setDiscoveringSchema] = useState({}); // connectionId → bool
  const [connSchemas, setConnSchemas] = useState({});      // connectionId → [schemaName]
  const [connTables, setConnTables] = useState({});        // connectionId → [tableObj]
  const [expandedConn, setExpandedConn] = useState(null);
  const [profilingTable, setProfilingTable] = useState(null);
  const [profileResult, setProfileResult] = useState(null);

  // ── Live catalog state (real data from backend) ────────────────────────────
  const [catalogTables, setCatalogTables] = useState(mockTables);
  const [catalogIssues, setCatalogIssues] = useState(mockIssues);

  const loadRealConnections = async () => {
    const data = await apiFetch("/api/connections");
    if (data) setRealConnections(Array.isArray(data) ? data : []);
  };

  const loadConnectorTypes = async () => {
    const data = await apiFetch("/api/connections/types");
    if (data) setConnTypes(Array.isArray(data) ? data : []);
  };

  const loadCatalogData = async () => {
    const tables = await apiFetch("/api/catalog/tables");
    if (tables && Array.isArray(tables) && tables.length > 0) setCatalogTables(tables);
    const issues = await apiFetch("/api/catalog/issues");
    if (issues && Array.isArray(issues)) setCatalogIssues(issues);
  };

  useEffect(() => {
    if (backendOnline) {
      loadRealConnections();
      loadConnectorTypes();
      loadCatalogData();
    }
  }, [backendOnline]);

  // Reset wizard state whenever it's closed
  useEffect(() => {
    if (!showNewConn) {
      setWizStep(1); setWizSelectedType(null); setWizFormData({});
      setWizConnName(""); setWizTesting(false); setWizTestRes(null); setWizSaving(false);
    }
  }, [showNewConn]);

  // Check backend health
  useEffect(() => {
    const check = async () => {
      const data = await apiFetch("/health");
      setBackendOnline(!!data);
    };
    check();
    const t = setInterval(check, 10000);
    return () => clearInterval(t);
  }, []);

  // Connect WebSocket when backend is online
  useEffect(() => {
    if (!backendOnline) return;
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      if (msg.type === "init") {
        // Seed agents + tasks
        const agentMap = {};
        (msg.agents || []).forEach(a => { agentMap[a.id] = a; });
        setLiveAgents(agentMap);
        setLiveTasks(msg.tasks || []);
      } else if (msg.type === "agent_log") {
        setAgentLogs(prev => [...prev.slice(-49), {
          id: Date.now(), agent: msg.agent, message: msg.message,
          level: msg.level, timestamp: new Date(msg.timestamp).toLocaleTimeString(),
        }]);
      } else if (msg.type === "agent_status") {
        setLiveAgents(prev => {
          if (!prev[msg.agent_id]) return prev;
          return { ...prev, [msg.agent_id]: { ...prev[msg.agent_id], status: msg.status } };
        });
        // Refresh full agent data
        apiFetch(`/agents/${msg.agent_id}`).then(d => {
          if (d) setLiveAgents(prev => ({ ...prev, [d.id]: d }));
        });
      } else if (msg.type === "task_created" || msg.type === "task_updated") {
        setLiveTasks(prev => {
          const idx = prev.findIndex(t => t.id === msg.task.id);
          if (idx >= 0) { const n = [...prev]; n[idx] = msg.task; return n; }
          return [msg.task, ...prev];
        });
      } else if (msg.type === "task_deleted") {
        setLiveTasks(prev => prev.filter(t => t.id !== msg.task_id));
      }
    };

    ws.onopen = () => {
      const ping = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send("ping");
      }, 25000);
      ws._pingInterval = ping;
    };

    ws.onclose = () => {
      clearInterval(ws._pingInterval);
      wsRef.current = null;
    };

    return () => { ws.close(); };
  }, [backendOnline]);

  // Live agent logs
  useEffect(() => {
    const msgs = [
      "Profiling table customer_master — 4.2M rows scanned",
      "Quality check passed on orders_fact.amount_usd",
      "Lineage edge detected: crm_contacts_raw → customer_master",
      "Anomaly: orders_fact volume 18% below 7-day average",
      "PII column detected: email_campaigns.customer_email",
      "Schema sync complete for Snowflake Enterprise DW",
      "Validation rule NullCheck failed: email_campaigns.customer_id",
      "Profiling complete: product_catalog (125K rows, quality: 94%)",
      "Lineage: orders_fact → revenue_summary job finished",
      "PII scan complete: hr_employees — 12 PII columns flagged",
    ];
    const agentNames = ["Data Profiler", "Quality Validator", "Lineage Tracker", "Anomaly Detector", "PII Scanner", "Schema Sync"];
    const levels = ["info", "info", "info", "warn", "info", "info", "error", "info", "info", "warn"];
    const interval = setInterval(() => {
      const i = Math.floor(Math.random() * msgs.length);
      setAgentLogs(prev => [...prev.slice(-19), {
        id: Date.now(), agent: agentNames[Math.floor(Math.random() * agentNames.length)],
        message: msgs[i], level: levels[i], timestamp: new Date().toLocaleTimeString()
      }]);
    }, 2500);
    return () => clearInterval(interval);
  }, []);

  const filteredTables = catalogTables.filter(t => {
    const q = searchQuery.toLowerCase();
    const matchSearch = !q || t.name.includes(q) || t.schema.includes(q) || t.description.toLowerCase().includes(q) || t.tags.some(tg => tg.toLowerCase().includes(q));
    const matchDomain = filterDomain === "All" || t.domain === filterDomain;
    const matchTrust = filterTrust === "All" || t.trust === filterTrust.toLowerCase();
    const matchConn = filterConn === "All" || t.connection === filterConn;
    return matchSearch && matchDomain && matchTrust && matchConn;
  });

  const openTableDetail = (table) => {
    setSelectedTable(table);
    setActiveTab("catalog");
  };

  const openLineage = (tableId) => {
    setLineageTableId(tableId);
    setActiveTab("lineage");
  };

  const totalIssues = catalogIssues.length;
  const highIssues = catalogIssues.filter(i => i.severity === "high").length;
  const avgQuality = catalogTables.length > 0 ? Math.round(catalogTables.reduce((a, t) => a + t.quality, 0) / catalogTables.length) : 0;
  const activeAgents = mockAgents.filter(a => a.status === "active").length;

  // ── SIDEBAR ────────────────────────────────────────────────────────────────
  const navItems = [
    { id: "dashboard",   icon: Home,      label: "Dashboard" },
    { id: "catalog",     icon: Layers,    label: "Data Catalog" },
    { id: "quality",     icon: Target,    label: "Data Quality" },
    { id: "lineage",     icon: Network,   label: "Lineage" },
    { id: "agents",      icon: Bot,       label: "AI Agents" },
    { id: "tasks",       icon: ListTodo,  label: "Tasks" },
    { id: "governance",  icon: Shield,    label: "Governance" },
    { id: "connections", icon: Database,  label: "Connections" },
  ];

  const Sidebar = () => (
    <div className={`${sidebarCollapsed ? "w-16" : "w-60"} bg-[#0F1F3D] text-white flex flex-col transition-all duration-200 flex-shrink-0`}>
      {/* Logo */}
      <div className={`flex items-center ${sidebarCollapsed ? "justify-center p-3" : "gap-3 p-4"} border-b border-white/10`}>
        <div className="w-8 h-8 bg-blue-500 rounded-lg flex items-center justify-center flex-shrink-0">
          <Database className="w-5 h-5 text-white" />
        </div>
        {!sidebarCollapsed && (
          <div>
            <div className="font-bold text-white text-sm tracking-wide">DataIQ</div>
            <div className="text-blue-300 text-xs">Enterprise Platform</div>
          </div>
        )}
      </div>

      {/* Search shortcut */}
      {!sidebarCollapsed && (
        <div className="px-3 pt-4 pb-2">
          <button
            onClick={() => { setActiveTab("catalog"); setSearchFocused(true); }}
            className="w-full flex items-center gap-2 px-3 py-2 rounded-lg bg-white/10 hover:bg-white/15 text-slate-300 text-sm transition-colors"
          >
            <Search className="w-4 h-4" />
            <span className="text-xs">Search assets…</span>
            <span className="ml-auto text-xs bg-white/10 px-1.5 py-0.5 rounded">⌘K</span>
          </button>
        </div>
      )}

      {/* Nav */}
      <nav className="flex-1 px-2 py-3 space-y-0.5">
        {navItems.map(({ id, icon: Icon, label }) => (
          <button
            key={id}
            onClick={() => setActiveTab(id)}
            className={`w-full flex items-center ${sidebarCollapsed ? "justify-center" : "gap-3"} px-3 py-2.5 rounded-lg text-left transition-colors group relative ${
              activeTab === id
                ? "bg-blue-600 text-white"
                : "text-slate-400 hover:bg-white/10 hover:text-white"
            }`}
            title={sidebarCollapsed ? label : undefined}
          >
            <Icon className="w-4.5 h-4.5 flex-shrink-0" />
            {!sidebarCollapsed && <span className="text-sm font-medium">{label}</span>}
            {!sidebarCollapsed && id === "quality" && highIssues > 0 && (
              <span className="ml-auto bg-red-500 text-white text-xs rounded-full w-5 h-5 flex items-center justify-center font-bold">{highIssues}</span>
            )}
          </button>
        ))}
      </nav>

      {/* User */}
      {/* Backend status */}
      {!sidebarCollapsed && (
        <div className={`mx-3 mb-2 px-3 py-2 rounded-lg flex items-center gap-2 text-xs ${backendOnline ? "bg-emerald-900/40 text-emerald-300" : "bg-red-900/30 text-red-300"}`}>
          {backendOnline ? <Wifi className="w-3 h-3 flex-shrink-0" /> : <WifiOff className="w-3 h-3 flex-shrink-0" />}
          <span>{backendOnline ? "Backend connected" : "Backend offline"}</span>
        </div>
      )}
      <div className={`border-t border-white/10 p-3 flex items-center ${sidebarCollapsed ? "justify-center" : "gap-2"}`}>
        <div className="w-7 h-7 rounded-full bg-blue-500 flex items-center justify-center text-white text-xs font-bold flex-shrink-0">MA</div>
        {!sidebarCollapsed && (
          <div className="flex-1 min-w-0">
            <div className="text-xs font-medium text-white truncate">Milan</div>
            <div className="text-xs text-slate-400 truncate">Admin</div>
          </div>
        )}
      </div>

      {/* Collapse btn */}
      <button
        onClick={() => setSidebarCollapsed(c => !c)}
        className="absolute left-0 bottom-24 translate-x-[calc(100%-0px)] bg-[#0F1F3D] border border-white/10 p-1 rounded-r-lg text-slate-400 hover:text-white opacity-0 group-hover:opacity-100 z-20"
        style={{ position: "static", margin: "0 auto 4px" }}
      >
        {sidebarCollapsed ? <ChevronRight className="w-3 h-3" /> : <ChevronDown className="w-3 h-3 rotate-90" />}
      </button>
    </div>
  );

  // ── TOPBAR ─────────────────────────────────────────────────────────────────
  const TopBar = () => {
    const labels = { dashboard: "Dashboard", catalog: "Data Catalog", quality: "Data Quality", lineage: "Data Lineage", agents: "AI Agents", tasks: "Task Queue", governance: "Governance & Compliance", connections: "Connections" };
    return (
      <div className="bg-white border-b border-slate-200 px-6 py-3 flex items-center justify-between flex-shrink-0 h-14">
        <div className="flex items-center gap-2 text-sm text-slate-500">
          <button onClick={() => setSidebarCollapsed(c => !c)} className="p-1 hover:bg-slate-100 rounded mr-2">
            <svg className="w-5 h-5" viewBox="0 0 20 20" fill="currentColor"><path fillRule="evenodd" d="M3 5h14a1 1 0 010 2H3a1 1 0 010-2zm0 4h14a1 1 0 010 2H3a1 1 0 010-2zm0 4h14a1 1 0 010 2H3a1 1 0 010-2z" clipRule="evenodd" /></svg>
          </button>
          <Globe className="w-4 h-4 text-blue-600" />
          <span className="text-slate-400">/</span>
          <span className="text-slate-700 font-medium">{labels[activeTab] || activeTab}</span>
          {selectedTable && activeTab === "catalog" && (
            <><span className="text-slate-400">/</span><span className="text-blue-600 font-medium">{selectedTable.name}</span></>
          )}
        </div>
        <div className="flex items-center gap-3">
          <div className="relative">
            <Search className="w-4 h-4 absolute left-3 top-2.5 text-slate-400" />
            <input
              type="text" placeholder="Search data assets, tables, columns…"
              className="pl-9 pr-4 py-2 w-64 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              value={searchQuery}
              onChange={e => setSearchQuery(e.target.value)}
            />
          </div>
          <div className="relative">
            <button onClick={() => setNotifOpen(n => !n)} className="p-2 hover:bg-slate-100 rounded-lg relative">
              <Bell className="w-5 h-5 text-slate-600" />
              <span className="absolute top-1 right-1 w-2 h-2 bg-red-500 rounded-full" />
            </button>
            {notifOpen && (
              <div className="absolute right-0 top-10 w-80 bg-white border border-slate-200 rounded-xl shadow-xl z-50">
                <div className="px-4 py-3 border-b border-slate-100 font-semibold text-sm text-slate-700">Notifications</div>
                {catalogIssues.slice(0, 4).map(issue => (
                  <div key={issue.id} onClick={() => { setActiveTab("quality"); setNotifOpen(false); }} className="px-4 py-3 hover:bg-slate-50 cursor-pointer border-b border-slate-50">
                    <div className="flex items-start gap-2">
                      <AlertCircle className={`w-4 h-4 flex-shrink-0 mt-0.5 ${issue.severity === "high" ? "text-red-500" : "text-yellow-500"}`} />
                      <div>
                        <div className="text-xs font-medium text-slate-800">{issue.type} — {issue.table}</div>
                        <div className="text-xs text-slate-500 mt-0.5">{issue.description.slice(0, 60)}…</div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
          <div className="w-8 h-8 rounded-full bg-blue-500 flex items-center justify-center text-white text-xs font-bold">MA</div>
        </div>
      </div>
    );
  };

  // ── DASHBOARD TAB ──────────────────────────────────────────────────────────
  const Dashboard = () => (
    <div className="flex-1 overflow-auto bg-slate-50">
      <div className="p-6 space-y-6 max-w-7xl mx-auto">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-xl font-bold text-slate-900">Enterprise Data Intelligence</h2>
            <p className="text-sm text-slate-500 mt-0.5">Real-time health across {realConnections.filter(c => c.status === "ok").length} connected systems · {catalogTables.length} catalogued tables</p>
          </div>
          <button className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 shadow-sm">
            <RefreshCw className="w-4 h-4" /> Refresh All
          </button>
        </div>

        {/* KPI cards */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          {[
            { label: "Avg Quality Score", value: `${avgQuality}%`, sub: "+2.1% from last week", icon: Target, color: "text-emerald-600", bg: "bg-emerald-50", border: "border-emerald-200", trend: "up" },
            { label: "Active Data Sources", value: realConnections.filter(c => c.status === "ok").length, sub: `${realConnections.length} total connections`, icon: Database, color: "text-blue-600", bg: "bg-blue-50", border: "border-blue-200", trend: "flat" },
            { label: "Open Issues", value: totalIssues, sub: `${highIssues} high severity`, icon: AlertTriangle, color: "text-red-600", bg: "bg-red-50", border: "border-red-200", trend: "down" },
            { label: "AI Agents Active", value: `${activeAgents}/${mockAgents.length}`, sub: "All agents healthy", icon: Bot, color: "text-purple-600", bg: "bg-purple-50", border: "border-purple-200", trend: "flat" },
          ].map(({ label, value, sub, icon: Icon, color, bg, border, trend }) => (
            <div key={label} className={`bg-white rounded-xl border ${border} p-5 shadow-sm`}>
              <div className="flex items-start justify-between">
                <div>
                  <p className="text-xs text-slate-500 font-medium uppercase tracking-wide">{label}</p>
                  <p className={`text-2xl font-bold mt-1 ${color}`}>{value}</p>
                  <p className="text-xs text-slate-400 mt-1">{sub}</p>
                </div>
                <div className={`${bg} ${color} p-2.5 rounded-xl`}><Icon className="w-5 h-5" /></div>
              </div>
            </div>
          ))}
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Top assets */}
          <div className="lg:col-span-2 bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
            <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between">
              <h3 className="font-semibold text-slate-800 text-sm">Most Accessed Data Assets</h3>
              <button onClick={() => setActiveTab("catalog")} className="text-xs text-blue-600 hover:underline">View catalog →</button>
            </div>
            <div className="divide-y divide-slate-50">
              {catalogTables.slice(0, 6).map(t => (
                <div key={t.id} className="px-5 py-3.5 hover:bg-slate-50 cursor-pointer flex items-center gap-4" onClick={() => openTableDetail(t)}>
                  <div className="w-8 h-8 rounded-lg bg-blue-50 flex items-center justify-center flex-shrink-0">
                    <Table className="w-4 h-4 text-blue-600" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-semibold text-slate-800 font-mono">{t.name}</span>
                      <TrustBadge trust={t.trust} />
                    </div>
                    <div className="text-xs text-slate-500 truncate mt-0.5">{t.connection} · {t.schema} · {t.records.toLocaleString()} records</div>
                  </div>
                  <div className="w-24 flex-shrink-0">
                    <QualityBar score={t.quality} />
                  </div>
                  <button onClick={e => { e.stopPropagation(); openLineage(t.id); }} className="text-slate-400 hover:text-blue-600 flex-shrink-0" title="View lineage">
                    <Network className="w-4 h-4" />
                  </button>
                </div>
              ))}
            </div>
          </div>

          {/* Live agent log */}
          <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
            <div className="px-4 py-4 border-b border-slate-100 flex items-center justify-between">
              <h3 className="font-semibold text-slate-800 text-sm flex items-center gap-2">
                <span className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse inline-block" />
                Agent Activity Feed
              </h3>
              <span className="text-xs text-slate-400">live</span>
            </div>
            <div className="p-3 h-80 overflow-auto space-y-1.5 font-mono">
              {agentLogs.slice().reverse().map(log => (
                <div key={log.id} className="flex items-start gap-2 text-xs">
                  <span className="text-slate-400 flex-shrink-0 mt-0.5">{log.timestamp}</span>
                  <span className={`flex-shrink-0 mt-0.5 ${log.level === "error" ? "text-red-500" : log.level === "warn" ? "text-yellow-500" : "text-emerald-500"}`}>
                    {log.level === "error" ? "✗" : log.level === "warn" ? "⚠" : "✓"}
                  </span>
                  <div>
                    <span className="text-blue-600">[{log.agent}]</span>{" "}
                    <span className="text-slate-700">{log.message}</span>
                  </div>
                </div>
              ))}
              {agentLogs.length === 0 && <div className="text-slate-400 text-xs p-2">Waiting for agent activity…</div>}
            </div>
          </div>
        </div>

        {/* Issues snapshot */}
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
          <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between">
            <h3 className="font-semibold text-slate-800 text-sm">Active Issues</h3>
            <button onClick={() => setActiveTab("quality")} className="text-xs text-blue-600 hover:underline">View all →</button>
          </div>
          <div className="divide-y divide-slate-50">
            {catalogIssues.map(issue => (
              <div key={issue.id} className="px-5 py-3 flex items-center gap-4 hover:bg-slate-50">
                <SeverityBadge severity={issue.severity} />
                <span className="text-sm font-medium text-slate-700 w-40 flex-shrink-0">{issue.type}</span>
                <code className="text-xs bg-slate-100 px-2 py-0.5 rounded text-blue-700 cursor-pointer hover:bg-blue-50" onClick={() => { const t = catalogTables.find(t => t.id === issue.tableId); if (t) openTableDetail(t); }}>{issue.table}</code>
                <span className="text-xs text-slate-500 flex-1">{issue.description}</span>
                <span className="text-xs text-slate-400 flex-shrink-0">{issue.detectedAt}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );

  // ── CATALOG TAB ────────────────────────────────────────────────────────────
  const CatalogTab = () => (
    <div className="flex-1 overflow-auto bg-slate-50">
      <div className="p-6 max-w-7xl mx-auto space-y-4">
        {/* Filters */}
        <div className="flex flex-wrap items-center gap-3">
          <div className="relative flex-1 min-w-64">
            <Search className="w-4 h-4 absolute left-3 top-2.5 text-slate-400" />
            <input type="text" placeholder="Search tables, columns, descriptions, tags…"
              className="w-full pl-9 pr-4 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white"
              value={searchQuery} onChange={e => setSearchQuery(e.target.value)} autoFocus={searchFocused} />
          </div>
          <select value={filterDomain} onChange={e => setFilterDomain(e.target.value)} className="px-3 py-2 text-sm border border-slate-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-blue-500">
            <option value="All">All Domains</option>
            {[...new Set(catalogTables.map(t => t.domain).filter(Boolean))].sort().map(d => <option key={d}>{d}</option>)}
          </select>
          <select value={filterTrust} onChange={e => setFilterTrust(e.target.value)} className="px-3 py-2 text-sm border border-slate-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-blue-500">
            <option value="All">All Trust</option>
            <option value="Gold">Gold</option>
            <option value="Silver">Silver</option>
            <option value="Bronze">Bronze</option>
          </select>
          <select value={filterConn} onChange={e => setFilterConn(e.target.value)} className="px-3 py-2 text-sm border border-slate-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-blue-500">
            <option value="All">All Sources</option>
            {realConnections.map(c => <option key={c.id}>{c.name}</option>)}
          </select>
          <span className="text-xs text-slate-500">{filteredTables.length} results</span>
        </div>

        <div className="flex gap-6">
          {/* Table list */}
          <div className={`${selectedTable ? "w-80 flex-shrink-0" : "flex-1"} space-y-2`}>
            {filteredTables.map(t => (
              <div key={t.id}
                onClick={() => setSelectedTable(t)}
                className={`bg-white rounded-xl border shadow-sm cursor-pointer hover:shadow-md transition-all ${selectedTable?.id === t.id ? "border-blue-500 ring-2 ring-blue-100" : "border-slate-200 hover:border-blue-200"}`}>
                <div className="p-4">
                  <div className="flex items-start gap-3">
                    <div className="w-9 h-9 rounded-lg bg-blue-50 flex items-center justify-center flex-shrink-0 mt-0.5">
                      <Table className="w-4.5 h-4.5 text-blue-600" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="font-semibold text-slate-900 font-mono text-sm">{t.name}</span>
                        <TrustBadge trust={t.trust} />
                        {t.issues > 0 && <span className="text-xs bg-red-100 text-red-700 border border-red-200 rounded-full px-2 py-0.5">{t.issues} issues</span>}
                      </div>
                      <div className="text-xs text-slate-500 mt-0.5">{t.connection} · <code className="bg-slate-100 px-1 rounded">{t.schema}</code> · {t.records.toLocaleString()} records</div>
                      {!selectedTable && <p className="text-xs text-slate-600 mt-1.5 line-clamp-2">{t.description}</p>}
                      <div className="flex items-center gap-3 mt-2">
                        <div className="flex-1"><QualityBar score={t.quality} /></div>
                        <div className="flex gap-1">
                          {t.tags.slice(0, 2).map(tag => <span key={tag} className="text-xs bg-slate-100 text-slate-600 px-1.5 py-0.5 rounded">{tag}</span>)}
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>

          {/* Table detail panel */}
          {selectedTable && (
            <div className="flex-1 bg-white rounded-xl border border-slate-200 shadow-sm overflow-auto">
              {/* Detail header */}
              <div className="px-6 py-4 border-b border-slate-100 bg-gradient-to-r from-blue-50 to-white">
                <div className="flex items-start justify-between">
                  <div className="flex items-start gap-4">
                    <div className="w-12 h-12 rounded-xl bg-blue-100 flex items-center justify-center">
                      <Table className="w-6 h-6 text-blue-600" />
                    </div>
                    <div>
                      <div className="flex items-center gap-2 flex-wrap">
                        <h3 className="text-lg font-bold text-slate-900 font-mono">{selectedTable.name}</h3>
                        <TrustBadge trust={selectedTable.trust} />
                      </div>
                      <div className="text-sm text-slate-500 mt-0.5">
                        {selectedTable.connection} · <code className="bg-slate-100 text-slate-700 px-1 py-0.5 rounded text-xs">{selectedTable.schema}.{selectedTable.name}</code>
                      </div>
                      <div className="flex flex-wrap gap-1.5 mt-2">
                        {selectedTable.tags.map(tag => <span key={tag} className="text-xs bg-blue-50 text-blue-700 border border-blue-200 px-2 py-0.5 rounded-full">{tag}</span>)}
                      </div>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <button onClick={() => openLineage(selectedTable.id)} className="flex items-center gap-1.5 px-3 py-1.5 text-xs border border-slate-200 rounded-lg hover:bg-slate-50 text-slate-700">
                      <Network className="w-3.5 h-3.5" /> Lineage
                    </button>
                    <button className="flex items-center gap-1.5 px-3 py-1.5 text-xs border border-slate-200 rounded-lg hover:bg-slate-50 text-slate-700">
                      <MessageCircle className="w-3.5 h-3.5" /> Discuss
                    </button>
                    <button onClick={() => setSelectedTable(null)} className="p-1.5 hover:bg-slate-100 rounded-lg">
                      <X className="w-4 h-4 text-slate-500" />
                    </button>
                  </div>
                </div>
              </div>

              <div className="p-6 space-y-6">
                {/* Quick stats */}
                <div className="grid grid-cols-4 gap-3">
                  {[
                    { label: "Records", value: selectedTable.records.toLocaleString(), icon: Hash },
                    { label: "Columns", value: selectedTable.columns, icon: Columns },
                    { label: "Quality Score", value: `${selectedTable.quality}%`, icon: Target },
                    { label: "Last Profiled", value: selectedTable.lastProfiled, icon: Clock },
                  ].map(({ label, value, icon: Icon }) => (
                    <div key={label} className="text-center p-3 bg-slate-50 rounded-xl">
                      <Icon className="w-4 h-4 text-slate-400 mx-auto mb-1" />
                      <div className="font-bold text-slate-900 text-lg">{value}</div>
                      <div className="text-xs text-slate-500">{label}</div>
                    </div>
                  ))}
                </div>

                {/* Description */}
                <div>
                  <h4 className="text-sm font-semibold text-slate-700 mb-2">Description</h4>
                  <p className="text-sm text-slate-600 leading-relaxed">{selectedTable.description}</p>
                </div>

                {/* Ownership */}
                <div className="flex gap-6">
                  <div>
                    <h4 className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-2">Owner</h4>
                    <div className="flex items-center gap-2">
                      <Avatar name={selectedTable.owner} />
                      <span className="text-sm text-slate-700 font-medium">{selectedTable.owner}</span>
                    </div>
                  </div>
                  <div>
                    <h4 className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-2">Data Steward</h4>
                    <div className="flex items-center gap-2">
                      <Avatar name={selectedTable.steward} />
                      <span className="text-sm text-slate-700 font-medium">{selectedTable.steward}</span>
                    </div>
                  </div>
                  <div>
                    <h4 className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-2">Domain</h4>
                    <span className="inline-flex items-center gap-1 text-sm font-medium text-slate-700"><Globe className="w-3.5 h-3.5 text-slate-400" />{selectedTable.domain}</span>
                  </div>
                </div>

                {/* Lineage mini preview */}
                <div>
                  <div className="flex items-center justify-between mb-2">
                    <h4 className="text-sm font-semibold text-slate-700">Lineage Preview</h4>
                    <button onClick={() => openLineage(selectedTable.id)} className="text-xs text-blue-600 hover:underline">Open full lineage →</button>
                  </div>
                  <div className="h-48 rounded-xl overflow-hidden border border-slate-200">
                    <LineageGraph
                      tableId={selectedTable.id}
                      allTables={catalogTables}
                      onNodeClick={id => {
                        const t = catalogTables.find(t => t.id === id);
                        if (t && t.id !== selectedTable.id) setSelectedTable(t);
                      }}
                    />
                  </div>
                </div>

                {/* Columns */}
                {selectedTable.columns_detail && selectedTable.columns_detail.length > 0 && (
                  <div>
                    <h4 className="text-sm font-semibold text-slate-700 mb-3">Columns ({selectedTable.columns_detail.length} shown)</h4>
                    <div className="border border-slate-200 rounded-xl overflow-hidden">
                      <table className="w-full text-sm">
                        <thead className="bg-slate-50 border-b border-slate-200">
                          <tr>
                            {["Column", "Type", "Nullable", "PII", "Quality", "Description"].map(h => (
                              <th key={h} className="text-left px-3 py-2 text-xs font-semibold text-slate-500 uppercase tracking-wide">{h}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-100">
                          {selectedTable.columns_detail.map(col => (
                            <tr key={col.name} className="hover:bg-slate-50">
                              <td className="px-3 py-2.5"><code className="text-blue-700 font-medium text-xs bg-blue-50 px-1.5 py-0.5 rounded">{col.name}</code></td>
                              <td className="px-3 py-2.5"><span className="text-xs font-mono text-slate-600 bg-slate-100 px-1.5 py-0.5 rounded">{col.type}</span></td>
                              <td className="px-3 py-2.5">{col.nullable ? <span className="text-xs text-slate-400">YES</span> : <span className="text-xs text-slate-700 font-medium">NO</span>}</td>
                              <td className="px-3 py-2.5">{col.pii ? <span className="text-xs bg-red-100 text-red-700 px-1.5 py-0.5 rounded border border-red-200">PII</span> : <span className="text-xs text-slate-400">—</span>}</td>
                              <td className="px-3 py-2.5 w-24"><QualityBar score={col.quality} /></td>
                              <td className="px-3 py-2.5 text-xs text-slate-500">{col.description}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );

  // ── QUALITY TAB ────────────────────────────────────────────────────────────
  const QualityTab = () => {
    const [activeSection, setActiveSection] = useState("issues");
    return (
      <div className="flex-1 overflow-auto bg-slate-50">
        <div className="p-6 max-w-7xl mx-auto space-y-5">
          <div className="flex items-center justify-between">
            <h2 className="text-xl font-bold text-slate-900">Data Quality Center</h2>
            <div className="flex gap-2">
              {["issues", "scores", "rules"].map(s => (
                <button key={s} onClick={() => setActiveSection(s)}
                  className={`px-4 py-1.5 text-sm rounded-lg font-medium transition-colors ${activeSection === s ? "bg-blue-600 text-white" : "bg-white border border-slate-200 text-slate-600 hover:bg-slate-50"}`}>
                  {s.charAt(0).toUpperCase() + s.slice(1)}
                </button>
              ))}
            </div>
          </div>

          {activeSection === "issues" && (
            <>
              <div className="grid grid-cols-3 gap-4">
                {[
                  { label: "High Severity", count: highIssues, color: "text-red-700 bg-red-50 border-red-200" },
                  { label: "Medium Severity", count: catalogIssues.filter(i => i.severity === "medium").length, color: "text-yellow-700 bg-yellow-50 border-yellow-200" },
                  { label: "Low Severity", count: catalogIssues.filter(i => i.severity === "low").length, color: "text-blue-700 bg-blue-50 border-blue-200" },
                ].map(({ label, count, color }) => (
                  <div key={label} className={`rounded-xl border p-4 ${color}`}>
                    <div className="text-3xl font-bold">{count}</div>
                    <div className="text-sm font-medium mt-0.5">{label}</div>
                  </div>
                ))}
              </div>
              <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
                <div className="px-5 py-4 border-b border-slate-100">
                  <h3 className="font-semibold text-slate-800 text-sm">All Active Issues</h3>
                </div>
                <div className="divide-y divide-slate-100">
                  {catalogIssues.map(issue => (
                    <div key={issue.id} className="px-5 py-4 hover:bg-slate-50">
                      <div className="flex items-start gap-4">
                        <SeverityBadge severity={issue.severity} />
                        <div className="flex-1">
                          <div className="flex items-center gap-2">
                            <span className="font-semibold text-slate-800 text-sm">{issue.type}</span>
                            <span className="text-xs text-slate-500">in</span>
                            <code className="text-xs bg-blue-50 text-blue-700 px-2 py-0.5 rounded cursor-pointer hover:bg-blue-100"
                              onClick={() => { const t = catalogTables.find(t => t.id === issue.tableId); if (t) openTableDetail(t); }}>
                              {issue.table}
                            </code>
                          </div>
                          <p className="text-sm text-slate-600 mt-1">{issue.description}</p>
                          {issue.count > 0 && <span className="text-xs text-slate-400 mt-1 inline-block">{issue.count.toLocaleString()} records affected</span>}
                        </div>
                        <div className="flex items-center gap-2 flex-shrink-0">
                          <span className="text-xs text-slate-400">{issue.detectedAt}</span>
                          <button onClick={() => { const t = catalogTables.find(t => t.id === issue.tableId); if (t) openLineage(t.id); }}
                            className="p-1.5 hover:bg-slate-100 rounded-lg" title="View lineage">
                            <Network className="w-4 h-4 text-slate-400" />
                          </button>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </>
          )}

          {activeSection === "scores" && (
            <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
              <div className="px-5 py-4 border-b border-slate-100">
                <h3 className="font-semibold text-slate-800 text-sm">Quality Scores by Table</h3>
              </div>
              <div className="divide-y divide-slate-100">
                {[...catalogTables].sort((a, b) => b.quality - a.quality).map(t => (
                  <div key={t.id} className="px-5 py-3.5 flex items-center gap-4 hover:bg-slate-50 cursor-pointer" onClick={() => openTableDetail(t)}>
                    <div className="w-40 flex-shrink-0">
                      <div className="text-sm font-mono font-semibold text-slate-800">{t.name}</div>
                      <div className="text-xs text-slate-500">{t.connection}</div>
                    </div>
                    <div className="flex-1">
                      <QualityBar score={t.quality} />
                    </div>
                    <TrustBadge trust={t.trust} />
                    {t.issues > 0 && <span className="text-xs bg-red-100 text-red-700 border border-red-200 rounded px-2 py-0.5">{t.issues} issues</span>}
                  </div>
                ))}
              </div>
            </div>
          )}

          {activeSection === "rules" && (
            <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
              <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between">
                <h3 className="font-semibold text-slate-800 text-sm">Validation Rules</h3>
                <button className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-blue-600 text-white rounded-lg hover:bg-blue-700">
                  <Plus className="w-3.5 h-3.5" /> Add Rule
                </button>
              </div>
              <div className="divide-y divide-slate-100">
                {[
                  { name: "Null Check — customer_id", table: "email_campaigns", type: "Not Null", status: "failing", lastRun: "2 min ago" },
                  { name: "Format Check — email", table: "customer_master", type: "Regex", status: "passing", lastRun: "3 min ago" },
                  { name: "Range Check — amount_usd", table: "orders_fact", type: "Range", status: "passing", lastRun: "5 min ago" },
                  { name: "Referential Integrity — product_id", table: "inventory_snapshot", type: "FK Check", status: "failing", lastRun: "4 min ago" },
                  { name: "Freshness — 24h SLA", table: "marketing_attribution", type: "Freshness", status: "failing", lastRun: "26 hours ago" },
                  { name: "Uniqueness — order_id", table: "orders_fact", type: "Unique", status: "passing", lastRun: "7 min ago" },
                ].map((rule, i) => (
                  <div key={i} className="px-5 py-3.5 flex items-center gap-4 hover:bg-slate-50">
                    <span className={`w-2 h-2 rounded-full flex-shrink-0 ${rule.status === "passing" ? "bg-emerald-500" : "bg-red-500"}`} />
                    <div className="flex-1">
                      <div className="text-sm font-medium text-slate-800">{rule.name}</div>
                      <div className="text-xs text-slate-500">{rule.table} · {rule.type}</div>
                    </div>
                    <span className={`text-xs font-semibold ${rule.status === "passing" ? "text-emerald-700" : "text-red-700"}`}>
                      {rule.status === "passing" ? "Passing" : "Failing"}
                    </span>
                    <span className="text-xs text-slate-400">{rule.lastRun}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    );
  };

  // ── LINEAGE TAB ────────────────────────────────────────────────────────────
  const LineageTab = () => {
    const currentTable = catalogTables.find(t => t.id === lineageTableId);
    const { nodes, edges } = buildLineageGraph(lineageTableId, catalogTables);
    const tableNodes = nodes.filter(n => n.type === "table");
    const upstreamCount = tableNodes.filter(n => (n.layer ?? 0) < 0).length;
    const downstreamCount = tableNodes.filter(n => (n.layer ?? 0) > 0).length;

    return (
      <div className="flex-1 overflow-hidden flex flex-col">
        {/* Lineage toolbar */}
        <div className="bg-white border-b border-slate-200 px-5 py-3 flex items-center gap-4 flex-shrink-0">
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold text-slate-700">Viewing lineage for:</span>
            <div className="flex items-center gap-1.5 bg-blue-50 border border-blue-200 rounded-lg px-3 py-1.5">
              <Table className="w-4 h-4 text-blue-600" />
              <code className="text-sm font-bold text-blue-900">{currentTable?.name || lineageTableId}</code>
              {currentTable && <TrustBadge trust={currentTable.trust} />}
            </div>
          </div>
          <div className="flex items-center gap-3 text-xs text-slate-500">
            <span className="flex items-center gap-1"><ArrowLeft className="w-3.5 h-3.5 text-slate-400" />{upstreamCount} upstream</span>
            <span className="flex items-center gap-1"><ArrowRight className="w-3.5 h-3.5 text-slate-400" />{downstreamCount} downstream</span>
            <span>{edges.length} lineage edges</span>
          </div>
          <div className="flex-1" />
          <select
            value={lineageTableId}
            onChange={e => setLineageTableId(e.target.value)}
            className="px-3 py-1.5 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white"
          >
            {catalogTables.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
          </select>
        </div>

        {/* Graph area */}
        <div className="flex-1 p-4 overflow-hidden flex gap-4">
          <div className="flex-1 min-w-0">
            <LineageGraph
              tableId={lineageTableId}
              allTables={catalogTables}
              onNodeClick={id => setLineageTableId(id)}
            />
          </div>

          {/* Right panel: node list */}
          <div className="w-64 flex-shrink-0 bg-white rounded-xl border border-slate-200 shadow-sm overflow-auto">
            <div className="px-4 py-3 border-b border-slate-100">
              <h3 className="text-sm font-semibold text-slate-700">Nodes in View</h3>
            </div>
            <div className="divide-y divide-slate-50">
              {tableNodes.map(n => {
                const t = catalogTables.find(x => x.id === n.id);
                if (!t) return null;
                const layerLabel = n.layer === 0 ? "Selected" : n.layer < 0 ? `Upstream L${Math.abs(n.layer)}` : `Downstream L${n.layer}`;
                return (
                  <div key={n.id} onClick={() => setLineageTableId(n.id)}
                    className={`px-3 py-2.5 cursor-pointer hover:bg-slate-50 ${n.isRoot ? "bg-blue-50" : ""}`}>
                    <div className="flex items-center gap-2">
                      <Table className="w-3.5 h-3.5 text-slate-400 flex-shrink-0" />
                      <div className="flex-1 min-w-0">
                        <div className="text-xs font-mono font-semibold text-slate-800 truncate">{t.name}</div>
                        <div className="text-xs text-slate-500 truncate">{layerLabel} · QS {t.quality}%</div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        {/* Current table info strip */}
        {currentTable && (
          <div className="bg-white border-t border-slate-200 px-5 py-3 flex items-center gap-4 flex-shrink-0 text-sm">
            <div className="flex items-center gap-2">
              <span className="text-slate-500">Owner:</span>
              <Avatar name={currentTable.steward} size={6} />
              <span className="font-medium text-slate-700">{currentTable.steward}</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-slate-500">Quality:</span>
              <span className={`font-bold ${currentTable.quality >= 90 ? "text-emerald-600" : currentTable.quality >= 75 ? "text-yellow-600" : "text-red-600"}`}>{currentTable.quality}%</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-slate-500">Domain:</span>
              <span className="font-medium text-slate-700">{currentTable.domain}</span>
            </div>
            <div className="flex-1" />
            <button onClick={() => openTableDetail(currentTable)} className="flex items-center gap-1.5 text-xs text-blue-600 hover:underline">
              <ExternalLink className="w-3.5 h-3.5" /> Open in Catalog
            </button>
          </div>
        )}
      </div>
    );
  };

  // ── AGENTS TAB ─────────────────────────────────────────────────────────────
  const AgentsTab = () => {
    // Use live backend agents if available, fall back to mock
    const displayAgents = backendOnline && Object.keys(liveAgents).length > 0
      ? Object.values(liveAgents)
      : mockAgents.map(a => ({ ...a, tasks_completed: a.tasksCompleted }));

    const toggleAgent = async (agentId, currentStatus) => {
      if (!backendOnline) return;
      const endpoint = currentStatus === "active" ? "stop" : "start";
      await apiFetch(`/agents/${agentId}/${endpoint}`, { method: "POST" });
    };

    const liveCount = displayAgents.filter(a => a.status === "active").length;

    return (
      <div className="flex-1 overflow-auto bg-slate-50">
        <div className="p-6 max-w-7xl mx-auto space-y-5">
          <div className="flex items-center justify-between">
            <h2 className="text-xl font-bold text-slate-900">AI Agent Orchestration</h2>
            <div className="flex items-center gap-3">
              {backendOnline
                ? <span className="flex items-center gap-1.5 text-xs bg-emerald-50 text-emerald-700 border border-emerald-200 px-3 py-1.5 rounded-full font-medium"><Wifi className="w-3.5 h-3.5" />Live — {liveCount} of {displayAgents.length} running</span>
                : <span className="flex items-center gap-1.5 text-xs bg-amber-50 text-amber-700 border border-amber-200 px-3 py-1.5 rounded-full font-medium"><WifiOff className="w-3.5 h-3.5" />Demo mode — start backend to go live</span>
              }
            </div>
          </div>

          {!backendOnline && (
            <div className="bg-blue-50 border border-blue-200 rounded-xl px-5 py-4 flex items-start gap-3">
              <Bot className="w-5 h-5 text-blue-600 flex-shrink-0 mt-0.5" />
              <div>
                <div className="text-sm font-semibold text-blue-900">Start the backend to activate real agents</div>
                <div className="text-xs text-blue-700 mt-1">Open a Terminal in <code className="bg-blue-100 px-1 py-0.5 rounded">Data Quality App/backend/</code> and double-click <strong>Start Backend.command</strong>. Agents will connect automatically.</div>
              </div>
            </div>
          )}

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {displayAgents.map(agent => {
              const agentId = agent.id;
              const status  = agent.status;
              const isActive = status === "active";
              const mockMeta = mockAgents.find(m => m.id === agentId || m.name === agent.name) || {};
              return (
                <div key={agentId} className="bg-white rounded-xl border border-slate-200 shadow-sm p-5">
                  <div className="flex items-start justify-between mb-3">
                    <div className="flex items-center gap-3">
                      <div className={`w-10 h-10 rounded-xl flex items-center justify-center ${isActive ? "bg-emerald-50" : "bg-slate-100"}`}>
                        <Bot className={`w-5 h-5 ${isActive ? "text-emerald-600" : "text-slate-400"}`} />
                      </div>
                      <div>
                        <div className="font-semibold text-slate-800 text-sm">{agent.name}</div>
                        <AgentTypeBadge type={agent.type} />
                      </div>
                    </div>
                    <button
                      onClick={() => toggleAgent(agentId, status)}
                      disabled={!backendOnline}
                      className={`p-2 rounded-lg transition-colors ${!backendOnline ? "opacity-40 cursor-not-allowed" : ""} ${isActive ? "hover:bg-red-50 text-emerald-600 hover:text-red-600" : "hover:bg-emerald-50 text-slate-400 hover:text-emerald-600"}`}
                      title={backendOnline ? (isActive ? "Stop agent" : "Start agent") : "Start backend first"}
                    >
                      {isActive ? <Pause className="w-4 h-4" /> : <Play className="w-4 h-4" />}
                    </button>
                  </div>
                  <p className="text-xs text-slate-500 mb-4 leading-relaxed">{mockMeta.description || agent.description || ""}</p>
                  <div className="grid grid-cols-3 gap-2 text-center">
                    {[
                      { label: "Completed", value: (agent.tasks_completed ?? agent.tasksCompleted ?? 0).toLocaleString() },
                      { label: "Failed",    value: (agent.tasks_failed ?? 0).toLocaleString() },
                      { label: "Uptime",    value: agent.uptime ? agent.uptime.split(".")[0] : (mockMeta.avgRuntime || "—") },
                    ].map(({ label, value }) => (
                      <div key={label} className="bg-slate-50 rounded-lg p-2">
                        <div className="text-sm font-bold text-slate-800">{value}</div>
                        <div className="text-xs text-slate-500">{label}</div>
                      </div>
                    ))}
                  </div>
                  {/* Recent activity from live agent */}
                  {backendOnline && agent.recent_activity?.length > 0 && (
                    <div className="mt-3 pt-3 border-t border-slate-100">
                      <div className="text-xs text-slate-400 mb-1">Recent activity</div>
                      {agent.recent_activity.slice(-3).reverse().map((a, i) => (
                        <div key={i} className="text-xs text-slate-600 truncate flex items-center gap-1">
                          <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${a.status === "error" ? "bg-red-400" : a.status === "success" ? "bg-emerald-400" : "bg-blue-400"}`} />
                          {a.activity}
                        </div>
                      ))}
                    </div>
                  )}
                  <div className="mt-3 pt-3 border-t border-slate-100 flex items-center justify-between">
                    <div className="flex items-center gap-1.5">
                      <span className={`w-2 h-2 rounded-full ${isActive ? "bg-emerald-500 animate-pulse" : "bg-slate-300"}`} />
                      <span className="text-xs text-slate-500 capitalize">{status}</span>
                    </div>
                    <span className="text-xs text-slate-400">
                      {agent.last_run ? `Last: ${new Date(agent.last_run).toLocaleTimeString()}` : (agent.lastRun ? `Last: ${agent.lastRun}` : "Never run")}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>

          {/* Live log */}
          <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
            <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span className={`w-2 h-2 rounded-full ${backendOnline ? "bg-emerald-500 animate-pulse" : "bg-slate-400"}`} />
                <h3 className="text-sm font-semibold text-slate-800">
                  {backendOnline ? "Live WebSocket Log" : "Simulated Activity Log"}
                </h3>
              </div>
              <span className="text-xs text-slate-400">{agentLogs.length} entries</span>
            </div>
            <div className="p-4 h-64 overflow-auto font-mono space-y-1">
              {agentLogs.slice().reverse().map(log => (
                <div key={log.id} className="text-xs flex gap-3">
                  <span className="text-slate-400 flex-shrink-0">{log.timestamp}</span>
                  <span className={`flex-shrink-0 font-bold ${log.level === "error" ? "text-red-500" : log.level === "warn" ? "text-yellow-500" : "text-emerald-500"}`}>
                    [{(log.level || "info").toUpperCase()}]
                  </span>
                  <span className="text-blue-600 flex-shrink-0">[{log.agent}]</span>
                  <span className="text-slate-700">{log.message}</span>
                </div>
              ))}
              {agentLogs.length === 0 && <div className="text-slate-400 text-xs">Waiting for agent activity…</div>}
            </div>
          </div>
        </div>
      </div>
    );
  };

  // ── TASKS TAB ──────────────────────────────────────────────────────────────
  const TasksTab = () => {
    const [newTask, setNewTask] = useState({ title: "", agentId: "data_profiler", tableId: "", priority: "medium" });
    const [filter, setFilter] = useState("all");
    const [submitting, setSubmitting] = useState(false);

    // Merge live tasks with mock tasks for demo mode
    const allTasks = backendOnline ? liveTasks : [];

    const filtered = allTasks.filter(t => filter === "all" || t.status === filter);

    const AGENT_OPTIONS = backendOnline && Object.keys(liveAgents).length > 0
      ? Object.values(liveAgents).map(a => ({ id: a.id, label: a.name }))
      : [
          { id: "data_profiler",    label: "Data Profiler" },
          { id: "quality_validator", label: "Quality Validator" },
          { id: "lineage_tracker",   label: "Lineage Tracker" },
          { id: "anomaly_detector",  label: "Anomaly Detector" },
        ];

    const submitTask = async () => {
      if (!newTask.title.trim()) return;
      setSubmitting(true);
      const table = catalogTables.find(t => t.id === newTask.tableId);
      await apiFetch("/tasks", {
        method: "POST",
        body: JSON.stringify({
          agent_id: newTask.agentId,
          title: newTask.title,
          table_id: newTask.tableId || null,
          table_name: table?.name || null,
          priority: newTask.priority,
          task_data: newTask.tableId ? { table_id: newTask.tableId, table_name: table?.name } : {},
        }),
      });
      setNewTask({ title: "", agentId: newTask.agentId, tableId: "", priority: "medium" });
      setSubmitting(false);
    };

    const deleteTask = async (taskId) => {
      await apiFetch(`/tasks/${taskId}`, { method: "DELETE" });
    };

    const statusColor = { pending: "text-slate-500 bg-slate-100 border-slate-200", in_progress: "text-blue-700 bg-blue-50 border-blue-200", completed: "text-emerald-700 bg-emerald-50 border-emerald-200", failed: "text-red-700 bg-red-50 border-red-200" };
    const priorityColor = { high: "text-red-600", medium: "text-yellow-600", low: "text-slate-400" };
    const counts = { all: allTasks.length, pending: allTasks.filter(t => t.status === "pending").length, in_progress: allTasks.filter(t => t.status === "in_progress").length, completed: allTasks.filter(t => t.status === "completed").length };

    return (
      <div className="flex-1 overflow-auto bg-slate-50">
        <div className="p-6 max-w-5xl mx-auto space-y-5">
          <div className="flex items-center justify-between">
            <h2 className="text-xl font-bold text-slate-900">Agent Task Queue</h2>
            {backendOnline
              ? <span className="flex items-center gap-1.5 text-xs bg-emerald-50 text-emerald-700 border border-emerald-200 px-3 py-1.5 rounded-full"><Wifi className="w-3 h-3" />Live backend</span>
              : <span className="flex items-center gap-1.5 text-xs bg-amber-50 text-amber-700 border border-amber-200 px-3 py-1.5 rounded-full"><WifiOff className="w-3 h-3" />Backend offline — tasks won't persist</span>
            }
          </div>

          {/* New task form */}
          <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-5">
            <h3 className="text-sm font-semibold text-slate-700 mb-4">Submit New Task</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mb-3">
              <div className="md:col-span-2">
                <input
                  type="text"
                  placeholder="Task title — e.g. Profile customer_master, Validate orders_fact…"
                  className="w-full px-3 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  value={newTask.title}
                  onChange={e => setNewTask(n => ({ ...n, title: e.target.value }))}
                  onKeyDown={e => e.key === "Enter" && backendOnline && submitTask()}
                />
              </div>
              <div>
                <label className="text-xs font-medium text-slate-500 block mb-1">Assign to Agent</label>
                <select className="w-full px-3 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white"
                  value={newTask.agentId} onChange={e => setNewTask(n => ({ ...n, agentId: e.target.value }))}>
                  {AGENT_OPTIONS.map(a => <option key={a.id} value={a.id}>{a.label}</option>)}
                </select>
              </div>
              <div>
                <label className="text-xs font-medium text-slate-500 block mb-1">Target Table (optional)</label>
                <select className="w-full px-3 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white"
                  value={newTask.tableId} onChange={e => setNewTask(n => ({ ...n, tableId: e.target.value }))}>
                  <option value="">— No specific table —</option>
                  {catalogTables.map(t => <option key={t.id} value={t.id}>{t.name} ({t.schema})</option>)}
                </select>
              </div>
              <div>
                <label className="text-xs font-medium text-slate-500 block mb-1">Priority</label>
                <select className="w-full px-3 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white"
                  value={newTask.priority} onChange={e => setNewTask(n => ({ ...n, priority: e.target.value }))}>
                  <option value="high">High</option>
                  <option value="medium">Medium</option>
                  <option value="low">Low</option>
                </select>
              </div>
              <div className="flex items-end">
                <button
                  onClick={submitTask}
                  disabled={!backendOnline || !newTask.title.trim() || submitting}
                  className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                >
                  <Send className="w-4 h-4" />
                  {submitting ? "Submitting…" : "Submit Task"}
                </button>
              </div>
            </div>
          </div>

          {/* Filter tabs */}
          <div className="flex items-center gap-2">
            {Object.entries(counts).map(([key, count]) => (
              <button key={key} onClick={() => setFilter(key)}
                className={`px-3 py-1.5 text-xs rounded-lg font-medium transition-colors ${filter === key ? "bg-blue-600 text-white" : "bg-white border border-slate-200 text-slate-600 hover:bg-slate-50"}`}>
                {key.replace("_", " ").replace(/\b\w/g, c => c.toUpperCase())} ({count})
              </button>
            ))}
          </div>

          {/* Task list */}
          {filtered.length === 0 ? (
            <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-12 text-center">
              <ListTodo className="w-10 h-10 text-slate-300 mx-auto mb-3" />
              <div className="text-sm font-medium text-slate-500">No tasks yet</div>
              <div className="text-xs text-slate-400 mt-1">
                {backendOnline ? "Submit a task above to get started" : "Start the backend first, then submit tasks"}
              </div>
            </div>
          ) : (
            <div className="space-y-2">
              {filtered.map(task => (
                <div key={task.id} className="bg-white rounded-xl border border-slate-200 shadow-sm px-5 py-4">
                  <div className="flex items-start gap-4">
                    {/* Status icon */}
                    <div className="mt-0.5 flex-shrink-0">
                      {task.status === "completed" && <CheckCircle className="w-5 h-5 text-emerald-500" />}
                      {task.status === "failed"    && <AlertCircle  className="w-5 h-5 text-red-500" />}
                      {task.status === "in_progress" && <RefreshCw className="w-5 h-5 text-blue-500 animate-spin" />}
                      {task.status === "pending"   && <Clock className="w-5 h-5 text-slate-400" />}
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="font-semibold text-slate-800 text-sm">{task.title}</span>
                        <span className={`text-xs font-semibold border px-2 py-0.5 rounded-full ${statusColor[task.status] || "bg-slate-100 text-slate-600 border-slate-200"}`}>
                          {task.status.replace("_", " ")}
                        </span>
                        <span className={`text-xs font-bold ${priorityColor[task.priority]}`}>
                          {task.priority === "high" ? "↑" : task.priority === "low" ? "↓" : "→"} {task.priority}
                        </span>
                      </div>
                      <div className="flex items-center gap-3 mt-1.5 text-xs text-slate-500 flex-wrap">
                        <span className="flex items-center gap-1"><Bot className="w-3 h-3" />{task.agent_name || task.agent_id}</span>
                        {task.table_name && <span className="flex items-center gap-1"><Table className="w-3 h-3" /><code className="bg-slate-100 px-1 py-0.5 rounded">{task.table_name}</code></span>}
                        <span><Clock className="w-3 h-3 inline mr-0.5" />{new Date(task.created_at).toLocaleTimeString()}</span>
                      </div>
                      {task.result && (
                        <div className="mt-2 bg-slate-50 rounded-lg px-3 py-2 text-xs text-slate-700 font-mono">
                          {typeof task.result === "object"
                            ? Object.entries(task.result).slice(0, 4).map(([k, v]) => (
                                <span key={k} className="mr-3"><span className="text-slate-400">{k}:</span> {String(v)}</span>
                              ))
                            : String(task.result)}
                        </div>
                      )}
                      {task.error && (
                        <div className="mt-2 bg-red-50 rounded-lg px-3 py-2 text-xs text-red-700">{task.error}</div>
                      )}
                    </div>
                    <button onClick={() => deleteTask(task.id)} className="p-1.5 hover:bg-red-50 rounded-lg text-slate-400 hover:text-red-500 flex-shrink-0">
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    );
  };

  // ── GOVERNANCE TAB ─────────────────────────────────────────────────────────
  const GovernanceTab = () => (
    <div className="flex-1 overflow-auto bg-slate-50">
      <div className="p-6 max-w-7xl mx-auto space-y-6">
        <h2 className="text-xl font-bold text-slate-900">Governance & Compliance</h2>

        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          {[
            { label: "PII Tables", value: catalogTables.filter(t => t.tags.includes("PII")).length, icon: Lock, color: "text-red-600 bg-red-50 border-red-200" },
            { label: "Active Policies", value: mockPolicies.filter(p => p.status === "active").length, icon: Shield, color: "text-blue-600 bg-blue-50 border-blue-200" },
            { label: "Data Stewards", value: [...new Set(catalogTables.map(t => t.steward))].length, icon: Users, color: "text-purple-600 bg-purple-50 border-purple-200" },
            { label: "Compliance Domains", value: 4, icon: Globe, color: "text-teal-600 bg-teal-50 border-teal-200" },
          ].map(({ label, value, icon: Icon, color }) => (
            <div key={label} className={`bg-white rounded-xl border p-4 shadow-sm flex items-center gap-3 ${color.split(" ")[2]}`}>
              <div className={`p-2.5 rounded-xl ${color.split(" ")[1]}`}><Icon className={`w-5 h-5 ${color.split(" ")[0]}`} /></div>
              <div>
                <div className={`text-2xl font-bold ${color.split(" ")[0]}`}>{value}</div>
                <div className="text-xs text-slate-500">{label}</div>
              </div>
            </div>
          ))}
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Policies */}
          <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
            <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between">
              <h3 className="font-semibold text-slate-800 text-sm">Data Policies</h3>
              <button className="flex items-center gap-1 px-3 py-1.5 text-xs bg-blue-600 text-white rounded-lg hover:bg-blue-700"><Plus className="w-3.5 h-3.5" />New Policy</button>
            </div>
            <div className="divide-y divide-slate-100">
              {mockPolicies.map(p => (
                <div key={p.id} className="px-5 py-3.5 flex items-center gap-3">
                  <Shield className={`w-4 h-4 flex-shrink-0 ${p.status === "active" ? "text-emerald-500" : "text-yellow-500"}`} />
                  <div className="flex-1">
                    <div className="text-sm font-medium text-slate-800">{p.name}</div>
                    <div className="text-xs text-slate-500">{p.domain} · {p.tables} tables · Updated {p.lastUpdated}</div>
                  </div>
                  <span className={`text-xs font-semibold px-2 py-0.5 rounded-full border ${p.status === "active" ? "bg-emerald-50 text-emerald-700 border-emerald-200" : "bg-yellow-50 text-yellow-700 border-yellow-200"}`}>
                    {p.status === "active" ? "Active" : "Under Review"}
                  </span>
                </div>
              ))}
            </div>
          </div>

          {/* PII Inventory */}
          <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
            <div className="px-5 py-4 border-b border-slate-100">
              <h3 className="font-semibold text-slate-800 text-sm">PII & Sensitive Data Inventory</h3>
            </div>
            <div className="divide-y divide-slate-100">
              {catalogTables.filter(t => t.tags.some(tag => ["PII", "Sensitive", "Restricted", "PCI", "HRIS"].includes(tag))).map(t => (
                <div key={t.id} className="px-5 py-3.5 flex items-center gap-3 hover:bg-slate-50 cursor-pointer" onClick={() => openTableDetail(t)}>
                  <Lock className="w-4 h-4 text-red-400 flex-shrink-0" />
                  <div className="flex-1 min-w-0">
                    <div className="text-sm font-mono font-semibold text-slate-800">{t.name}</div>
                    <div className="flex gap-1 mt-0.5 flex-wrap">
                      {t.tags.filter(tg => ["PII", "Sensitive", "Restricted", "PCI", "HRIS", "GDPR", "SOX"].includes(tg)).map(tg => (
                        <span key={tg} className="text-xs bg-red-50 text-red-700 border border-red-200 px-1.5 py-0.5 rounded">{tg}</span>
                      ))}
                    </div>
                  </div>
                  <div className="flex items-center gap-2 flex-shrink-0">
                    <Avatar name={t.steward} size={6} />
                    <span className="text-xs text-slate-500">{t.steward}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Stewards */}
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
          <div className="px-5 py-4 border-b border-slate-100">
            <h3 className="font-semibold text-slate-800 text-sm">Data Stewards</h3>
          </div>
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-0 divide-x divide-y divide-slate-100">
            {[...new Set(catalogTables.map(t => t.steward))].map(steward => {
              const tables = catalogTables.filter(t => t.steward === steward);
              return (
                <div key={steward} className="p-4">
                  <div className="flex items-center gap-3 mb-2">
                    <Avatar name={steward} />
                    <div>
                      <div className="text-sm font-semibold text-slate-800">{steward}</div>
                      <div className="text-xs text-slate-400">{tables[0]?.domain}</div>
                    </div>
                  </div>
                  <div className="text-xs text-slate-500">{tables.length} table{tables.length !== 1 ? "s" : ""} stewarded</div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );

  // ── CONNECTIONS TAB ────────────────────────────────────────────────────────
  // ── New Connection Wizard ──────────────────────────────────────────────────
  const NewConnectionWizard = () => {
    // State lives in parent App so it survives re-mounts (fixes modal closing on Continue)
    const step = wizStep;               const setStep = setWizStep;
    const selectedType = wizSelectedType; const setSelectedType = setWizSelectedType;
    const formData = wizFormData;       const setFormData = setWizFormData;
    const connName = wizConnName;       const setConnName = setWizConnName;
    const testing = wizTesting;         const setTesting = setWizTesting;
    const testRes = wizTestRes;         const setTestRes = setWizTestRes;
    const saving = wizSaving;           const setSaving = setWizSaving;

    // Use live API types if available, fall back to hardcoded list
    const effectiveConnTypes = connTypes.length > 0 ? connTypes : FALLBACK_CONNECTOR_TYPES;
    const typeInfo = effectiveConnTypes.find(t => t.type === selectedType);

    const handleCreate = async () => {
      setSaving(true);
      const created = await apiFetch("/api/connections", {
        method: "POST",
        body: JSON.stringify({ name: connName, connector_type: selectedType, config: formData }),
      });
      if (created) {
        // Auto-test
        setTesting(true);
        const tr = await apiFetch(`/api/connections/${created.id}/test`, { method: "POST" });
        setTesting(false);
        setTestRes(tr);
        await loadRealConnections();
        setStep(3);
      }
      setSaving(false);
    };

    const connectorIcons = { postgresql: "🐘", fabric: "🪟", snowflake: "❄️", bigquery: "☁️", redshift: "🔴", databricks: "🧱" };

    return (
      <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-50 p-4">
        <div className="bg-white rounded-2xl shadow-2xl w-full max-w-lg">
          {/* Header */}
          <div className="flex items-center justify-between p-6 border-b border-slate-100">
            <div>
              <h2 className="text-lg font-bold text-slate-900">New Data Connection</h2>
              <p className="text-sm text-slate-500 mt-0.5">Step {step} of 3</p>
            </div>
            <button onClick={() => setShowNewConn(false)} className="p-2 hover:bg-slate-100 rounded-lg">
              <X className="w-5 h-5 text-slate-500" />
            </button>
          </div>

          {/* Progress bar */}
          <div className="h-1 bg-slate-100">
            <div className="h-1 bg-blue-600 transition-all" style={{ width: `${(step / 3) * 100}%` }} />
          </div>

          <div className="p-6 space-y-4">
            {/* Step 1: Choose type */}
            {step === 1 && (
              <>
                <p className="text-sm text-slate-600 mb-3">Select your data warehouse type to get started.</p>
                {!backendOnline && (
                  <div className="mb-3 flex items-center gap-2 px-3 py-2 bg-amber-50 border border-amber-200 rounded-lg text-xs text-amber-700">
                    <AlertTriangle className="w-3.5 h-3.5 flex-shrink-0" />
                    <span>Backend offline — start it to save & test connections. You can still browse the form.</span>
                  </div>
                )}
                <div className="grid grid-cols-2 gap-3">
                  {effectiveConnTypes.map(ct => (
                    <button key={ct.type}
                      onClick={() => setSelectedType(ct.type)}
                      className={`flex items-center gap-3 p-4 rounded-xl border-2 text-left transition-all ${
                        selectedType === ct.type
                          ? "border-blue-500 bg-blue-50"
                          : "border-slate-200 hover:border-slate-300 bg-white"
                      }`}>
                      <span className="text-2xl">{connectorIcons[ct.type] || "🔌"}</span>
                      <div>
                        <div className="font-semibold text-slate-800 text-sm">{ct.display_name}</div>
                        <div className="text-xs text-slate-400 capitalize">{ct.type}</div>
                      </div>
                    </button>
                  ))}
                </div>
                <div className="flex justify-end pt-2">
                  <button onClick={() => setStep(2)} disabled={!selectedType}
                    className="px-5 py-2.5 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed font-medium">
                    Continue →
                  </button>
                </div>
              </>
            )}

            {/* Step 2: Configure */}
            {step === 2 && typeInfo && (
              <>
                <div className="flex items-center gap-2 mb-2">
                  <span className="text-xl">{connectorIcons[selectedType] || "🔌"}</span>
                  <span className="font-semibold text-slate-700">{typeInfo.display_name} connection</span>
                </div>

                {/* Connection Name */}
                <div>
                  <label className="block text-xs font-semibold text-slate-600 mb-1.5 uppercase tracking-wide">Connection Name</label>
                  <input
                    value={connName}
                    onChange={e => setConnName(e.target.value)}
                    placeholder="e.g. Production Database"
                    className="w-full px-3 py-2.5 border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  />
                </div>

                {/* Dynamic fields from connector definition */}
                <div className="space-y-3 max-h-64 overflow-y-auto">
                  {(typeInfo.fields || []).filter(f => !f.show_when).map(field => (
                    <div key={field.name}>
                      <label className="block text-xs font-semibold text-slate-600 mb-1.5 uppercase tracking-wide">
                        {field.label}
                        {field.required && <span className="text-red-500 ml-1">*</span>}
                      </label>
                      {field.type === "select" ? (
                        <select
                          value={formData[field.name] || field.default || ""}
                          onChange={e => setFormData(p => ({ ...p, [field.name]: e.target.value }))}
                          className="w-full px-3 py-2.5 border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white">
                          {(field.options || []).map(o => <option key={o} value={o}>{o}</option>)}
                        </select>
                      ) : (
                        <input
                          type={field.type}
                          value={formData[field.name] || ""}
                          onChange={e => setFormData(p => ({ ...p, [field.name]: field.type === "number" ? Number(e.target.value) : e.target.value }))}
                          placeholder={field.placeholder}
                          className="w-full px-3 py-2.5 border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                        />
                      )}
                      {field.help && <p className="text-xs text-slate-400 mt-1">{field.help}</p>}
                    </div>
                  ))}

                  {/* auth_mode conditional fields for Fabric */}
                  {selectedType === "fabric" && (typeInfo.fields || []).filter(f => f.show_when?.auth_mode === (formData.auth_mode || "sql_auth")).map(field => (
                    <div key={field.name}>
                      <label className="block text-xs font-semibold text-slate-600 mb-1.5 uppercase tracking-wide">{field.label}</label>
                      <input
                        type={field.type}
                        value={formData[field.name] || ""}
                        onChange={e => setFormData(p => ({ ...p, [field.name]: e.target.value }))}
                        placeholder={field.placeholder}
                        className="w-full px-3 py-2.5 border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                      />
                    </div>
                  ))}
                </div>

                <div className="flex gap-3 pt-2">
                  <button onClick={() => setStep(1)} className="px-4 py-2.5 border border-slate-200 text-slate-600 text-sm rounded-lg hover:bg-slate-50">
                    ← Back
                  </button>
                  <button onClick={handleCreate} disabled={!connName || saving}
                    className="flex-1 px-5 py-2.5 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 disabled:opacity-40 font-medium flex items-center justify-center gap-2">
                    {saving ? <><RefreshCw className="w-4 h-4 animate-spin" /> Testing…</> : "Save & Test Connection"}
                  </button>
                </div>
              </>
            )}

            {/* Step 3: Result */}
            {step === 3 && (
              <div className="text-center py-4 space-y-4">
                {testing && (
                  <><RefreshCw className="w-10 h-10 text-blue-500 animate-spin mx-auto" />
                  <p className="text-slate-600">Testing connection…</p></>
                )}
                {!testing && testRes && (
                  <>
                    <div className={`w-16 h-16 rounded-full flex items-center justify-center mx-auto ${testRes.success ? "bg-emerald-100" : "bg-red-100"}`}>
                      {testRes.success ? <CheckCircle className="w-8 h-8 text-emerald-600" /> : <AlertTriangle className="w-8 h-8 text-red-600" />}
                    </div>
                    <div>
                      <h3 className={`font-bold text-lg ${testRes.success ? "text-emerald-700" : "text-red-700"}`}>
                        {testRes.success ? "Connection Successful!" : "Connection Failed"}
                      </h3>
                      <p className="text-sm text-slate-500 mt-1">{testRes.message}</p>
                      {testRes.latency_ms && <p className="text-xs text-slate-400 mt-1">Latency: {testRes.latency_ms}ms</p>}
                      {testRes.server_version && <p className="text-xs text-slate-400 mt-1 font-mono">{testRes.server_version?.substring(0, 80)}</p>}
                      {testRes.error && <p className="text-xs text-red-500 mt-2 font-mono">{testRes.error}</p>}
                    </div>
                    <button onClick={() => setShowNewConn(false)}
                      className="px-6 py-2.5 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 font-medium">
                      Done
                    </button>
                  </>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    );
  };

  // ── Connection Card ────────────────────────────────────────────────────────
  const RealConnectionCard = ({ conn }) => {
    const isExpanded = expandedConn === conn.id;
    const schemas = connSchemas[conn.id] || [];
    const tables  = connTables[conn.id]  || [];
    const tr      = testResults[conn.id];
    const discovering = discoveringSchema[conn.id];

    const handleTest = async () => {
      setTestResults(p => ({ ...p, [conn.id]: { testing: true } }));
      const r = await apiFetch(`/api/connections/${conn.id}/test`, { method: "POST" });
      setTestResults(p => ({ ...p, [conn.id]: r }));
      await loadRealConnections();
    };

    const handleDiscover = async (schema) => {
      setDiscoveringSchema(p => ({ ...p, [conn.id]: true }));
      const r = await apiFetch(`/api/connections/${conn.id}/discover?schema=${schema}`, { method: "POST" });
      if (r?.tables) setConnTables(p => ({ ...p, [conn.id]: r.tables }));
      setDiscoveringSchema(p => ({ ...p, [conn.id]: false }));
    };

    const handleLoadSchemas = async () => {
      if (!isExpanded) {
        setExpandedConn(conn.id);
        const r = await apiFetch(`/api/connections/${conn.id}/schemas`);
        if (r?.schemas) setConnSchemas(p => ({ ...p, [conn.id]: r.schemas }));
        // Also load any already-discovered tables
        const t = await apiFetch(`/api/connections/${conn.id}/tables`);
        if (Array.isArray(t)) setConnTables(p => ({ ...p, [conn.id]: t }));
      } else {
        setExpandedConn(null);
      }
    };

    const handleProfileTable = async (schema, tableName) => {
      setProfilingTable(`${schema}.${tableName}`);
      setProfileResult(null);
      const r = await apiFetch(`/api/connections/${conn.id}/profile?schema=${schema}&table=${tableName}`, { method: "POST" });
      setProfileResult(r);
      setProfilingTable(null);
      await loadRealConnections();
    };

    const statusDot = conn.status === "ok"
      ? "bg-emerald-500"
      : conn.status === "error"
      ? "bg-red-500"
      : "bg-yellow-400";

    const connectorIcons = { postgresql: "🐘", fabric: "🪟", snowflake: "❄️", bigquery: "☁️", redshift: "🔴" };

    return (
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
        {/* Card header */}
        <div className="p-5">
          <div className="flex items-start justify-between mb-3">
            <div className="flex items-start gap-3">
              <div className="w-10 h-10 rounded-xl bg-blue-50 flex items-center justify-center text-xl flex-shrink-0">
                {connectorIcons[conn.connector_type] || "🔌"}
              </div>
              <div>
                <div className="flex items-center gap-2">
                  <span className="font-semibold text-slate-800">{conn.name}</span>
                  <span className="text-xs bg-slate-100 text-slate-600 px-2 py-0.5 rounded font-mono">{conn.connector_type}</span>
                </div>
                {conn.description && <div className="text-xs text-slate-500 mt-0.5">{conn.description}</div>}
              </div>
            </div>
            <div className="flex items-center gap-1.5">
              <div className={`w-2.5 h-2.5 rounded-full ${statusDot}`} />
              <span className="text-xs font-medium text-slate-600 capitalize">{conn.status}</span>
            </div>
          </div>

          {/* Stats row */}
          <div className="grid grid-cols-3 gap-2 text-center">
            {[
              { label: "Tables Found", value: conn.table_count || "—" },
              { label: "Latency",      value: conn.latency_ms ? `${conn.latency_ms}ms` : "—" },
              { label: "Last Tested",  value: conn.last_tested_at ? new Date(conn.last_tested_at).toLocaleTimeString() : "Never" },
            ].map(({ label, value }) => (
              <div key={label} className="bg-slate-50 rounded-lg p-2.5">
                <div className="text-sm font-bold text-slate-800">{value}</div>
                <div className="text-xs text-slate-500">{label}</div>
              </div>
            ))}
          </div>

          {/* Error message */}
          {conn.last_test_error && (
            <div className="mt-3 p-2.5 bg-red-50 border border-red-200 rounded-lg">
              <p className="text-xs text-red-600 font-mono">{conn.last_test_error}</p>
            </div>
          )}

          {/* Inline test result */}
          {tr && !tr.testing && (
            <div className={`mt-3 p-2.5 rounded-lg border text-xs ${tr.success ? "bg-emerald-50 border-emerald-200 text-emerald-700" : "bg-red-50 border-red-200 text-red-600"}`}>
              {tr.success ? `✓ ${tr.message} · ${tr.latency_ms}ms` : `✗ ${tr.error || tr.message}`}
            </div>
          )}

          {/* Action buttons */}
          <div className="flex gap-2 mt-4">
            <button onClick={handleTest}
              disabled={tr?.testing}
              className="flex-1 flex items-center justify-center gap-1.5 text-xs py-2 border border-slate-200 rounded-lg hover:bg-slate-50 text-slate-600 disabled:opacity-50">
              {tr?.testing ? <RefreshCw className="w-3.5 h-3.5 animate-spin" /> : <Zap className="w-3.5 h-3.5" />}
              Test
            </button>
            <button onClick={handleLoadSchemas}
              className="flex-1 flex items-center justify-center gap-1.5 text-xs py-2 border border-slate-200 rounded-lg hover:bg-slate-50 text-slate-600">
              <Layers className="w-3.5 h-3.5" />
              {isExpanded ? "Hide Schema" : "Browse Schema"}
            </button>
            <button
              onClick={async () => {
                if (!window.confirm(`Delete connection "${conn.name}"?`)) return;
                await apiFetch(`/api/connections/${conn.id}`, { method: "DELETE" });
                await loadRealConnections();
              }}
              className="px-3 py-2 border border-red-200 rounded-lg hover:bg-red-50 text-red-500">
              <Trash2 className="w-3.5 h-3.5" />
            </button>
          </div>
        </div>

        {/* Schema browser */}
        {isExpanded && (
          <div className="border-t border-slate-100 bg-slate-50">
            <div className="p-4">
              <div className="flex items-center justify-between mb-3">
                <h4 className="text-xs font-semibold text-slate-600 uppercase tracking-wide">Schemas & Tables</h4>
                {discovering && <RefreshCw className="w-3.5 h-3.5 animate-spin text-blue-500" />}
              </div>

              {schemas.length === 0 && !discovering && (
                <p className="text-xs text-slate-400 text-center py-2">
                  {conn.status !== "ok" ? "Test connection first to browse schemas" : "No schemas found"}
                </p>
              )}

              <div className="space-y-1.5">
                {schemas.map(schema => (
                  <div key={schema} className="rounded-lg border border-slate-200 bg-white overflow-hidden">
                    <button
                      onClick={() => handleDiscover(schema)}
                      className="w-full flex items-center justify-between px-3 py-2.5 text-xs font-semibold text-slate-700 hover:bg-slate-50">
                      <div className="flex items-center gap-2">
                        <Layers className="w-3.5 h-3.5 text-blue-500" />
                        {schema}
                      </div>
                      <span className="text-slate-400">Discover →</span>
                    </button>

                    {/* Tables under this schema */}
                    {tables.filter(t => t.schema_name === schema).length > 0 && (
                      <div className="border-t border-slate-100">
                        {tables.filter(t => t.schema_name === schema).map(tbl => (
                          <div key={tbl.id}
                            className="flex items-center justify-between px-3 py-2 text-xs hover:bg-blue-50 group border-t border-slate-50 first:border-0">
                            <div className="flex items-center gap-2 min-w-0">
                              <Table className="w-3 h-3 text-slate-400 flex-shrink-0" />
                              <span className="font-mono text-slate-700 truncate">{tbl.table_name}</span>
                              <span className="text-slate-400">{tbl.row_count?.toLocaleString()} rows</span>
                            </div>
                            <div className="flex items-center gap-2">
                              {tbl.quality_score && (
                                <span className={`px-1.5 py-0.5 rounded text-xs font-semibold ${
                                  tbl.quality_score >= 90 ? "bg-emerald-100 text-emerald-700"
                                  : tbl.quality_score >= 70 ? "bg-yellow-100 text-yellow-700"
                                  : "bg-red-100 text-red-700"
                                }`}>{tbl.quality_score}%</span>
                              )}
                              <button
                                onClick={() => handleProfileTable(schema, tbl.table_name)}
                                disabled={profilingTable === `${schema}.${tbl.table_name}`}
                                className="opacity-0 group-hover:opacity-100 px-2 py-1 bg-blue-600 text-white rounded text-xs hover:bg-blue-700 disabled:opacity-50 flex items-center gap-1">
                                {profilingTable === `${schema}.${tbl.table_name}`
                                  ? <RefreshCw className="w-3 h-3 animate-spin" />
                                  : <Zap className="w-3 h-3" />}
                                Profile
                              </button>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </div>
    );
  };

  // ── Profile Result Modal ───────────────────────────────────────────────────
  const ProfileResultModal = () => {
    if (!profileResult) return null;
    return (
      <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-50 p-4">
        <div className="bg-white rounded-2xl shadow-2xl w-full max-w-3xl max-h-[85vh] flex flex-col">
          <div className="flex items-center justify-between p-6 border-b border-slate-100">
            <div>
              <h2 className="text-lg font-bold text-slate-900">{profileResult.full_name}</h2>
              <p className="text-sm text-slate-500 mt-0.5">
                {profileResult.row_count?.toLocaleString()} rows · {profileResult.column_count} columns · Quality: {profileResult.quality_score}%
              </p>
            </div>
            <button onClick={() => setProfileResult(null)} className="p-2 hover:bg-slate-100 rounded-lg">
              <X className="w-5 h-5 text-slate-500" />
            </button>
          </div>
          <div className="overflow-auto p-6">
            {profileResult.issues?.length > 0 && (
              <div className="mb-4 p-3 bg-amber-50 border border-amber-200 rounded-lg">
                <h4 className="text-xs font-semibold text-amber-700 mb-2">Issues Detected ({profileResult.issues.length})</h4>
                {profileResult.issues.map((issue, i) => (
                  <div key={i} className="text-xs text-amber-700 flex items-center gap-2 mb-1">
                    <span className={`px-1.5 py-0.5 rounded font-semibold ${
                      issue.severity === "high" ? "bg-red-100 text-red-700" : "bg-amber-100 text-amber-700"
                    }`}>{issue.severity}</span>
                    <span>{issue.description}</span>
                  </div>
                ))}
              </div>
            )}
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-slate-200">
                  {["Column", "Type", "Nullable", "Null%", "Distinct", "Min", "Max", "Quality"].map(h => (
                    <th key={h} className="text-left pb-2 pr-3 text-slate-500 font-semibold">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(profileResult.columns || []).map(col => (
                  <tr key={col.name} className="border-b border-slate-50 hover:bg-slate-50">
                    <td className="py-2 pr-3 font-mono font-semibold text-slate-800">{col.name}</td>
                    <td className="py-2 pr-3 text-slate-500 font-mono">{col.data_type?.split("(")[0]}</td>
                    <td className="py-2 pr-3">{col.nullable ? "✓" : "✗"}</td>
                    <td className="py-2 pr-3">
                      <span className={col.null_pct > 20 ? "text-red-600 font-semibold" : "text-slate-600"}>
                        {col.null_pct?.toFixed(1)}%
                      </span>
                    </td>
                    <td className="py-2 pr-3 text-slate-600">{col.distinct_count?.toLocaleString()}</td>
                    <td className="py-2 pr-3 text-slate-400 font-mono max-w-20 truncate">{col.min_value ?? "—"}</td>
                    <td className="py-2 pr-3 text-slate-400 font-mono max-w-20 truncate">{col.max_value ?? "—"}</td>
                    <td className="py-2 pr-3">
                      <span className={`px-1.5 py-0.5 rounded text-xs font-semibold ${
                        col.quality_score >= 90 ? "bg-emerald-100 text-emerald-700"
                        : col.quality_score >= 70 ? "bg-yellow-100 text-yellow-700"
                        : "bg-red-100 text-red-700"
                      }`}>{col.quality_score}%</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    );
  };

  // ── Connections Tab ────────────────────────────────────────────────────────
  const ConnectionsTab = () => (
    <div className="flex-1 overflow-auto bg-slate-50">
      {showNewConn && <NewConnectionWizard />}
      {profileResult && <ProfileResultModal />}

      <div className="p-6 max-w-7xl mx-auto space-y-5">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-xl font-bold text-slate-900">Data Connections</h2>
            <p className="text-sm text-slate-500 mt-0.5">
              {realConnections.length} connection{realConnections.length !== 1 ? "s" : ""} · plug-and-play warehouse connectivity
            </p>
          </div>
          <button
            onClick={() => setShowNewConn(true)}
            className="flex items-center gap-1.5 px-4 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 shadow-sm font-medium">
            <Plus className="w-4 h-4" /> New Connection
          </button>
        </div>

        {/* Backend offline banner */}
        {!backendOnline && (
          <div className="bg-amber-50 border border-amber-200 rounded-xl p-4 flex items-center gap-3">
            <AlertTriangle className="w-5 h-5 text-amber-500 flex-shrink-0" />
            <div>
              <p className="text-sm font-semibold text-amber-800">Backend Offline</p>
              <p className="text-xs text-amber-600 mt-0.5">Start the backend: <code className="font-mono bg-amber-100 px-1 rounded">uvicorn main:app --reload --port 8000</code></p>
            </div>
          </div>
        )}

        {/* Empty state */}
        {backendOnline && realConnections.length === 0 && (
          <div className="bg-white rounded-xl border border-dashed border-slate-300 p-12 text-center">
            <Database className="w-12 h-12 text-slate-300 mx-auto mb-4" />
            <h3 className="font-semibold text-slate-700 mb-2">No connections yet</h3>
            <p className="text-sm text-slate-500 mb-5 max-w-sm mx-auto">
              Connect your first data warehouse to start profiling, discovering, and monitoring data quality.
            </p>
            <button onClick={() => setShowNewConn(true)}
              className="px-5 py-2.5 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 font-medium">
              Add your first connection
            </button>
          </div>
        )}

        {/* Real connection cards */}
        {realConnections.length > 0 && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {realConnections.map(conn => (
              <RealConnectionCard key={conn.id} conn={conn} />
            ))}
          </div>
        )}

        {/* Supported warehouse types */}
        <div className="bg-white rounded-xl border border-slate-200 p-5">
          <h3 className="text-sm font-semibold text-slate-700 mb-3">Supported Warehouse Types</h3>
          <div className="flex flex-wrap gap-2">
            {[
              { icon: "🐘", name: "PostgreSQL", status: "available" },
              { icon: "🪟", name: "Microsoft Fabric", status: "available" },
              { icon: "❄️", name: "Snowflake", status: "coming-soon" },
              { icon: "☁️", name: "BigQuery", status: "coming-soon" },
              { icon: "🔴", name: "Redshift", status: "coming-soon" },
              { icon: "🧱", name: "Databricks", status: "coming-soon" },
            ].map(wh => (
              <div key={wh.name} className={`flex items-center gap-2 px-3 py-2 rounded-lg border text-sm ${
                wh.status === "available" ? "border-blue-200 bg-blue-50 text-blue-700" : "border-slate-200 bg-slate-50 text-slate-400"
              }`}>
                <span>{wh.icon}</span>
                <span className="font-medium">{wh.name}</span>
                {wh.status === "coming-soon" && <span className="text-xs">soon</span>}
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );

  // ── RENDER ─────────────────────────────────────────────────────────────────
  const renderTab = () => {
    switch (activeTab) {
      case "dashboard":    return <Dashboard />;
      case "catalog":      return <CatalogTab />;
      case "quality":      return <QualityTab />;
      case "lineage":      return <LineageTab />;
      case "agents":       return <AgentsTab />;
      case "tasks":        return <TasksTab />;
      case "governance":   return <GovernanceTab />;
      case "connections":  return <ConnectionsTab />;
      default:             return <Dashboard />;
    }
  };

  return (
    <div className="flex h-screen bg-slate-100 overflow-hidden font-sans" onClick={() => notifOpen && setNotifOpen(false)}>
      <Sidebar />
      <div className="flex-1 flex flex-col min-w-0">
        <TopBar />
        {renderTab()}
      </div>
    </div>
  );
}
