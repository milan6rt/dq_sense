import { useState, useEffect, useRef, useCallback, Component } from "react";

// ─── ERROR BOUNDARY ───────────────────────────────────────────────────────────
class ErrorBoundary extends Component {
  constructor(props) { super(props); this.state = { error: null }; }
  static getDerivedStateFromError(error) { return { error }; }
  componentDidCatch(error, info) { console.error("ErrorBoundary caught:", error, info); }
  render() {
    if (this.state.error) {
      return (
        <div className="flex-1 flex items-center justify-center bg-slate-50 p-8">
          <div className="bg-white rounded-2xl border border-red-200 shadow-sm p-8 max-w-md text-center">
            <div className="w-12 h-12 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-4">
              <span className="text-red-600 text-xl">⚠</span>
            </div>
            <h3 className="text-lg font-semibold text-slate-900 mb-2">Something went wrong</h3>
            <p className="text-sm text-slate-500 mb-4">{this.state.error?.message}</p>
            <button
              onClick={() => this.setState({ error: null })}
              className="px-4 py-2 bg-[#e8622b] text-white text-sm rounded-lg hover:opacity-90"
            >
              Try again
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}
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
  Check, Circle, ListTodo, Trash2, Send, Wifi, WifiOff, Pencil,
  Cpu, EyeOff
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
      { name: "host",     label: "Host",     type: "text",     placeholder: "localhost",  required: true, default: "localhost" },
      { name: "port",     label: "Port",     type: "number",   placeholder: "5432",       required: true, default: 5432 },
      { name: "database", label: "Database", type: "text",     placeholder: "postgres",   required: true, default: "postgres" },
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

// ─── AUTH TOKEN STORE (in-memory — never localStorage) ───────────────────────
let _authToken = null;
export function setAuthToken(t) { _authToken = t; }
export function getAuthToken()  { return _authToken; }

async function apiFetch(path, opts = {}) {
  try {
    const headers = { "Content-Type": "application/json" };
    if (_authToken) headers["Authorization"] = `Bearer ${_authToken}`;
    const res = await fetch(`${API_BASE}${path}`, { headers, ...opts });
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

function computeLayout(nodes) {
  // Fixed sizing constants — consistent with LineageGraph NODE_W/NODE_H
  const NODE_W = 190, NODE_H = 80, COL_GAP = 110, ROW_GAP = 32;

  const layers = {};
  nodes.forEach(n => {
    if (n.type === "etl") return;
    const l = n.layer ?? 0;
    if (!layers[l]) layers[l] = [];
    layers[l].push(n);
  });

  const layerKeys = Object.keys(layers).map(Number).sort((a, b) => a - b);
  const positions = {};
  const colStride = NODE_W + COL_GAP;

  layerKeys.forEach((lk, li) => {
    const col = layers[lk];
    const x = 60 + li * colStride + NODE_W / 2;
    // Total height of this column
    const totalH = col.length * NODE_H + (col.length - 1) * ROW_GAP;
    // Start Y so the column is centred around y=0 (we'll auto-fit the viewport)
    const startY = -totalH / 2 + NODE_H / 2;
    col.forEach((n, ni) => {
      positions[n.id] = { x, y: startY + ni * (NODE_H + ROW_GAP) };
    });
  });

  // ETL midpoints
  nodes.forEach(n => {
    if (n.type === "etl") {
      const p1 = positions[n.fromEdge] || { x: 0, y: 0 };
      const p2 = positions[n.toEdge]   || { x: 0, y: 0 };
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
    low:    "bg-[#fdf3ee] text-[#c94d1a] border-[#e8622b]/30",
  };
  return <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold border ${cfg[severity]}`}>{severity.charAt(0).toUpperCase() + severity.slice(1)}</span>;
};

const AgentTypeBadge = ({ type }) => {
  const cfg = {
    profiling:   "bg-purple-100 text-purple-700",
    validation:  "bg-blue-100 text-[#c94d1a]",
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

const Avatar = ({ name = '?', size = 7 }) => {
  const colors = ["bg-[#fdf3ee]0","bg-purple-500","bg-emerald-500","bg-orange-500","bg-rose-500","bg-indigo-500"];
  const safeName = name || '?';
  const idx = safeName.charCodeAt(0) % colors.length;
  return (
    <div className={`w-${size} h-${size} rounded-full ${colors[idx]} flex items-center justify-center text-white text-xs font-bold flex-shrink-0`}>
      {safeName.split(" ").map(p => p[0]).join("").slice(0, 2)}
    </div>
  );
};

// ─── LINEAGE VISUALIZATION ───────────────────────────────────────────────────

const NODE_W = 190, NODE_H = 80;

const LineageGraph = ({ tableId, allTables, onNodeClick }) => {
  const containerRef = useRef(null);
  const [dims, setDims] = useState({ w: 900, h: 500 });
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [dragging, setDragging] = useState(null);
  const [hoveredNode, setHoveredNode] = useState(null);
  const [highlightedPath, setHighlightedPath] = useState(null); // set of node ids on selected path
  const dimsRef = useRef(dims);
  dimsRef.current = dims;

  // Observe container resize
  useEffect(() => {
    const obs = new ResizeObserver(entries => {
      const { width, height } = entries[0].contentRect;
      setDims({ w: Math.max(600, width), h: Math.max(400, height) });
    });
    if (containerRef.current) obs.observe(containerRef.current);
    return () => obs.disconnect();
  }, []);

  const { nodes, edges } = buildLineageGraph(tableId, allTables);
  const tableNodes = nodes.filter(n => n.type === "table");
  const positions = computeLayout(nodes);

  // Auto-fit: compute bounding box of all positioned table nodes and center + scale to fill viewport
  const fitView = useCallback((posMap, d) => {
    const posArr = tableNodes.map(n => posMap[n.id]).filter(Boolean);
    if (!posArr.length) return;
    const xs = posArr.map(p => p.x);
    const ys = posArr.map(p => p.y);
    const minX = Math.min(...xs) - NODE_W / 2 - 40;
    const maxX = Math.max(...xs) + NODE_W / 2 + 40;
    const minY = Math.min(...ys) - NODE_H / 2 - 40;
    const maxY = Math.max(...ys) + NODE_H / 2 + 40;
    const contentW = maxX - minX;
    const contentH = maxY - minY;
    const newZoom = Math.min(1.2, Math.min(d.w / contentW, d.h / contentH) * 0.88);
    const newPanX = d.w / 2 - (minX + contentW / 2) * newZoom;
    const newPanY = d.h / 2 - (minY + contentH / 2) * newZoom;
    setZoom(newZoom);
    setPan({ x: newPanX, y: newPanY });
  }, [tableNodes]);

  // Trigger auto-fit when tableId changes or dims settle
  useEffect(() => {
    fitView(positions, dimsRef.current);
    setHighlightedPath(null);
  }, [tableId]);

  useEffect(() => {
    fitView(positions, dims);
  }, [dims.w, dims.h]);

  // Build upstream/downstream path from a node to root and all its parents/children
  const getPathNodes = (nodeId) => {
    const pathIds = new Set();
    const addUpstream = (id) => {
      pathIds.add(id);
      edges.filter(e => e.to === id).forEach(e => { if (!pathIds.has(e.from)) addUpstream(e.from); });
    };
    const addDownstream = (id) => {
      pathIds.add(id);
      edges.filter(e => e.from === id).forEach(e => { if (!pathIds.has(e.to)) addDownstream(e.to); });
    };
    addUpstream(nodeId);
    addDownstream(nodeId);
    return pathIds;
  };

  const trustColor = t => t === "gold" ? "#F59E0B" : t === "silver" ? "#94A3B8" : "#F97316";
  const qualityFill = q => !q ? "#94A3B8" : q >= 90 ? "#10B981" : q >= 75 ? "#F59E0B" : "#EF4444";

  // Drag-to-pan (on SVG background only)
  const handleMouseDown = (e) => {
    if (e.target.closest(".lineage-node")) return;
    e.preventDefault();
    setDragging({ startX: e.clientX - pan.x, startY: e.clientY - pan.y });
  };
  const handleMouseMove = (e) => {
    if (!dragging) return;
    setPan({ x: e.clientX - dragging.startX, y: e.clientY - dragging.startY });
  };
  const handleMouseUp = () => setDragging(null);

  const handleWheel = (e) => {
    e.preventDefault();
    const factor = e.deltaY < 0 ? 1.1 : 0.9;
    const rect = containerRef.current?.getBoundingClientRect();
    if (!rect) return;
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    setZoom(z => {
      const newZ = Math.max(0.2, Math.min(3, z * factor));
      setPan(p => ({
        x: mx - (mx - p.x) * (newZ / z),
        y: my - (my - p.y) * (newZ / z),
      }));
      return newZ;
    });
  };

  // Minimap constants
  const MM_W = 140, MM_H = 90, MM_PAD = 8;
  const mmPosArr = tableNodes.map(n => positions[n.id]).filter(Boolean);
  const mmXs = mmPosArr.map(p => p.x);
  const mmYs = mmPosArr.map(p => p.y);
  const mmMinX = mmXs.length ? Math.min(...mmXs) - NODE_W / 2 - 10 : 0;
  const mmMaxX = mmXs.length ? Math.max(...mmXs) + NODE_W / 2 + 10 : 1;
  const mmMinY = mmYs.length ? Math.min(...mmYs) - NODE_H / 2 - 10 : 0;
  const mmMaxY = mmYs.length ? Math.max(...mmYs) + NODE_H / 2 + 10 : 1;
  const mmCW = mmMaxX - mmMinX, mmCH = mmMaxY - mmMinY;
  const mmScaleX = MM_W / mmCW, mmScaleY = MM_H / mmCH;

  // Viewport rectangle in minimap coords
  const vpLeft   = (-pan.x / zoom - mmMinX) * mmScaleX;
  const vpTop    = (-pan.y / zoom - mmMinY) * mmScaleY;
  const vpWidth  = (dims.w / zoom) * mmScaleX;
  const vpHeight = (dims.h / zoom) * mmScaleY;

  return (
    <div ref={containerRef} className="relative w-full h-full bg-slate-50 rounded-xl overflow-hidden border border-slate-200" style={{ minHeight: 420 }}>

      {/* Toolbar */}
      <div className="absolute top-3 right-3 flex flex-col gap-1 z-20">
        <button onClick={() => setZoom(z => Math.min(3, z * 1.2))}
          className="w-8 h-8 bg-white border border-slate-200 rounded-lg flex items-center justify-center shadow-sm hover:bg-slate-50 transition-colors" title="Zoom in">
          <ZoomIn className="w-4 h-4 text-slate-600" />
        </button>
        <button onClick={() => setZoom(z => Math.max(0.2, z / 1.2))}
          className="w-8 h-8 bg-white border border-slate-200 rounded-lg flex items-center justify-center shadow-sm hover:bg-slate-50 transition-colors" title="Zoom out">
          <ZoomOut className="w-4 h-4 text-slate-600" />
        </button>
        <button onClick={() => fitView(positions, dims)}
          className="w-8 h-8 bg-white border border-slate-200 rounded-lg flex items-center justify-center shadow-sm hover:bg-slate-50 transition-colors" title="Fit to screen">
          <Maximize2 className="w-4 h-4 text-slate-600" />
        </button>
        <button onClick={() => { fitView(positions, dims); setHighlightedPath(null); }}
          className="w-8 h-8 bg-white border border-slate-200 rounded-lg flex items-center justify-center shadow-sm hover:bg-slate-50 transition-colors" title="Reset">
          <RotateCcw className="w-4 h-4 text-slate-600" />
        </button>
        {/* Zoom % */}
        <div className="w-8 text-center text-[10px] font-semibold text-slate-400 pt-0.5 select-none">
          {Math.round(zoom * 100)}%
        </div>
      </div>

      {/* Layer swim-lane labels */}
      <div className="absolute top-3 left-3 z-20 flex gap-1.5">
        <span className="bg-slate-600/75 text-white text-xs px-2 py-0.5 rounded-full backdrop-blur-sm">← Upstream</span>
        <span className="bg-[#e8622b]/80 text-white text-xs px-2 py-0.5 rounded-full backdrop-blur-sm">Selected</span>
        <span className="bg-slate-600/75 text-white text-xs px-2 py-0.5 rounded-full backdrop-blur-sm">Downstream →</span>
        {highlightedPath && (
          <button onClick={() => setHighlightedPath(null)}
            className="bg-white border border-slate-200 text-slate-600 text-xs px-2 py-0.5 rounded-full shadow-sm hover:bg-slate-50">
            ✕ Clear path
          </button>
        )}
      </div>

      {/* Legend */}
      <div className="absolute bottom-3 left-3 flex gap-2.5 z-20 bg-white/90 backdrop-blur-sm px-3 py-1.5 rounded-lg border border-slate-200 text-xs text-slate-600 shadow-sm">
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-full bg-emerald-500 inline-block" />≥90%</span>
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-full bg-yellow-400 inline-block" />75–89%</span>
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-full bg-red-400 inline-block" />&lt;75%</span>
        <span className="flex items-center gap-1 text-slate-400">Scroll to zoom · Drag to pan · Click node for path</span>
      </div>

      {/* Minimap */}
      {mmPosArr.length > 0 && (
        <div className="absolute bottom-3 right-3 z-20 bg-white/90 backdrop-blur-sm rounded-lg border border-slate-200 shadow-sm overflow-hidden" style={{ width: MM_W + MM_PAD * 2, height: MM_H + MM_PAD * 2 }}>
          <svg width={MM_W + MM_PAD * 2} height={MM_H + MM_PAD * 2}>
            <g transform={`translate(${MM_PAD},${MM_PAD})`}>
              {/* Mini nodes */}
              {tableNodes.map(n => {
                const p = positions[n.id];
                if (!p) return null;
                const mx = (p.x - mmMinX) * mmScaleX;
                const my = (p.y - mmMinY) * mmScaleY;
                const mw = NODE_W * mmScaleX, mh = NODE_H * mmScaleY;
                const isRoot = n.isRoot;
                const dimmed = highlightedPath && !highlightedPath.has(n.id);
                return (
                  <rect key={n.id}
                    x={mx - mw / 2} y={my - mh / 2} width={mw} height={mh} rx="2"
                    fill={isRoot ? "#1E40AF" : qualityFill(n.quality)}
                    opacity={dimmed ? 0.2 : 0.7}
                  />
                );
              })}
              {/* Viewport indicator */}
              <rect x={vpLeft} y={vpTop} width={Math.min(vpWidth, MM_W)} height={Math.min(vpHeight, MM_H)}
                fill="rgba(59,130,246,0.08)" stroke="#3B82F6" strokeWidth="1" rx="2" />
            </g>
          </svg>
        </div>
      )}

      {/* Empty state */}
      {tableNodes.length === 0 && (
        <div className="absolute inset-0 flex flex-col items-center justify-center gap-3 text-slate-400">
          <GitBranch className="w-12 h-12 opacity-30" />
          <p className="text-sm font-medium">No lineage data for this table</p>
          <p className="text-xs">Connect a database and run profiling to generate lineage</p>
        </div>
      )}

      {/* Main SVG canvas */}
      <svg
        width="100%" height="100%"
        className={dragging ? "cursor-grabbing select-none" : "cursor-grab"}
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseUp}
        onWheel={handleWheel}
        style={{ display: 'block' }}
      >
        <defs>
          <marker id="lg-arrow" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#94A3B8" />
          </marker>
          <marker id="lg-arrow-hi" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#F97316" />
          </marker>
          <filter id="lg-shadow" x="-25%" y="-25%" width="150%" height="150%">
            <feDropShadow dx="0" dy="2" stdDeviation="3" floodOpacity="0.10" />
          </filter>
          <filter id="lg-shadow-root" x="-25%" y="-25%" width="150%" height="150%">
            <feDropShadow dx="0" dy="4" stdDeviation="8" floodOpacity="0.22" />
          </filter>
        </defs>

        <g transform={`translate(${pan.x},${pan.y}) scale(${zoom})`}>

          {/* Swim-lane backgrounds per layer */}
          {[-2, -1, 0, 1, 2].map(l => {
            const lane = tableNodes.filter(n => n.layer === l);
            if (!lane.length) return null;
            const xs = lane.map(n => positions[n.id]?.x ?? 0);
            const ys = lane.map(n => positions[n.id]?.y ?? 0);
            const laneMinX = Math.min(...xs) - NODE_W / 2 - 18;
            const laneMaxX = Math.max(...xs) + NODE_W / 2 + 18;
            const laneMinY = Math.min(...ys) - NODE_H / 2 - 28;
            const laneMaxY = Math.max(...ys) + NODE_H / 2 + 12;
            const label = l < 0 ? `L${l} Upstream` : l > 0 ? `L+${l} Downstream` : "Selected";
            return (
              <g key={l}>
                <rect x={laneMinX} y={laneMinY} width={laneMaxX - laneMinX} height={laneMaxY - laneMinY}
                  fill={l === 0 ? "#EFF6FF" : l < 0 ? "#F8FAFC" : "#FFF7ED"}
                  stroke={l === 0 ? "#BFDBFE" : "#E2E8F0"}
                  strokeWidth="1" rx="10" opacity="0.85" />
                <text x={(laneMinX + laneMaxX) / 2} y={laneMinY + 16}
                  textAnchor="middle" fontSize="10" fill={l === 0 ? "#3B82F6" : "#94A3B8"}
                  fontWeight="600" style={{ fontFamily: "ui-sans-serif, system-ui" }}>
                  {label}
                </text>
              </g>
            );
          })}

          {/* Edges */}
          {edges.map((e, i) => {
            const p1 = positions[e.from];
            const p2 = positions[e.to];
            if (!p1 || !p2) return null;
            const onPath = highlightedPath && highlightedPath.has(e.from) && highlightedPath.has(e.to);
            const dimmed = highlightedPath && !onPath;
            const mx = (p1.x + p2.x) / 2;
            return (
              <path key={i}
                d={`M ${p1.x + NODE_W / 2} ${p1.y} C ${mx} ${p1.y}, ${mx} ${p2.y}, ${p2.x - NODE_W / 2} ${p2.y}`}
                fill="none"
                stroke={onPath ? "#F97316" : "#CBD5E1"}
                strokeWidth={onPath ? 2.5 : 1.5}
                strokeDasharray={e.type === "etl" ? "6,4" : undefined}
                markerEnd={`url(#${onPath ? "lg-arrow-hi" : "lg-arrow"})`}
                opacity={dimmed ? 0.15 : onPath ? 1 : 0.65}
              />
            );
          })}

          {/* Table Nodes */}
          {tableNodes.map(n => {
            const pos = positions[n.id];
            if (!pos) return null;
            const isRoot = n.isRoot;
            const isHovered = hoveredNode === n.id;
            const onPath = highlightedPath ? highlightedPath.has(n.id) : true;
            const dimmed = !onPath;
            const qColor = qualityFill(n.quality);
            const tColor = trustColor(n.trust);

            return (
              <g key={n.id} className="lineage-node"
                transform={`translate(${pos.x - NODE_W / 2}, ${pos.y - NODE_H / 2})`}
                onClick={() => {
                  const path = getPathNodes(n.id);
                  setHighlightedPath(prev => (prev && prev.has(n.id) && prev.size === path.size) ? null : path);
                  onNodeClick && onNodeClick(n.id);
                }}
                onMouseEnter={() => setHoveredNode(n.id)}
                onMouseLeave={() => setHoveredNode(null)}
                style={{ cursor: "pointer", opacity: dimmed ? 0.3 : 1, transition: "opacity 0.2s" }}
                filter={isRoot ? "url(#lg-shadow-root)" : "url(#lg-shadow)"}
              >
                {/* Node body */}
                <rect width={NODE_W} height={NODE_H} rx="10"
                  fill={isRoot ? "#1E40AF" : "white"}
                  stroke={isRoot ? "#1D4ED8" : isHovered ? "#F97316" : "#E2E8F0"}
                  strokeWidth={isRoot ? 0 : isHovered ? 2 : 1}
                />
                {/* Right quality stripe */}
                <rect x={NODE_W - 5} y={10} width={5} height={NODE_H - 20}
                  fill={qColor} rx="3" opacity={isRoot ? 0.5 : 0.9} />

                {/* Trust star */}
                <text x={10} y={14} fontSize="9" fill={tColor} fontWeight="bold">★</text>

                {/* Table icon */}
                <rect x={10} y={18} width={16} height={16} rx="3"
                  fill={isRoot ? "rgba(255,255,255,0.18)" : "#EFF6FF"} />
                <text x={18} y={30} fontSize="8" textAnchor="middle" fill={isRoot ? "white" : "#3B82F6"}>⊞</text>

                {/* Table name */}
                <text x={32} y={31} fontSize="11" fontWeight="700"
                  fill={isRoot ? "white" : "#1E293B"}
                  style={{ fontFamily: "ui-monospace, monospace" }}>
                  {n.label.length > 17 ? n.label.slice(0, 16) + "…" : n.label}
                </text>

                {/* Schema */}
                <text x={32} y={44} fontSize="9" fill={isRoot ? "rgba(255,255,255,0.65)" : "#64748B"}>
                  {n.schema}
                </text>

                {/* Quality score + domain pill */}
                {n.quality && (
                  <text x={10} y={64} fontSize="9" fill={isRoot ? "rgba(255,255,255,0.6)" : "#64748B"}>
                    QS: {n.quality}%
                  </text>
                )}
                {n.domain && (
                  <>
                    <rect x={60} y={54} width={Math.min(n.domain.length * 5 + 8, 80)} height={13} rx="6"
                      fill={isRoot ? "rgba(255,255,255,0.12)" : "#F1F5F9"} />
                    <text x={64} y={64} fontSize="8" fill={isRoot ? "rgba(255,255,255,0.75)" : "#475569"}>
                      {n.domain.length > 11 ? n.domain.slice(0, 10) + "…" : n.domain}
                    </text>
                  </>
                )}

                {/* Hover tooltip */}
                {isHovered && (
                  <g transform={`translate(${NODE_W / 2 - 70}, ${-52})`}>
                    <rect x="0" y="0" width="140" height="42" rx="7" fill="#1E293B" />
                    <polygon points="65,42 75,52 85,42" fill="#1E293B" />
                    <text x="10" y="16" fontSize="10" fill="white" fontWeight="700">{n.label}</text>
                    <text x="10" y="30" fontSize="9" fill="#94A3B8">Click to highlight path</text>
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

// ─── LOGIN SCREEN ────────────────────────────────────────────────────────────
function LoginScreen({ onToken }) {
  const [checking, setChecking] = useState(false);
  const [oauthConfigured, setOauthConfigured] = useState(null); // null=checking, true/false
  const [error, setError] = useState(null);

  useEffect(() => {
    // Handle ?token= or ?auth_error= returned from Google OAuth callback
    const params = new URLSearchParams(window.location.search);
    const token = params.get("token");
    const authError = params.get("auth_error");
    if (token) { window.history.replaceState({}, "", window.location.pathname); onToken(token); return; }
    if (authError) setError(`Authentication failed: ${authError}`);

    // Check backend auth status with a short timeout
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 3000);
    fetch(`${API_BASE}/api/auth/status`, { signal: ctrl.signal })
      .then(r => r.json())
      .then(d => setOauthConfigured(d.google_oauth_configured))
      .catch(() => setOauthConfigured(false))
      .finally(() => clearTimeout(timer));
  }, []);

  const handleGoogleLogin = () => {
    setChecking(true);
    window.location.href = `${API_BASE}/api/auth/google`;
  };

  // Demo / bypass mode — skip auth entirely
  const handleDemoMode = () => {
    onToken("demo");
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-blue-950 to-slate-900 flex items-center justify-center p-4">
      <div className="w-full max-w-md">
        {/* Logo */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center gap-3 mb-4">
            <div className="w-12 h-12 bg-[#fdf3ee]0 rounded-xl flex items-center justify-center shadow-lg shadow-blue-500/30">
              <Database className="w-7 h-7 text-white" />
            </div>
            <div className="text-left">
              <div className="text-2xl font-bold text-white tracking-tight">DataIQ</div>
              <div className="text-xs text-blue-300 font-medium tracking-widest uppercase">Enterprise Platform</div>
            </div>
          </div>
          <p className="text-slate-400 text-sm">Data Quality & Governance</p>
        </div>

        {/* Card */}
        <div className="bg-slate-800/60 backdrop-blur border border-slate-700/50 rounded-2xl p-8 shadow-2xl">
          <h2 className="text-xl font-semibold text-white text-center mb-2">Sign in to DataIQ</h2>
          <p className="text-slate-400 text-sm text-center mb-6">Use your Google account to continue</p>

          {error && (
            <div className="mb-4 p-3 bg-red-500/10 border border-red-500/30 rounded-lg text-red-400 text-sm text-center">
              {error}
            </div>
          )}

          {oauthConfigured === false && (
            <div className="mb-5 p-4 bg-amber-500/10 border border-amber-500/30 rounded-lg text-sm text-amber-300">
              <div className="font-semibold mb-1">⚠️ Google OAuth not configured</div>
              <div className="text-amber-400 text-xs leading-relaxed">
                Add <code className="bg-slate-700 px-1 rounded">GOOGLE_CLIENT_ID</code> &amp;{" "}
                <code className="bg-slate-700 px-1 rounded">GOOGLE_CLIENT_SECRET</code> to{" "}
                <code className="bg-slate-700 px-1 rounded">backend/.env</code> and restart the backend.
                Or use <strong className="text-amber-300">Demo Mode</strong> below.
              </div>
            </div>
          )}

          <button
            onClick={handleGoogleLogin}
            disabled={checking || oauthConfigured === false}
            className="w-full flex items-center justify-center gap-3 bg-white hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed text-slate-900 font-semibold py-3 px-6 rounded-xl transition-all duration-200 shadow-lg mb-3"
          >
            {checking ? (
              <div className="w-5 h-5 border-2 border-slate-400 border-t-transparent rounded-full animate-spin" />
            ) : (
              <svg viewBox="0 0 24 24" className="w-5 h-5" xmlns="http://www.w3.org/2000/svg">
                <path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z" fill="#4285F4"/>
                <path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853"/>
                <path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z" fill="#FBBC05"/>
                <path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" fill="#EA4335"/>
              </svg>
            )}
            {checking ? "Redirecting to Google…" : "Sign in with Google"}
          </button>

          {/* Divider */}
          <div className="flex items-center gap-3 my-4">
            <div className="flex-1 h-px bg-slate-700/50" />
            <span className="text-xs text-slate-500">or</span>
            <div className="flex-1 h-px bg-slate-700/50" />
          </div>

          {/* Demo / bypass button */}
          <button
            onClick={handleDemoMode}
            className="w-full flex items-center justify-center gap-2 border border-slate-600 hover:border-slate-400 text-slate-300 hover:text-white font-medium py-2.5 px-6 rounded-xl transition-all duration-200 text-sm"
          >
            <Zap className="w-4 h-4 text-blue-400" />
            Continue in Demo Mode
          </button>
          <p className="text-xs text-slate-600 text-center mt-2">Skip login · Full access · No credentials needed</p>

          <p className="text-xs text-slate-500 text-center mt-5">
            By signing in you agree to DataIQ's terms of use.
            <br />Your data stays on your own infrastructure.
          </p>
        </div>

        <p className="text-xs text-slate-600 text-center mt-4">
          DataIQ Platform · Enterprise Data Quality
        </p>
      </div>
    </div>
  );
}


// ─── AUTH WRAPPER (default export) ───────────────────────────────────────────
export default function DataIQPlatform() {
  const [authUser, setAuthUser] = useState(null);
  const [authReady, setAuthReady] = useState(false);

  const handleToken = useCallback(async (token) => {
    // Demo mode — bypass auth entirely
    if (token === "demo") {
      setAuthUser({ name: "Demo User", email: "demo@dataiq.local", picture: null, role: "admin" });
      setAuthReady(true);
      return;
    }
    setAuthToken(token);
    try {
      const me = await apiFetch("/api/auth/me");
      if (me && me.email) {
        setAuthUser(me);
      } else {
        setAuthUser({ name: "Demo User", email: "", picture: null, role: "admin" });
      }
    } catch {
      setAuthUser({ name: "Demo User", email: "", picture: null, role: "admin" });
    }
    setAuthReady(true);
  }, []);

  const handleLogout = useCallback(() => {
    setAuthToken(null);
    setAuthUser(null);
  }, []);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const token = params.get("token");
    if (token) {
      window.history.replaceState({}, "", window.location.pathname);
      handleToken(token);
    } else {
      setAuthReady(true);
    }
  }, [handleToken]);

  if (!authReady) {
    return (
      <div className="min-h-screen bg-slate-900 flex items-center justify-center">
        <div className="w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
      </div>
    );
  }
  if (!authUser) {
    return <LoginScreen onToken={handleToken} />;
  }
  return <DataIQApp authUser={authUser} handleLogout={handleLogout} />;
}

// ─── MAIN APP ─────────────────────────────────────────────────────────────────
// ─── CONNECTION FORM STEP (memoised so keystrokes stay local, no parent re-render) ──
const ConnectionFormStep = ({ typeInfo, selectedType, connectorIcons, saving, onBack, onSave }) => {
  const [connName, setConnName] = useState("");
  const [formData, setFormData] = useState({});

  return (
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
          className="w-full px-3 py-2.5 border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-[#e8622b] focus:border-[#e8622b]"
        />
      </div>

      {/* Dynamic fields */}
      <div className="space-y-3 max-h-96 overflow-y-auto">
        {(typeInfo.fields || []).filter(f => !f.show_when).map(field => (
          <div key={field.name}>
            <label className="block text-xs font-semibold text-slate-600 mb-1.5 uppercase tracking-wide">
              {field.label}{field.required && <span className="text-red-500 ml-1">*</span>}
            </label>
            {field.type === "select" ? (
              <select
                value={formData[field.name] || field.default || ""}
                onChange={e => setFormData(p => ({ ...p, [field.name]: e.target.value }))}
                className="w-full px-3 py-2.5 border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-[#e8622b] bg-white">
                {(field.options || []).map(o => <option key={o} value={o}>{o}</option>)}
              </select>
            ) : (
              <input
                type={field.type}
                value={formData[field.name] ?? (field.default ?? "")}
                onChange={e => setFormData(p => ({ ...p, [field.name]: field.type === "number" ? Number(e.target.value) : e.target.value }))}
                placeholder={field.placeholder}
                className="w-full px-3 py-2.5 border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-[#e8622b] focus:border-[#e8622b]"
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
              className="w-full px-3 py-2.5 border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-[#e8622b]"
            />
          </div>
        ))}
      </div>

      <div className="flex gap-3 pt-2">
        <button onClick={onBack} className="px-4 py-2.5 border border-slate-200 text-slate-600 text-sm rounded-lg hover:bg-slate-50">
          ← Back
        </button>
        <button
          onClick={() => onSave(connName, formData)}
          disabled={!connName || saving}
          className="flex-1 px-5 py-2.5 bg-[#e8622b] text-white text-sm rounded-lg hover:opacity-90 disabled:opacity-40 font-medium flex items-center justify-center gap-2">
          {saving ? <><RefreshCw className="w-4 h-4 animate-spin" /> Testing…</> : "Save & Test Connection"}
        </button>
      </div>
    </>
  );
};


// ─── NEW CONNECTION WIZARD (module-level — stable identity, no remount on parent re-render) ──
const NewConnectionWizard = ({ connTypes, backendOnline, onClose, onConnectionCreated }) => {
  const [step, setStep]               = useState(1);
  const [selectedType, setSelectedType] = useState(null);
  const [saving, setSaving]           = useState(false);
  const [testing, setTesting]         = useState(false);
  const [testRes, setTestRes]         = useState(null);

  const connectorIcons = { postgresql: "🐘", fabric: "🪟", snowflake: "❄️", bigquery: "☁️", redshift: "🔴" };
  const allTypes = (connTypes && connTypes.length > 0) ? connTypes : FALLBACK_CONNECTOR_TYPES;
  const typeInfo = allTypes.find(t => t.type === selectedType);

  const handleSave = async (connName, formData) => {
    setSaving(true);
    setTestRes(null);
    try {
      // Use raw fetch so we can capture the actual backend error message
      const headers = { "Content-Type": "application/json" };
      if (_authToken) headers["Authorization"] = `Bearer ${_authToken}`;
      const createRes = await fetch(`${API_BASE}/api/connections`, {
        method: "POST",
        headers,
        body: JSON.stringify({ name: connName, connector_type: selectedType, ...formData, config: formData }),
      });
      if (!createRes.ok) {
        let detail = `Server error ${createRes.status}`;
        try {
          const body = await createRes.json();
          // FastAPI 422 sends detail as array of validation error objects
          if (Array.isArray(body.detail)) {
            detail = body.detail.map(e => `${e.loc?.slice(-1)[0] || ''}: ${e.msg}`).join("; ");
          } else {
            detail = body.detail || JSON.stringify(body);
          }
        } catch (_) {}
        setTestRes({ status: "error", message: detail });
        return;
      }
      const created = await createRes.json();
      if (created && created.id) {
        setTesting(true);
        const testFetch = await fetch(`${API_BASE}/api/connections/${created.id}/test`, { method: "POST", headers });
        setTesting(false);
        let result = null;
        try { result = await testFetch.json(); } catch (_) {}
        // Backend returns { success: bool, message, error, latency_ms, server_version }
        const normalized = result ? { ...result, status: result.success ? "ok" : "error" } : null;
        setTestRes(normalized || { status: "error", message: "Test call failed" });
        if (normalized && normalized.status === "ok") {
          await onConnectionCreated();
          setTimeout(onClose, 800);
        }
      } else {
        setTestRes({ status: "error", message: "Unexpected response from server" });
      }
    } catch (e) {
      setTestRes({ status: "error", message: String(e) });
    } finally {
      setSaving(false);
      setTesting(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-lg max-h-[90vh] overflow-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-slate-100">
          <div>
            <h2 className="text-lg font-bold text-slate-900">New Connection</h2>
            <p className="text-xs text-slate-500 mt-0.5">
              Step {step} of 2 — {step === 1 ? "Choose connector type" : "Configure credentials"}
            </p>
          </div>
          <button onClick={onClose} className="p-1.5 rounded-lg hover:bg-slate-100 text-slate-400">
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="p-5 space-y-4">
          {!backendOnline && (
            <div className="bg-amber-50 border border-amber-200 rounded-lg p-3 flex items-center gap-2 text-sm text-amber-700">
              <AlertTriangle className="w-4 h-4 flex-shrink-0" />
              Backend offline — start <code className="font-mono bg-amber-100 px-1 rounded text-xs">uvicorn main:app --reload</code> first
            </div>
          )}

          {step === 1 && (
            <>
              <p className="text-sm text-slate-600">Select the database or warehouse you want to connect.</p>
              <div className="grid grid-cols-2 gap-3">
                {allTypes.map(ct => (
                  <button
                    key={ct.type}
                    onClick={() => { setSelectedType(ct.type); setStep(2); }}
                    className="flex items-center gap-3 p-4 border border-slate-200 rounded-xl hover:border-[#e8622b]/40 hover:bg-[#fdf3ee] text-left transition-colors group">
                    <span className="text-2xl">{connectorIcons[ct.type] || "🔌"}</span>
                    <div>
                      <div className="font-semibold text-slate-800 text-sm group-hover:text-[#c94d1a]">{ct.display_name}</div>
                      <div className="text-xs text-slate-400">{ct.type}</div>
                    </div>
                  </button>
                ))}
              </div>
            </>
          )}

          {step === 2 && typeInfo && (
            <ConnectionFormStep
              typeInfo={typeInfo}
              selectedType={selectedType}
              connectorIcons={connectorIcons}
              saving={saving || testing}
              onBack={() => { setStep(1); setTestRes(null); }}
              onSave={handleSave}
            />
          )}

          {testRes && (
            <div className={`rounded-lg p-3 text-sm ${
              testRes.status === "ok"
                ? "bg-green-50 text-green-700 border border-green-200"
                : "bg-red-50 text-red-700 border border-red-200"
            }`}>
              {testRes.status === "ok"
                ? "✅ Connection successful! Closing…"
                : `❌ ${typeof (testRes.error || testRes.message) === "string" ? (testRes.error || testRes.message) : "Connection failed"}`}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};


// ─── DQ RULES TAB (module-level for stable React identity) ───────────────────
const BLANK_FORM = { name: "", rule_type: "not_null", table_id: "", column_name: "", severity: "medium", description: "", parameters: {} };

const RulesTab = () => {
  const [rules, setRules] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [editingId, setEditingId] = useState(null);   // null = create, string = edit
  const [templates, setTemplates] = useState([]);
  const [runResults, setRunResults] = useState({});
  const [running, setRunning] = useState({});
  const [allTables, setAllTables] = useState([]);
  const [tableColumns, setTableColumns] = useState([]);  // columns for selected table
  const [form, setForm] = useState(BLANK_FORM);

  useEffect(() => {
    apiFetch("/api/rules/").then(d => { if (d) setRules(d); setLoading(false); });
    apiFetch("/api/rules/templates").then(d => { if (d) setTemplates(d); });
    apiFetch("/api/connections").then(async conns => {
      if (!conns) return;
      const tableArrays = await Promise.all(
        conns.map(c => apiFetch(`/api/connections/${c.id}/tables`).then(t => t || []))
      );
      setAllTables(tableArrays.flat());
    });
  }, []);

  // When table selection changes, fetch that table's columns
  useEffect(() => {
    if (!form.table_id) { setTableColumns([]); return; }
    const tbl = allTables.find(t => t.id === form.table_id);
    if (!tbl) return;
    apiFetch(`/api/connections/${tbl.connection_id}/tables/${tbl.id}/columns`)
      .then(cols => { if (cols) setTableColumns(cols); else setTableColumns([]); });
  }, [form.table_id, allTables]);

  const openCreate = () => { setForm(BLANK_FORM); setEditingId(null); setShowForm(true); };
  const openEdit   = (rule) => {
    setForm({
      name:        rule.name,
      rule_type:   rule.rule_type,
      table_id:    rule.table_id,
      column_name: rule.column_name || "",
      severity:    rule.severity,
      description: rule.description || "",
      sql:         rule.parameters?.sql || "",
      pattern:     rule.parameters?.pattern || "",
      threshold:   rule.parameters?.threshold ?? "",
      min_rows:    rule.parameters?.min_rows ?? "",
      max_age_hours: rule.parameters?.max_age_hours ?? 24,
      ts_col:      rule.parameters?.timestamp_col || "updated_at",
      parameters:  rule.parameters || {},
    });
    setEditingId(rule.id);
    setShowForm(true);
  };
  const closeForm = () => { setShowForm(false); setEditingId(null); setForm(BLANK_FORM); };

  const severityColor = { low: "text-blue-500 bg-[#fdf3ee]", medium: "text-yellow-600 bg-yellow-50", high: "text-orange-500 bg-orange-50", critical: "text-red-600 bg-red-50" };
  const statusColor   = { pass: "text-green-600 bg-green-50", fail: "text-red-600 bg-red-50", error: "text-slate-500 bg-slate-100" };

  const runRule = async (id) => {
    setRunning(r => ({ ...r, [id]: true }));
    const res = await apiFetch(`/api/rules/${id}/run`, { method: "POST" });
    setRunning(r => ({ ...r, [id]: false }));
    if (res) {
      setRunResults(r => ({ ...r, [id]: res }));
      setRules(prev => prev.map(r => r.id === id ? { ...r, last_run_status: res.status, last_run_at: res.ran_at } : r));
    }
  };

  const deleteRule = async (id) => {
    await apiFetch(`/api/rules/${id}`, { method: "DELETE" });
    setRules(prev => prev.filter(r => r.id !== id));
  };

  const saveRule = async () => {
    const paramStr = form.rule_type === "custom_sql" ? { sql: form.sql || "" }
                   : form.rule_type === "regex"      ? { pattern: form.pattern || ".*" }
                   : form.rule_type === "min_value"  ? { threshold: parseFloat(form.threshold || 0) }
                   : form.rule_type === "max_value"  ? { threshold: parseFloat(form.threshold || 0) }
                   : form.rule_type === "row_count"  ? { min_rows: parseInt(form.min_rows || 1) }
                   : form.rule_type === "freshness"  ? { max_age_hours: parseInt(form.max_age_hours || 24), timestamp_col: form.ts_col || "updated_at" }
                   : {};
    const body = { ...form, parameters: paramStr };
    if (editingId) {
      const res = await apiFetch(`/api/rules/${editingId}`, { method: "PUT", body: JSON.stringify(body) });
      if (res) { setRules(prev => prev.map(r => r.id === editingId ? res : r)); closeForm(); }
    } else {
      const res = await apiFetch("/api/rules/", { method: "POST", body: JSON.stringify(body) });
      if (res) { setRules(prev => [res, ...prev]); closeForm(); }
    }
  };

  return (
    <div className="flex-1 overflow-auto" style={{ background: "var(--bg)" }}>
      <div className="p-6 max-w-6xl mx-auto space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-slate-800">DQ Rule Engine</h1>
            <p className="text-slate-500 text-sm mt-1">Custom data quality rules that run against your tables</p>
          </div>
          <div className="flex gap-2">
            <button onClick={async () => { const r = await apiFetch("/api/rules/run-all", { method: "POST" }); if (r) alert(`Ran ${r.total} rules: ${r.passed} passed, ${r.failed} failed`); }} className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg text-sm font-medium hover:bg-green-700">
              <Play className="w-4 h-4" /> Run All
            </button>
            <button onClick={openCreate} className="flex items-center gap-2 px-4 py-2 bg-[#e8622b] text-white rounded-lg text-sm font-medium hover:opacity-90">
              <Plus className="w-4 h-4" /> New Rule
            </button>
          </div>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-4 gap-4">
          {[
            { label: "Total Rules",  value: rules.length,                                           color: "blue"   },
            { label: "Active",       value: rules.filter(r => r.is_active).length,                  color: "green"  },
            { label: "Passing",      value: rules.filter(r => r.last_run_status === "pass").length, color: "emerald"},
            { label: "Failing",      value: rules.filter(r => r.last_run_status === "fail").length, color: "red"    },
          ].map(s => (
            <div key={s.label} className="bg-white rounded-xl p-4 border border-slate-200 shadow-sm">
              <div className={`text-2xl font-bold text-${s.color}-600`}>{s.value}</div>
              <div className="text-xs text-slate-500 mt-1">{s.label}</div>
            </div>
          ))}
        </div>

        {/* Create / Edit Rule form */}
        {showForm && (
          <div className="bg-white border border-[#e8622b]/30 rounded-xl p-6 shadow-sm">
            <h3 className="font-semibold text-slate-800 mb-4">{editingId ? "Edit Rule" : "Create Rule"}</h3>
            <div className="grid grid-cols-2 gap-4">
              <div><label className="text-xs font-medium text-slate-600">Rule Name</label><input className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm" placeholder="e.g. Orders must have valid customer" value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} /></div>
              <div><label className="text-xs font-medium text-slate-600">Rule Type</label>
                <select className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm" value={form.rule_type} onChange={e => setForm(f => ({ ...f, rule_type: e.target.value }))}>
                  {templates.map(t => <option key={t.rule_type} value={t.rule_type}>{t.name}</option>)}
                </select>
              </div>
              <div><label className="text-xs font-medium text-slate-600">Table</label>
                <select className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm" value={form.table_id} onChange={e => setForm(f => ({ ...f, table_id: e.target.value, column_name: "" }))}>
                  <option value="">— select a table —</option>
                  {allTables.map(t => (
                    <option key={t.id} value={t.id}>{t.connection_name} / {t.schema_name}.{t.table_name}</option>
                  ))}
                </select>
              </div>
              <div><label className="text-xs font-medium text-slate-600">Column {tableColumns.length > 0 ? `(${tableColumns.length} available)` : ""}</label>
                {tableColumns.length > 0 ? (
                  <select className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm" value={form.column_name} onChange={e => setForm(f => ({ ...f, column_name: e.target.value }))}>
                    <option value="">— select a column (optional) —</option>
                    {tableColumns.map(c => (
                      <option key={c.column_name} value={c.column_name}>{c.column_name}{c.data_type ? ` (${c.data_type})` : ""}</option>
                    ))}
                  </select>
                ) : (
                  <input className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm" placeholder={form.table_id ? "Loading columns…" : "Select a table first"} value={form.column_name} onChange={e => setForm(f => ({ ...f, column_name: e.target.value }))} />
                )}
              </div>
              <div><label className="text-xs font-medium text-slate-600">Severity</label>
                <select className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm" value={form.severity} onChange={e => setForm(f => ({ ...f, severity: e.target.value }))}>
                  {["low","medium","high","critical"].map(s => <option key={s} value={s}>{s.charAt(0).toUpperCase()+s.slice(1)}</option>)}
                </select>
              </div>
              <div><label className="text-xs font-medium text-slate-600">Description</label><input className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm" placeholder="What does this rule check?" value={form.description} onChange={e => setForm(f => ({ ...f, description: e.target.value }))} /></div>
              {/* Dynamic parameter fields */}
              {form.rule_type === "regex"     && <div><label className="text-xs font-medium text-slate-600">Regex Pattern</label><input className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm font-mono" placeholder="^[A-Z]" value={form.pattern||""} onChange={e => setForm(f => ({ ...f, pattern: e.target.value }))} /></div>}
              {(form.rule_type === "min_value" || form.rule_type === "max_value") && <div><label className="text-xs font-medium text-slate-600">Threshold</label><input type="number" className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm" value={form.threshold||""} onChange={e => setForm(f => ({ ...f, threshold: e.target.value }))} /></div>}
              {form.rule_type === "row_count" && <div><label className="text-xs font-medium text-slate-600">Minimum Rows</label><input type="number" className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm" value={form.min_rows||""} onChange={e => setForm(f => ({ ...f, min_rows: e.target.value }))} /></div>}
              {form.rule_type === "freshness" && <>
                <div><label className="text-xs font-medium text-slate-600">Max Age (hours)</label><input type="number" className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm" value={form.max_age_hours||24} onChange={e => setForm(f => ({ ...f, max_age_hours: e.target.value }))} /></div>
                <div><label className="text-xs font-medium text-slate-600">Timestamp Column</label><input className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm" value={form.ts_col||"updated_at"} onChange={e => setForm(f => ({ ...f, ts_col: e.target.value }))} /></div>
              </>}
              {form.rule_type === "custom_sql" && <div className="col-span-2"><label className="text-xs font-medium text-slate-600">SQL (0 rows = pass)</label><textarea className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm font-mono" rows={3} placeholder="SELECT * FROM schema.table WHERE condition_fails" value={form.sql||""} onChange={e => setForm(f => ({ ...f, sql: e.target.value }))} /></div>}
            </div>
            <div className="flex justify-end gap-2 mt-4">
              <button onClick={closeForm} className="px-4 py-2 text-sm text-slate-600 hover:bg-slate-100 rounded-lg">Cancel</button>
              <button onClick={saveRule} disabled={!form.name || !form.table_id} className="px-4 py-2 bg-[#e8622b] text-white text-sm rounded-lg font-medium hover:opacity-90 disabled:opacity-50">{editingId ? "Update Rule" : "Save Rule"}</button>
            </div>
          </div>
        )}

        {/* Rules list */}
        {loading ? (
          <div className="text-center text-slate-400 py-12">Loading rules…</div>
        ) : rules.length === 0 ? (
          <div className="text-center text-slate-400 py-12 bg-white rounded-xl border border-slate-200">
            <CheckCircle className="w-10 h-10 mx-auto mb-3 text-slate-300" />
            <div className="font-medium text-slate-500">No rules yet</div>
            <div className="text-sm mt-1">Click "New Rule" to create your first data quality rule</div>
          </div>
        ) : (
          <div className="space-y-3">
            {rules.map(rule => {
              const res = runResults[rule.id];
              const lastStatus = res?.status || rule.last_run_status;
              return (
                <div key={rule.id} className="bg-white border border-slate-200 rounded-xl p-4 shadow-sm">
                  <div className="flex items-start justify-between gap-4">
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="font-semibold text-slate-800">{rule.name}</span>
                        <span className="text-xs px-2 py-0.5 bg-slate-100 text-slate-600 rounded-full font-mono">{rule.rule_type}</span>
                        <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${severityColor[rule.severity] || "text-slate-500 bg-slate-100"}`}>{rule.severity}</span>
                        {lastStatus && <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${statusColor[lastStatus] || ""}`}>{lastStatus === "pass" ? "✓ Pass" : lastStatus === "fail" ? "✗ Fail" : "⚠ Error"}</span>}
                        {!rule.is_active && <span className="text-xs px-2 py-0.5 bg-slate-100 text-slate-400 rounded-full">disabled</span>}
                      </div>
                      {rule.description && <p className="text-sm text-slate-500 mt-1">{rule.description}</p>}
                      {rule.column_name && <p className="text-xs text-slate-400 mt-0.5">Column: <span className="font-mono">{rule.column_name}</span></p>}
                      {res && <p className={`text-xs mt-1 font-medium ${res.status === "pass" ? "text-green-600" : "text-red-600"}`}>{res.message}</p>}
                      {rule.last_run_at && !res && <p className="text-xs text-slate-400 mt-0.5">Last run: {new Date(rule.last_run_at).toLocaleString()}</p>}
                    </div>
                    <div className="flex items-center gap-2 flex-shrink-0">
                      <button onClick={() => runRule(rule.id)} disabled={running[rule.id]} className="flex items-center gap-1 px-3 py-1.5 bg-green-50 text-green-700 hover:bg-green-100 rounded-lg text-xs font-medium disabled:opacity-50">
                        {running[rule.id] ? <RefreshCw className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />} Run
                      </button>
                      <button onClick={() => openEdit(rule)} className="flex items-center gap-1 px-3 py-1.5 bg-[#fdf3ee] text-[#c94d1a] hover:bg-blue-100 rounded-lg text-xs font-medium">
                        <Pencil className="w-3 h-3" /> Edit
                      </button>
                      <button onClick={() => deleteRule(rule.id)} className="p-1.5 text-slate-400 hover:text-red-500 hover:bg-red-50 rounded-lg">
                        <Trash2 className="w-4 h-4" />
                      </button>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
};

// ─── SCHEDULER TAB (module-level for stable React identity) ──────────────────
const SchedulerTab = () => {
  const [scans, setScans] = useState([]);
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showNew, setShowNew] = useState(false);
  const [running, setRunning] = useState({});
  const [form, setForm] = useState({ name: "", schedule_cron: "0 6 * * *", connection_id: "", is_active: true });

  useEffect(() => {
    Promise.all([
      apiFetch("/api/scheduler/"),
      apiFetch("/api/scheduler/history"),
    ]).then(([s, h]) => {
      if (s) setScans(s);
      if (h) setHistory(h);
      setLoading(false);
    });
  }, []);

  const runNow = async (id) => {
    setRunning(r => ({ ...r, [id]: true }));
    const res = await apiFetch(`/api/scheduler/${id}/run-now`, { method: "POST" });
    setRunning(r => ({ ...r, [id]: false }));
    if (res) setScans(prev => prev.map(s => s.id === id ? { ...s, last_run_status: res.status, last_run_at: res.ran_at } : s));
  };

  const saveScan = async () => {
    const res = await apiFetch("/api/scheduler/", { method: "POST", body: JSON.stringify(form) });
    if (res) { setScans(prev => [res, ...prev]); setShowNew(false); setForm({ name: "", schedule_cron: "0 6 * * *", connection_id: "", is_active: true }); }
  };

  const toggleScan = async (scan) => {
    const res = await apiFetch(`/api/scheduler/${scan.id}`, { method: "PUT", body: JSON.stringify({ is_active: !scan.is_active }) });
    if (res) setScans(prev => prev.map(s => s.id === scan.id ? res : s));
  };

  const statusColor = { completed: "text-green-600 bg-green-50", failed: "text-red-600 bg-red-50", running: "text-[#e8622b] bg-[#fdf3ee]" };
  const PRESETS = [
    { label: "Every day at 6am",   cron: "0 6 * * *"   },
    { label: "Every 6 hours",      cron: "0 */6 * * *" },
    { label: "Every hour",         cron: "0 * * * *"   },
    { label: "Every 30 minutes",   cron: "*/30 * * * *" },
    { label: "Weekdays at 8am",    cron: "0 8 * * 1-5" },
  ];

  return (
    <div className="flex-1 overflow-auto" style={{ background: "var(--bg)" }}>
      <div className="p-6 max-w-6xl mx-auto space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-slate-800">Profiling Scheduler</h1>
            <p className="text-slate-500 text-sm mt-1">Automatically re-profile tables and run DQ rules on a cron schedule</p>
          </div>
          <button onClick={() => setShowNew(true)} className="flex items-center gap-2 px-4 py-2 bg-[#e8622b] text-white rounded-lg text-sm font-medium hover:opacity-90">
            <Plus className="w-4 h-4" /> New Schedule
          </button>
        </div>

        {/* New scan form */}
        {showNew && (
          <div className="bg-white border border-slate-200 rounded-xl p-6 shadow-sm">
            <h3 className="font-semibold text-slate-800 mb-4">Create Scheduled Scan</h3>
            <div className="grid grid-cols-2 gap-4">
              <div><label className="text-xs font-medium text-slate-600">Name</label><input className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm" placeholder="e.g. Daily Gold Layer Scan" value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} /></div>
              <div>
                <label className="text-xs font-medium text-slate-600">Schedule</label>
                <select className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm" value={form.schedule_cron} onChange={e => setForm(f => ({ ...f, schedule_cron: e.target.value }))}>
                  {PRESETS.map(p => <option key={p.cron} value={p.cron}>{p.label} ({p.cron})</option>)}
                </select>
              </div>
              <div className="col-span-2"><label className="text-xs font-medium text-slate-600">Custom Cron (optional override)</label><input className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm font-mono" placeholder="minute hour day month weekday" value={form.schedule_cron} onChange={e => setForm(f => ({ ...f, schedule_cron: e.target.value }))} /></div>
            </div>
            <div className="flex justify-end gap-2 mt-4">
              <button onClick={() => setShowNew(false)} className="px-4 py-2 text-sm text-slate-600 hover:bg-slate-100 rounded-lg">Cancel</button>
              <button onClick={saveScan} disabled={!form.name} className="px-4 py-2 bg-[#e8622b] text-white text-sm rounded-lg font-medium hover:opacity-90 disabled:opacity-50">Save Schedule</button>
            </div>
          </div>
        )}

        {/* Scans list */}
        <div className="space-y-3">
          {loading ? (
            <div className="text-center text-slate-400 py-8">Loading…</div>
          ) : scans.length === 0 ? (
            <div className="text-center text-slate-400 py-12 bg-white rounded-xl border border-slate-200">
              <Calendar className="w-10 h-10 mx-auto mb-3 text-slate-300" />
              <div className="font-medium text-slate-500">No scheduled scans</div>
              <div className="text-sm mt-1">Create a schedule to automatically run quality checks</div>
            </div>
          ) : scans.map(scan => (
            <div key={scan.id} className="bg-white border border-slate-200 rounded-xl p-4 shadow-sm">
              <div className="flex items-center justify-between gap-4">
                <div className="flex-1">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="font-semibold text-slate-800">{scan.name}</span>
                    <span className="text-xs px-2 py-0.5 bg-slate-100 text-slate-600 rounded-full font-mono">{scan.schedule_cron}</span>
                    {scan.last_run_status && <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${statusColor[scan.last_run_status] || "text-slate-500 bg-slate-100"}`}>{scan.last_run_status}</span>}
                    <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${scan.is_active ? "text-green-600 bg-green-50" : "text-slate-400 bg-slate-100"}`}>{scan.is_active ? "Active" : "Paused"}</span>
                  </div>
                  <div className="flex gap-4 mt-1.5 text-xs text-slate-400">
                    {scan.last_run_at && <span>Last run: {new Date(scan.last_run_at).toLocaleString()}</span>}
                    {scan.next_run_at && <span>Next run: {new Date(scan.next_run_at).toLocaleString()}</span>}
                  </div>
                </div>
                <div className="flex items-center gap-2 flex-shrink-0">
                  <button onClick={() => runNow(scan.id)} disabled={running[scan.id]} className="flex items-center gap-1 px-3 py-1.5 bg-[#fdf3ee] text-[#c94d1a] hover:bg-blue-100 rounded-lg text-xs font-medium disabled:opacity-50">
                    {running[scan.id] ? <RefreshCw className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />} Run Now
                  </button>
                  <button onClick={() => toggleScan(scan)} className={`px-3 py-1.5 rounded-lg text-xs font-medium ${scan.is_active ? "bg-slate-100 text-slate-600 hover:bg-slate-200" : "bg-green-50 text-green-700 hover:bg-green-100"}`}>
                    {scan.is_active ? <Pause className="w-3 h-3" /> : <Play className="w-3 h-3" />}
                  </button>
                </div>
              </div>
            </div>
          ))}
        </div>

        {/* History */}
        {history.length > 0 && (
          <div>
            <h2 className="text-lg font-semibold text-slate-700 mb-3">Recent Run History</h2>
            <div style={{ background: 'var(--card)', border: '1px solid var(--border)', borderRadius: '16px', overflow: 'hidden' }}>
              <table className="w-full text-sm">
                <thead style={{ background: 'var(--bg)', borderBottom: '1px solid var(--border)' }}>
                  <tr>{["Status","Tables Scanned","Issues Found","Quality Score","Triggered By","Started At"].map(h => <th key={h} className="px-4 py-2.5 text-left" style={{ fontSize: '11px', fontWeight: '700', letterSpacing: '.05em', textTransform: 'uppercase', color: 'var(--muted)' }}>{h}</th>)}</tr>
                </thead>
                <tbody>
                  {history.slice(0, 10).map(run => (
                    <tr key={run.id} style={{ borderBottom: '1px solid #f5f0ea' }}
                      onMouseEnter={e => e.currentTarget.style.background='#faf8f5'} onMouseLeave={e => e.currentTarget.style.background='transparent'}>
                      <td className="px-4 py-2.5"><span className={`text-xs px-2 py-0.5 rounded-full font-medium ${statusColor[run.status] || "text-slate-500 bg-slate-100"}`}>{run.status}</span></td>
                      <td className="px-4 py-2.5" style={{ color: 'var(--text)' }}>{run.tables_scanned}</td>
                      <td className="px-4 py-2.5" style={{ color: 'var(--text)' }}>{run.issues_found}</td>
                      <td className="px-4 py-2.5" style={{ color: 'var(--text)' }}>{run.quality_score ? `${run.quality_score.toFixed(1)}%` : "—"}</td>
                      <td className="px-4 py-2.5" style={{ color: 'var(--muted)', textTransform: 'capitalize' }}>{run.triggered_by}</td>
                      <td className="px-4 py-2.5" style={{ color: 'var(--muted)', fontSize: '11.5px' }}>{run.started_at ? new Date(run.started_at).toLocaleString() : "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

// ─── INFO PANEL (module-level for stable React identity) ──────────────────────
const InfoPanel = ({ activeTab, catalogTables, catalogIssues, mockAgents, realConnections, backendOnline, onAddConnection }) => {
  // Shared style helpers
  const IC = ({ children, style = {} }) => (
    <div style={{ background: 'var(--card)', borderRadius: '16px', padding: '16px', ...style }}>{children}</div>
  );
  const SecLbl = ({ children }) => (
    <div style={{ fontSize: '10px', fontWeight: '700', textTransform: 'uppercase', letterSpacing: '0.07em', color: 'var(--orange)' }}>{children}</div>
  );
  const SecHdr = ({ label, right }) => (
    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '10px' }}>
      <SecLbl>{label}</SecLbl>
      {right}
    </div>
  );
  const LiveDot = () => (
    <div style={{ display: 'flex', alignItems: 'center', gap: '5px', fontSize: '11px', color: '#16a34a', fontWeight: '500' }}>
      <div className="live-dot" /><span>Live</span>
    </div>
  );
  const Stats3 = ({ items }) => (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', paddingTop: '10px', borderTop: '1px solid var(--border)' }}>
      {items.map(([num, lbl, color]) => (
        <div key={lbl} style={{ textAlign: 'center' }}>
          <div style={{ display: 'block', fontSize: '17px', fontWeight: '700', color: color || 'var(--text)' }}>{num}</div>
          <div style={{ fontSize: '10px', color: 'var(--muted)' }}>{lbl}</div>
        </div>
      ))}
    </div>
  );
  const OrangeBtn = ({ children, onClick }) => (
    <button onClick={onClick} className="btn-brand-orange">{children}</button>
  );
  const DarkBtn = ({ children, onClick }) => (
    <button onClick={onClick} style={{ width: '100%', padding: '9px 12px', background: 'var(--dark)', color: '#fff', border: 'none', borderRadius: '10px', fontSize: '13px', fontWeight: '600', cursor: 'pointer', fontFamily: 'inherit' }}>{children}</button>
  );
  const FgOpt = ({ label, count, checked = true }) => (
    <label style={{ display: 'flex', alignItems: 'center', gap: '8px', padding: '4px 0', fontSize: '12.5px', color: 'var(--text)', cursor: 'pointer' }}>
      <input type="checkbox" defaultChecked={checked} style={{ accentColor: 'var(--dark)' }} /> {label}
      {count !== undefined && <span style={{ marginLeft: 'auto', fontSize: '11px', fontWeight: '600', color: 'var(--muted)' }}>{count}</span>}
    </label>
  );
  const ScanItem = ({ icon, name, meta, state }) => (
    <div style={{ display: 'flex', alignItems: 'center', gap: '9px', padding: '7px 8px', borderRadius: '8px', background: state === 'done' ? 'rgba(22,163,74,.06)' : state === 'running' ? 'rgba(108,71,255,.06)' : state === 'fail' ? 'rgba(232,98,43,.06)' : 'transparent' }}>
      <span style={{ fontSize: '13px', width: '18px', textAlign: 'center', display: 'inline-block', animation: state === 'running' ? 'iq-spin 1.2s linear infinite' : 'none' }}>{icon}</span>
      <div>
        <div style={{ fontSize: '12.5px', fontWeight: '600', color: 'var(--text)' }}>{name}</div>
        <div style={{ fontSize: '11px', color: 'var(--muted)', marginTop: '1px' }}>{meta}</div>
      </div>
    </div>
  );

  const highIssues = catalogIssues.filter(i => i.severity === "high").length;
  const medIssues = catalogIssues.filter(i => i.severity === "medium").length;
  const lowIssues = catalogIssues.filter(i => i.severity === "low").length;

  const panels = {
    dashboard: () => <>
      <IC>
        <SecHdr label="Platform Health" right={<LiveDot />} />
        <div style={{ display: 'flex', alignItems: 'baseline', gap: '8px', margin: '8px 0 4px' }}>
          <div style={{ fontSize: '32px', fontWeight: '800', color: 'var(--text)', lineHeight: '1' }}>94.2<span style={{ fontSize: '17px', fontWeight: '600' }}>%</span></div>
          <div style={{ fontSize: '11px', fontWeight: '700', background: 'rgba(22,163,74,.1)', color: '#16a34a', padding: '2px 8px', borderRadius: '20px' }}>↑ 3.1%</div>
        </div>
        <div style={{ fontSize: '11.5px', color: 'var(--muted)', marginBottom: '10px' }}>Avg quality across all tables</div>
        <Stats3 items={[[realConnections.length, 'Connections'], [highIssues, 'Open Issues', 'var(--orange)'], [catalogTables.length ? 247 : 0, 'Rules Run']]} />
      </IC>
      <IC>
        <SecHdr label="Today's Scans" right={<span style={{ fontSize: '12px', color: 'var(--muted)', cursor: 'pointer' }}>View all ›</span>} />
        <div style={{ display: 'flex', flexDirection: 'column', gap: '2px' }}>
          <ScanItem icon="✓" name="Full Profile Scan" meta="Operational DB · 9:00am" state="done" />
          <ScanItem icon="✓" name="Rules Validation" meta="Finance schema · 9:45am" state="done" />
          <ScanItem icon="↻" name="PII Scanner" meta="HR data · Running…" state="running" />
          <ScanItem icon="◦" name="Anomaly Check" meta="Enterprise DW · 2:00pm" state="pending" />
        </div>
      </IC>
      <IC>
        <SecHdr label="Quick Actions" />
        <div style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
          {[['🔗', 'Add connection'], ['⚡', 'Add DQ rule'], ['📅', 'Scheduled scans'], ['🚨', 'View all issues']].map(([ic, lbl]) => (
            <button key={lbl} style={{ width: '100%', textAlign: 'left', padding: '8px 12px', background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: '10px', fontSize: '12.5px', fontWeight: '500', color: 'var(--text)', cursor: 'pointer', fontFamily: 'inherit' }}>{ic} &nbsp;{lbl}</button>
          ))}
        </div>
      </IC>
    </>,

    scheduler: () => <>
      <IC>
        <SecHdr label="Schedule Overview" right={<LiveDot />} />
        <Stats3 items={[['7', 'Active jobs', '#16a34a'], ['1', 'Failing', 'var(--orange)'], ['23', 'Runs today']]} />
      </IC>
      <IC>
        <OrangeBtn>+ Add Scheduled Scan</OrangeBtn>
        <div style={{ marginTop: '12px' }}>
          <SecHdr label="Quick Info" />
          <p style={{ fontSize: '12.5px', color: 'var(--text)', lineHeight: '1.5', marginBottom: '8px' }}>Automate profiling, validation, and anomaly detection on any cron schedule.</p>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
            {[['var(--orange)','Cron or interval-based triggers'],['#16a34a','Slack & email alerts on failure'],['var(--orange)','AI detects anomalies mid-scan'],['#16a34a','Full run history & log viewer']].map(([c,t]) => (
              <div key={t} style={{ display: 'flex', alignItems: 'center', gap: '7px', fontSize: '12px', color: 'var(--muted)' }}><span style={{ color: c }}>●</span> {t}</div>
            ))}
          </div>
        </div>
      </IC>
      <IC>
        <SecHdr label="Recent Run History" />
        <div style={{ display: 'flex', flexDirection: 'column', gap: '2px' }}>
          <ScanItem icon="✓" name="Weekly Full Profile" meta="All connections · just now · 4m 12s" state="done" />
          <ScanItem icon="✓" name="Rules Validation — Finance" meta="Analytics Cluster · 45m ago · 1m 38s" state="done" />
          <ScanItem icon="↻" name="Anomaly Detection" meta="Marketing CDP · Running…" state="running" />
          <ScanItem icon="✗" name="Schema Sync — Staging" meta="Staging Env · 8h ago · Failed" state="fail" />
          <ScanItem icon="✓" name="Freshness Check" meta="Data Lake · 10h ago · 22s" state="done" />
        </div>
      </IC>
    </>,

    connections: () => <>
      <IC>
        <SecHdr label="Connection Health" right={<LiveDot />} />
        <Stats3 items={[[realConnections.filter(c=>c.status==='ok').length,'Connected','#16a34a'],[realConnections.filter(c=>c.status==='warning').length,'Warning','var(--orange)'],[realConnections.reduce((a,c)=>a+(c.table_count||0),0).toLocaleString(),'Tables']]} />
      </IC>
      <IC>
        <SecHdr label="Filter by Type" />
        {['PostgreSQL','Snowflake','BigQuery','Redshift','S3/Hive'].map((t,i) => <FgOpt key={t} label={t} count={[2,1,1,1,1][i]} />)}
      </IC>
      <IC>
        <SecHdr label="Filter by Status" />
        <FgOpt label="Connected" count={5} />
        <FgOpt label="Warning" count={1} />
        <FgOpt label="Offline" count={0} checked={false} />
      </IC>
      <IC><DarkBtn onClick={onAddConnection}>+ Add Connection</DarkBtn></IC>
    </>,

    catalog: () => <>
      <IC>
        <SecLbl>Search</SecLbl>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: '10px', padding: '8px 12px', marginTop: '8px' }}>
          <span>🔍</span>
          <input placeholder="Table name, tag, steward…" style={{ border: 'none', background: 'transparent', fontSize: '12.5px', color: 'var(--text)', outline: 'none', width: '100%', fontFamily: 'inherit' }} />
        </div>
      </IC>
      <IC>
        <SecHdr label="Domain" />
        {[['Finance',3],['Marketing',3],['Sales',2],['Product',1],['HR',1]].map(([d,c]) => <FgOpt key={d} label={d} count={c} />)}
      </IC>
      <IC>
        <SecHdr label="Trust Level" />
        <FgOpt label="★ Gold" count={4} />
        <FgOpt label="★ Silver" count={4} />
        <FgOpt label="★ Bronze" count={2} />
      </IC>
      <IC>
        <SecHdr label="Quality" />
        <FgOpt label="≥ 90% (Good)" count={6} />
        <FgOpt label="75–89% (Fair)" count={3} />
        <FgOpt label="< 75% (Poor)" count={1} />
      </IC>
    </>,

    quality: () => <>
      <IC>
        <SecHdr label="Severity Breakdown" />
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: '8px', marginBottom: '12px' }}>
          {[['hi',highIssues||2,'High','rgba(232,98,43,.1)','var(--orange)'],['med',medIssues||3,'Medium','rgba(246,185,77,.1)','#a07000'],['lo',lowIssues||1,'Low','rgba(59,130,246,.08)','#1d4ed8']].map(([k,n,l,bg,c]) => (
            <div key={k} style={{ borderRadius: '10px', padding: '10px 8px', textAlign: 'center', background: bg }}>
              <span style={{ fontSize: '20px', fontWeight: '800', display: 'block', color: c }}>{n}</span>
              <span style={{ fontSize: '10.5px', color: 'var(--muted)' }}>{l}</span>
            </div>
          ))}
        </div>
      </IC>
      <IC>
        <SecHdr label="Filter by Type" />
        {['Missing Values','Schema Change','Data Drift','Freshness','Duplicates','Referential Integrity'].map(t => <FgOpt key={t} label={t} />)}
      </IC>
      <IC>
        <SecHdr label="Filter by Connection" />
        {(realConnections.length ? realConnections : mockConnections).map(c => <FgOpt key={c.id} label={c.name} />)}
      </IC>
    </>,

    rules: () => <>
      <IC>
        <SecHdr label="Rule Types" />
        <div style={{ display: 'flex', flexDirection: 'column', gap: '3px', marginBottom: '14px' }}>
          {[['🔴','Not Null',4],['🟣','Unique',2],['📝','Regex',1],['⏱','Freshness',2],['📉','Min Value',1],['📈','Max Value',1],['#️⃣','Row Count',1],['⚙️','Custom SQL',1]].map(([ic,lb,ct]) => (
            <div key={lb} style={{ display: 'flex', alignItems: 'center', gap: '8px', padding: '6px 8px', borderRadius: '8px', cursor: 'pointer' }}>
              <span style={{ fontSize: '13px', width: '20px', textAlign: 'center' }}>{ic}</span>
              <span style={{ fontSize: '12.5px', color: 'var(--text)', flex: 1 }}>{lb}</span>
              <span style={{ fontSize: '11.5px', fontWeight: '700', color: 'var(--muted)' }}>{ct}</span>
            </div>
          ))}
        </div>
        <DarkBtn>+ Add Rule</DarkBtn>
      </IC>
      <IC>
        <SecHdr label="This Week" />
        <Stats3 items={[['247','Runs'],['221','Passed','#16a34a'],['26','Failed','var(--orange)']]} />
      </IC>
    </>,

    lineage: () => <>
      <IC>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '10px' }}>
          <div style={{ width: '40px', height: '40px', borderRadius: '50%', background: 'var(--bg)', border: '2px solid var(--border)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '19px', flexShrink: 0 }}>📋</div>
          <div><div style={{ fontSize: '14.5px', fontWeight: '700', color: 'var(--text)' }}>customer_master</div></div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', background: 'var(--bg)', borderRadius: '10px', padding: '8px 12px', marginBottom: '12px', fontSize: '12px', fontWeight: '500', color: 'var(--text)' }}>public · Enterprise DW</div>
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap', marginBottom: '12px' }}>
          {['PII','Golden Record','GDPR'].map(t => <span key={t} style={{ fontSize: '10.5px', fontWeight: '500', padding: '2px 7px', borderRadius: '20px', background: t==='PII'?'rgba(220,38,38,.08)':'var(--bg)', color: t==='PII'?'#dc2626':'var(--muted)' }}>{t}</span>)}
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: '12px', paddingTop: '10px', borderTop: '1px solid var(--border)' }}>
          {[['2','Upstream'],['4','Downstream'],['48','Columns']].map(([n,l]) => (
            <div key={l} style={{ textAlign: 'center' }}>
              <span style={{ fontSize: '18px', fontWeight: '700', color: 'var(--text)', display: 'block' }}>{n}</span>
              <span style={{ fontSize: '10px', color: 'var(--muted)' }}>{l}</span>
            </div>
          ))}
        </div>
      </IC>
      <IC>
        <SecHdr label="Explore Table" />
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: '10px', padding: '8px 12px', marginBottom: '12px' }}>
          <span>🔍</span>
          <input placeholder="Search tables…" style={{ border: 'none', background: 'transparent', fontSize: '12.5px', color: 'var(--text)', outline: 'none', width: '100%', fontFamily: 'inherit' }} />
        </div>
        <SecHdr label="Show Layers" />
        <FgOpt label="2 Upstream layers" />
        <FgOpt label="2 Downstream layers" />
        <FgOpt label="ETL nodes" />
      </IC>
    </>,

    agents: () => <>
      <IC>
        <SecHdr label="Agent Status" right={<LiveDot />} />
        {mockAgents.map(a => (
          <div key={a.id} style={{ display: 'flex', alignItems: 'center', gap: '9px', padding: '7px 8px', borderRadius: '8px', cursor: 'pointer' }}>
            <div style={{ width: '7px', height: '7px', borderRadius: '50%', flexShrink: 0, background: a.status==='active'?'#16a34a':'#f59e0b', boxShadow: a.status==='active'?'0 0 5px rgba(22,163,74,.4)':undefined }} />
            <span style={{ fontSize: '12.5px', color: 'var(--text)', flex: 1 }}>{a.name}</span>
            <span style={{ fontSize: '11px', color: 'var(--muted)' }}>{a.tasksToday} today</span>
          </div>
        ))}
      </IC>
      <IC>
        <SecHdr label="Today's Activity" />
        <Stats3 items={[['170','Tasks run'],['5/6','Active','#16a34a'],['2.8s','Avg time']]} />
      </IC>
    </>,
  };

  const content = panels[activeTab];
  return (
    <div style={{ width: '262px', minWidth: '262px', background: 'var(--bg)', borderRight: '1px solid var(--border)', overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: '10px', padding: '14px 12px 20px' }}>
      {content ? content() : (
        <IC><SecHdr label={activeTab} /><p style={{ fontSize: '12.5px', color: 'var(--muted)' }}>Select a tab to see details.</p></IC>
      )}
    </div>
  );
};

// ─── MAIN APP ─────────────────────────────────────────────────────────────────
// ── AGENTS TAB (module-level — must NOT be defined inside DataIQApp)
// Defined here to prevent React unmount/remount on every parent re-render.
// AgentLogs WebSocket updates trigger frequent parent state changes; an inner
// component definition would be a new type each render → scroll resets to top.
const AgentsTab = ({ backendOnline, liveAgents, agentLogs, llmConfigured }) => {
  // ── LLM config state ──────────────────────────────────────────────────────
  const [llmConfig, setLlmConfig] = useState(null);          // saved config from backend
  const [llmModels, setLlmModels] = useState([]);            // provider/model options
  const [llmProvider, setLlmProvider] = useState("anthropic");
  const [llmModel, setLlmModel] = useState("claude-sonnet-4-6");
  const [llmApiKey, setLlmApiKey] = useState("");
  const [llmSaving, setLlmSaving] = useState(false);
  const [llmTesting, setLlmTesting] = useState(false);
  const [llmTestResult, setLlmTestResult] = useState(null);  // {ok, message}
  const [llmShowKey, setLlmShowKey] = useState(false);

  useEffect(() => {
    if (!backendOnline) return;
    apiFetch("/api/llm/models").then(data => {
      if (Array.isArray(data)) setLlmModels(data);
    });
    apiFetch("/api/llm/config").then(data => {
      if (data && data.configured) {
        setLlmConfig(data);
        setLlmProvider(data.provider);
        setLlmModel(data.model);
      }
    });
  }, [backendOnline]);

  // Reset model when provider changes
  useEffect(() => {
    const group = llmModels.find(g => g.provider === llmProvider);
    if (group && group.models.length > 0) {
      setLlmModel(group.models[0].id);
    }
  }, [llmProvider, llmModels]);

  const saveLlmConfig = async () => {
    setLlmSaving(true);
    setLlmTestResult(null);
    try {
      const res = await apiFetch("/api/llm/config", {
        method: "PUT",
        body: JSON.stringify({ provider: llmProvider, model: llmModel, api_key: llmApiKey }),
      });
      if (res && res.configured) {
        setLlmConfig(res);
        setLlmApiKey("");
      }
    } finally {
      setLlmSaving(false);
    }
  };

  const testLlmConfig = async () => {
    setLlmTesting(true);
    setLlmTestResult(null);
    try {
      const res = await apiFetch("/api/llm/test", { method: "POST" });
      setLlmTestResult(res);
    } catch (e) {
      setLlmTestResult({ ok: false, message: String(e) });
    } finally {
      setLlmTesting(false);
    }
  };

  const removeLlmConfig = async () => {
    await apiFetch("/api/llm/config", { method: "DELETE" });
    setLlmConfig(null);
    setLlmApiKey("");
    setLlmTestResult(null);
  };

  const providerLabel = { anthropic: "Claude (Anthropic)", openai: "OpenAI", groq: "Groq", gemini: "Google Gemini", ollama: "Ollama (local)" };
  const providerIcon  = { anthropic: "🟣", openai: "🟢", groq: "⚡", gemini: "🔵", ollama: "🖥" };
  const providerFree  = { groq: true, gemini: true, ollama: true };

  // ── Use live backend agents if available, fall back to mock ───────────────
  // Live agents from backend always shown; mock fallback only when LLM is configured
  const displayAgents = backendOnline && Object.keys(liveAgents).length > 0
    ? Object.values(liveAgents)
    : llmConfigured ? mockAgents.map(a => ({ ...a, tasks_completed: a.tasksCompleted })) : [];

  const toggleAgent = async (agentId, currentStatus) => {
    if (!backendOnline) return;
    const endpoint = currentStatus === "active" ? "stop" : "start";
    await apiFetch(`/agents/${agentId}/${endpoint}`, { method: "POST" });
  };

  const liveCount = displayAgents.filter(a => a.status === "active").length;

  const agentIconBg = { profiling: '#eff6ff', validation: '#f0fdf4', lineage: '#f5f3ff', monitoring: '#fff7ed', governance: '#fff1f2', ingestion: '#f0fdf4' };
  return (
    <div className="flex-1 overflow-auto" style={{ background: 'var(--bg)' }}>
      <div style={{ padding: '22px 24px', maxWidth: '1200px', margin: '0 auto' }}>
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '20px' }}>
          <h1 style={{ fontSize: '20px', fontWeight: '700', color: 'var(--text)', display: 'flex', alignItems: 'center', gap: '6px' }}>
            AI Agents <span style={{ width: '6px', height: '6px', borderRadius: '50%', background: 'var(--orange)', display: 'inline-block', verticalAlign: 'middle' }} />
          </h1>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            {backendOnline
              ? <span style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '12px', fontWeight: '600', background: 'rgba(22,163,74,.08)', color: '#16a34a', border: '1px solid rgba(22,163,74,.2)', padding: '5px 12px', borderRadius: '20px' }}><Wifi style={{ width: '13px', height: '13px' }} />Live — {liveCount} of {displayAgents.length} active</span>
              : <span style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '12px', fontWeight: '600', background: 'rgba(202,138,4,.08)', color: '#ca8a04', border: '1px solid rgba(202,138,4,.2)', padding: '5px 12px', borderRadius: '20px' }}><WifiOff style={{ width: '13px', height: '13px' }} />Demo mode</span>
            }
          </div>
        </div>

        {!backendOnline && (
          <div style={{ background: 'rgba(232,98,43,.06)', border: '1px solid rgba(232,98,43,.2)', borderRadius: '12px', padding: '14px 18px', display: 'flex', gap: '12px', alignItems: 'flex-start', marginBottom: '16px' }}>
            <Bot style={{ width: '18px', height: '18px', color: 'var(--orange)', flexShrink: 0, marginTop: '2px' }} />
            <div>
              <div style={{ fontSize: '13px', fontWeight: '600', color: 'var(--dark)' }}>Start the backend to activate real agents</div>
              <div style={{ fontSize: '12px', color: 'var(--muted)', marginTop: '3px' }}>Double-click <strong>Start Backend.command</strong> — agents will connect automatically.</div>
            </div>
          </div>
        )}

        {/* Agent grid */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '14px', marginBottom: '16px' }}>
          {displayAgents.map(agent => {
            const agentId = agent.id;
            const status  = agent.status;
            const isActive = status === "active";
            const mockMeta = mockAgents.find(m => m.id === agentId || m.name === agent.name) || {};
            const bg = agentIconBg[agent.type] || '#f8f8f8';
            return (
              <div key={agentId} style={{
                background: 'var(--card)', borderRadius: '16px', border: '1px solid var(--border)',
                borderTop: `3px solid ${isActive ? '#16a34a' : 'var(--border)'}`,
                padding: '16px 18px',
              }}>
                <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: '12px' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                    <div style={{ width: '38px', height: '38px', borderRadius: '10px', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '18px', background: bg, flexShrink: 0 }}>
                      {mockMeta.icon || '🤖'}
                    </div>
                    <div>
                      <div style={{ fontSize: '13.5px', fontWeight: '700', color: 'var(--text)' }}>{agent.name}</div>
                      <AgentTypeBadge type={agent.type} />
                    </div>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span style={{ fontSize: '11px', fontWeight: '700', padding: '3px 9px', borderRadius: '20px', background: isActive ? 'rgba(22,163,74,.1)' : 'rgba(107,114,128,.08)', color: isActive ? '#16a34a' : '#6b7280' }}>
                      {isActive ? 'Active' : 'Idle'}
                    </span>
                    <button
                      onClick={() => toggleAgent(agentId, status)}
                      disabled={!backendOnline}
                      style={{ padding: '6px', background: 'none', border: 'none', borderRadius: '8px', cursor: backendOnline ? 'pointer' : 'not-allowed', opacity: backendOnline ? 1 : 0.4, color: isActive ? '#16a34a' : 'var(--muted)' }}
                      title={backendOnline ? (isActive ? "Stop agent" : "Start agent") : "Start backend first"}
                    >
                      {isActive ? <Pause style={{ width: '14px', height: '14px' }} /> : <Play style={{ width: '14px', height: '14px' }} />}
                    </button>
                  </div>
                </div>
                <p style={{ fontSize: '12px', color: 'var(--muted)', lineHeight: '1.5', marginBottom: '12px' }}>{mockMeta.description || agent.description || ""}</p>
                {/* Stats */}
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '6px', marginBottom: '10px' }}>
                  {[
                    { label: "Total", value: (agent.tasks_completed ?? agent.tasksCompleted ?? mockMeta.tasksCompleted ?? 0).toLocaleString() },
                    { label: "Today",    value: (agent.tasks_today ?? mockMeta.today ?? 0).toString() },
                    { label: "Avg time",    value: agent.uptime ? agent.uptime.split(".")[0] : (mockMeta.avgRuntime || "—") },
                  ].map(({ label, value }) => (
                    <div key={label} style={{ background: 'var(--bg)', borderRadius: '8px', padding: '8px 6px', textAlign: 'center' }}>
                      <div style={{ fontSize: '14px', fontWeight: '700', color: 'var(--text)' }}>{value}</div>
                      <div style={{ fontSize: '10.5px', color: 'var(--muted)', marginTop: '2px' }}>{label}</div>
                    </div>
                  ))}
                </div>
                {/* Footer */}
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', paddingTop: '10px', borderTop: '1px solid var(--border)' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
                    <span style={{ width: '7px', height: '7px', borderRadius: '50%', background: isActive ? '#16a34a' : '#d1d5db', display: 'inline-block' }} />
                    <span style={{ fontSize: '11.5px', color: 'var(--muted)', textTransform: 'capitalize' }}>{status}</span>
                  </div>
                  <span style={{ fontSize: '11px', color: 'var(--muted)' }}>
                    {agent.last_run ? `Last: ${new Date(agent.last_run).toLocaleTimeString()}` : (agent.lastRun ? `Last: ${agent.lastRun}` : "Never run")}
                  </span>
                </div>
              </div>
            );
          })}
        </div>

        {/* Live log — only shown when LLM is configured */}
        <div style={{ background: 'var(--card)', borderRadius: '16px', border: '1px solid var(--border)', overflow: 'hidden' }}>
          <div style={{ padding: '14px 18px', borderBottom: '1px solid var(--border)', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span style={{ width: '7px', height: '7px', borderRadius: '50%', background: llmConfig?.configured && backendOnline ? '#16a34a' : '#d1d5db', display: 'inline-block' }} />
              <span style={{ fontSize: '13px', fontWeight: '600', color: 'var(--text)' }}>Agent Activity Log</span>
            </div>
            {llmConfig?.configured && backendOnline && (
              <span style={{ fontSize: '11.5px', color: 'var(--muted)' }}>{agentLogs.length} entries</span>
            )}
          </div>

          {/* Gate: LLM not configured */}
          {!llmConfig?.configured ? (
            <div style={{ padding: '36px 24px', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '12px', height: '180px', justifyContent: 'center' }}>
              <div style={{ width: '42px', height: '42px', borderRadius: '12px', background: 'rgba(232,98,43,.08)', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <Cpu style={{ width: '20px', height: '20px', color: 'var(--orange)' }} />
              </div>
              <div style={{ textAlign: 'center' }}>
                <div style={{ fontSize: '13.5px', fontWeight: '700', color: 'var(--text)', marginBottom: '4px' }}>No LLM connected</div>
                <div style={{ fontSize: '12px', color: 'var(--muted)', lineHeight: '1.5' }}>
                  Agent logs are only available when an LLM is active.<br />Connect one below using your API key.
                </div>
              </div>
              <button
                onClick={() => { const el = document.getElementById('llm-config-card'); if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' }); }}
                style={{ padding: '7px 18px', background: 'var(--orange)', color: '#fff', border: 'none', borderRadius: '9px', fontSize: '12.5px', fontWeight: '600', cursor: 'pointer', fontFamily: 'inherit' }}
              >
                Connect LLM via API key ↓
              </button>
            </div>
          ) : !backendOnline ? (
            /* Gate: backend offline */
            <div style={{ padding: '36px 24px', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '10px', height: '180px', justifyContent: 'center' }}>
              <WifiOff style={{ width: '22px', height: '22px', color: 'var(--muted)' }} />
              <div style={{ textAlign: 'center' }}>
                <div style={{ fontSize: '13px', fontWeight: '600', color: 'var(--text)', marginBottom: '4px' }}>Backend offline</div>
                <div style={{ fontSize: '12px', color: 'var(--muted)' }}>Start the backend to stream live agent logs.</div>
              </div>
            </div>
          ) : (
            /* Live log stream */
            <div style={{ padding: '14px 18px', height: '220px', overflow: 'auto', fontFamily: 'ui-monospace, monospace', display: 'flex', flexDirection: 'column', gap: '4px' }}>
              {agentLogs.slice().reverse().map(log => (
                <div key={log.id} style={{ fontSize: '11.5px', display: 'flex', gap: '10px' }}>
                  <span style={{ color: 'var(--muted)', flexShrink: 0 }}>{log.timestamp}</span>
                  <span style={{ flexShrink: 0, fontWeight: '700', color: log.level === "error" ? "#dc2626" : log.level === "warn" ? "#ca8a04" : "#16a34a" }}>
                    [{(log.level || "info").toUpperCase()}]
                  </span>
                  <span style={{ color: 'var(--orange)', flexShrink: 0 }}>[{log.agent}]</span>
                  <span style={{ color: 'var(--text)' }}>{log.message}</span>
                </div>
              ))}
              {agentLogs.length === 0 && <div style={{ color: 'var(--muted)', fontSize: '12px' }}>Waiting for agent activity…</div>}
            </div>
          )}
        </div>

        {/* ── AI Configuration card ─────────────────────────────────────── */}
        <div id="llm-config-card" style={{ background: 'var(--card)', borderRadius: '16px', border: '1px solid var(--border)', borderTop: `3px solid ${llmConfig?.configured ? '#16a34a' : 'var(--orange)'}`, marginTop: '14px', overflow: 'hidden' }}>
          <div style={{ padding: '14px 18px', borderBottom: '1px solid var(--border)', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <Cpu style={{ width: '15px', height: '15px', color: llmConfig?.configured ? '#16a34a' : 'var(--orange)' }} />
              <span style={{ fontSize: '13px', fontWeight: '700', color: 'var(--text)' }}>AI Configuration</span>
              {llmConfig?.configured
                ? <span style={{ fontSize: '11px', fontWeight: '700', padding: '2px 9px', borderRadius: '20px', background: 'rgba(22,163,74,.1)', color: '#16a34a' }}>
                    {providerLabel[llmConfig.provider] || llmConfig.provider} · {llmConfig.model}
                  </span>
                : <span style={{ fontSize: '11px', fontWeight: '700', padding: '2px 9px', borderRadius: '20px', background: 'rgba(232,98,43,.1)', color: 'var(--orange)' }}>
                    Not configured — rule-based mode
                  </span>
              }
            </div>
            {llmConfig?.configured && (
              <button onClick={removeLlmConfig} style={{ fontSize: '11.5px', color: '#dc2626', background: 'none', border: 'none', cursor: 'pointer', padding: '4px 8px', borderRadius: '6px' }}>Remove key</button>
            )}
          </div>

          <div style={{ padding: '18px', display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '14px' }}>
            {/* Provider */}
            <div style={{ gridColumn: '1 / -1' }}>
              <label style={{ fontSize: '11.5px', fontWeight: '600', color: 'var(--muted)', display: 'block', marginBottom: '6px' }}>Provider</label>
              <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                {(llmModels.length
                  ? llmModels.map(g => g.provider)
                  : ["anthropic","openai","groq","gemini","ollama"]
                ).map(p => (
                  <button
                    key={p}
                    onClick={() => setLlmProvider(p)}
                    disabled={!backendOnline}
                    style={{
                      position: 'relative', padding: '8px 10px', border: `1.5px solid ${llmProvider === p ? 'var(--dark)' : 'var(--border)'}`,
                      borderRadius: '10px', background: llmProvider === p ? 'var(--dark)' : 'var(--card)',
                      color: llmProvider === p ? '#fff' : 'var(--text)', fontSize: '12px', fontWeight: '600',
                      cursor: backendOnline ? 'pointer' : 'not-allowed', transition: 'all .15s', whiteSpace: 'nowrap',
                    }}
                  >
                    {providerIcon[p] || '🤖'} {providerLabel[p] || p}
                    {providerFree[p] && <span style={{ marginLeft: '5px', fontSize: '9px', fontWeight: '800', padding: '1px 5px', borderRadius: '4px', background: llmProvider === p ? 'rgba(255,255,255,.2)' : 'rgba(22,163,74,.12)', color: llmProvider === p ? '#fff' : '#16a34a' }}>FREE</span>}
                  </button>
                ))}
              </div>
            </div>

            {/* Model */}
            <div>
              <label style={{ fontSize: '11.5px', fontWeight: '600', color: 'var(--muted)', display: 'block', marginBottom: '6px' }}>Model</label>
              <select
                value={llmModel}
                onChange={e => setLlmModel(e.target.value)}
                disabled={!backendOnline}
                style={{ width: '100%', padding: '8px 10px', border: '1.5px solid var(--border)', borderRadius: '10px', background: 'var(--card)', color: 'var(--text)', fontSize: '12.5px', fontWeight: '500', cursor: backendOnline ? 'pointer' : 'not-allowed', outline: 'none', fontFamily: 'inherit' }}
              >
                {(llmModels.find(g => g.provider === llmProvider)?.models || [
                  { id: "claude-sonnet-4-6", label: "Claude Sonnet 4 (recommended)" },
                  { id: "claude-haiku-4-5-20251001", label: "Claude Haiku 4 (fast / low cost)" },
                  { id: "claude-opus-4-6", label: "Claude Opus 4 (most capable)" },
                ]).map(m => (
                  <option key={m.id} value={m.id}>{m.label}</option>
                ))}
              </select>
            </div>

            {/* API Key */}
            <div style={{ gridColumn: '1 / -1' }}>
              <label style={{ fontSize: '11.5px', fontWeight: '600', color: 'var(--muted)', display: 'block', marginBottom: '6px' }}>
                API Key {llmConfig?.configured && <span style={{ fontWeight: '400', color: 'var(--muted)' }}>(leave blank to keep existing)</span>}
              </label>
              <div style={{ position: 'relative' }}>
                <input
                  type={llmShowKey ? "text" : "password"}
                  placeholder={llmProvider === "ollama" ? "No key needed — type \"local\" or leave blank" : llmConfig?.configured ? "••••••••••••  (saved)" : "sk-ant-...  /  gsk_...  /  AIza..."}
                  value={llmApiKey}
                  onChange={e => setLlmApiKey(e.target.value)}
                  disabled={!backendOnline || llmProvider === "ollama"}
                  style={{ width: '100%', padding: '8px 40px 8px 10px', border: '1.5px solid var(--border)', borderRadius: '10px', background: llmProvider === "ollama" ? 'var(--bg)' : 'var(--card)', color: 'var(--text)', fontSize: '12.5px', fontFamily: 'ui-monospace, monospace', outline: 'none', boxSizing: 'border-box' }}
                />
                <button
                  onClick={() => setLlmShowKey(v => !v)}
                  style={{ position: 'absolute', right: '10px', top: '50%', transform: 'translateY(-50%)', background: 'none', border: 'none', cursor: 'pointer', color: 'var(--muted)', padding: '2px' }}
                >
                  {llmShowKey ? <EyeOff style={{ width: '14px', height: '14px' }} /> : <Eye style={{ width: '14px', height: '14px' }} />}
                </button>
              </div>
            </div>

            {/* Buttons + test result */}
            <div style={{ gridColumn: '1 / -1', display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
              <button
                onClick={saveLlmConfig}
                disabled={!backendOnline || llmSaving || (llmProvider !== 'ollama' && !llmApiKey.trim() && !llmConfig?.configured)}
                style={{ padding: '8px 20px', background: 'var(--dark)', color: '#fff', border: 'none', borderRadius: '10px', fontSize: '12.5px', fontWeight: '600', cursor: backendOnline ? 'pointer' : 'not-allowed', opacity: (llmSaving || (!llmApiKey.trim() && !llmConfig?.configured)) ? 0.5 : 1, transition: 'opacity .15s', fontFamily: 'inherit' }}
              >
                {llmSaving ? "Saving…" : "Save Configuration"}
              </button>
              <button
                onClick={testLlmConfig}
                disabled={!backendOnline || !llmConfig?.configured || llmTesting}
                style={{ padding: '8px 20px', background: 'none', color: 'var(--dark)', border: '1.5px solid var(--border)', borderRadius: '10px', fontSize: '12.5px', fontWeight: '600', cursor: (backendOnline && llmConfig?.configured) ? 'pointer' : 'not-allowed', opacity: (!llmConfig?.configured || llmTesting) ? 0.5 : 1, transition: 'opacity .15s', fontFamily: 'inherit' }}
              >
                {llmTesting ? "Testing…" : "Test Connection"}
              </button>
              {llmTestResult && (
                <span style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '12px', fontWeight: '600', padding: '6px 12px', borderRadius: '20px', background: llmTestResult.ok ? 'rgba(22,163,74,.1)' : 'rgba(220,38,38,.1)', color: llmTestResult.ok ? '#16a34a' : '#dc2626' }}>
                  {llmTestResult.ok ? <CheckCircle style={{ width: '13px', height: '13px' }} /> : <AlertTriangle style={{ width: '13px', height: '13px' }} />}
                  {llmTestResult.message}
                </span>
              )}
              {!backendOnline && (
                <span style={{ fontSize: '12px', color: 'var(--muted)' }}>Start the backend to configure AI</span>
              )}
            </div>
          </div>
        </div>

      </div>
    </div>
  );

};

function DataIQApp({ authUser, handleLogout }) {
  const [activeTab, setActiveTab] = useState("dashboard");
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedTable, setSelectedTable] = useState(null);
  const [lineageTableId, setLineageTableId] = useState("gold.fact_orders");
  // Lifted tab-level UI state — prevents reset when parent re-renders cause inner component remounts
  const [qualitySection, setQualitySection] = useState("issues");
  const [taskFilter, setTaskFilter] = useState("all");
  const [chartPeriod, setChartPeriod] = useState('week');
  const [agentLogs, setAgentLogs] = useState([]);
  const [searchFocused, setSearchFocused] = useState(false);
  const [agentStates, setAgentStates] = useState({});
  const [filterDomain, setFilterDomain] = useState("All");
  const [filterTrust, setFilterTrust] = useState("All");
  const [filterConn, setFilterConn] = useState("All");

  // ── Backend state ────────────────────────────────────────────────────────
  const [backendOnline, setBackendOnline] = useState(false);
  const [llmConfigured, setLlmConfigured] = useState(false);  // true once /api/llm/config confirms a key exists
  const [liveAgents, setLiveAgents] = useState({});   // agent_id → agent object from API
  const [liveTasks, setLiveTasks] = useState([]);      // tasks from backend
  const wsRef = useRef(null);

  // ── Real connections state ────────────────────────────────────────────────
  const [realConnections, setRealConnections] = useState([]);
  const [connTypes, setConnTypes] = useState([]);          // connector type metadata from API
  const [showNewConn, setShowNewConn] = useState(false);
  const [testResults, setTestResults] = useState({});      // connectionId → test result
  const [discoveringSchema, setDiscoveringSchema] = useState({}); // connectionId → bool
  const [connSchemas, setConnSchemas] = useState({});      // connectionId → [schemaName]
  const [connTables, setConnTables] = useState({});        // connectionId → [tableObj]
  const [expandedConn, setExpandedConn] = useState(null);
  const [profilingTable, setProfilingTable] = useState(null);
  const [profileResult, setProfileResult] = useState(null);

  // ── Live catalog state (real data from backend) ────────────────────────────
  const [catalogTables, setCatalogTables] = useState([]);  // empty until real DB connected
  const [catalogIssues, setCatalogIssues] = useState([]);  // empty until real DB connected

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

  // Sync LLM configured flag — gates agent UI and log stream across all tabs
  useEffect(() => {
    if (!backendOnline) { setLlmConfigured(false); return; }
    const checkLlm = () =>
      apiFetch("/api/llm/config").then(d => setLlmConfigured(!!(d && d.configured)));
    checkLlm();
    const id = setInterval(checkLlm, 30000);
    return () => clearInterval(id);
  }, [backendOnline]);

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
  const avgQuality = catalogTables.length > 0 ? Math.round(catalogTables.reduce((a, t) => a + (t.quality ?? 0), 0) / catalogTables.length) : 0;
  const activeAgents = mockAgents.filter(a => a.status === "active").length;

  // ── SIDEBAR ────────────────────────────────────────────────────────────────
  const navItems = [
    { id: "dashboard",   icon: "⬡",   label: "Dashboard" },
    { id: "scheduler",   icon: "📅",  label: "Scheduler" },
    { id: null }, // separator
    { id: "connections", icon: "🔗",  label: "Connections" },
    { id: "catalog",     icon: "🗄",   label: "Data Catalog" },
    { id: "quality",     icon: "🚨",   label: "Issues" },
    { id: "rules",       icon: "⚡",   label: "DQ Rules" },
    { id: "lineage",     icon: "🔀",   label: "Lineage" },
    { id: "agents",      icon: "🤖",   label: "AI Agents" },
    { id: null }, // separator
    { id: "governance",  icon: "🛡",   label: "Governance" },
  ];

  const Sidebar = () => (
    <div style={{ width: '150px', minWidth: '150px', background: 'var(--card)', borderRight: '1px solid var(--border)', display: 'flex', flexDirection: 'column', padding: '20px 10px 18px' }}>
      {/* Logo */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '9px', padding: '0 6px', marginBottom: '28px' }}>
        <div style={{ width: '28px', height: '28px', borderRadius: '8px', background: 'var(--orange)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '13px', fontWeight: '800', color: '#fff', flexShrink: 0 }}>D</div>
        <div style={{ fontSize: '14px', fontWeight: '700', color: 'var(--text)' }}>DataIQ</div>
      </div>

      {/* Nav */}
      <nav style={{ display: 'flex', flexDirection: 'column', gap: '1px', flex: 1 }}>
        {navItems.map(({ id, icon, label }, idx) => {
          const isActive = activeTab === id;
          const isSeparator = id === null;
          if (isSeparator) return <div key={idx} style={{ height: '1px', background: 'var(--border)', margin: '10px 0' }} />;
          return (
            <button
              key={id}
              onClick={() => setActiveTab(id)}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '9px',
                padding: '8px 10px',
                borderRadius: '8px',
                fontSize: '13px',
                fontWeight: isActive ? '600' : '500',
                color: isActive ? 'var(--dark)' : 'var(--muted)',
                cursor: 'pointer',
                background: isActive ? 'rgba(232,98,43,.06)' : 'transparent',
                borderLeft: isActive ? '3px solid var(--orange)' : 'none',
                marginLeft: isActive ? '-10px' : '0',
                paddingLeft: isActive ? '12px' : '10px',
                transition: 'all .15s',
              }}
            >
              <span style={{ fontSize: '14px', width: '18px', textAlign: 'center' }}>{icon}</span>
              <span>{label}</span>
            </button>
          );
        })}
      </nav>

      {/* Backend status */}
      <div style={{ margin: '4px 0', padding: '6px 10px', borderRadius: '8px', display: 'flex', alignItems: 'center', gap: '6px', fontSize: '11px', fontWeight: '500', background: backendOnline ? 'rgba(22,163,74,.1)' : 'rgba(220,38,38,.08)', color: backendOnline ? '#16a34a' : '#dc2626' }}>
        <div style={{ width: '5px', height: '5px', borderRadius: '50%', background: backendOnline ? '#16a34a' : '#dc2626', flexShrink: 0 }} />
        {backendOnline ? 'Backend connected' : 'Backend offline'}
      </div>

      {/* Separator before profile */}
      <div style={{ height: '1px', background: 'var(--border)', margin: '6px 0' }} />

      {/* Profile */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '9px', padding: '8px 10px', borderRadius: '8px', fontSize: '13px', fontWeight: '500', color: 'var(--muted)', cursor: 'pointer' }}>
        <div style={{ width: '22px', height: '22px', borderRadius: '50%', background: 'var(--dark)', color: '#fff', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '10px', fontWeight: '700', flexShrink: 0 }}>
          {authUser?.name ? authUser.name[0].toUpperCase() : 'M'}
        </div>
        <div style={{ minWidth: 0 }}>
          <div style={{ fontSize: '12px', fontWeight: '600', color: 'var(--text)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{authUser?.name || 'Milan'}</div>
          <div style={{ fontSize: '11px', color: 'var(--muted)' }}>Admin</div>
        </div>
      </div>
    </div>
  );


  // ── DASHBOARD TAB ──────────────────────────────────────────────────────────
  const Dashboard = () => {
    // chartPeriod lifted to DataIQApp parent — prevents reset on parent re-renders
    const today = new Date().toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' });
    const base = avgQuality || 80;

    // Data sets for each period
    const chartData = {
      hours: {
        points: [
          { label: '6am',  score: base-8  },
          { label: '8am',  score: base-5  },
          { label: '10am', score: base-3  },
          { label: '12pm', score: base-6  },
          { label: '2pm',  score: base-1  },
          { label: '4pm',  score: base    },
          { label: '6pm',  score: base-2  },
          { label: '8pm',  score: base-4  },
        ].map(p => ({ ...p, score: Math.min(100, Math.max(60, Math.round(p.score))) })),
        todayIdx: (() => { const h = new Date().getHours(); return Math.min(7, Math.max(0, Math.floor((h - 6) / 2))); })(),
        title: 'Quality Score — Today',
        sub: 'Hourly avg',
        trendLabel: '↑ 1.8% vs yesterday',
        compareLabel: `Today's score vs yesterday's avg (${base-2}%)`,
      },
      day: {
        points: [
          { label: 'M', score: base-4 }, { label: 'T', score: base-2 },
          { label: 'W', score: base-5 }, { label: 'T', score: base   },
          { label: 'F', score: base-3 }, { label: 'S', score: base-6 },
          { label: 'S', score: base-5 },
        ].map(p => ({ ...p, score: Math.min(100, Math.max(60, Math.round(p.score))) })),
        todayIdx: [1,2,3,4,5,6,0].indexOf(new Date().getDay()),
        title: 'Quality Score — This Week',
        sub: 'Daily avg',
        trendLabel: '↑ 3.1%',
        compareLabel: `This week's score is higher than last week's (${base-3}%)`,
      },
      week: {
        points: [
          { label: 'W1', score: base-7 }, { label: 'W2', score: base-4 },
          { label: 'W3', score: base-6 }, { label: 'W4', score: base-3 },
          { label: 'W5', score: base-1 }, { label: 'W6', score: base-5 },
          { label: 'W7', score: base-2 }, { label: 'W8', score: base   },
        ].map(p => ({ ...p, score: Math.min(100, Math.max(60, Math.round(p.score))) })),
        todayIdx: 7,
        title: 'Quality Score — Last 8 Weeks',
        sub: 'Weekly avg',
        trendLabel: '↑ 4.2% vs 8w ago',
        compareLabel: `8-week trend — improving steadily from ${base-7}%`,
      },
    };

    const cd = chartData[chartPeriod];
    const maxH = 72;
    const scores = cd.points.map(p => p.score);
    const minScore = Math.min(...scores);
    const maxScore = Math.max(...scores);
    const stemH = v => Math.round(((v - minScore) / (maxScore - minScore || 1)) * (maxH - 20) + 20);
    const kpiCards = [
      { icon: '🗄', value: catalogTables.length, label: 'Total tables profiled', trend: realConnections.length ? `across ${realConnections.length} connections` : 'Connect a database to start', trendType: 'nt', topColor: '#1c3d34' },
      { icon: '📊', value: catalogTables.length ? `${avgQuality}%` : '—', label: 'Avg quality score', trend: catalogTables.length ? '↑ 3.1% this week' : 'No data yet', trendType: catalogTables.length ? 'up' : 'nt', topColor: '#16a34a' },
      { icon: '🚨', value: totalIssues, label: 'Open issues', trend: `↑ ${highIssues} high severity`, trendType: totalIssues > 0 ? 'dn' : 'nt', topColor: '#e8622b' },
      { icon: '⚡', value: catalogTables.length ? `${221}/${247}` : '—', label: 'Rules passing today', trend: catalogTables.length ? '89.5% pass rate' : 'No data yet', trendType: catalogTables.length ? 'up' : 'nt', topColor: '#16a34a' },
    ];
    const trendStyle = t => t === 'up' ? { background: 'rgba(22,163,74,.1)', color: '#16a34a' } : t === 'dn' ? { background: 'rgba(220,38,38,.08)', color: '#dc2626' } : { background: 'rgba(107,114,128,.08)', color: '#6b7280' };
    const dashCardStyle = { background: '#fff', borderRadius: '16px', padding: '20px', border: '1px solid var(--border)' };
    return (
      <div className="flex-1 overflow-auto" style={{ padding: '22px 24px' }}>
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '20px' }}>
          <h1 style={{ fontSize: '20px', fontWeight: '700', color: 'var(--text)', flex: 1, display: 'flex', alignItems: 'center', gap: '6px' }}>
            Dashboard <span style={{ width: '6px', height: '6px', borderRadius: '50%', background: 'var(--orange)', display: 'inline-block', verticalAlign: 'middle' }} />
          </h1>
          <span style={{ fontSize: '12.5px', color: 'var(--muted)' }}>{today}</span>
          <button onClick={() => {}} style={{ display: 'flex', alignItems: 'center', gap: '6px', padding: '8px 18px', background: 'var(--orange)', color: '#fff', border: 'none', borderRadius: '10px', fontSize: '13px', fontWeight: '600', cursor: 'pointer', fontFamily: 'inherit' }}>
            <RefreshCw style={{ width: '14px', height: '14px' }} /> Refresh All
          </button>
        </div>

        {/* KPI row */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: '14px', marginBottom: '18px' }}>
          {kpiCards.map(({ icon, value, label, trend, trendType, topColor }) => (
            <div key={label} style={{ background: '#fff', borderRadius: '16px', padding: '18px', border: '1px solid var(--border)', borderTop: `3px solid ${topColor}` }}>
              <div style={{ fontSize: '20px', marginBottom: '10px' }}>{icon}</div>
              <div style={{ fontSize: '26px', fontWeight: '800', color: 'var(--text)', lineHeight: '1' }}>{value}</div>
              <div style={{ fontSize: '11.5px', color: 'var(--muted)', marginTop: '4px' }}>{label}</div>
              <div style={{ display: 'inline-flex', alignItems: 'center', gap: '3px', fontSize: '11px', fontWeight: '600', padding: '2px 7px', borderRadius: '20px', marginTop: '8px', ...trendStyle(trendType) }}>{trend}</div>
            </div>
          ))}
        </div>

        {/* 2×2 grid */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '14px' }}>
          {/* Quality Score lollipop chart */}
          <div style={dashCardStyle}>
            {/* Card header */}
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px' }}>
              <span style={{ fontSize: '14px', fontWeight: '600', color: 'var(--text)' }}>{cd.title}</span>
              <span style={{ fontSize: '12px', color: 'var(--muted)' }}>{cd.sub}</span>
            </div>

            {/* Period toggle */}
            <div style={{ display: 'flex', gap: '4px', marginBottom: '16px', background: 'var(--bg)', borderRadius: '10px', padding: '3px' }}>
              {[['hours','Hours'],['day','Day'],['week','Weeks']].map(([key, label]) => (
                <button key={key} onClick={() => setChartPeriod(key)} style={{
                  flex: 1, padding: '5px 0', fontSize: '12px', fontWeight: '600',
                  borderRadius: '8px', border: 'none', cursor: 'pointer', fontFamily: 'inherit',
                  transition: 'all .15s',
                  background: chartPeriod === key ? 'var(--card)' : 'transparent',
                  color: chartPeriod === key ? 'var(--dark)' : 'var(--muted)',
                  boxShadow: chartPeriod === key ? '0 1px 4px rgba(0,0,0,.08)' : 'none',
                }}>{label}</button>
              ))}
            </div>

            {/* Lollipop chart */}
            <div style={{ display: 'flex', alignItems: 'flex-end', justifyContent: 'space-between', height: `${maxH + 28}px`, position: 'relative' }}>
              {cd.points.map(({ label, score }, i) => {
                const isActive = i === cd.todayIdx;
                const h = stemH(score);
                return (
                  <div key={i} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', flex: 1, position: 'relative' }}>
                    {/* Tooltip badge */}
                    {isActive && (
                      <div style={{
                        position: 'absolute', top: `${maxH - h - 28}px`,
                        background: 'var(--dark)', color: '#fff',
                        fontSize: '11px', fontWeight: '700',
                        padding: '3px 8px', borderRadius: '6px',
                        whiteSpace: 'nowrap', zIndex: 2,
                        boxShadow: '0 2px 8px rgba(0,0,0,.18)',
                      }}>
                        {score}%
                        <div style={{ position: 'absolute', bottom: '-5px', left: '50%', transform: 'translateX(-50%)', width: 0, height: 0, borderLeft: '5px solid transparent', borderRight: '5px solid transparent', borderTop: '5px solid var(--dark)' }} />
                      </div>
                    )}
                    {/* Stem + dot */}
                    <div style={{ flex: 1, display: 'flex', flexDirection: 'column', justifyContent: 'flex-end', alignItems: 'center', width: '100%', paddingTop: '28px' }}>
                      <div style={{
                        width: isActive ? '11px' : '9px', height: isActive ? '11px' : '9px',
                        borderRadius: '50%',
                        border: `2px solid ${isActive ? 'var(--dark)' : '#d1cec9'}`,
                        background: 'var(--card)', zIndex: 1, flexShrink: 0, marginBottom: '-1px',
                      }} />
                      <div style={{
                        width: isActive ? '2px' : '1.5px', height: `${h}px`,
                        background: isActive ? 'var(--dark)' : '#d1cec9', borderRadius: '1px',
                      }} />
                    </div>
                    {/* Label */}
                    <div style={{
                      fontSize: '11px', fontWeight: isActive ? '700' : '500',
                      color: isActive ? 'var(--dark)' : 'var(--muted)', marginTop: '6px',
                    }}>{label}</div>
                  </div>
                );
              })}
              {/* Baseline rule */}
              <div style={{ position: 'absolute', bottom: '24px', left: 0, right: 0, height: '1px', background: 'var(--border)' }} />
            </div>

            {/* Score + trend */}
            <div style={{ marginTop: '16px', paddingTop: '16px', borderTop: '1px solid var(--border)' }}>
              <div style={{ display: 'flex', alignItems: 'baseline', gap: '10px', marginBottom: '4px' }}>
                <span style={{ fontSize: '32px', fontWeight: '800', color: 'var(--text)', lineHeight: '1' }}>{cd.points[cd.todayIdx]?.score}%</span>
                <span style={{ fontSize: '12px', fontWeight: '700', padding: '3px 8px', borderRadius: '20px', background: 'rgba(22,163,74,.1)', color: '#16a34a' }}>{cd.trendLabel}</span>
              </div>
              <div style={{ fontSize: '12px', color: 'var(--muted)' }}>{cd.compareLabel}</div>
            </div>
          </div>

          {/* Open Issues */}
          <div style={dashCardStyle}>
            <div style={{ fontSize: '14px', fontWeight: '600', color: 'var(--text)', marginBottom: '14px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
              Open Issues
              <button onClick={() => setActiveTab("quality")} style={{ fontSize: '11.5px', padding: '4px 10px', background: '#fff', border: '1px solid var(--border)', borderRadius: '8px', cursor: 'pointer', fontFamily: 'inherit', color: 'var(--text)' }}>View all</button>
            </div>
            {catalogIssues.map(i => {
              const sev = i.severity === 'high' ? 'hi' : i.severity === 'medium' ? 'med' : 'lo';
              const sevIcon = i.severity === 'high' ? '🚨' : i.severity === 'medium' ? '⚠️' : '💡';
              const sevBgMap = { hi: 'rgba(232,98,43,.1)', med: 'rgba(246,185,77,.1)', lo: 'rgba(59,130,246,.08)' };
              const sevColorMap = { hi: 'var(--orange)', med: '#a07000', lo: '#1d4ed8' };
              return (
                <div key={i.id} onClick={() => setActiveTab("quality")} style={{ display: 'flex', alignItems: 'center', gap: '10px', padding: '9px 8px', borderBottom: '1px solid #f5f0ea', cursor: 'pointer', borderRadius: '8px', margin: '0 -8px' }}
                  onMouseEnter={e => e.currentTarget.style.background='#faf8f5'} onMouseLeave={e => e.currentTarget.style.background='transparent'}>
                  <div style={{ width: '28px', height: '28px', borderRadius: '8px', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '13px', background: sevBgMap[sev], flexShrink: 0 }}>{sevIcon}</div>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: '12.5px', fontWeight: '600', color: 'var(--text)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{i.table}</div>
                    <div style={{ fontSize: '11px', color: 'var(--muted)', marginTop: '1px' }}>{i.type} · {i.detectedAt}</div>
                  </div>
                  <span style={{ fontSize: '10.5px', fontWeight: '700', padding: '3px 9px', borderRadius: '20px', background: sevBgMap[sev], color: sevColorMap[sev], flexShrink: 0 }}>{i.severity.charAt(0).toUpperCase()+i.severity.slice(1)}</span>
                </div>
              );
            })}
          </div>

          {/* Recent Activity */}
          <div style={dashCardStyle}>
            <div style={{ fontSize: '14px', fontWeight: '600', color: 'var(--text)', marginBottom: '14px' }}>Recent Activity</div>
            {[
              ['#16a34a', 'Full Profile Scan completed on Operational DB — 203 tables, 0 new issues', '3 mins ago'],
              ['#ca8a04', 'Data drift detected in orders_fact · amount_usd distribution anomaly', '7 mins ago'],
              ['#16a34a', 'PII Scanner completed on HR data — 63 columns scanned, 12 flagged', '10 mins ago'],
              ['var(--orange)', 'Rules Validation failed on email_campaigns — 89,450 null customer_id rows', '2 hrs ago'],
              ['#1d4ed8', 'Schema Sync detected new column analytics.customer_360.churn_v2', '1 hr ago'],
              ['#16a34a', 'Weekly Profile Run completed — All connections healthy', 'Yesterday'],
            ].map(([c,t,ts], i) => (
              <div key={i} style={{ display: 'flex', gap: '10px', padding: '8px 0', borderBottom: '1px solid #f5f0ea' }}>
                <div style={{ width: '8px', height: '8px', borderRadius: '50%', marginTop: '4px', flexShrink: 0, background: c }} />
                <div style={{ fontSize: '12.5px', color: 'var(--text)', lineHeight: '1.4', flex: 1 }}>{t}</div>
                <div style={{ fontSize: '11px', color: 'var(--muted)', whiteSpace: 'nowrap' }}>{ts}</div>
              </div>
            ))}
          </div>

          {/* Connection Health */}
          <div style={dashCardStyle}>
            <div style={{ fontSize: '14px', fontWeight: '600', color: 'var(--text)', marginBottom: '14px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
              Connection Health
              <button onClick={() => setActiveTab("connections")} style={{ fontSize: '11.5px', padding: '4px 10px', background: '#fff', border: '1px solid var(--border)', borderRadius: '8px', cursor: 'pointer', fontFamily: 'inherit', color: 'var(--text)' }}>View all</button>
            </div>
            {(realConnections.length ? realConnections : mockConnections).map(c => {
              const statusColor = c.status === 'ok' || c.status === 'connected' ? '#16a34a' : c.status === 'warning' ? '#ca8a04' : '#dc2626';
              const statusLabel = c.status === 'ok' || c.status === 'connected' ? 'Live' : c.status === 'warning' ? 'Warning' : 'Offline';
              return (
                <div key={c.id} style={{ display: 'flex', alignItems: 'center', gap: '10px', padding: '9px 0', borderBottom: '1px solid #f5f0ea' }}>
                  <span style={{ fontSize: '18px' }}>{c.icon || '🔗'}</span>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: '13px', fontWeight: '600', color: 'var(--text)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{c.name}</div>
                    <div style={{ fontSize: '11px', color: 'var(--muted)' }}>{c.type} · {(c.tables || c.table_count || 0)} tables</div>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '5px', fontSize: '12px', fontWeight: '600', color: statusColor }}>
                    <span style={{ width: '7px', height: '7px', borderRadius: '50%', background: statusColor, display: 'inline-block' }} />
                    {statusLabel}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    );
  };

  // ── CATALOG TAB ────────────────────────────────────────────────────────────
  const CatalogTab = () => (
    <div className="flex-1 overflow-auto" style={{ background: 'var(--bg)' }}>
      <div className="p-6 max-w-7xl mx-auto space-y-4">
        {/* Filters */}
        <div className="flex flex-wrap items-center gap-3">
          <div className="relative flex-1 min-w-64">
            <Search className="w-4 h-4 absolute left-3 top-2.5 text-slate-400" />
            <input type="text" placeholder="Search tables, columns, descriptions, tags…"
              className="w-full pl-9 pr-4 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-[#e8622b] bg-white"
              value={searchQuery} onChange={e => setSearchQuery(e.target.value)} autoFocus={searchFocused} />
          </div>
          <select value={filterDomain} onChange={e => setFilterDomain(e.target.value)} className="px-3 py-2 text-sm border border-slate-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-[#e8622b]">
            <option value="All">All Domains</option>
            {[...new Set(catalogTables.map(t => t.domain).filter(Boolean))].sort().map(d => <option key={d}>{d}</option>)}
          </select>
          <select value={filterTrust} onChange={e => setFilterTrust(e.target.value)} className="px-3 py-2 text-sm border border-slate-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-[#e8622b]">
            <option value="All">All Trust</option>
            <option value="Gold">Gold</option>
            <option value="Silver">Silver</option>
            <option value="Bronze">Bronze</option>
          </select>
          <select value={filterConn} onChange={e => setFilterConn(e.target.value)} className="px-3 py-2 text-sm border border-slate-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-[#e8622b]">
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
                className={`bg-white rounded-xl border shadow-sm cursor-pointer hover:shadow-md transition-all ${selectedTable?.id === t.id ? "border-[#e8622b]" : "border-slate-200 hover:border-[#e8622b]/40"}`}>
                <div className="p-4">
                  <div className="flex items-start gap-3">
                    <div className="w-9 h-9 rounded-lg bg-[#fdf3ee] flex items-center justify-center flex-shrink-0 mt-0.5">
                      <Table className="w-4.5 h-4.5 text-[#e8622b]" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="font-semibold text-slate-900 font-mono text-sm">{t.name}</span>
                        <TrustBadge trust={t.trust} />
                        {t.issues > 0 && <span className="text-xs bg-red-100 text-red-700 border border-red-200 rounded-full px-2 py-0.5">{t.issues} issues</span>}
                      </div>
                      <div className="text-xs text-slate-500 mt-0.5">{t.connection} · <code className="bg-slate-100 px-1 rounded">{t.schema}</code> · {(t.records ?? t.row_count ?? 0).toLocaleString()} records</div>
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
              <div className="px-6 py-4 border-b border-slate-100" style={{ background: '#faf8f5' }}>
                <div className="flex items-start justify-between">
                  <div className="flex items-start gap-4">
                    <div className="w-12 h-12 rounded-xl flex items-center justify-center" style={{ background: 'rgba(232,98,43,.1)' }}>
                      <Table className="w-6 h-6 text-[#e8622b]" />
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
                        {selectedTable.tags.map(tag => <span key={tag} className="text-xs bg-[#fdf3ee] text-[#c94d1a] border border-[#e8622b]/30 px-2 py-0.5 rounded-full">{tag}</span>)}
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
                    { label: "Records", value: (selectedTable.records ?? selectedTable.row_count ?? 0).toLocaleString(), icon: Hash },
                    { label: "Columns", value: selectedTable.columns, icon: Columns },
                    { label: "Quality Score", value: `${selectedTable.quality}%`, icon: Target },
                    { label: "Last Profiled", value: selectedTable.lastProfiled, icon: Clock },
                  ].map(({ label, value, icon: Icon }) => (
                    <div key={label} style={{ textAlign: 'center', padding: '12px 8px', background: 'var(--bg)', borderRadius: '12px' }}>
                      <Icon className="w-4 h-4 mx-auto mb-1" style={{ color: 'var(--muted)' }} />
                      <div style={{ fontWeight: '700', color: 'var(--text)', fontSize: '18px' }}>{value}</div>
                      <div style={{ fontSize: '11px', color: 'var(--muted)', marginTop: '2px' }}>{label}</div>
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
                    <button onClick={() => openLineage(selectedTable.id)} className="text-xs text-[#e8622b] hover:underline">Open full lineage →</button>
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
                              <td className="px-3 py-2.5"><code className="text-[#c94d1a] font-medium text-xs bg-[#fdf3ee] px-1.5 py-0.5 rounded">{col.name}</code></td>
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
    // activeSection is lifted to DataIQApp to survive remounts caused by parent re-renders
    const activeSection = qualitySection;
    const setActiveSection = setQualitySection;
    return (
      <div className="flex-1 overflow-auto" style={{ background: 'var(--bg)' }}>
        <div style={{ padding: '22px 24px', maxWidth: '1200px', margin: '0 auto' }}>
          {/* Header */}
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '20px' }}>
            <h1 style={{ fontSize: '20px', fontWeight: '700', color: 'var(--text)', display: 'flex', alignItems: 'center', gap: '6px' }}>
              Data Quality <span style={{ width: '6px', height: '6px', borderRadius: '50%', background: 'var(--orange)', display: 'inline-block', verticalAlign: 'middle' }} />
            </h1>
            <div style={{ display: 'flex', gap: '6px' }}>
              {["issues", "scores", "rules"].map(s => (
                <button key={s} onClick={() => setActiveSection(s)} style={{
                  padding: '6px 16px', fontSize: '13px', fontWeight: '600', borderRadius: '10px', border: 'none',
                  cursor: 'pointer', fontFamily: 'inherit', transition: 'all .15s',
                  background: activeSection === s ? 'var(--orange)' : 'var(--card)',
                  color: activeSection === s ? '#fff' : 'var(--muted)',
                  boxShadow: activeSection === s ? 'none' : '0 1px 3px rgba(0,0,0,.06)',
                  outline: activeSection === s ? 'none' : '1px solid var(--border)',
                }}>
                  {s.charAt(0).toUpperCase() + s.slice(1)}
                </button>
              ))}
            </div>
          </div>

          {activeSection === "issues" && (
            <>
              {/* Severity cards */}
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: '12px', marginBottom: '16px' }}>
                {[
                  { label: "High Severity", count: highIssues, topColor: '#dc2626', bg: 'rgba(220,38,38,.06)', color: '#dc2626' },
                  { label: "Medium Severity", count: catalogIssues.filter(i => i.severity === "medium").length, topColor: '#ca8a04', bg: 'rgba(202,138,4,.06)', color: '#ca8a04' },
                  { label: "Low Severity", count: catalogIssues.filter(i => i.severity === "low").length, topColor: '#3b82f6', bg: 'rgba(59,130,246,.06)', color: '#3b82f6' },
                ].map(({ label, count, topColor, bg, color }) => (
                  <div key={label} style={{ background: 'var(--card)', borderRadius: '16px', border: '1px solid var(--border)', borderTop: `3px solid ${topColor}`, padding: '16px 18px' }}>
                    <div style={{ fontSize: '28px', fontWeight: '800', color, lineHeight: '1' }}>{count}</div>
                    <div style={{ fontSize: '12.5px', fontWeight: '600', color: 'var(--muted)', marginTop: '4px' }}>{label}</div>
                  </div>
                ))}
              </div>
              {/* Issues list */}
              <div style={{ background: 'var(--card)', borderRadius: '16px', border: '1px solid var(--border)', overflow: 'hidden' }}>
                <div style={{ padding: '14px 18px', borderBottom: '1px solid var(--border)' }}>
                  <span style={{ fontSize: '13px', fontWeight: '600', color: 'var(--text)' }}>All Active Issues</span>
                </div>
                {catalogIssues.map(issue => (
                  <div key={issue.id} style={{ padding: '14px 18px', borderBottom: '1px solid #f5f0ea', display: 'flex', alignItems: 'flex-start', gap: '14px' }}
                    onMouseEnter={e => e.currentTarget.style.background='#faf8f5'} onMouseLeave={e => e.currentTarget.style.background='transparent'}>
                    <SeverityBadge severity={issue.severity} />
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap' }}>
                        <span style={{ fontSize: '13px', fontWeight: '600', color: 'var(--text)' }}>{issue.type}</span>
                        <span style={{ fontSize: '11.5px', color: 'var(--muted)' }}>in</span>
                        <code style={{ fontSize: '11.5px', background: 'rgba(232,98,43,.08)', color: 'var(--orange)', padding: '2px 7px', borderRadius: '6px', cursor: 'pointer', fontFamily: 'ui-monospace, monospace' }}
                          onClick={() => { const t = catalogTables.find(t => t.id === issue.tableId); if (t) openTableDetail(t); }}>
                          {issue.table}
                        </code>
                      </div>
                      <p style={{ fontSize: '12.5px', color: 'var(--muted)', marginTop: '4px', lineHeight: '1.45' }}>{issue.description}</p>
                      {issue.count > 0 && <span style={{ fontSize: '11px', color: 'var(--muted)', marginTop: '2px', display: 'inline-block' }}>{issue.count.toLocaleString()} records affected</span>}
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexShrink: 0 }}>
                      <span style={{ fontSize: '11.5px', color: 'var(--muted)' }}>{issue.detectedAt}</span>
                      <button onClick={() => { const t = catalogTables.find(t => t.id === issue.tableId); if (t) openLineage(t.id); }}
                        style={{ padding: '6px', background: 'none', border: 'none', cursor: 'pointer', borderRadius: '8px', color: 'var(--muted)' }}
                        onMouseEnter={e => e.currentTarget.style.background='var(--bg)'} onMouseLeave={e => e.currentTarget.style.background='none'}
                        title="View lineage">
                        <Network style={{ width: '14px', height: '14px' }} />
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            </>
          )}

          {activeSection === "scores" && (
            <div style={{ background: 'var(--card)', borderRadius: '16px', border: '1px solid var(--border)', overflow: 'hidden' }}>
              <div style={{ padding: '14px 18px', borderBottom: '1px solid var(--border)' }}>
                <span style={{ fontSize: '13px', fontWeight: '600', color: 'var(--text)' }}>Quality Scores by Table</span>
              </div>
              {[...catalogTables].sort((a, b) => b.quality - a.quality).map(t => (
                <div key={t.id} style={{ padding: '12px 18px', borderBottom: '1px solid #f5f0ea', display: 'flex', alignItems: 'center', gap: '14px', cursor: 'pointer' }}
                  onClick={() => openTableDetail(t)}
                  onMouseEnter={e => e.currentTarget.style.background='#faf8f5'} onMouseLeave={e => e.currentTarget.style.background='transparent'}>
                  <div style={{ width: '160px', flexShrink: 0 }}>
                    <div style={{ fontSize: '12.5px', fontFamily: 'ui-monospace, monospace', fontWeight: '600', color: 'var(--text)' }}>{t.name}</div>
                    <div style={{ fontSize: '11px', color: 'var(--muted)', marginTop: '2px' }}>{t.connection}</div>
                  </div>
                  <div style={{ flex: 1 }}><QualityBar score={t.quality} /></div>
                  <TrustBadge trust={t.trust} />
                  {t.issues > 0 && <span style={{ fontSize: '11px', fontWeight: '700', padding: '2px 8px', borderRadius: '20px', background: 'rgba(220,38,38,.08)', color: '#dc2626' }}>{t.issues} issues</span>}
                </div>
              ))}
            </div>
          )}

          {activeSection === "rules" && (
            <div style={{ background: 'var(--card)', borderRadius: '16px', border: '1px solid var(--border)', overflow: 'hidden' }}>
              <div style={{ padding: '14px 18px', borderBottom: '1px solid var(--border)', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ fontSize: '13px', fontWeight: '600', color: 'var(--text)' }}>Validation Rules</span>
                <button style={{ display: 'flex', alignItems: 'center', gap: '5px', padding: '6px 14px', background: 'var(--orange)', color: '#fff', border: 'none', borderRadius: '8px', fontSize: '12.5px', fontWeight: '600', cursor: 'pointer', fontFamily: 'inherit' }}>
                  <Plus style={{ width: '13px', height: '13px' }} /> Add Rule
                </button>
              </div>
              {[
                { name: "Null Check — customer_id", table: "email_campaigns", type: "Not Null", status: "failing", lastRun: "2 min ago" },
                { name: "Format Check — email", table: "customer_master", type: "Regex", status: "passing", lastRun: "3 min ago" },
                { name: "Range Check — amount_usd", table: "orders_fact", type: "Range", status: "passing", lastRun: "5 min ago" },
                { name: "Referential Integrity — product_id", table: "inventory_snapshot", type: "FK Check", status: "failing", lastRun: "4 min ago" },
                { name: "Freshness — 24h SLA", table: "marketing_attribution", type: "Freshness", status: "failing", lastRun: "26 hours ago" },
                { name: "Uniqueness — order_id", table: "orders_fact", type: "Unique", status: "passing", lastRun: "7 min ago" },
              ].map((rule, i) => (
                <div key={i} style={{
                  padding: '12px 18px', borderBottom: '1px solid #f5f0ea', display: 'flex', alignItems: 'center', gap: '12px',
                  borderLeft: `3px solid ${rule.status === "passing" ? "#16a34a" : "var(--orange)"}`,
                }}
                  onMouseEnter={e => e.currentTarget.style.background='#faf8f5'} onMouseLeave={e => e.currentTarget.style.background='transparent'}>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: '13px', fontWeight: '600', color: 'var(--text)' }}>{rule.name}</div>
                    <div style={{ fontSize: '11.5px', color: 'var(--muted)', marginTop: '2px' }}>{rule.table} · {rule.type}</div>
                  </div>
                  <span style={{ fontSize: '12px', fontWeight: '700', color: rule.status === "passing" ? "#16a34a" : "var(--orange)" }}>
                    {rule.status === "passing" ? "✓ Pass" : "✗ Fail"}
                  </span>
                  <span style={{ fontSize: '11.5px', color: 'var(--muted)' }}>{rule.lastRun}</span>
                </div>
              ))}
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
            <div className="flex items-center gap-1.5 bg-[#fdf3ee] border border-[#e8622b]/30 rounded-lg px-3 py-1.5">
              <Table className="w-4 h-4 text-[#e8622b]" />
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
            className="px-3 py-1.5 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-[#e8622b] bg-white"
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
                    className={`px-3 py-2.5 cursor-pointer hover:bg-slate-50 ${n.isRoot ? "bg-[#fdf3ee]" : ""}`}>
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
            <button onClick={() => openTableDetail(currentTable)} className="flex items-center gap-1.5 text-xs text-[#e8622b] hover:underline">
              <ExternalLink className="w-3.5 h-3.5" /> Open in Catalog
            </button>
          </div>
        )}
      </div>
    );
  };

  // ── AGENTS TAB ─────────────────────────────────────────────────────────────

  // ── TASKS TAB ──────────────────────────────────────────────────────────────
  const TasksTab = () => {
    const [newTask, setNewTask] = useState({ title: "", agentId: "data_profiler", tableId: "", priority: "medium" });
    const filter = taskFilter;           // lifted to parent — survives remounts
    const setFilter = setTaskFilter;
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

    const statusColor = { pending: "text-slate-500 bg-slate-100 border-slate-200", in_progress: "text-[#c94d1a] bg-[#fdf3ee] border-[#e8622b]/30", completed: "text-emerald-700 bg-emerald-50 border-emerald-200", failed: "text-red-700 bg-red-50 border-red-200" };
    const priorityColor = { high: "text-red-600", medium: "text-yellow-600", low: "text-slate-400" };
    const counts = { all: allTasks.length, pending: allTasks.filter(t => t.status === "pending").length, in_progress: allTasks.filter(t => t.status === "in_progress").length, completed: allTasks.filter(t => t.status === "completed").length };

    return (
      <div className="flex-1 overflow-auto" style={{ background: "var(--bg)" }}>
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
                  className="w-full px-3 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-[#e8622b]"
                  value={newTask.title}
                  onChange={e => setNewTask(n => ({ ...n, title: e.target.value }))}
                  onKeyDown={e => e.key === "Enter" && backendOnline && submitTask()}
                />
              </div>
              <div>
                <label className="text-xs font-medium text-slate-500 block mb-1">Assign to Agent</label>
                <select className="w-full px-3 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-[#e8622b] bg-white"
                  value={newTask.agentId} onChange={e => setNewTask(n => ({ ...n, agentId: e.target.value }))}>
                  {AGENT_OPTIONS.map(a => <option key={a.id} value={a.id}>{a.label}</option>)}
                </select>
              </div>
              <div>
                <label className="text-xs font-medium text-slate-500 block mb-1">Target Table (optional)</label>
                <select className="w-full px-3 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-[#e8622b] bg-white"
                  value={newTask.tableId} onChange={e => setNewTask(n => ({ ...n, tableId: e.target.value }))}>
                  <option value="">— No specific table —</option>
                  {catalogTables.map(t => <option key={t.id} value={t.id}>{t.name} ({t.schema})</option>)}
                </select>
              </div>
              <div>
                <label className="text-xs font-medium text-slate-500 block mb-1">Priority</label>
                <select className="w-full px-3 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-[#e8622b] bg-white"
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
                  className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-[#e8622b] text-white text-sm rounded-lg hover:opacity-90 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
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
                className={`px-3 py-1.5 text-xs rounded-lg font-medium transition-colors ${filter === key ? "bg-[#e8622b] text-white" : "bg-white border border-slate-200 text-slate-600 hover:bg-slate-50"}`}>
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
    <div className="flex-1 overflow-auto" style={{ background: "var(--bg)" }}>
      <div className="p-6 max-w-7xl mx-auto space-y-6">
        <h1 style={{ fontSize: '20px', fontWeight: '700', color: 'var(--text)', display: 'flex', alignItems: 'center', gap: '6px' }}>
          Governance <span style={{ width: '6px', height: '6px', borderRadius: '50%', background: 'var(--orange)', display: 'inline-block', verticalAlign: 'middle' }} />
        </h1>

        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          {[
            { label: "PII Tables", value: catalogTables.filter(t => t.tags.includes("PII")).length, icon: Lock, color: "text-red-600 bg-red-50 border-red-200" },
            { label: "Active Policies", value: mockPolicies.filter(p => p.status === "active").length, icon: Shield, color: "text-[#e8622b] bg-[#fdf3ee] border-[#e8622b]/30" },
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
              <button className="flex items-center gap-1 px-3 py-1.5 text-xs bg-[#e8622b] text-white rounded-lg hover:opacity-90"><Plus className="w-3.5 h-3.5" />New Policy</button>
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

    const statusColor = conn.status === "ok" ? "#16a34a" : conn.status === "error" ? "#dc2626" : "#ca8a04";
    const statusLabel = conn.status === "ok" ? "Live" : conn.status === "error" ? "Error" : conn.status || "Unknown";
    const connectorIcons = { postgresql: "🐘", fabric: "🪟", snowflake: "❄️", bigquery: "☁️", redshift: "🔴" };

    return (
      <div style={{ background: 'var(--card)', borderRadius: '16px', border: '1px solid var(--border)', borderLeft: `3px solid ${statusColor}`, overflow: 'hidden' }}>
        {/* Card header */}
        <div style={{ padding: '18px' }}>
          <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: '14px' }}>
            <div style={{ display: 'flex', alignItems: 'flex-start', gap: '12px' }}>
              <div style={{ width: '40px', height: '40px', borderRadius: '10px', background: 'var(--bg)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '20px', flexShrink: 0 }}>
                {connectorIcons[conn.connector_type] || "🔌"}
              </div>
              <div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <span style={{ fontSize: '14px', fontWeight: '700', color: 'var(--text)' }}>{conn.name}</span>
                  <span style={{ fontSize: '11px', background: 'var(--bg)', color: 'var(--muted)', padding: '2px 7px', borderRadius: '6px', fontFamily: 'ui-monospace, monospace' }}>{conn.connector_type}</span>
                </div>
                {conn.description && <div style={{ fontSize: '11.5px', color: 'var(--muted)', marginTop: '2px' }}>{conn.description}</div>}
              </div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
              <span style={{ width: '7px', height: '7px', borderRadius: '50%', background: statusColor, display: 'inline-block' }} />
              <span style={{ fontSize: '12px', fontWeight: '600', color: statusColor }}>{statusLabel}</span>
            </div>
          </div>

          {/* Stats row */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: '8px', textAlign: 'center' }}>
            {[
              { label: "Tables Found", value: conn.table_count || "—" },
              { label: "Latency",      value: conn.latency_ms ? `${conn.latency_ms}ms` : "—" },
              { label: "Last Tested",  value: conn.last_tested_at ? new Date(conn.last_tested_at).toLocaleTimeString() : "Never" },
            ].map(({ label, value }) => (
              <div key={label} style={{ background: 'var(--bg)', borderRadius: '8px', padding: '9px 6px' }}>
                <div style={{ fontSize: '14px', fontWeight: '700', color: 'var(--text)' }}>{value}</div>
                <div style={{ fontSize: '10.5px', color: 'var(--muted)', marginTop: '2px' }}>{label}</div>
              </div>
            ))}
          </div>

          {/* Error message */}
          {conn.last_test_error && (
            <div style={{ marginTop: '10px', padding: '9px 12px', background: 'rgba(220,38,38,.06)', border: '1px solid rgba(220,38,38,.2)', borderRadius: '8px' }}>
              <p style={{ fontSize: '11px', color: '#dc2626', fontFamily: 'ui-monospace, monospace' }}>{conn.last_test_error}</p>
            </div>
          )}

          {/* Inline test result */}
          {tr && !tr.testing && (
            <div style={{ marginTop: '10px', padding: '9px 12px', borderRadius: '8px', fontSize: '12px', border: `1px solid ${tr.success ? 'rgba(22,163,74,.2)' : 'rgba(220,38,38,.2)'}`, background: tr.success ? 'rgba(22,163,74,.06)' : 'rgba(220,38,38,.06)', color: tr.success ? '#16a34a' : '#dc2626' }}>
              {tr.success ? `✓ ${tr.message} · ${tr.latency_ms}ms` : `✗ ${tr.error || tr.message}`}
            </div>
          )}

          {/* Action buttons */}
          <div style={{ display: 'flex', gap: '8px', marginTop: '14px' }}>
            <button onClick={handleTest}
              disabled={tr?.testing}
              style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '5px', fontSize: '12.5px', fontWeight: '500', padding: '7px 0', border: '1px solid var(--border)', borderRadius: '8px', background: 'none', cursor: tr?.testing ? 'not-allowed' : 'pointer', color: 'var(--text)', fontFamily: 'inherit', opacity: tr?.testing ? 0.5 : 1 }}>
              {tr?.testing ? <RefreshCw style={{ width: '13px', height: '13px' }} className="animate-spin" /> : <Zap style={{ width: '13px', height: '13px' }} />}
              Test
            </button>
            <button onClick={handleLoadSchemas}
              style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '5px', fontSize: '12.5px', fontWeight: '500', padding: '7px 0', border: '1px solid var(--border)', borderRadius: '8px', background: 'none', cursor: 'pointer', color: 'var(--text)', fontFamily: 'inherit' }}>
              <Layers style={{ width: '13px', height: '13px' }} />
              {isExpanded ? "Hide Schema" : "Browse Schema"}
            </button>
            <button
              onClick={async () => {
                if (!window.confirm(`Delete connection "${conn.name}"?`)) return;
                await apiFetch(`/api/connections/${conn.id}`, { method: "DELETE" });
                await loadRealConnections();
              }}
              style={{ padding: '7px 12px', border: '1px solid rgba(220,38,38,.3)', borderRadius: '8px', background: 'none', cursor: 'pointer', color: '#dc2626' }}>
              <Trash2 style={{ width: '13px', height: '13px' }} />
            </button>
          </div>
        </div>

        {/* Schema browser */}
        {isExpanded && (
          <div style={{ borderTop: '1px solid var(--border)', background: 'var(--bg)' }}>
            <div style={{ padding: '14px 18px' }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '10px' }}>
                <span style={{ fontSize: '11px', fontWeight: '700', letterSpacing: '.07em', textTransform: 'uppercase', color: 'var(--orange)' }}>Schemas & Tables</span>
                {discovering && <RefreshCw style={{ width: '13px', height: '13px' }} className="animate-spin text-blue-500" />}
              </div>

              {schemas.length === 0 && !discovering && (
                <p style={{ fontSize: '12px', color: 'var(--muted)', textAlign: 'center', padding: '8px 0' }}>
                  {conn.status !== "ok" ? "Test connection first to browse schemas" : "No schemas found"}
                </p>
              )}

              <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                {schemas.map(schema => (
                  <div key={schema} style={{ borderRadius: '10px', border: '1px solid var(--border)', background: 'var(--card)', overflow: 'hidden' }}>
                    <button
                      onClick={() => handleDiscover(schema)}
                      style={{ width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '9px 12px', background: 'none', border: 'none', cursor: 'pointer', fontFamily: 'inherit' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '7px', fontSize: '12.5px', fontWeight: '600', color: 'var(--text)' }}>
                        <Layers style={{ width: '13px', height: '13px', color: 'var(--orange)' }} />
                        {schema}
                      </div>
                      <span style={{ fontSize: '11.5px', color: 'var(--muted)' }}>Discover →</span>
                    </button>

                    {/* Tables under this schema */}
                    {tables.filter(t => t.schema_name === schema).length > 0 && (
                      <div style={{ borderTop: '1px solid var(--border)' }}>
                        {tables.filter(t => t.schema_name === schema).map(tbl => (
                          <div key={tbl.id}
                            style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '7px 12px', borderTop: '1px solid #f5f0ea', fontSize: '12px', cursor: 'default' }}
                            onMouseEnter={e => e.currentTarget.style.background='rgba(232,98,43,.04)'} onMouseLeave={e => e.currentTarget.style.background='transparent'}
                            className="group">
                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px', minWidth: 0, flex: 1 }}>
                              <Table style={{ width: '12px', height: '12px', color: 'var(--muted)', flexShrink: 0 }} />
                              <span style={{ fontFamily: 'ui-monospace, monospace', color: 'var(--text)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{tbl.table_name}</span>
                              <span style={{ color: 'var(--muted)', fontSize: '11px', flexShrink: 0 }}>{tbl.row_count?.toLocaleString()} rows</span>
                            </div>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                              {tbl.quality_score && (
                                <span style={{ fontSize: '11px', fontWeight: '700', padding: '2px 6px', borderRadius: '20px', background: tbl.quality_score >= 90 ? 'rgba(22,163,74,.1)' : tbl.quality_score >= 70 ? 'rgba(202,138,4,.1)' : 'rgba(220,38,38,.1)', color: tbl.quality_score >= 90 ? '#16a34a' : tbl.quality_score >= 70 ? '#ca8a04' : '#dc2626' }}>{tbl.quality_score}%</span>
                              )}
                              <button
                                onClick={() => handleProfileTable(schema, tbl.table_name)}
                                disabled={profilingTable === `${schema}.${tbl.table_name}`}
                                className="opacity-0 group-hover:opacity-100"
                                style={{ padding: '3px 8px', background: 'var(--orange)', color: '#fff', border: 'none', borderRadius: '6px', fontSize: '11px', cursor: 'pointer', fontFamily: 'inherit', display: 'flex', alignItems: 'center', gap: '4px' }}>
                                {profilingTable === `${schema}.${tbl.table_name}`
                                  ? <RefreshCw style={{ width: '11px', height: '11px' }} className="animate-spin" />
                                  : <Zap style={{ width: '11px', height: '11px' }} />}
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
    <div className="flex-1 overflow-auto" style={{ background: 'var(--bg)' }}>
      {profileResult && <ProfileResultModal />}

      <div style={{ padding: '22px 24px', maxWidth: '1200px', margin: '0 auto' }}>
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '20px' }}>
          <div>
            <h1 style={{ fontSize: '20px', fontWeight: '700', color: 'var(--text)', display: 'flex', alignItems: 'center', gap: '6px' }}>
              Connections <span style={{ width: '6px', height: '6px', borderRadius: '50%', background: 'var(--orange)', display: 'inline-block', verticalAlign: 'middle' }} />
            </h1>
            <p style={{ fontSize: '12.5px', color: 'var(--muted)', marginTop: '3px' }}>
              {realConnections.length} connection{realConnections.length !== 1 ? "s" : ""} · plug-and-play warehouse connectivity
            </p>
          </div>
          <button
            onClick={() => setShowNewConn(true)}
            style={{ display: 'flex', alignItems: 'center', gap: '6px', padding: '9px 18px', background: 'var(--orange)', color: '#fff', border: 'none', borderRadius: '10px', fontSize: '13px', fontWeight: '600', cursor: 'pointer', fontFamily: 'inherit' }}>
            <Plus style={{ width: '14px', height: '14px' }} /> New Connection
          </button>
        </div>

        {/* Backend offline banner */}
        {!backendOnline && (
          <div style={{ background: 'rgba(202,138,4,.07)', border: '1px solid rgba(202,138,4,.25)', borderRadius: '12px', padding: '12px 18px', display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '16px' }}>
            <AlertTriangle style={{ width: '18px', height: '18px', color: '#ca8a04', flexShrink: 0 }} />
            <div>
              <p style={{ fontSize: '13px', fontWeight: '600', color: 'var(--dark)' }}>Backend Offline</p>
              <p style={{ fontSize: '12px', color: 'var(--muted)', marginTop: '2px' }}>Start the backend: <code style={{ fontFamily: 'ui-monospace, monospace', background: 'rgba(202,138,4,.12)', padding: '1px 5px', borderRadius: '4px', fontSize: '11px' }}>uvicorn main:app --reload --port 8000</code></p>
            </div>
          </div>
        )}

        {/* Empty state */}
        {backendOnline && realConnections.length === 0 && (
          <div style={{ background: 'var(--card)', borderRadius: '16px', border: '2px dashed var(--border)', padding: '48px 24px', textAlign: 'center', marginBottom: '16px' }}>
            <Database style={{ width: '48px', height: '48px', color: 'var(--border)', margin: '0 auto 16px' }} />
            <h3 style={{ fontSize: '15px', fontWeight: '700', color: 'var(--text)', marginBottom: '8px' }}>No connections yet</h3>
            <p style={{ fontSize: '13px', color: 'var(--muted)', marginBottom: '20px', maxWidth: '360px', margin: '0 auto 20px', lineHeight: '1.5' }}>
              Connect your first data warehouse to start profiling, discovering, and monitoring data quality.
            </p>
            <button onClick={() => setShowNewConn(true)}
              style={{ padding: '9px 22px', background: 'var(--orange)', color: '#fff', border: 'none', borderRadius: '10px', fontSize: '13px', fontWeight: '600', cursor: 'pointer', fontFamily: 'inherit' }}>
              Add your first connection
            </button>
          </div>
        )}

        {/* Real connection cards */}
        {realConnections.length > 0 && (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '14px', marginBottom: '16px' }}>
            {realConnections.map(conn => (
              <RealConnectionCard key={conn.id} conn={conn} />
            ))}
          </div>
        )}

        {/* Supported warehouse types */}
        <div style={{ background: 'var(--card)', borderRadius: '16px', border: '1px solid var(--border)', padding: '18px' }}>
          <div style={{ fontSize: '12px', fontWeight: '700', letterSpacing: '.07em', textTransform: 'uppercase', color: 'var(--orange)', marginBottom: '12px' }}>Supported Warehouse Types</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
            {[
              { icon: "🐘", name: "PostgreSQL", status: "available" },
              { icon: "🪟", name: "Microsoft Fabric", status: "available" },
              { icon: "❄️", name: "Snowflake", status: "coming-soon" },
              { icon: "☁️", name: "BigQuery", status: "coming-soon" },
              { icon: "🔴", name: "Redshift", status: "coming-soon" },
              { icon: "🧱", name: "Databricks", status: "coming-soon" },
            ].map(wh => (
              <div key={wh.name} style={{
                display: 'flex', alignItems: 'center', gap: '6px', padding: '7px 12px', borderRadius: '8px', fontSize: '12.5px', fontWeight: '500',
                border: wh.status === "available" ? '1px solid rgba(232,98,43,.3)' : '1px solid var(--border)',
                background: wh.status === "available" ? 'rgba(232,98,43,.06)' : 'var(--bg)',
                color: wh.status === "available" ? 'var(--orange)' : 'var(--muted)',
              }}>
                <span>{wh.icon}</span>
                <span>{wh.name}</span>
                {wh.status === "coming-soon" && <span style={{ fontSize: '10px' }}>soon</span>}
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );

  // ── RENDER ─────────────────────────────────────────────────────────────────
  // (RulesTab and SchedulerTab are defined at module level above DataIQApp)
  const renderTab = () => {
    switch (activeTab) {
      case "dashboard":    return <Dashboard />;
      case "catalog":      return <CatalogTab />;
      case "quality":      return <QualityTab />;
      case "lineage":      return <LineageTab />;
      case "agents":       return <AgentsTab backendOnline={backendOnline} liveAgents={liveAgents} agentLogs={agentLogs} llmConfigured={llmConfigured} />;
      case "tasks":        return <TasksTab />;
      case "governance":   return <GovernanceTab />;
      case "connections":  return <ConnectionsTab />;
      case "rules":        return <RulesTab />;
      case "scheduler":    return <SchedulerTab />;
      default:             return <Dashboard />;
    }
  };

  return (
    <div style={{ display: 'flex', height: '100vh', overflow: 'hidden', background: 'var(--bg)' }}>
      <Sidebar />
      <InfoPanel activeTab={activeTab} catalogTables={catalogTables} catalogIssues={catalogIssues} mockAgents={mockAgents} realConnections={realConnections} backendOnline={backendOnline} onAddConnection={() => setShowNewConn(true)} />
      <div style={{ flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column', minWidth: 0 }}>
        <ErrorBoundary key={activeTab}>{renderTab()}</ErrorBoundary>
      </div>
      {/* Connection wizard rendered at top level so DataIQApp re-renders never reset its state */}
      {showNewConn && (
        <NewConnectionWizard
          connTypes={connTypes}
          backendOnline={backendOnline}
          onClose={() => setShowNewConn(false)}
          onConnectionCreated={loadRealConnections}
        />
      )}
    </div>
  );
}
