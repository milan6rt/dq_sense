# DataIQ Platform — Claude Code Context

## What this project is
DataIQ is an **enterprise data quality platform** (SaaS-ready, single-tenant by default).
It profiles databases, tracks data quality issues, runs custom DQ rules, and schedules
automated scans. Think "data observability tool" — similar to Monte Carlo or Great Expectations
but self-hosted.

---

## How to run it

### Frontend (React + Vite)
```bash
cd ~/Documents/claude/Data\ Quality\ App
npm install
npm run dev          # starts on http://localhost:3000 (auto-increments if port taken)
```

### Backend (FastAPI + Python)
```bash
cd ~/Documents/claude/Data\ Quality\ App/backend
pip3 install -r requirements.txt
uvicorn main:app --reload --port 8000
```

The frontend proxies nothing — it hits `http://localhost:8000` directly via `API_BASE` in `App.jsx`.

### Login
- Google OAuth is wired up but requires a `.env` file (see `backend/.env.example`)
- **Demo mode**: click "Continue in Demo Mode" on the login screen — no backend needed for auth

---

## Project structure

```
Data Quality App/
├── App.jsx                  # Entire React frontend (single-file, ~3500 lines)
├── main.jsx                 # Vite entry point
├── index.html / index.css   # HTML shell + global styles
├── vite.config.js           # Vite config (React + Tailwind)
├── backend/
│   ├── main.py              # FastAPI app, lifespan, mounts all routers
│   ├── agents.py            # 4 background AI agents (profiler, validator, lineage, anomaly)
│   ├── api/
│   │   ├── auth.py          # Google OAuth 2.0 + JWT login
│   │   ├── connections.py   # CRUD + test + discover + profile endpoints
│   │   ├── rules.py         # DQ rule engine (8 rule types)
│   │   └── scheduler.py     # APScheduler cron jobs for automated scans
│   ├── connectors/
│   │   ├── registry.py      # Connector type registry
│   │   ├── postgresql.py    # PostgreSQL connector
│   │   └── fabric.py        # Microsoft Fabric connector
│   ├── db/
│   │   ├── database.py      # SQLAlchemy setup (SQLite dev / Postgres prod)
│   │   └── models.py        # All ORM models (see below)
│   ├── services/
│   │   └── connection_service.py  # Credential encryption + engine factory
│   ├── requirements.txt
│   └── .env.example         # Template for Google OAuth credentials
```

---

## Key architecture decisions

### Frontend
- **Single-file React app** (`App.jsx`) — intentional, avoids build complexity for a solo dev
- **Auth token**: stored in a module-level `let _authToken` variable (never localStorage)
- **Demo mode**: token value `"demo"` bypasses backend auth entirely
- **Component stability rule**: components must be defined at **module level** (outside DataIQApp),
  never as `const` inside the parent function. React treats inner consts as new types on every
  render → unmount/remount → state reset and input focus loss. This burned us twice.
- **`DataIQPlatform`** (default export): thin auth wrapper
- **`DataIQApp`**: main app, receives `authUser` and `handleLogout` as props
- **Modals rendered at top level**: `NewConnectionWizard` is rendered inside `DataIQApp`'s return,
  not inside `ConnectionsTab`, to avoid ConnectionsTab re-creation resetting the wizard state

### Backend
- **SQLite** for development (file: `backend/dataiq.db`)
- **Fernet encryption** for stored database credentials (`services/connection_service.py`)
- **APScheduler** (`AsyncIOScheduler`) for cron-based profiling — jobs restored at startup
- **4 background agents** run as asyncio tasks, emit structured logs over WebSocket `/ws/logs`
- All routers prefixed: `/api/connections`, `/api/rules`, `/api/scheduler`, `/api/auth`
- Health check: `GET /health` → `{"status": "ok"}`

---

## Database models (SQLite)

| Model | Purpose |
|---|---|
| `User` | Google OAuth users, role: viewer/editor/admin |
| `Connection` | DB credentials (Fernet-encrypted config blob) |
| `DiscoveredTable` | Tables found by schema discovery |
| `DiscoveredColumn` | Column-level profiling stats + PII flags |
| `QualityIssue` | Issues detected during profiling |
| `ProfilingRun` | History of profiling jobs |
| `DQRule` | Custom quality rules (8 types) |
| `DQRuleRun` | Per-run results for each DQ rule |
| `ScheduledScan` | Cron-scheduled profiling jobs |

---

## DQ Rule types
`not_null`, `unique`, `min_value`, `max_value`, `regex`, `freshness`, `row_count`, `custom_sql`

---

## Supported connectors
- **PostgreSQL** — fully working
- **Microsoft Fabric** — working (SQL auth + service principal + access token)
- Snowflake, BigQuery, Redshift, Databricks — UI shows "coming soon"

---

## Known issues / next steps
- [ ] **Salesforce / NetSuite / Workday connectors** — backend scaffold exists (`simple-salesforce`
  in requirements) but UI tabs not built yet. Deferred by user.
- [ ] **Google OAuth `.env` setup** — user hasn't created `backend/.env` yet; app runs in demo mode
- [ ] **`pyodbc` for Fabric** — needs ODBC Driver 18 installed separately on the host OS
- [ ] **Push to GitHub** — script at `Push to GitHub.command`, remote: `https://github.com/milan6rt/dq_sense`

---

## Bug registry — "inner component" scroll-reset class

**Root cause:** React treats a component defined *inside* a parent function as a new type on
every parent re-render. This causes unmount → remount, resetting scroll position and all local
state. Frequent parent re-renders are triggered by WebSocket updates (`agentLogs`, `liveAgents`,
`liveTasks`), polling (`backendOnline`), and data loads.

**Rule:** Any component rendered by `renderTab()` must be defined at **module level**, never as
`const Foo = () => {}` inside `DataIQApp`. This is in the Architecture section above; listed here
as a tracked bug class.

| Tab | Module-level? | Re-render trigger | Risk |
|-----|--------------|-------------------|------|
| `RulesTab` | ✅ yes (line ~1085) | — | none |
| `SchedulerTab` | ✅ yes (line ~1316) | — | none |
| `AgentsTab` | ✅ **fixed 2026-04-21** | `agentLogs` WebSocket (high freq) | fixed |
| `TasksTab` | ⚠️ inside DataIQApp | `liveTasks` WebSocket | high |
| `CatalogTab` | ⚠️ inside DataIQApp | `catalogTables` on load | medium |
| `QualityTab` | ⚠️ inside DataIQApp | `catalogIssues` on load | medium |
| `LineageTab` | ⚠️ inside DataIQApp | `lineageTableId` changes | low |
| `GovernanceTab` | ⚠️ inside DataIQApp | mostly static | low |
| `ConnectionsTab` | ⚠️ inside DataIQApp | `realConnections` on load | low |

**Next:** move `TasksTab` to module level (same pattern as `AgentsTab` fix) — it uses `liveTasks`
which polls the backend every few seconds and will exhibit the same scroll-reset bug.

---

## GitHub
- Remote: `https://github.com/milan6rt/dq_sense`
- User: `milan6rt`
- Branch: `main`
- Script: double-click `Push to GitHub.command` to stage, commit, and push

---

## Environment variables (backend/.env)
```
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
JWT_SECRET=change-me-in-production
FRONTEND_URL=http://localhost:3000
BACKEND_URL=http://localhost:8000
```
See `backend/.env.example` for full template.
