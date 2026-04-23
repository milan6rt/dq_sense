#!/bin/bash
cd "$(dirname "$0")"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  DataIQ Platform — Push to GitHub (dq_sense)          "
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

REMOTE_URL="https://github.com/milan6rt/dq_sense.git"

# Clean up any stale git lock files
rm -f .git/index.lock .git/HEAD.lock .git/MERGE_HEAD.lock 2>/dev/null

# Init if not already a repo
if [ ! -d ".git" ]; then
    echo "📁 Initialising git repository..."
    git init
    git branch -m main
fi

# Configure git identity if not set
git config user.email "milanairre@gmail.com" 2>/dev/null
git config user.name  "Milan" 2>/dev/null

# Set remote (overwrite if exists)
git remote remove origin 2>/dev/null
git remote add origin "$REMOTE_URL"
echo "🔗 Remote: $REMOTE_URL"
echo ""

# Stage everything (respecting .gitignore)
echo "📦 Staging files..."
git add \
    .gitignore \
    App.jsx \
    main.jsx \
    index.html \
    index.css \
    package.json \
    package-lock.json \
    vite.config.js \
    tailwind.config.js \
    postcss.config.js \
    dist.html \
    "Seed Demo Data.command" \
    "Start DataIQ.command" \
    "Restart Backend.command" \
    "Push to GitHub.command" \
    backend/main.py \
    backend/requirements.txt \
    backend/agents.py \
    backend/llm_provider.py \
    backend/api/ \
    backend/connectors/ \
    backend/db/ \
    backend/services/ \
    "backend/.env.example" \
    checker.py \
    "Run Checker.command" 2>/dev/null

git status --short
echo ""

# Commit
if git diff --cached --quiet; then
    echo "✅ Nothing new to commit — already up to date."
else
    echo "💾 Committing..."
    git commit -m "$(cat <<'EOF'
Add real LLM-powered agents, Settings tab, and agent scheduling

Backend:
- llm_provider.py: Anthropic (Claude) + OpenAI abstraction layer with
  test endpoint, model registry, and Fernet-encrypted API key storage
- agents.py: all 4 agents now make real LLM calls — profiler interprets
  column stats, validator synthesizes DQ rule results, lineage tracker
  infers data flow from schema, anomaly detector compares profiling runs
- db/models.py: added LLMConfig, AgentInsight, AgentSchedule ORM models
- api/agents_api.py: new router — GET/POST agent controls (start/stop/run),
  cron schedule CRUD with APScheduler integration, insights feed,
  GET/PUT /api/llm/config, POST /api/llm/test, GET /api/llm/models
- main.py: registered agents_api_router

Frontend (App.jsx):
- Settings tab: LLM provider selector (Claude/OpenAI), model dropdown,
  API key field, Save + Test Connection buttons
- AI Agents tab: fully rebuilt — Start/Stop, Run Now modal with
  connection/table targeting, cron schedule editor with presets,
  LLM Insights feed (expandable full analysis), severity badges
- New Settings nav item
EOF
)"
    echo ""
fi

# Push
echo "🚀 Pushing to GitHub..."
echo ""
git push -u origin main

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ✅ Done! Check: https://github.com/milan6rt/dq_sense"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
