#!/bin/bash
cd "$(dirname "$0")"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  DataIQ Platform — Push to GitHub (dq_sense)          "
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

REMOTE_URL="https://github.com/milan6rt/dq_sense.git"

# Clean up any stale git lock files
rm -f .git/index.lock 2>/dev/null

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
    backend/api/ \
    backend/connectors/ \
    backend/db/ \
    backend/services/ \
    "backend/.env.example" 2>/dev/null

git status --short
echo ""

# Commit
if git diff --cached --quiet; then
    echo "✅ Nothing new to commit — already up to date."
else
    echo "💾 Committing..."
    git commit -m "$(cat <<'EOF'
Connect demo PostgreSQL schema; fix backend startup deps

- Made httpx and python-jose imports optional in auth.py so the
  backend starts cleanly without Google OAuth env vars configured
- Commented out pyodbc in requirements.txt (requires native ODBC
  Driver 18 installed separately — not needed for PostgreSQL)
- Added Restart Backend.command: kills old backend on port 8000,
  wipes dataiq.db, reinstalls deps, starts new backend cleanly
- Demo schema (customers, products, orders, order_items, employees,
  events) discovered and profiled — 98% avg quality score
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
