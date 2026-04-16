#!/bin/bash
cd "$(dirname "$0")"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  DataIQ Platform — Push to GitHub (dq_sense)          "
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

REMOTE_URL="https://github.com/milanairre/dq_sense.git"

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
    "Push to GitHub.command" \
    backend/main.py \
    backend/requirements.txt \
    backend/agents.py \
    backend/api/ \
    backend/connectors/ \
    backend/db/ \
    backend/services/ 2>/dev/null

git status --short
echo ""

# Commit
if git diff --cached --quiet; then
    echo "✅ Nothing new to commit — already up to date."
else
    echo "💾 Committing..."
    git commit -m "$(cat <<'EOF'
Add medallion architecture, catalog API, and real data wiring

- Bronze/Silver/Gold medallion schemas seeded in PostgreSQL
  (Nexus Commerce scenario: Salesforce CRM + NetSuite ERP + Workday HR)
- /discover-medallion and /profile-medallion endpoints
- /api/catalog/tables and /api/catalog/issues endpoints with
  schema→trust mapping, rich metadata, and lineage hints
- App.jsx wired to real catalog API: catalogTables/catalogIssues
  state replaces all mock data across Dashboard, Catalog, Quality,
  Lineage, Governance, and Tasks views
- .gitignore excluding node_modules, dist, keys, and DB files
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
echo "  ✅ Done! Check: https://github.com/milanairre/dq_sense"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
