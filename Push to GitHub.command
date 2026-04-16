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
Fix DQ Rules: edit flow, column dropdown, columns API endpoint

- RulesTab: full edit-rule flow with pre-populated form (BLANK_FORM
  constant, editingId state, openEdit/openCreate/closeForm helpers,
  PUT /api/rules/:id for updates vs POST for create)
- RulesTab: column field becomes a dropdown when table is selected;
  fetches /api/connections/:conn/tables/:table/columns on table change;
  shows column count badge e.g. "Column (18 available)"
- Edit pencil button added to each rule card
- connections.py: added GET /api/connections/{conn}/tables/{table}/columns
  endpoint; fixed AttributeError (model field is distinct_count, not
  unique_count) that caused uvicorn worker to drop the connection
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
