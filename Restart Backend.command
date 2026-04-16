#!/bin/bash
cd "$(dirname "$0")/backend"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  DataIQ — Restarting backend (fresh start)        "
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Kill existing uvicorn/backend on port 8000
echo "🛑 Stopping old backend..."
lsof -ti:8000 | xargs kill -9 2>/dev/null || true
sleep 2

# Remove old database so fresh schema is created
echo "🗑️  Clearing old database..."
rm -f ~/dataiq.db

# Install dependencies — use python3 -m pip to ensure correct interpreter
echo "📦 Installing Python dependencies..."
python3 -m pip install httpx psycopg2-binary cryptography python-jose passlib \
    fastapi uvicorn sqlalchemy apscheduler python-multipart aiofiles \
    python-dotenv websockets pydantic simple-salesforce --quiet 2>&1 | tail -5

# Install remaining requirements (skip pyodbc if ODBC driver not installed)
python3 -m pip install -r requirements.txt --quiet --ignore-requires-python 2>&1 | grep -v "pyodbc" | tail -10 || true

echo ""
echo "🚀 Starting fresh backend on http://localhost:8000"
echo ""

# Use python3 -m uvicorn so it uses the same interpreter as pip
python3 -m uvicorn main:app --reload --port 8000
