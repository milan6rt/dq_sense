#!/bin/bash
cd "$(dirname "$0")/backend"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  DataIQ Platform — Backend API (port 8000)        "
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Install Python dependencies using the same python3 that will run uvicorn
echo "📦 Installing Python dependencies..."
python3 -m pip install -r requirements.txt -q --break-system-packages 2>/dev/null \
  || python3 -m pip install -r requirements.txt -q

echo ""
echo "🚀 Starting backend on http://localhost:8000"
echo "   Press Ctrl+C to stop"
echo ""

python3 -m uvicorn main:app --reload --port 8000
