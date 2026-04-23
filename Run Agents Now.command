#!/bin/bash
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  DataIQ — Starting & Running All Agents"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

AGENTS=("data_profiler" "quality_validator" "lineage_tracker" "anomaly_detector")
BASE="http://localhost:8000"
TOKEN="demo"

# Step 1: Start all agents
echo "▶ Starting agents..."
for agent in "${AGENTS[@]}"; do
  result=$(curl -s -X POST "$BASE/api/agents/$agent/start" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json")
  echo "  $agent: $result"
done

echo ""
sleep 1

# Step 2: Trigger immediate run on each agent
echo "▶ Triggering runs (GPT-4o)..."
for agent in "${AGENTS[@]}"; do
  result=$(curl -s -X POST "$BASE/api/agents/$agent/run" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{}')
  echo "  $agent: $result"
done

echo ""
echo "✅ All agents started and running!"
echo "   Switch to the AI Agents tab in the app to see live insights."
echo ""
echo "   Press any key to close..."
read -n 1
