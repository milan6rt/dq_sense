# DataIQ QA Report — Inner Component State Reset
Generated: 2026-04-21 (initial baseline)

---

## What This Bug Class Is

React treats any component defined *inside* a parent function as a **new component type on every
parent re-render**. This causes React to unmount the old component and mount a new one, which:

- Resets all local `useState` values (active tab, filters, form inputs)
- Resets scroll position to the top
- Cancels any in-progress `useEffect` cleanups

In DataIQ, `DataIQApp` re-renders frequently because of:
- **WebSocket updates** — `agentLogs`, `liveAgents`, `liveTasks` (up to several times/second)
- **Polling** — `backendOnline` checked every 5 seconds
- **Data loads** — `catalogTables`, `catalogIssues`, `realConnections` on mount

Any tab component defined *inside* `DataIQApp` is affected.

---

## Fix Patterns

### Pattern A — Move to module level (full fix)
Define the component *before* `function DataIQApp`, pass required parent state as explicit props.
Best for components with many state variables or scroll containers.

### Pattern B — Lift state to parent (targeted fix)
Keep the component inside `DataIQApp` but move its `useState` declarations up to the parent.
Inside the component, alias the parent state: `const x = parentX; const setX = setParentX`.
Best for components with 1–3 interactive state variables.

---

## Component Status Table

| Component | Inside DataIQApp? | Local State That Resets | Re-render Trigger | Status |
|---|---|---|---|---|
| `RulesTab` | ✅ Module level | — | — | ✅ Clean |
| `SchedulerTab` | ✅ Module level | — | — | ✅ Clean |
| `AgentsTab` | ✅ **Fixed 2026-04-21** (Pattern A) | scroll position | `agentLogs` WebSocket | ✅ Fixed |
| `QualityTab` | ⚠️ Inside DataIQApp | `activeSection` (Issues/Scores/Rules) | `catalogIssues` on load | ✅ Fixed 2026-04-21 (Pattern B) |
| `TasksTab` | ⚠️ Inside DataIQApp | `filter` (all/pending/done) | `liveTasks` WebSocket | ✅ Fixed 2026-04-21 (Pattern B) |
| `Dashboard` | ⚠️ Inside DataIQApp | `chartPeriod` (hours/day/week) | `backendOnline` polling | ✅ Fixed 2026-04-21 (Pattern B) |
| `CatalogTab` | ⚠️ Inside DataIQApp | filters already in parent | `catalogTables` on load | ✅ Filters safe, scroll low-risk |
| `LineageTab` | ⚠️ Inside DataIQApp | no local state | `lineageTableId` changes | ⚠️ Scroll-only risk (low) |
| `GovernanceTab` | ⚠️ Inside DataIQApp | no local state | mostly static | ⚠️ Scroll-only risk (low) |
| `ConnectionsTab` | ⚠️ Inside DataIQApp | no local state | `realConnections` on load | ⚠️ Scroll-only risk (low) |

---

## Fixes Applied (2026-04-21)

1. **AgentsTab** → moved to module level (Pattern A). Props: `backendOnline`, `liveAgents`, `agentLogs`. Fixes scroll reset caused by WebSocket log flood.
2. **QualityTab `activeSection`** → lifted to `DataIQApp` as `qualitySection`. Fixes Issues/Scores/Rules tab jumping back to Issues on re-render.
3. **TasksTab `filter`** → lifted to `DataIQApp` as `taskFilter`. Fixes task filter resetting to "All" on `liveTasks` WebSocket updates.
4. **Dashboard `chartPeriod`** → lifted to `DataIQApp`. Fixes chart period (Hours/Day/Week) resetting on `backendOnline` poll.

---

## Remaining Work

| Priority | Component | Recommended Fix | Notes |
|---|---|---|---|
| Medium | `CatalogTab` | Pattern A (module level) | Only scroll resets; filters already safe in parent |
| Low | `LineageTab` | Pattern A (module level) | Scroll-only; no local state to lose |
| Low | `GovernanceTab` | Pattern A (module level) | Mostly static; lowest priority |
| Low | `ConnectionsTab` | Pattern A (module level) | Scroll-only risk |

All four remaining tabs can be fixed in one session by moving them to module level and passing
`catalogTables`, `catalogIssues`, `realConnections`, etc. as props.

---

## Prevention Rule

**Never define a tab/page component inside `DataIQApp`.**
New components must be defined at module level (before line ~1726) and receive data as props.
This rule is enforced by the bug registry in `CLAUDE.md`.

---

## Automated QA Schedule

A scheduled task (`dataiq-component-state-qa`) runs every **Monday at 9:00 AM** to:
- Detect any new inner component definitions added since the last run
- Verify existing fixes are still in place
- Apply Pattern B fixes automatically where safe
- Update this report

