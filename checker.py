#!/usr/bin/env python3
"""
DataIQ Checker Agent
====================
Autonomous quality-assurance script for the DataIQ platform.
Tests: API correctness, backend health, rule execution, performance, and basic security.
Output: checker_report.md in the same directory as this script.

Run manually:  python3 checker.py
Scheduled:     set via Cowork schedule skill (runs on an interval)
"""

import json
import time
import sys
import os
import traceback
from datetime import datetime
from urllib import request, error as url_error

BASE = "http://localhost:8000"
FRONTEND = "http://localhost:3000"
REPORT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "checker_report.md")

# ── Helpers ────────────────────────────────────────────────────────────────────

class Result:
    def __init__(self, name, status, detail="", latency_ms=None):
        self.name = name
        self.status = status        # "pass" | "warn" | "fail"
        self.detail = detail
        self.latency_ms = latency_ms

    def emoji(self):
        return {"pass": "✅", "warn": "⚠️", "fail": "❌"}.get(self.status, "❓")

results: list[Result] = []

def check(name, status, detail="", latency_ms=None):
    r = Result(name, status, detail, latency_ms)
    results.append(r)
    icon = r.emoji()
    lat = f"  ({latency_ms:.0f}ms)" if latency_ms is not None else ""
    print(f"  {icon} {name}{lat}")
    if detail:
        print(f"       {detail}")
    return r

def api_get(path, timeout=10):
    """GET request → (data, status_code, latency_ms). Returns (None, error_str, None) on failure."""
    url = BASE + path
    t0 = time.time()
    try:
        req = request.Request(url, headers={"Authorization": "Bearer demo"})
        with request.urlopen(req, timeout=timeout) as resp:
            latency = (time.time() - t0) * 1000
            body = resp.read().decode()
            return json.loads(body), resp.status, latency
    except url_error.HTTPError as e:
        latency = (time.time() - t0) * 1000
        return None, e.code, latency
    except Exception as e:
        return None, str(e), None

def api_post(path, body=None, timeout=10):
    url = BASE + path
    t0 = time.time()
    data = json.dumps(body or {}).encode()
    try:
        req = request.Request(url, data=data, method="POST",
                              headers={"Authorization": "Bearer demo",
                                       "Content-Type": "application/json"})
        with request.urlopen(req, timeout=timeout) as resp:
            latency = (time.time() - t0) * 1000
            body_resp = resp.read().decode()
            return json.loads(body_resp), resp.status, latency
    except url_error.HTTPError as e:
        latency = (time.time() - t0) * 1000
        try:
            err_body = e.read().decode()
        except Exception:
            err_body = str(e)
        return None, f"HTTP {e.code}: {err_body[:200]}", latency
    except Exception as e:
        return None, str(e), None


# ── Test sections ──────────────────────────────────────────────────────────────

def section(title):
    print(f"\n{'─'*55}")
    print(f"  {title}")
    print(f"{'─'*55}")


def test_health():
    section("1. BACKEND HEALTH")
    data, code, lat = api_get("/health")
    if isinstance(code, int) and code == 200 and isinstance(data, dict) and data.get("status") == "ok":
        check("Backend reachable", "pass", f"status=ok", lat)
    else:
        check("Backend reachable", "fail", f"Got: code={code}, data={data}", lat)
        print("\n  ⛔  Backend is down — remaining tests skipped.")
        return False

    # Performance threshold
    if lat and lat > 500:
        check("Health latency", "warn", f"{lat:.0f}ms > 500ms threshold")
    else:
        check("Health latency", "pass", f"{lat:.0f}ms")
    return True


def test_connections():
    section("2. CONNECTIONS API")
    data, code, lat = api_get("/api/connections")
    if not isinstance(data, list):
        check("List connections", "fail", f"Expected list, got: code={code}", lat)
        return []
    check("List connections", "pass", f"{len(data)} connection(s)", lat)
    if lat and lat > 1000:
        check("List connections latency", "warn", f"{lat:.0f}ms > 1000ms")

    # Check each connection has required fields
    required_fields = {"id", "name", "connector_type", "status"}
    for conn in data:
        missing = required_fields - set(conn.keys())
        if missing:
            check(f"Connection '{conn.get('name','?')}' schema", "warn",
                  f"Missing fields: {missing}")
        else:
            check(f"Connection '{conn.get('name','?')}' schema", "pass",
                  f"type={conn['connector_type']}, status={conn['status']}")

    return data


def test_tables(connections):
    section("3. TABLES API")
    all_tables = []
    for conn in connections:
        cid = conn["id"]
        data, code, lat = api_get(f"/api/connections/{cid}/tables")
        if not isinstance(data, list):
            check(f"Tables for '{conn['name']}'", "fail", f"code={code}", lat)
            continue
        check(f"Tables for '{conn['name']}'", "pass", f"{len(data)} table(s)", lat)
        if lat and lat > 1000:
            check(f"Tables latency '{conn['name']}'", "warn", f"{lat:.0f}ms")
        all_tables.extend(data)

    if not all_tables:
        check("Tables available", "warn", "No tables discovered — profile a connection first")
    return all_tables


def test_columns(tables, connections):
    section("4. COLUMNS API")
    # Test first 3 tables to keep runtime reasonable
    tested = 0
    for tbl in tables[:3]:
        cid = tbl.get("connection_id")
        tid = tbl.get("id")
        name = tbl.get("full_name") or tbl.get("table_name", "?")
        if not cid or not tid:
            check(f"Columns for '{name}'", "warn", "Missing connection_id or id on table object")
            continue
        data, code, lat = api_get(f"/api/connections/{cid}/tables/{tid}/columns")
        if not isinstance(data, list):
            check(f"Columns for '{name}'", "fail", f"code={code}, data={data}", lat)
        else:
            check(f"Columns for '{name}'", "pass", f"{len(data)} column(s)", lat)
            if lat and lat > 1500:
                check(f"Columns latency '{name}'", "warn", f"{lat:.0f}ms")
        tested += 1

    if tested == 0:
        check("Columns API", "warn", "No tables to test against")


def test_rules():
    section("5. DQ RULES API")
    data, code, lat = api_get("/api/rules/")
    if not isinstance(data, list):
        check("List rules", "fail", f"code={code}", lat)
        return []
    check("List rules", "pass", f"{len(data)} rule(s)", lat)

    # Templates endpoint
    templates, tcode, tlat = api_get("/api/rules/templates")
    if isinstance(templates, list) and len(templates) > 0:
        check("Rule templates", "pass", f"{len(templates)} template(s)", tlat)
    else:
        check("Rule templates", "warn", f"code={tcode}, data={templates}")

    # Run each rule and verify response shape
    for rule in data:
        rid = rule.get("id")
        rname = rule.get("name", "?")
        rdata, rcode, rlat = api_post(f"/api/rules/{rid}/run")
        if rdata and rdata.get("status") in ("pass", "fail", "error"):
            status_val = rdata["status"]
            if status_val == "error":
                msg = rdata.get("message", "")
                # Distinguish "missing column" (user config issue) vs real error
                if "requires a column" in msg:
                    check(f"Rule '{rname}' run", "warn",
                          f"Needs column configured: {msg[:100]}", rlat)
                else:
                    check(f"Rule '{rname}' run", "fail", f"error: {msg[:120]}", rlat)
            else:
                check(f"Rule '{rname}' run", "pass",
                      f"status={status_val}, failing_rows={rdata.get('failing_rows',0)}", rlat)
        else:
            check(f"Rule '{rname}' run", "fail",
                  f"Bad response: code={rcode}, data={str(rdata)[:100]}")

    return data


def test_scheduler():
    section("6. SCHEDULER API")
    data, code, lat = api_get("/api/scheduler/scans")
    if isinstance(data, list):
        check("List scheduled scans", "pass", f"{len(data)} scan(s)", lat)
    else:
        check("List scheduled scans", "fail", f"code={code}", lat)


def test_security():
    section("7. SECURITY CHECKS")

    # 1. Auth bypass: access protected endpoint without token
    url = BASE + "/api/connections"
    t0 = time.time()
    try:
        req = request.Request(url)   # no Authorization header
        with request.urlopen(req, timeout=5) as resp:
            lat = (time.time() - t0) * 1000
            body = resp.read().decode()
            # If we got 200 without a token, that's only OK in demo mode
            check("Unauthenticated access", "warn",
                  f"Returned {resp.status} without auth token — OK in demo mode, "
                  f"but MUST require auth in production", lat)
    except url_error.HTTPError as e:
        lat = (time.time() - t0) * 1000
        if e.code in (401, 403):
            check("Unauthenticated access", "pass",
                  f"Correctly blocked with {e.code}", lat)
        else:
            check("Unauthenticated access", "warn", f"Unexpected HTTP {e.code}", lat)
    except Exception as e:
        check("Unauthenticated access", "warn", str(e))

    # 2. Non-existent resource → should return 404, not 500
    data, code, lat = api_get("/api/connections/non-existent-id-00000/tables")
    if code == 404:
        check("404 for missing resource", "pass", "Correctly returns 404", lat)
    elif code == 200 and data == []:
        check("404 for missing resource", "warn",
              "Returns 200 [] instead of 404 for unknown connection ID", lat)
    elif isinstance(code, int) and code >= 500:
        check("404 for missing resource", "fail",
              f"Returns {code} (server error) for unknown ID — leaks internals", lat)
    else:
        check("404 for missing resource", "warn", f"code={code}", lat)

    # 3. SQL injection probe in path param (should not 500)
    probe = "%27%20OR%201%3D1%20--"  # URL-encoded ' OR 1=1 --
    data, code, lat = api_get(f"/api/connections/{probe}/tables")
    if isinstance(code, int) and code < 500:
        check("SQL injection probe (path param)", "pass",
              f"Handled gracefully with {code}", lat)
    else:
        check("SQL injection probe (path param)", "fail",
              f"Server error {code} on injection probe — investigate", lat)

    # 4. Check that error details aren't overly verbose in responses
    data, code, lat = api_get("/api/connections/badid-xyz/tables")
    if isinstance(data, dict):
        detail = data.get("detail", "") or ""
        if "traceback" in detail.lower() or "file " in detail.lower():
            check("Error response verbosity", "fail",
                  "Response contains Python traceback — remove before production")
        else:
            check("Error response verbosity", "pass", "No traceback in error response")


def test_performance():
    section("8. PERFORMANCE SWEEP")
    endpoints = [
        ("/health", 200),
        ("/api/connections", 1000),
        ("/api/rules/", 1000),
        ("/api/rules/templates", 1000),
        ("/api/scheduler/scans", 1000),
    ]
    for path, threshold_ms in endpoints:
        data, code, lat = api_get(path)
        if lat is None:
            check(f"Perf: {path}", "fail", "No response (connection error)")
            continue
        if lat > threshold_ms:
            check(f"Perf: {path}", "warn", f"{lat:.0f}ms > {threshold_ms}ms threshold", lat)
        else:
            check(f"Perf: {path}", "pass", f"{lat:.0f}ms ✓", lat)


def test_frontend():
    section("9. FRONTEND REACHABILITY")
    try:
        req = request.Request(FRONTEND)
        t0 = time.time()
        with request.urlopen(req, timeout=5) as resp:
            lat = (time.time() - t0) * 1000
            body = resp.read().decode()
            if "DataIQ" in body or "data-quality" in body.lower() or "<div id" in body:
                check("Frontend serving HTML", "pass", f"HTTP {resp.status}", lat)
            else:
                check("Frontend serving HTML", "warn",
                      f"HTTP {resp.status} but unexpected content", lat)
    except Exception as e:
        check("Frontend serving HTML", "fail", f"Cannot reach {FRONTEND}: {e}")


# ── Report writer ──────────────────────────────────────────────────────────────

def write_report():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    passes  = sum(1 for r in results if r.status == "pass")
    warns   = sum(1 for r in results if r.status == "warn")
    fails   = sum(1 for r in results if r.status == "fail")
    total   = len(results)
    score   = int(100 * passes / total) if total else 0

    lines = [
        f"# DataIQ Checker Report",
        f"",
        f"**Run at:** {now}  ",
        f"**Score:** {score}% ({passes} pass / {warns} warn / {fails} fail out of {total} checks)",
        f"",
        f"---",
        f"",
    ]

    if fails > 0:
        lines += ["## ❌ Failures (action required)", ""]
        for r in results:
            if r.status == "fail":
                lat = f" — {r.latency_ms:.0f}ms" if r.latency_ms else ""
                lines.append(f"- **{r.name}**{lat}: {r.detail}")
        lines.append("")

    if warns > 0:
        lines += ["## ⚠️ Warnings (review recommended)", ""]
        for r in results:
            if r.status == "warn":
                lat = f" — {r.latency_ms:.0f}ms" if r.latency_ms else ""
                lines.append(f"- **{r.name}**{lat}: {r.detail}")
        lines.append("")

    lines += ["## ✅ Passing checks", ""]
    for r in results:
        if r.status == "pass":
            lat = f" — {r.latency_ms:.0f}ms" if r.latency_ms else ""
            lines.append(f"- {r.name}{lat}")
    lines.append("")
    lines += ["---", f"*Generated by DataIQ Checker Agent*"]

    report = "\n".join(lines)
    with open(REPORT_PATH, "w") as f:
        f.write(report)
    return report, score, passes, warns, fails


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "═"*55)
    print("  DataIQ Checker Agent")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═"*55)

    backend_ok = test_health()
    if not backend_ok:
        write_report()
        sys.exit(1)

    connections = test_connections()
    tables = test_tables(connections) if connections else []
    test_columns(tables, connections)
    test_rules()
    test_scheduler()
    test_security()
    test_performance()
    test_frontend()

    print("\n" + "═"*55)
    report, score, passes, warns, fails = write_report()
    print(f"  Score: {score}%  ({passes} ✅  {warns} ⚠️  {fails} ❌)")
    print(f"  Report saved → {REPORT_PATH}")
    print("═"*55 + "\n")

    sys.exit(0 if fails == 0 else 1)


if __name__ == "__main__":
    main()
