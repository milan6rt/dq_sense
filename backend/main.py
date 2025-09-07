# main.py
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import uvicorn

from database import DatabaseManager, get_db, init_db
from agents import AgentOrchestrator
from models import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="DataIQ Platform API",
    description="Multi-Agent Data Quality Platform Backend",
    version="1.0.0"
)

# CORS middleware for frontend communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000", "http://localhost:3001", "http://127.0.0.1:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global agent orchestrator
agent_orchestrator = AgentOrchestrator()

@app.on_event("startup")
async def startup_event():
    """Initialize the application on startup"""
    logger.info("Starting DataIQ Platform API...")
    await init_db()
    await agent_orchestrator.start_agents()
    logger.info("All systems initialized successfully")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Shutting down DataIQ Platform API...")
    await agent_orchestrator.stop_agents()

# Root endpoint
@app.get("/")
async def root():
    return {
        "message": "DataIQ Platform API", 
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
        "health": "/health"
    }

# Health Check
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

# Dashboard Endpoints
@app.get("/api/dashboard/metrics", response_model=DashboardMetrics)
async def get_dashboard_metrics(db: DatabaseManager = Depends(get_db)):
    """Get dashboard metrics and statistics"""
    try:
        connections = await db.get_connections()
        tables = await db.get_all_tables()
        agents = agent_orchestrator.get_agent_status()
        issues = await db.get_quality_issues()
        
        # Calculate metrics
        active_connections = len([c for c in connections if c.get('status') == "connected"])
        total_records = sum(table.get('record_count', 0) if isinstance(table, dict) else table.record_count for table in tables)
        avg_quality = sum(table.get('quality_score', 0) if isinstance(table, dict) else table.quality_score for table in tables) / len(tables) if tables else 0
        active_agents = len([a for a in agents if (a.get('status') if isinstance(a, dict) else a.status) == "active"])
        critical_issues = len([i for i in issues if (i.get('severity') if isinstance(i, dict) else i.severity) == "high"])
        
        return DashboardMetrics(
            total_connections=len(connections),
            active_connections=active_connections,
            total_tables=len(tables),
            total_records=total_records,
            average_quality_score=round(avg_quality, 1),
            active_agents=active_agents,
            total_issues=len(issues),
            critical_issues=critical_issues,
            last_updated=datetime.now()
        )
    except Exception as e:
        logger.error(f"Error fetching dashboard metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch dashboard metrics")

@app.get("/api/dashboard/agent-activity")
async def get_agent_activity():
    """Get recent agent activity logs"""
    try:
        return agent_orchestrator.get_recent_activity(limit=20)
    except Exception as e:
        logger.error(f"Error fetching agent activity: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch agent activity")

# Connection Management Endpoints
@app.get("/api/connections", response_model=List[DatabaseConnection])
async def get_connections(db: DatabaseManager = Depends(get_db)):
    """Get all database connections"""
    try:
        return await db.get_connections()
    except Exception as e:
        logger.error(f"Error fetching connections: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch connections")

@app.post("/api/connections", response_model=DatabaseConnection)
async def create_connection(connection: CreateConnectionRequest, db: DatabaseManager = Depends(get_db)):
    """Create a new database connection"""
    try:
        # Test connection first
        test_result = await db.test_connection(connection)
        if not test_result:
            raise HTTPException(status_code=400, detail="Connection test failed")
        
        # Create connection
        new_connection = await db.create_connection(connection)
        
        # Start profiling for the new connection
        await agent_orchestrator.start_connection_profiling(new_connection.id)
        
        return new_connection
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating connection: {e}")
        raise HTTPException(status_code=500, detail="Failed to create connection")

@app.post("/api/connections/{connection_id}/test")
async def test_connection(connection_id: int, db: DatabaseManager = Depends(get_db)):
    """Test a database connection"""
    try:
        connection = await db.get_connection(connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")
        
        result = await db.test_connection_by_id(connection_id)
        return {"success": result, "timestamp": datetime.now().isoformat()}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error testing connection: {e}")
        raise HTTPException(status_code=500, detail="Failed to test connection")

@app.post("/api/connections/{connection_id}/disconnect")
async def disconnect_connection(connection_id: int, db: DatabaseManager = Depends(get_db)):
    """Disconnect a database connection"""
    try:
        connection = await db.get_connection(connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")
        
        success = await db.disconnect_connection(connection_id)
        if success:
            return {"message": "Connection disconnected successfully", "connection_id": connection_id}
        else:
            raise HTTPException(status_code=500, detail="Failed to disconnect connection")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error disconnecting connection: {e}")
        raise HTTPException(status_code=500, detail="Failed to disconnect connection")

@app.post("/api/connections/{connection_id}/refresh")
async def refresh_connection(connection_id: int, db: DatabaseManager = Depends(get_db)):
    """Refresh a database connection (reconnect and update status)"""
    try:
        connection = await db.get_connection(connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")
        
        result = await db.refresh_connection(connection_id)
        
        # Trigger lineage discovery after successful connection
        if result["status"] == "connected":
            try:
                await db.discover_lineage_relationships(connection_id)
                logger.info(f"Lineage discovery completed for connection {connection_id}")
            except Exception as lineage_error:
                logger.warning(f"Lineage discovery failed for connection {connection_id}: {lineage_error}")
                # Don't fail the refresh if lineage discovery fails
        
        return {
            "message": "Connection refreshed successfully", 
            "connection_id": connection_id,
            "status": result["status"],
            "timestamp": datetime.now().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error refreshing connection: {e}")
        raise HTTPException(status_code=500, detail="Failed to refresh connection")

@app.get("/api/connections/status")
async def get_connection_status(db: DatabaseManager = Depends(get_db)):
    """Get summary of connection statuses for real-time updates"""
    try:
        connections = await db.get_connections()
        connected_count = len([c for c in connections if c.get('status') == 'connected'])
        disconnected_count = len([c for c in connections if c.get('status') == 'disconnected'])
        
        return {
            "total_connections": len(connections),
            "connected": connected_count,
            "disconnected": disconnected_count,
            "has_connected_db": connected_count > 0,
            "timestamp": datetime.now().isoformat(),
            "connections": connections
        }
    except Exception as e:
        logger.error(f"Error fetching connection status: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch connection status")

# Data Catalog Endpoints
@app.get("/api/tables", response_model=List[TableInfo])
async def get_tables(
    connection_id: Optional[int] = None,
    schema_name: Optional[str] = None,
    search: Optional[str] = None,
    db: DatabaseManager = Depends(get_db)
):
    """Get all tables from connected databases (used by frontend)"""
    try:
        tables = await db.get_all_tables(
            connection_id=connection_id,
            schema_name=schema_name,
            search=search
        )
        return tables
    except Exception as e:
        logger.error(f"Error fetching tables: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch tables")

@app.get("/api/catalog/tables", response_model=List[TableInfo])
async def get_catalog_tables(
    connection_id: Optional[int] = None,
    schema_name: Optional[str] = None,
    search: Optional[str] = None,
    db: DatabaseManager = Depends(get_db)
):
    """Get all tables in the data catalog with optional filtering"""
    try:
        tables = await db.get_all_tables(
            connection_id=connection_id,
            schema_name=schema_name,
            search=search
        )
        return tables
    except Exception as e:
        logger.error(f"Error fetching catalog tables: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch catalog tables")

@app.get("/api/catalog/tables/{table_id}", response_model=TableDetailInfo)
async def get_table_details(table_id: int, db: DatabaseManager = Depends(get_db)):
    """Get detailed information about a specific table"""
    try:
        table = await db.get_table_details(table_id)
        if not table:
            raise HTTPException(status_code=404, detail="Table not found")
        return table
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching table details: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch table details")

@app.get("/api/catalog/tables/{table_id}/columns", response_model=List[ColumnInfo])
async def get_table_columns(table_id: int, db: DatabaseManager = Depends(get_db)):
    """Get columns for a specific table"""
    try:
        columns = await db.get_table_columns(table_id)
        return columns
    except Exception as e:
        logger.error(f"Error fetching table columns: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch table columns")

@app.post("/api/catalog/tables/{table_id}/profile")
async def profile_table(table_id: int, background_tasks: BackgroundTasks, db: DatabaseManager = Depends(get_db)):
    """Trigger table profiling"""
    try:
        table = await db.get_table_details(table_id)
        if not table:
            raise HTTPException(status_code=404, detail="Table not found")
        
        # Queue profiling task
        background_tasks.add_task(agent_orchestrator.profile_table, table_id)
        
        return {"message": "Table profiling queued", "table_id": table_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error queuing table profiling: {e}")
        raise HTTPException(status_code=500, detail="Failed to queue table profiling")

# Data Quality Endpoints
@app.get("/api/quality/overview", response_model=QualityOverview)
async def get_quality_overview(db: DatabaseManager = Depends(get_db)):
    """Get data quality overview metrics"""
    try:
        tables = await db.get_all_tables()
        issues = await db.get_quality_issues()
        
        if not tables:
            overall_score = 0
        else:
            overall_score = sum(table.quality_score for table in tables) / len(tables)
        
        # Count issues by severity
        critical_count = len([i for i in issues if i.severity == "high"])
        warning_count = len([i for i in issues if i.severity == "medium"])
        info_count = len([i for i in issues if i.severity == "low"])
        
        return QualityOverview(
            overall_score=round(overall_score, 1),
            total_issues=len(issues),
            critical_issues=critical_count,
            warning_issues=warning_count,
            info_issues=info_count,
            trend_direction="up",  # Would calculate based on historical data
            trend_percentage=2.1
        )
    except Exception as e:
        logger.error(f"Error fetching quality overview: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch quality overview")

@app.get("/api/quality/issues", response_model=List[QualityIssue])
async def get_quality_issues(
    severity: Optional[str] = None,
    table_id: Optional[int] = None,
    db: DatabaseManager = Depends(get_db)
):
    """Get data quality issues with optional filtering"""
    try:
        issues = await db.get_quality_issues(severity=severity, table_id=table_id)
        return issues
    except Exception as e:
        logger.error(f"Error fetching quality issues: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch quality issues")

@app.post("/api/quality/issues/{issue_id}/resolve")
async def resolve_quality_issue(issue_id: int, db: DatabaseManager = Depends(get_db)):
    """Mark a quality issue as resolved"""
    try:
        await db.resolve_quality_issue(issue_id)
        return {"message": "Quality issue resolved", "issue_id": issue_id}
    except Exception as e:
        logger.error(f"Error resolving quality issue: {e}")
        raise HTTPException(status_code=500, detail="Failed to resolve quality issue")

@app.post("/api/quality/run-check")
async def run_quality_check(
    table_id: Optional[int] = None,
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Run data quality checks"""
    try:
        if table_id:
            background_tasks.add_task(agent_orchestrator.run_quality_check, table_id)
            message = f"Quality check queued for table {table_id}"
        else:
            background_tasks.add_task(agent_orchestrator.run_all_quality_checks)
            message = "Quality checks queued for all tables"
        
        return {"message": message}
    except Exception as e:
        logger.error(f"Error running quality check: {e}")
        raise HTTPException(status_code=500, detail="Failed to run quality check")

# Data Lineage Endpoints
@app.get("/api/lineage/graph")
async def get_lineage_graph(
    table_id: Optional[int] = None,
    db: DatabaseManager = Depends(get_db)
):
    """Get data lineage graph"""
    try:
        if table_id:
            # Get lineage for specific table
            lineage = await db.get_table_lineage(table_id)
        else:
            # Get overall lineage graph
            lineage = await db.get_overall_lineage()
        
        return lineage
    except Exception as e:
        logger.error(f"Error fetching lineage graph: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch lineage graph")

@app.get("/api/lineage/impact/{table_id}")
async def get_impact_analysis(table_id: int, db: DatabaseManager = Depends(get_db)):
    """Get impact analysis for a table"""
    try:
        upstream = await db.get_upstream_tables(table_id)
        downstream = await db.get_downstream_tables(table_id)
        
        return {
            "table_id": table_id,
            "upstream_tables": upstream,
            "downstream_tables": downstream,
            "total_impacted": len(upstream) + len(downstream)
        }
    except Exception as e:
        logger.error(f"Error fetching impact analysis: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch impact analysis")

@app.post("/api/lineage/discover/{connection_id}")
async def discover_lineage(connection_id: int, db: DatabaseManager = Depends(get_db)):
    """Manually trigger lineage discovery for a connection"""
    try:
        connection = await db.get_connection(connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")
        
        if connection.status.value != "connected":
            raise HTTPException(status_code=400, detail="Connection must be active to discover lineage")
        
        relationships = await db.discover_lineage_relationships(connection_id)
        
        return {
            "message": "Lineage discovery completed",
            "connection_id": connection_id,
            "relationships_found": len(relationships),
            "timestamp": datetime.now().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error discovering lineage: {e}")
        raise HTTPException(status_code=500, detail="Failed to discover lineage relationships")

# AI Agents Management Endpoints
@app.get("/api/agents", response_model=List[AgentStatus])
async def get_agents_status():
    """Get status of all AI agents"""
    try:
        return agent_orchestrator.get_agent_status()
    except Exception as e:
        logger.error(f"Error fetching agent status: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch agent status")

@app.post("/api/agents/{agent_id}/start")
async def start_agent(agent_id: str):
    """Start a specific agent"""
    try:
        success = await agent_orchestrator.start_agent(agent_id)
        if not success:
            raise HTTPException(status_code=404, detail="Agent not found")
        
        return {"message": f"Agent {agent_id} started successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting agent: {e}")
        raise HTTPException(status_code=500, detail="Failed to start agent")

@app.post("/api/agents/{agent_id}/stop")
async def stop_agent(agent_id: str):
    """Stop a specific agent"""
    try:
        success = await agent_orchestrator.stop_agent(agent_id)
        if not success:
            raise HTTPException(status_code=404, detail="Agent not found")
        
        return {"message": f"Agent {agent_id} stopped successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping agent: {e}")
        raise HTTPException(status_code=500, detail="Failed to stop agent")

@app.get("/api/agents/logs")
async def get_agent_logs(limit: int = 100):
    """Get agent execution logs"""
    try:
        return agent_orchestrator.get_execution_logs(limit=limit)
    except Exception as e:
        logger.error(f"Error fetching agent logs: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch agent logs")

# Data Governance Endpoints
@app.get("/api/governance/overview", response_model=GovernanceOverview)
async def get_governance_overview(db: DatabaseManager = Depends(get_db)):
    """Get data governance overview"""
    try:
        policies = await db.get_active_policies()
        classifications = await db.get_data_classifications()
        violations = await db.get_policy_violations()
        
        # Calculate compliance score
        total_tables = await db.get_table_count()
        compliant_tables = total_tables - len(violations)
        compliance_score = (compliant_tables / total_tables * 100) if total_tables > 0 else 100
        
        return GovernanceOverview(
            active_policies=len(policies),
            compliance_score=round(compliance_score, 1),
            policy_violations=len(violations),
            data_classifications=classifications
        )
    except Exception as e:
        logger.error(f"Error fetching governance overview: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch governance overview")

@app.get("/api/governance/policies", response_model=List[GovernancePolicy])
async def get_governance_policies(db: DatabaseManager = Depends(get_db)):
    """Get all governance policies"""
    try:
        return await db.get_active_policies()
    except Exception as e:
        logger.error(f"Error fetching governance policies: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch governance policies")

@app.post("/api/governance/policies", response_model=GovernancePolicy)
async def create_governance_policy(policy: CreatePolicyRequest, db: DatabaseManager = Depends(get_db)):
    """Create a new governance policy"""
    try:
        new_policy = await db.create_policy(policy)
        return new_policy
    except Exception as e:
        logger.error(f"Error creating governance policy: {e}")
        raise HTTPException(status_code=500, detail="Failed to create governance policy")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )