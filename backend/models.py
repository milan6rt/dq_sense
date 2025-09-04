# models.py
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Dict, Any, Optional
from datetime import datetime
from enum import Enum

class ConnectionStatus(str, Enum):
    connected = "connected"
    disconnected = "disconnected"
    connecting = "connecting"
    error = "error"

class AgentStatusEnum(str, Enum):
    active = "active"
    inactive = "inactive"
    error = "error"
    starting = "starting"
    stopping = "stopping"

class SeverityLevel(str, Enum):
    high = "high"
    medium = "medium"
    low = "low"

class AgentType(str, Enum):
    profiling = "profiling"
    validation = "validation"
    lineage = "lineage"
    monitoring = "monitoring"

# Database Connection Models
class CreateConnectionRequest(BaseModel):
    name: str = Field(..., description="Connection name")
    host: str = Field(..., description="Database host")
    port: int = Field(5432, description="Database port")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")
    connection_type: str = Field("postgresql", description="Database type")

class DatabaseConnection(BaseModel):
    id: int
    name: str
    host: str
    port: int
    database: str
    username: str
    connection_type: str
    status: ConnectionStatus
    last_sync: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)

# Table and Column Models
class ColumnInfo(BaseModel):
    id: int
    name: str
    data_type: str
    is_nullable: bool
    is_primary_key: bool = False
    is_foreign_key: bool = False
    quality_score: float = 100.0
    null_percentage: float = 0.0
    unique_percentage: float = 100.0
    sample_values: List[str] = []

class TableInfo(BaseModel):
    id: int
    name: str
    schema_name: str
    connection_id: int
    connection_name: str
    record_count: int = 0
    quality_score: float = 100.0
    last_profiled: Optional[datetime] = None
    description: Optional[str] = None
    owner: Optional[str] = None
    tags: List[str] = []
    popularity: float = 0.0
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)

class TableDetailInfo(TableInfo):
    columns: List[ColumnInfo] = []
    sample_data: List[Dict[str, Any]] = []
    profiling_stats: Dict[str, Any] = {}

# Dashboard Models
class DashboardMetrics(BaseModel):
    total_connections: int
    active_connections: int
    total_tables: int
    total_records: int
    average_quality_score: float
    active_agents: int
    total_issues: int
    critical_issues: int
    last_updated: datetime

# Agent Models
class AgentStatus(BaseModel):
    id: str
    name: str
    type: AgentType
    status: AgentStatusEnum
    last_run: Optional[datetime] = None
    tasks_completed: int = 0
    tasks_failed: int = 0
    uptime: Optional[str] = None
    error_message: Optional[str] = None

class AgentActivity(BaseModel):
    id: str
    agent_id: str
    agent_name: str
    activity: str
    status: str
    timestamp: datetime
    details: Optional[Dict[str, Any]] = None

class AgentLog(BaseModel):
    id: str
    agent_id: str
    level: str
    message: str
    timestamp: datetime
    details: Optional[Dict[str, Any]] = None

# Data Quality Models
class QualityIssue(BaseModel):
    id: int
    table_id: int
    table_name: str
    issue_type: str
    severity: SeverityLevel
    description: str
    affected_records: int
    detected_at: datetime
    resolved_at: Optional[datetime] = None
    is_resolved: bool = False
    rule_id: Optional[int] = None

class QualityRule(BaseModel):
    id: int
    name: str
    description: str
    rule_type: str
    configuration: Dict[str, Any]
    is_active: bool = True
    tables_applied: List[int] = []
    created_at: datetime
    updated_at: datetime

class QualityOverview(BaseModel):
    overall_score: float
    total_issues: int
    critical_issues: int
    warning_issues: int
    info_issues: int
    trend_direction: str  # "up", "down", "stable"
    trend_percentage: float

# Data Lineage Models
class LineageNode(BaseModel):
    id: str
    name: str
    type: str  # "table", "view", "procedure"
    schema_name: str
    connection_id: int
    x: float = 0.0
    y: float = 0.0

class LineageEdge(BaseModel):
    source: str
    target: str
    relationship_type: str = "depends_on"

class LineageGraph(BaseModel):
    nodes: List[LineageNode]
    edges: List[LineageEdge]
    metadata: Dict[str, Any] = {}

# Data Governance Models
class DataClassification(BaseModel):
    level: str
    name: str
    description: str
    color: str
    table_count: int

class GovernancePolicy(BaseModel):
    id: int
    name: str
    description: str
    policy_type: str
    rules: Dict[str, Any]
    is_active: bool = True
    tables_applied: List[int] = []
    created_at: datetime
    updated_at: datetime

class CreatePolicyRequest(BaseModel):
    name: str = Field(..., description="Policy name")
    description: str = Field(..., description="Policy description")
    policy_type: str = Field(..., description="Policy type")
    rules: Dict[str, Any] = Field(..., description="Policy rules configuration")
    tables_applied: List[int] = Field([], description="Tables to apply policy to")

class PolicyViolation(BaseModel):
    id: int
    policy_id: int
    policy_name: str
    table_id: int
    table_name: str
    violation_type: str
    description: str
    severity: SeverityLevel
    detected_at: datetime
    resolved_at: Optional[datetime] = None
    is_resolved: bool = False

class GovernanceOverview(BaseModel):
    active_policies: int
    compliance_score: float
    policy_violations: int
    data_classifications: List[DataClassification]

# Task and Job Models
class ProfilingTask(BaseModel):
    id: str
    table_id: int
    status: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    results: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

class ValidationTask(BaseModel):
    id: str
    table_id: int
    rule_id: int
    status: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    issues_found: int = 0
    results: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

# API Response Models
class ApiResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Any] = None
    timestamp: datetime = Field(default_factory=datetime.now)

class PaginatedResponse(BaseModel):
    items: List[Any]
    total: int
    page: int
    size: int
    pages: int

# Search and Filter Models
class SearchFilters(BaseModel):
    query: Optional[str] = None
    connection_id: Optional[int] = None
    schema_name: Optional[str] = None
    tags: List[str] = []
    quality_min: Optional[float] = None
    quality_max: Optional[float] = None
    last_profiled_after: Optional[datetime] = None

class SortOptions(BaseModel):
    field: str = "name"
    direction: str = "asc"  # "asc" or "desc"