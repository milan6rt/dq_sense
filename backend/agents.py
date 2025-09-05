# agents.py
import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import json
import uuid
from abc import ABC, abstractmethod
import random

# Simple enum classes since models.py might not have these
class AgentStatusEnum:
    inactive = "inactive"
    starting = "starting"
    active = "active"
    stopping = "stopping"
    error = "error"

class AgentType:
    profiling = "profiling"
    validation = "validation"
    monitoring = "monitoring"
    lineage = "lineage"

class AgentStatus:
    def __init__(self, id, name, type, status, last_run=None, tasks_completed=0, tasks_failed=0, uptime=None, error_message=None):
        self.id = id
        self.name = name
        self.type = type
        self.status = status
        self.last_run = last_run
        self.tasks_completed = tasks_completed
        self.tasks_failed = tasks_failed
        self.uptime = uptime
        self.error_message = error_message

class AgentActivity:
    def __init__(self, id, agent_id, agent_name, activity, status, timestamp, details=None):
        self.id = id
        self.agent_id = agent_id
        self.agent_name = agent_name
        self.activity = activity
        self.status = status
        self.timestamp = timestamp
        self.details = details

class AgentLog:
    def __init__(self, id, agent_id, level, message, timestamp, details=None):
        self.id = id
        self.agent_id = agent_id
        self.level = level
        self.message = message
        self.timestamp = timestamp
        self.details = details

logger = logging.getLogger(__name__)

class BaseAgent(ABC):
    """Base class for all AI agents"""
    
    def __init__(self, agent_id: str, name: str, agent_type: str):
        self.agent_id = agent_id
        self.name = name
        self.agent_type = agent_type
        self.status = AgentStatusEnum.inactive
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.last_run = None
        self.start_time = None
        self.error_message = None
        self.is_running = False
        self.activity_log = []
        self.execution_log = []
    
    async def start(self):
        """Start the agent"""
        try:
            self.status = AgentStatusEnum.starting
            self.start_time = datetime.now()
            await self._initialize()
            self.status = AgentStatusEnum.active
            self.is_running = True
            await self._log_activity("Agent started successfully", "info")
            logger.info(f"Agent {self.name} started successfully")
        except Exception as e:
            self.status = AgentStatusEnum.error
            self.error_message = str(e)
            await self._log_activity(f"Failed to start agent: {e}", "error")
            logger.error(f"Failed to start agent {self.name}: {e}")
    
    async def stop(self):
        """Stop the agent"""
        try:
            self.status = AgentStatusEnum.stopping
            self.is_running = False
            await self._cleanup()
            self.status = AgentStatusEnum.inactive
            await self._log_activity("Agent stopped", "info")
            logger.info(f"Agent {self.name} stopped")
        except Exception as e:
            self.status = AgentStatusEnum.error
            self.error_message = str(e)
            await self._log_activity(f"Error stopping agent: {e}", "error")
            logger.error(f"Error stopping agent {self.name}: {e}")
    
    async def execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a task"""
        task_id = str(uuid.uuid4())
        start_time = datetime.now()
        
        try:
            await self._log_activity(f"Starting task {task_id}", "info")
            result = await self._execute_task(task_data)
            
            self.tasks_completed += 1
            self.last_run = datetime.now()
            
            execution_time = (datetime.now() - start_time).total_seconds()
            await self._log_activity(f"Task {task_id} completed in {execution_time:.2f}s", "success")
            
            return {
                "task_id": task_id,
                "status": "completed",
                "result": result,
                "execution_time": execution_time
            }
        except Exception as e:
            self.tasks_failed += 1
            await self._log_activity(f"Task {task_id} failed: {e}", "error")
            logger.error(f"Task failed in agent {self.name}: {e}")
            
            return {
                "task_id": task_id,
                "status": "failed",
                "error": str(e),
                "execution_time": (datetime.now() - start_time).total_seconds()
            }
    
    async def _log_activity(self, message: str, level: str = "info", details: Dict[str, Any] = None):
        """Log agent activity"""
        activity = AgentActivity(
            id=str(uuid.uuid4()),
            agent_id=self.agent_id,
            agent_name=self.name,
            activity=message,
            status=level,
            timestamp=datetime.now(),
            details=details
        )
        
        log_entry = AgentLog(
            id=str(uuid.uuid4()),
            agent_id=self.agent_id,
            level=level,
            message=message,
            timestamp=datetime.now(),
            details=details
        )
        
        # Keep only recent activities (last 100)
        self.activity_log.append(activity)
        if len(self.activity_log) > 100:
            self.activity_log = self.activity_log[-100:]
        
        # Keep only recent logs (last 500)
        self.execution_log.append(log_entry)
        if len(self.execution_log) > 500:
            self.execution_log = self.execution_log[-500:]
    
    def get_status(self) -> AgentStatus:
        """Get current agent status"""
        uptime = None
        if self.start_time and self.status == AgentStatusEnum.active:
            uptime = str(datetime.now() - self.start_time)
        
        return AgentStatus(
            id=self.agent_id,
            name=self.name,
            type=self.agent_type,
            status=self.status,
            last_run=self.last_run,
            tasks_completed=self.tasks_completed,
            tasks_failed=self.tasks_failed,
            uptime=uptime,
            error_message=self.error_message
        )
    
    @abstractmethod
    async def _initialize(self):
        """Initialize the agent (to be implemented by subclasses)"""
        pass
    
    @abstractmethod
    async def _cleanup(self):
        """Cleanup resources (to be implemented by subclasses)"""
        pass
    
    @abstractmethod
    async def _execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the main task logic (to be implemented by subclasses)"""
        pass

class DataProfilerAgent(BaseAgent):
    """Agent for data profiling tasks"""
    
    def __init__(self):
        super().__init__(
            agent_id="data_profiler",
            name="Data Profiler",
            agent_type=AgentType.profiling
        )
        self.profiling_queue = asyncio.Queue()
    
    async def _initialize(self):
        """Initialize the profiler agent"""
        await self._log_activity("Initializing data profiler agent")
        # Start background profiling task
        asyncio.create_task(self._background_profiler())
    
    async def _cleanup(self):
        """Cleanup profiler resources"""
        await self._log_activity("Cleaning up data profiler agent")
    
    async def _execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute data profiling task"""
        table_id = task_data.get("table_id")
        connection_id = task_data.get("connection_id")
        
        if not table_id:
            raise ValueError("table_id is required for profiling task")
        
        await self._log_activity(f"Profiling table {table_id}")
        
        # Simulate profiling work
        await asyncio.sleep(random.uniform(1, 3))
        
        # Generate mock profiling results
        profile_results = {
            "table_id": table_id,
            "record_count": random.randint(1000, 1000000),
            "column_count": random.randint(5, 50),
            "null_percentage": round(random.uniform(0, 15), 2),
            "duplicate_percentage": round(random.uniform(0, 5), 2),
            "quality_score": round(random.uniform(85, 100), 1),
            "profiled_at": datetime.now().isoformat()
        }
        
        await self._log_activity(f"Profiling completed for table {table_id}", "success", profile_results)
        
        return profile_results
    
    async def _background_profiler(self):
        """Background task for continuous profiling"""
        while self.is_running:
            try:
                # Simulate discovering tables that need profiling
                if random.random() < 0.3:  # 30% chance
                    await self._log_activity("Discovered table needing profiling")
                
                await asyncio.sleep(10)  # Check every 10 seconds
            except Exception as e:
                await self._log_activity(f"Background profiler error: {e}", "error")
                await asyncio.sleep(30)

class QualityValidatorAgent(BaseAgent):
    """Agent for data quality validation"""
    
    def __init__(self):
        super().__init__(
            agent_id="quality_validator",
            name="Quality Validator",
            agent_type=AgentType.validation
        )
        self.validation_rules = []
    
    async def _initialize(self):
        """Initialize the validator agent"""
        await self._log_activity("Initializing quality validator agent")
        await self._load_validation_rules()
    
    async def _cleanup(self):
        """Cleanup validator resources"""
        await self._log_activity("Cleaning up quality validator agent")
    
    async def _execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute quality validation task"""
        table_id = task_data.get("table_id")
        rule_id = task_data.get("rule_id")
        
        if not table_id:
            raise ValueError("table_id is required for validation task")
        
        await self._log_activity(f"Validating data quality for table {table_id}")
        
        # Simulate validation work
        await asyncio.sleep(random.uniform(1, 4))
        
        # Generate mock validation results
        issues_found = []
        if random.random() < 0.4:  # 40% chance of finding issues
            issues_found = [
                {
                    "issue_type": random.choice(["Missing Values", "Data Drift", "Duplicate Records", "Format Violations"]),
                    "severity": random.choice(["high", "medium", "low"]),
                    "description": "Quality issue detected during validation",
                    "affected_records": random.randint(1, 1000),
                    "detected_at": datetime.now().isoformat()
                }
            ]
        
        validation_results = {
            "table_id": table_id,
            "validation_passed": len(issues_found) == 0,
            "issues_found": issues_found,
            "quality_score": round(random.uniform(80, 100), 1),
            "validated_at": datetime.now().isoformat()
        }
        
        status = "success" if validation_results["validation_passed"] else "warning"
        await self._log_activity(f"Validation completed for table {table_id}", status, validation_results)
        
        return validation_results
    
    async def _load_validation_rules(self):
        """Load validation rules"""
        self.validation_rules = [
            {"id": 1, "name": "Null Check", "type": "null_validation"},
            {"id": 2, "name": "Format Check", "type": "format_validation"},
            {"id": 3, "name": "Range Check", "type": "range_validation"}
        ]
        await self._log_activity(f"Loaded {len(self.validation_rules)} validation rules")

class LineageTrackerAgent(BaseAgent):
    """Agent for tracking data lineage"""
    
    def __init__(self):
        super().__init__(
            agent_id="lineage_tracker",
            name="Lineage Tracker",
            agent_type=AgentType.lineage
        )
        self.lineage_graph = {}
    
    async def _initialize(self):
        """Initialize the lineage tracker agent"""
        await self._log_activity("Initializing lineage tracker agent")
    
    async def _cleanup(self):
        """Cleanup lineage tracker resources"""
        await self._log_activity("Cleaning up lineage tracker agent")
    
    async def _execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute lineage tracking task"""
        source_table = task_data.get("source_table")
        target_table = task_data.get("target_table")
        
        if not source_table or not target_table:
            raise ValueError("Both source_table and target_table are required for lineage tracking")
        
        await self._log_activity(f"Tracking lineage from {source_table} to {target_table}")
        
        # Simulate lineage tracking work
        await asyncio.sleep(random.uniform(1, 2))
        
        # Generate mock lineage results
        lineage_results = {
            "source_table": source_table,
            "target_table": target_table,
            "transformation_type": random.choice(["ETL", "View", "Aggregation", "Join"]),
            "confidence_score": round(random.uniform(0.8, 1.0), 2),
            "tracked_at": datetime.now().isoformat()
        }
        
        await self._log_activity(f"Lineage tracking completed", "success", lineage_results)
        
        return lineage_results

class AnomalyDetectorAgent(BaseAgent):
    """Agent for detecting data anomalies"""
    
    def __init__(self):
        super().__init__(
            agent_id="anomaly_detector",
            name="Anomaly Detector",
            agent_type=AgentType.monitoring
        )
        self.baseline_metrics = {}
    
    async def _initialize(self):
        """Initialize the anomaly detector agent"""
        await self._log_activity("Initializing anomaly detector agent")
        await self._load_baseline_metrics()
    
    async def _cleanup(self):
        """Cleanup anomaly detector resources"""
        await self._log_activity("Cleaning up anomaly detector agent")
    
    async def _execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute anomaly detection task"""
        table_id = task_data.get("table_id")
        metric_type = task_data.get("metric_type", "record_count")
        
        if not table_id:
            raise ValueError("table_id is required for anomaly detection")
        
        await self._log_activity(f"Detecting anomalies in table {table_id}")
        
        # Simulate anomaly detection work
        await asyncio.sleep(random.uniform(1, 3))
        
        # Generate mock anomaly detection results
        anomaly_detected = random.random() < 0.2  # 20% chance
        anomaly_results = {
            "table_id": table_id,
            "metric_type": metric_type,
            "anomaly_detected": anomaly_detected,
            "anomaly_score": round(random.uniform(0, 1), 3),
            "threshold": 0.8,
            "current_value": random.randint(1000, 10000),
            "expected_range": [800, 12000],
            "detected_at": datetime.now().isoformat()
        }
        
        if anomaly_detected:
            anomaly_results["anomaly_type"] = random.choice(["Spike", "Drop", "Drift", "Missing Data"])
            anomaly_results["severity"] = random.choice(["high", "medium", "low"])
        
        status = "warning" if anomaly_detected else "success"
        await self._log_activity(f"Anomaly detection completed for table {table_id}", status, anomaly_results)
        
        return anomaly_results
    
    async def _load_baseline_metrics(self):
        """Load baseline metrics for anomaly detection"""
        # Simulate loading baseline metrics
        self.baseline_metrics = {
            "record_counts": {"avg": 50000, "std": 5000},
            "null_percentages": {"avg": 2.5, "std": 1.0}
        }
        await self._log_activity("Loaded baseline metrics for anomaly detection")

class AgentOrchestrator:
    """Orchestrator for managing multiple agents"""
    
    def __init__(self):
        self.agents: Dict[str, BaseAgent] = {}
        self.task_queue = asyncio.Queue()
        self.running = False
        self.orchestrator_task = None
        
        # Initialize agents
        self._initialize_agents()
    
    def _initialize_agents(self):
        """Initialize all available agents"""
        self.agents = {
            "data_profiler": DataProfilerAgent(),
            "quality_validator": QualityValidatorAgent(),
            "lineage_tracker": LineageTrackerAgent(),
            "anomaly_detector": AnomalyDetectorAgent()
        }
        logger.info(f"Initialized {len(self.agents)} agents")
    
    async def start_agents(self):
        """Start all agents"""
        for agent_id, agent in self.agents.items():
            await agent.start()
        await self.start_orchestrator()
        logger.info("All agents started")
    
    async def stop_agents(self):
        """Stop all agents"""
        await self.stop_orchestrator()
        logger.info("All agents stopped")
    
    async def start_orchestrator(self):
        """Start the orchestrator"""
        self.running = True
        self.orchestrator_task = asyncio.create_task(self._orchestrator_loop())
        logger.info("Agent orchestrator started")
    
    async def stop_orchestrator(self):
        """Stop the orchestrator"""
        self.running = False
        if self.orchestrator_task:
            self.orchestrator_task.cancel()
            try:
                await self.orchestrator_task
            except asyncio.CancelledError:
                pass
        
        # Stop all agents
        for agent in self.agents.values():
            if agent.status == AgentStatusEnum.active:
                await agent.stop()
        
        logger.info("Agent orchestrator stopped")
    
    async def start_agent(self, agent_id: str) -> bool:
        """Start a specific agent"""
        if agent_id not in self.agents:
            logger.error(f"Agent {agent_id} not found")
            return False
        
        agent = self.agents[agent_id]
        await agent.start()
        return agent.status == AgentStatusEnum.active
    
    async def stop_agent(self, agent_id: str) -> bool:
        """Stop a specific agent"""
        if agent_id not in self.agents:
            logger.error(f"Agent {agent_id} not found")
            return False
        
        agent = self.agents[agent_id]
        await agent.stop()
        return agent.status == AgentStatusEnum.inactive
    
    def get_agent_status(self, agent_id: str = None) -> List[AgentStatus]:
        """Get status of agents"""
        if agent_id:
            if agent_id in self.agents:
                return [self.agents[agent_id].get_status()]
            else:
                return []
        
        # Return all agent statuses as list
        return [agent.get_status() for agent in self.agents.values()]
    
    def get_recent_activity(self, limit: int = 20) -> List[AgentActivity]:
        """Get recent agent activities"""
        all_activities = []
        for agent in self.agents.values():
            all_activities.extend(agent.activity_log)
        
        # Sort by timestamp and return most recent
        all_activities.sort(key=lambda x: x.timestamp, reverse=True)
        return all_activities[:limit]
    
    def get_execution_logs(self, limit: int = 100) -> List[AgentLog]:
        """Get agent execution logs"""
        all_logs = []
        for agent in self.agents.values():
            all_logs.extend(agent.execution_log)
        
        # Sort by timestamp and return most recent
        all_logs.sort(key=lambda x: x.timestamp, reverse=True)
        return all_logs[:limit]
    
    async def start_connection_profiling(self, connection_id: int):
        """Start profiling for a new connection"""
        task_data = {
            "action": "profile_connection",
            "connection_id": connection_id
        }
        await self.submit_task("data_profiler", task_data)
    
    async def profile_table(self, table_id: int):
        """Profile a specific table"""
        task_data = {
            "action": "profile_table", 
            "table_id": table_id
        }
        await self.submit_task("data_profiler", task_data)
    
    async def run_quality_check(self, table_id: int):
        """Run quality check for a table"""
        task_data = {
            "action": "quality_check",
            "table_id": table_id
        }
        await self.submit_task("quality_validator", task_data)
    
    async def run_all_quality_checks(self):
        """Run quality checks for all tables"""
        task_data = {
            "action": "quality_check_all"
        }
        await self.submit_task("quality_validator", task_data)
    
    async def submit_task(self, agent_id: str, task_data: Dict[str, Any]) -> str:
        """Submit a task to a specific agent"""
        if agent_id not in self.agents:
            raise ValueError(f"Agent {agent_id} not found")
        
        task_id = str(uuid.uuid4())
        task = {
            "task_id": task_id,
            "agent_id": agent_id,
            "task_data": task_data,
            "submitted_at": datetime.now()
        }
        
        await self.task_queue.put(task)
        logger.info(f"Task {task_id} submitted to agent {agent_id}")
        return task_id
    
    async def _orchestrator_loop(self):
        """Main orchestrator loop"""
        while self.running:
            try:
                # Process task queue
                if not self.task_queue.empty():
                    task = await self.task_queue.get()
                    await self._process_task(task)
                
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Orchestrator error: {e}")
                await asyncio.sleep(5)
    
    async def _process_task(self, task: Dict[str, Any]):
        """Process a task"""
        try:
            agent_id = task["agent_id"]
            agent = self.agents[agent_id]
            
            if agent.status != AgentStatusEnum.active:
                logger.warning(f"Agent {agent_id} is not active, skipping task {task['task_id']}")
                return
            
            result = await agent.execute_task(task["task_data"])
            logger.info(f"Task {task['task_id']} completed with status {result['status']}")
            
        except Exception as e:
            logger.error(f"Error processing task {task['task_id']}: {e}")

# Global orchestrator instance
agent_orchestrator = AgentOrchestrator()

async def get_orchestrator():
    """Dependency function for FastAPI"""
    return agent_orchestrator