# agents.py
import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import json
import uuid
from abc import ABC, abstractmethod
import random

from models import AgentStatus, AgentStatusEnum, AgentType, AgentActivity, AgentLog

logger = logging.getLogger(__name__)

class BaseAgent(ABC):
    """Base class for all AI agents"""
    
    def __init__(self, agent_id: str, name: str, agent_type: AgentType):
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
                    "severity":