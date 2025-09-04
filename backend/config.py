# config.py - Configuration Management
import os
from typing import List, Optional
from pydantic import BaseSettings, Field
from functools import lru_cache

class Settings(BaseSettings):
    # Application Settings
    app_name: str = Field(default="DataIQ Platform", env="APP_NAME")
    app_version: str = Field(default="1.0.0", env="APP_VERSION")
    debug: bool = Field(default=True, env="DEBUG")
    
    # API Settings
    api_host: str = Field(default="0.0.0.0", env="API_HOST")
    api_port: int = Field(default=8000, env="API_PORT")
    api_reload: bool = Field(default=True, env="API_RELOAD")
    
    # CORS Settings
    cors_origins: List[str] = Field(
        default=["http://localhost:3000", "http://127.0.0.1:3000"], 
        env="CORS_ORIGINS"
    )
    cors_credentials: bool = Field(default=True, env="CORS_CREDENTIALS")
    cors_methods: List[str] = Field(default=["*"], env="CORS_METHODS")
    cors_headers: List[str] = Field(default=["*"], env="CORS_HEADERS")
    
    # Database Settings
    database_host: str = Field(default="localhost", env="DATABASE_HOST")
    database_port: int = Field(default=5432, env="DATABASE_PORT")
    database_name: str = Field(default="postgres", env="DATABASE_NAME")
    database_user: str = Field(default="postgres", env="DATABASE_USER")
    database_password: str = Field(default="password", env="DATABASE_PASSWORD")
    database_pool_min_size: int = Field(default=1, env="DATABASE_POOL_MIN_SIZE")
    database_pool_max_size: int = Field(default=10, env="DATABASE_POOL_MAX_SIZE")
    
    # Redis Settings (for caching and task queues)
    redis_host: str = Field(default="localhost", env="REDIS_HOST")
    redis_port: int = Field(default=6379, env="REDIS_PORT")
    redis_db: int = Field(default=0, env="REDIS_DB")
    redis_password: Optional[str] = Field(default=None, env="REDIS_PASSWORD")
    
    # Agent Settings
    agent_check_interval: int = Field(default=5, env="AGENT_CHECK_INTERVAL")
    agent_max_concurrent_tasks: int = Field(default=10, env="AGENT_MAX_CONCURRENT_TASKS")
    agent_task_timeout: int = Field(default=300, env="AGENT_TASK_TIMEOUT")  # 5 minutes
    
    # Data Quality Settings
    quality_check_interval: int = Field(default=3600, env="QUALITY_CHECK_INTERVAL")  # 1 hour
    profiling_sample_size: int = Field(default=10000, env="PROFILING_SAMPLE_SIZE")
    anomaly_detection_threshold: float = Field(default=0.95, env="ANOMALY_DETECTION_THRESHOLD")
    
    # Logging Settings
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_format: str = Field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s", env="LOG_FORMAT")
    log_file: Optional[str] = Field(default=None, env="LOG_FILE")
    
    # Security Settings
    secret_key: str = Field(default="your-secret-key-change-this-in-production", env="SECRET_KEY")
    access_token_expire_minutes: int = Field(default=30, env="ACCESS_TOKEN_EXPIRE_MINUTES")
    
    # Data Governance Settings
    enable_data_governance: bool = Field(default=True, env="ENABLE_DATA_GOVERNANCE")
    policy_check_interval: int = Field(default=1800, env="POLICY_CHECK_INTERVAL")  # 30 minutes
    
    class Config:
        env_file = ".env"
        case_sensitive = False

@lru_cache()
def get_settings() -> Settings:
    """Get application settings (cached)"""
    return Settings()

# .env file template
ENV_TEMPLATE = """
# DataIQ Platform Environment Configuration

# Application Settings
APP_NAME=DataIQ Platform
APP_VERSION=1.0.0
DEBUG=True

# API Settings
API_HOST=0.0.0.0
API_PORT=8000
API_RELOAD=True

# CORS Settings
CORS_ORIGINS=["http://localhost:3000", "http://127.0.0.1:3000"]

# Database Settings (PostgreSQL)
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_NAME=postgres
DATABASE_USER=postgres
DATABASE_PASSWORD=password
DATABASE_POOL_MIN_SIZE=1
DATABASE_POOL_MAX_SIZE=10

# Redis Settings (Optional - for advanced features)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
# REDIS_PASSWORD=

# Agent Settings
AGENT_CHECK_INTERVAL=5
AGENT_MAX_CONCURRENT_TASKS=10
AGENT_TASK_TIMEOUT=300

# Data Quality Settings
QUALITY_CHECK_INTERVAL=3600
PROFILING_SAMPLE_SIZE=10000
ANOMALY_DETECTION_THRESHOLD=0.95

# Logging Settings
LOG_LEVEL=INFO
# LOG_FILE=dataiq.log

# Security Settings
SECRET_KEY=your-secret-key-change-this-in-production
ACCESS_TOKEN_EXPIRE_MINUTES=30

# Data Governance Settings
ENABLE_DATA_GOVERNANCE=True
POLICY_CHECK_INTERVAL=1800
"""

def create_env_file():
    """Create a sample .env file"""
    if not os.path.exists(".env"):
        with open(".env", "w") as f:
            f.write(ENV_TEMPLATE)
        print("Created .env file with default settings")
    else:
        print(".env file already exists")

if __name__ == "__main__":
    create_env_file()