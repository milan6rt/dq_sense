# database.py
import asyncio
import asyncpg
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import json
import hashlib

from models import *

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.connections_pool = {}
        self.metadata_pool = None
        
    async def initialize_metadata_db(self):
        """Initialize the metadata database for storing catalog information"""
        try:
            # Connect to default postgres database first
            self.metadata_pool = await asyncpg.create_pool(
                host="localhost",
                port=5432,
                user="postgres",
                password="password",  # Change this to your PostgreSQL password
                database="postgres",
                min_size=1,
                max_size=10
            )
            
            # Create metadata tables if they don't exist
            await self._create_metadata_tables()
            logger.info("Metadata database initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize metadata database: {e}")
            raise
    
    async def _create_metadata_tables(self):
        """Create metadata tables for storing catalog information"""
        async with self.metadata_pool.acquire() as conn:
            # Create connections table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS data_connections (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) UNIQUE NOT NULL,
                    host VARCHAR(255) NOT NULL,
                    port INTEGER NOT NULL,
                    database_name VARCHAR(255) NOT NULL,
                    username VARCHAR(255) NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    connection_type VARCHAR(50) DEFAULT 'postgresql',
                    status VARCHAR(50) DEFAULT 'disconnected',
                    last_sync TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create tables metadata table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS table_metadata (
                    id SERIAL PRIMARY KEY,
                    connection_id INTEGER REFERENCES data_connections(id),
                    name VARCHAR(255) NOT NULL,
                    schema_name VARCHAR(255) NOT NULL,
                    record_count BIGINT DEFAULT 0,
                    quality_score FLOAT DEFAULT 100.0,
                    last_profiled TIMESTAMP,
                    description TEXT,
                    owner VARCHAR(255),
                    tags TEXT[], -- PostgreSQL array type
                    popularity FLOAT DEFAULT 0.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(connection_id, schema_name, name)
                )
            """)
            
            # Create columns metadata table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS column_metadata (
                    id SERIAL PRIMARY KEY,
                    table_id INTEGER REFERENCES table_metadata(id) ON DELETE CASCADE,
                    name VARCHAR(255) NOT NULL,
                    data_type VARCHAR(100) NOT NULL,
                    is_nullable BOOLEAN DEFAULT TRUE,
                    is_primary_key BOOLEAN DEFAULT FALSE,
                    is_foreign_key BOOLEAN DEFAULT FALSE,
                    quality_score FLOAT DEFAULT 100.0,
                    null_percentage FLOAT DEFAULT 0.0,
                    unique_percentage FLOAT DEFAULT 100.0,
                    sample_values TEXT[],
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create quality issues table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS quality_issues (
                    id SERIAL PRIMARY KEY,
                    table_id INTEGER REFERENCES table_metadata(id) ON DELETE CASCADE,
                    issue_type VARCHAR(100) NOT NULL,
                    severity VARCHAR(20) NOT NULL,
                    description TEXT NOT NULL,
                    affected_records INTEGER DEFAULT 0,
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved_at TIMESTAMP,
                    is_resolved BOOLEAN DEFAULT FALSE,
                    rule_id INTEGER
                )
            """)
            
            # Create quality rules table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS quality_rules (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) UNIQUE NOT NULL,
                    description TEXT,
                    rule_type VARCHAR(100) NOT NULL,
                    configuration JSONB NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    tables_applied INTEGER[],
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create lineage table
            await conn.execute(