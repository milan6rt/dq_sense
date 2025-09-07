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
                user="milan",
                password="admin",  # Updated password
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
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS data_lineage (
                    id SERIAL PRIMARY KEY,
                    source_table_id INTEGER REFERENCES table_metadata(id) ON DELETE CASCADE,
                    target_table_id INTEGER REFERENCES table_metadata(id) ON DELETE CASCADE,
                    transformation_type VARCHAR(100),
                    transformation_logic TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            logger.info("Metadata tables created successfully")
    
    async def add_connection(self, connection_data: dict) -> int:
        """Add a new database connection"""
        async with self.metadata_pool.acquire() as conn:
            # Handle password - use empty string if None and don't hash empty passwords
            password = connection_data.get('password_hash', '') or ''
            if password:
                password_hash = hashlib.sha256(password.encode()).hexdigest()
            else:
                password_hash = ''
            
            connection_id = await conn.fetchval("""
                INSERT INTO data_connections 
                (name, host, port, database_name, username, password_hash, connection_type, status)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING id
            """, 
                connection_data['name'],
                connection_data['host'],
                connection_data.get('port', 5432),
                connection_data['database_name'],  # Fixed key name
                connection_data['username'],
                password_hash,
                connection_data.get('connection_type', 'postgresql'),  # Fixed key name
                connection_data.get('status', 'disconnected')
            )
            
            return connection_id
    
    async def get_connections(self) -> List[dict]:
        """Get all database connections"""
        async with self.metadata_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, name, host, port, database_name as database, username, 
                       connection_type, status, last_sync, created_at, updated_at
                FROM data_connections
                ORDER BY created_at DESC
            """)
            
            return [dict(row) for row in rows]
    
    async def test_connection(self, connection_request) -> bool:
        """Test a database connection using CreateConnectionRequest"""
        try:
            # Try to connect using the provided credentials
            test_conn = await asyncpg.connect(
                host=connection_request.host,
                port=connection_request.port,
                database=connection_request.database,
                user=connection_request.username,
                password=connection_request.password or ""
            )
            await test_conn.close()
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    async def test_connection_by_id(self, connection_id: int) -> dict:
        """Test a database connection"""
        async with self.metadata_pool.acquire() as conn:
            connection_data = await conn.fetchrow("""
                SELECT host, port, database_name, username, password_hash
                FROM data_connections WHERE id = $1
            """, connection_id)
            
            if not connection_data:
                return {"status": "error", "message": "Connection not found"}
            
            try:
                # Test the connection
                test_conn = await asyncpg.connect(
                    host=connection_data['host'],
                    port=connection_data['port'],
                    database=connection_data['database_name'],
                    user=connection_data['username'],
                    password=connection_data['password_hash']  # In real app, decrypt this
                )
                
                # Update status to connected
                await conn.execute("""
                    UPDATE data_connections 
                    SET status = 'connected', last_sync = CURRENT_TIMESTAMP
                    WHERE id = $1
                """, connection_id)
                
                await test_conn.close()
                return {"status": "success", "message": "Connection successful"}
                
            except Exception as e:
                await conn.execute("""
                    UPDATE data_connections 
                    SET status = 'error'
                    WHERE id = $1
                """, connection_id)
                
                return {"status": "error", "message": str(e)}
    
    async def disconnect_connection(self, connection_id: int) -> bool:
        """Disconnect a database connection and clear all associated data"""
        async with self.metadata_pool.acquire() as conn:
            try:
                # Start a transaction to ensure atomicity
                async with conn.transaction():
                    # Clear all tables and associated data for this connection
                    await conn.execute("""
                        DELETE FROM column_metadata 
                        WHERE table_id IN (
                            SELECT id FROM table_metadata WHERE connection_id = $1
                        )
                    """, connection_id)
                    
                    await conn.execute("""
                        DELETE FROM table_metadata WHERE connection_id = $1
                    """, connection_id)
                    
                    # Clear quality issues for this connection
                    await conn.execute("""
                        DELETE FROM quality_issues 
                        WHERE table_id IN (
                            SELECT id FROM table_metadata WHERE connection_id = $1
                        )
                    """, connection_id)
                    
                    # Update connection status to disconnected
                    await conn.execute("""
                        UPDATE data_connections 
                        SET status = 'disconnected', last_sync = NULL, updated_at = CURRENT_TIMESTAMP
                        WHERE id = $1
                    """, connection_id)
                
                logger.info(f"Connection {connection_id} disconnected and all associated data cleared")
                return True
            except Exception as e:
                logger.error(f"Failed to disconnect connection {connection_id}: {e}")
                return False
    
    async def refresh_connection(self, connection_id: int) -> dict:
        """Refresh a database connection (test and update status)"""
        async with self.metadata_pool.acquire() as conn:
            connection_data = await conn.fetchrow("""
                SELECT host, port, database_name, username, password_hash
                FROM data_connections WHERE id = $1
            """, connection_id)
            
            if not connection_data:
                return {"status": "error", "message": "Connection not found"}
            
            try:
                # Test the connection
                test_conn = await asyncpg.connect(
                    host=connection_data['host'],
                    port=connection_data['port'],
                    database=connection_data['database_name'],
                    user=connection_data['username'],
                    password=connection_data['password_hash']  # In real app, decrypt this
                )
                
                # Update status to connected
                await conn.execute("""
                    UPDATE data_connections 
                    SET status = 'connected', last_sync = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                    WHERE id = $1
                """, connection_id)
                
                await test_conn.close()
                logger.info(f"Connection {connection_id} refreshed successfully")
                
                # Discover and store tables for this connection
                try:
                    tables = await self.discover_tables(connection_id)
                    await self.store_discovered_tables(tables)
                    logger.info(f"Discovered {len(tables)} tables for connection {connection_id}")
                    
                    # Trigger agent profiling for all discovered tables
                    import agents
                    agent_orchestrator = agents.agent_orchestrator
                    await agent_orchestrator.submit_task(
                        agent_type="data_profiler",
                        action="profile_connection",
                        connection_id=connection_id
                    )
                    
                except Exception as e:
                    logger.error(f"Failed to discover tables for connection {connection_id}: {e}")
                    # Don't fail the connection refresh if table discovery fails
                    pass
                
                return {"status": "connected", "message": "Connection refreshed successfully"}
                
            except Exception as e:
                # Update status to error
                await conn.execute("""
                    UPDATE data_connections 
                    SET status = 'error', updated_at = CURRENT_TIMESTAMP
                    WHERE id = $1
                """, connection_id)
                
                logger.error(f"Failed to refresh connection {connection_id}: {e}")
                return {"status": "error", "message": str(e)}
    
    async def discover_tables(self, connection_id: int) -> List[dict]:
        """Discover all tables in a connected database"""
        async with self.metadata_pool.acquire() as meta_conn:
            connection_data = await meta_conn.fetchrow("""
                SELECT host, port, database_name, username, password_hash
                FROM data_connections WHERE id = $1
            """, connection_id)
            
            if not connection_data:
                raise ValueError("Connection not found")
        
        # Connect to target database
        target_conn = await asyncpg.connect(
            host=connection_data['host'],
            port=connection_data['port'],
            database=connection_data['database_name'],
            user=connection_data['username'],
            password=connection_data['password_hash']  # In real app, decrypt this
        )
        
        try:
            # Discover tables from information_schema
            discovered_tables = await target_conn.fetch("""
                SELECT 
                    schemaname,
                    tablename,
                    tableowner
                FROM pg_tables
                WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                ORDER BY schemaname, tablename
            """)
            
            tables = []
            for table_row in discovered_tables:
                schema_name = table_row['schemaname']
                table_name = table_row['tablename']
                owner = table_row['tableowner']
                
                # Get table row count
                try:
                    count_result = await target_conn.fetchval(f"""
                        SELECT COUNT(*) FROM "{schema_name}"."{table_name}"
                    """)
                    record_count = count_result or 0
                except Exception:
                    record_count = 0
                
                tables.append({
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'record_count': record_count,
                    'owner': owner,
                    'connection_id': connection_id
                })
            
            return tables
            
        finally:
            await target_conn.close()
    
    async def store_discovered_tables(self, tables: List[dict]) -> List[int]:
        """Store discovered tables in metadata database"""
        table_ids = []
        async with self.metadata_pool.acquire() as conn:
            for table in tables:
                # Check if table already exists
                existing = await conn.fetchval("""
                    SELECT id FROM table_metadata 
                    WHERE connection_id = $1 AND schema_name = $2 AND name = $3
                """, table['connection_id'], table['schema_name'], table['table_name'])
                
                if existing:
                    # Update existing table
                    await conn.execute("""
                        UPDATE table_metadata 
                        SET record_count = $1, owner = $2, updated_at = CURRENT_TIMESTAMP
                        WHERE id = $3
                    """, table['record_count'], table.get('owner'), existing)
                    table_ids.append(existing)
                else:
                    # Insert new table
                    table_id = await conn.fetchval("""
                        INSERT INTO table_metadata 
                        (connection_id, schema_name, name, record_count, owner, quality_score, last_profiled)
                        VALUES ($1, $2, $3, $4, $5, 100.0, NULL)
                        RETURNING id
                    """, table['connection_id'], table['schema_name'], table['table_name'], 
                         table['record_count'], table.get('owner'))
                    table_ids.append(table_id)
        
        logger.info(f"Stored {len(tables)} tables, got IDs: {table_ids}")
        return table_ids
    
    async def create_connection(self, connection_request):
        """Create a new database connection from CreateConnectionRequest"""
        try:
            # Add connection to metadata database
            connection_id = await self.add_connection({
                'name': connection_request.name,
                'host': connection_request.host,
                'port': connection_request.port,
                'database_name': connection_request.database,
                'username': connection_request.username,
                'password_hash': connection_request.password or "",  # In production, encrypt this
                'connection_type': connection_request.connection_type,
                'status': 'connected'
            })
            
            # Automatically discover and store tables
            try:
                logger.info(f"Discovering tables for connection {connection_id}")
                discovered_tables = await self.discover_tables(connection_id)
                table_ids = await self.store_discovered_tables(discovered_tables)
                logger.info(f"Discovered and stored {len(table_ids)} tables for connection {connection_id}")
            except Exception as discovery_error:
                logger.warning(f"Failed to discover tables for connection {connection_id}: {discovery_error}")
                # Don't fail the connection creation if table discovery fails
            
            # Return DatabaseConnection model
            from models import DatabaseConnection, ConnectionStatus
            return DatabaseConnection(
                id=connection_id,
                name=connection_request.name,
                host=connection_request.host,
                port=connection_request.port,
                database=connection_request.database,
                username=connection_request.username,
                connection_type=connection_request.connection_type,
                status=ConnectionStatus.connected,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
        except Exception as e:
            logger.error(f"Failed to create connection: {e}")
            raise
    
    async def get_connection(self, connection_id: int):
        """Get a connection by ID"""
        async with self.metadata_pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM data_connections WHERE id = $1
            """, connection_id)
            
            if row:
                from models import DatabaseConnection, ConnectionStatus
                return DatabaseConnection(
                    id=row['id'],
                    name=row['name'],
                    host=row['host'],
                    port=row['port'],
                    database=row['database_name'],
                    username=row['username'],
                    connection_type=row['connection_type'],
                    status=ConnectionStatus(row['status']),
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )
            return None
    
    async def get_tables(self, connection_id: Optional[int] = None) -> List[dict]:
        """Get table metadata"""
        async with self.metadata_pool.acquire() as conn:
            if connection_id:
                rows = await conn.fetch("""
                    SELECT tm.id, tm.connection_id, tm.name, tm.schema_name, tm.record_count,
                           tm.quality_score, tm.last_profiled, tm.description, tm.owner,
                           COALESCE(tm.tags, '{}') as tags, tm.popularity,
                           tm.created_at, tm.updated_at,
                           dc.name as connection_name
                    FROM table_metadata tm
                    JOIN data_connections dc ON tm.connection_id = dc.id
                    WHERE tm.connection_id = $1
                    ORDER BY tm.updated_at DESC
                """, connection_id)
            else:
                # Only return tables from connected databases
                rows = await conn.fetch("""
                    SELECT tm.id, tm.connection_id, tm.name, tm.schema_name, tm.record_count,
                           tm.quality_score, tm.last_profiled, tm.description, tm.owner,
                           COALESCE(tm.tags, '{}') as tags, tm.popularity,
                           tm.created_at, tm.updated_at,
                           dc.name as connection_name
                    FROM table_metadata tm
                    JOIN data_connections dc ON tm.connection_id = dc.id
                    WHERE dc.status = 'connected'
                    ORDER BY tm.updated_at DESC
                """)
            
            return [dict(row) for row in rows]
    
    async def get_all_tables(self, connection_id: Optional[int] = None, schema_name: Optional[str] = None, search: Optional[str] = None):
        """Get all tables with optional filtering - for API compatibility"""
        return await self.get_tables(connection_id)
    
    async def get_table_details(self, table_id: int):
        """Get detailed table information"""
        async with self.metadata_pool.acquire() as conn:
            # Get table metadata
            table = await conn.fetchrow("""
                SELECT tm.id, tm.connection_id, tm.name, tm.schema_name, tm.record_count,
                       tm.quality_score, tm.last_profiled, tm.description, tm.owner,
                       COALESCE(tm.tags, '{}') as tags, tm.popularity,
                       tm.created_at, tm.updated_at,
                       dc.name as connection_name
                FROM table_metadata tm
                JOIN data_connections dc ON tm.connection_id = dc.id
                WHERE tm.id = $1
            """, table_id)
            
            if not table:
                return None
                
            # Get columns for this table
            columns = await conn.fetch("""
                SELECT id, name, data_type, is_nullable, is_primary_key, is_foreign_key,
                       quality_score, null_percentage, unique_percentage, sample_values,
                       created_at, updated_at
                FROM column_metadata
                WHERE table_id = $1
                ORDER BY id
            """, table_id)
            
            # Convert to dict and add columns
            result = dict(table)
            result['columns'] = [dict(col) for col in columns]
            result['sample_data'] = []  # Could be expanded to return sample data
            
            return result
    
    async def get_table_columns(self, table_id: int):
        """Get columns for a table"""
        # Mock data for now
        return []
    
    async def get_table_lineage(self, table_id: int):
        """Get lineage for a table"""
        return {"nodes": [], "edges": []}
    
    async def get_overall_lineage(self):
        """Get overall lineage graph"""
        return {"nodes": [], "edges": []}
    
    async def get_upstream_tables(self, table_id: int):
        """Get upstream tables"""
        return []
    
    async def get_downstream_tables(self, table_id: int):
        """Get downstream tables"""
        return []
    
    async def resolve_quality_issue(self, issue_id: int):
        """Mark a quality issue as resolved"""
        # Mock implementation
        pass
    
    async def get_active_policies(self):
        """Get active governance policies"""
        return []
    
    async def get_data_classifications(self):
        """Get data classifications"""
        return []
    
    async def get_policy_violations(self):
        """Get policy violations"""
        return []
    
    async def get_table_count(self):
        """Get total table count"""
        return 0
    
    async def create_policy(self, policy_request):
        """Create a governance policy"""
        return {}
    
    async def get_quality_issues(self, severity: Optional[str] = None, table_id: Optional[int] = None) -> List[dict]:
        """Get quality issues"""
        async with self.metadata_pool.acquire() as conn:
            if severity:
                rows = await conn.fetch("""
                    SELECT qi.*, tm.name as table_name, tm.schema_name
                    FROM quality_issues qi
                    JOIN table_metadata tm ON qi.table_id = tm.id
                    WHERE qi.severity = $1 AND qi.is_resolved = FALSE
                    ORDER BY qi.detected_at DESC
                """, severity)
            else:
                rows = await conn.fetch("""
                    SELECT qi.*, tm.name as table_name, tm.schema_name
                    FROM quality_issues qi
                    JOIN table_metadata tm ON qi.table_id = tm.id
                    WHERE qi.is_resolved = FALSE
                    ORDER BY qi.detected_at DESC
                """)
            
            return [dict(row) for row in rows]
    
    async def profile_table(self, connection_id: int, schema: str, table: str):
        """Profile a table and store metadata"""
        try:
            # Get connection details
            async with self.metadata_pool.acquire() as meta_conn:
                connection_data = await meta_conn.fetchrow("""
                    SELECT host, port, database_name, username, password_hash
                    FROM data_connections WHERE id = $1
                """, connection_id)
                
                if not connection_data:
                    raise ValueError("Connection not found")
            
            # Connect to target database
            target_conn = await asyncpg.connect(
                host=connection_data['host'],
                port=connection_data['port'],
                database=connection_data['database_name'],
                user=connection_data['username'],
                password=connection_data['password_hash']  # In real app, decrypt this
            )
            
            # Get table row count
            row_count = await target_conn.fetchval(f"""
                SELECT COUNT(*) FROM {schema}.{table}
            """)
            
            # Get column information
            columns_info = await target_conn.fetch("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
            """, schema, table)
            
            # Store/update table metadata
            async with self.metadata_pool.acquire() as meta_conn:
                table_id = await meta_conn.fetchval("""
                    INSERT INTO table_metadata 
                    (connection_id, name, schema_name, record_count, last_profiled)
                    VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
                    ON CONFLICT (connection_id, schema_name, name)
                    DO UPDATE SET 
                        record_count = EXCLUDED.record_count,
                        last_profiled = EXCLUDED.last_profiled,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                """, connection_id, table, schema, row_count)
                
                # Store column metadata
                for col in columns_info:
                    await meta_conn.execute("""
                        INSERT INTO column_metadata 
                        (table_id, name, data_type, is_nullable)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (table_id, name)
                        DO UPDATE SET 
                            data_type = EXCLUDED.data_type,
                            is_nullable = EXCLUDED.is_nullable,
                            updated_at = CURRENT_TIMESTAMP
                    """, table_id, col['column_name'], col['data_type'], 
                         col['is_nullable'] == 'YES')
            
            await target_conn.close()
            logger.info(f"Successfully profiled table {schema}.{table}")
            
        except Exception as e:
            logger.error(f"Failed to profile table {schema}.{table}: {e}")
            raise
    
    async def close_all_connections(self):
        """Close all database connections"""
        if self.metadata_pool:
            await self.metadata_pool.close()
        
        for pool in self.connections_pool.values():
            await pool.close()
        
        logger.info("All database connections closed")


# Global database manager instance
db_manager = DatabaseManager()

async def get_db():
    """Dependency function for FastAPI"""
    return db_manager

async def init_db():
    """Initialize database on startup"""
    await db_manager.initialize_metadata_db()
    logger.info("Database initialization completed")