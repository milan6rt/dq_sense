# database.py
import asyncio
import asyncpg
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import json
import hashlib
from fastapi import HTTPException

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
    
    async def discover_lineage_relationships(self, connection_id: int):
        """Discover lineage relationships through database introspection"""
        async with self.metadata_pool.acquire() as meta_conn:
            # Get connection details
            connection_data = await meta_conn.fetchrow("""
                SELECT host, port, database_name, username, password_hash
                FROM data_connections WHERE id = $1
            """, connection_id)
            
            if not connection_data:
                return []
            
            # Connect to target database
            target_conn = await asyncpg.connect(
                host=connection_data['host'],
                port=connection_data['port'],
                database=connection_data['database_name'],
                user=connection_data['username'],
                password=connection_data['password_hash']
            )
            
            try:
                # Discover foreign key relationships
                fk_relationships = await target_conn.fetch("""
                    SELECT 
                        tc.table_schema as source_schema,
                        tc.table_name as source_table,
                        kcu.column_name as source_column,
                        ccu.table_schema as target_schema,
                        ccu.table_name as target_table,
                        ccu.column_name as target_column,
                        tc.constraint_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage ccu
                        ON ccu.constraint_name = tc.constraint_name
                        AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                """)
                
                # Discover naming convention relationships (e.g., raw_* -> transformed tables)
                naming_relationships = await self._discover_naming_patterns(target_conn, connection_id)
                
                # Store discovered relationships
                lineage_relationships = []
                
                # Process FK relationships
                for fk in fk_relationships:
                    source_table_id = await self._get_table_id_by_name(
                        connection_id, fk['source_schema'], fk['source_table']
                    )
                    target_table_id = await self._get_table_id_by_name(
                        connection_id, fk['target_schema'], fk['target_table']
                    )
                    
                    if source_table_id and target_table_id:
                        lineage_relationships.append({
                            'source_table_id': source_table_id,
                            'target_table_id': target_table_id,
                            'transformation_type': 'foreign_key',
                            'source_column': fk['source_column'],
                            'target_column': fk['target_column']
                        })
                
                # Process naming convention relationships
                lineage_relationships.extend(naming_relationships)
                
                # Store relationships in database
                await self._store_lineage_relationships(lineage_relationships)
                
                return lineage_relationships
                
            finally:
                await target_conn.close()
    
    async def _discover_naming_patterns(self, target_conn, connection_id):
        """Discover relationships based on table naming patterns"""
        relationships = []
        
        # Get all tables
        tables = await target_conn.fetch("""
            SELECT schemaname, tablename 
            FROM pg_tables 
            WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        """)
        
        # Group tables by patterns
        raw_tables = [t for t in tables if t['tablename'].startswith('raw_')]
        summary_tables = [t for t in tables if 'summary' in t['tablename'] or 'analytics' in t['tablename']]
        
        # Infer relationships: raw_* tables -> summary/analytics tables
        for raw_table in raw_tables:
            base_name = raw_table['tablename'][4:]  # Remove 'raw_' prefix
            
            for summary_table in summary_tables:
                # Check if base name appears in summary table name
                if base_name.replace('_', '') in summary_table['tablename'].replace('_', ''):
                    raw_table_id = await self._get_table_id_by_name(
                        connection_id, raw_table['schemaname'], raw_table['tablename']
                    )
                    summary_table_id = await self._get_table_id_by_name(
                        connection_id, summary_table['schemaname'], summary_table['tablename']
                    )
                    
                    if raw_table_id and summary_table_id:
                        relationships.append({
                            'source_table_id': raw_table_id,
                            'target_table_id': summary_table_id,
                            'transformation_type': 'transformation',
                            'transformation_logic': f'Aggregation/transformation from {raw_table["tablename"]}'
                        })
        
        return relationships
    
    async def _get_table_id_by_name(self, connection_id, schema_name, table_name):
        """Get table_id from metadata by schema and table name"""
        async with self.metadata_pool.acquire() as conn:
            table_id = await conn.fetchval("""
                SELECT id FROM table_metadata 
                WHERE connection_id = $1 AND schema_name = $2 AND name = $3
            """, connection_id, schema_name, table_name)
            return table_id
    
    async def _store_lineage_relationships(self, relationships):
        """Store lineage relationships in the database"""
        async with self.metadata_pool.acquire() as conn:
            for rel in relationships:
                # Check if relationship already exists
                existing = await conn.fetchval("""
                    SELECT id FROM data_lineage 
                    WHERE source_table_id = $1 AND target_table_id = $2
                """, rel['source_table_id'], rel['target_table_id'])
                
                if not existing:
                    await conn.execute("""
                        INSERT INTO data_lineage 
                        (source_table_id, target_table_id, transformation_type, transformation_logic)
                        VALUES ($1, $2, $3, $4)
                    """, 
                        rel['source_table_id'],
                        rel['target_table_id'],
                        rel.get('transformation_type', 'unknown'),
                        rel.get('transformation_logic', '')
                    )
    
    async def get_table_lineage(self, table_id: int):
        """Get lineage for a specific table"""
        async with self.metadata_pool.acquire() as conn:
            # Get the table info
            table_info = await conn.fetchrow("""
                SELECT tm.*, dc.name as connection_name
                FROM table_metadata tm
                JOIN data_connections dc ON tm.connection_id = dc.id
                WHERE tm.id = $1
            """, table_id)
            
            if not table_info:
                return {"nodes": [], "edges": []}
            
            # Get upstream and downstream relationships
            lineage_data = await conn.fetch("""
                WITH RECURSIVE lineage_tree AS (
                    -- Start with the target table
                    SELECT tm.id, tm.name, tm.schema_name, tm.connection_id, 
                           dc.name as connection_name, 0 as level, 'center' as node_type
                    FROM table_metadata tm
                    JOIN data_connections dc ON tm.connection_id = dc.id
                    WHERE tm.id = $1
                    
                    UNION ALL
                    
                    -- Get upstream tables (sources)
                    SELECT tm.id, tm.name, tm.schema_name, tm.connection_id,
                           dc.name as connection_name, lt.level - 1 as level, 'source' as node_type
                    FROM data_lineage dl
                    JOIN table_metadata tm ON dl.source_table_id = tm.id
                    JOIN data_connections dc ON tm.connection_id = dc.id
                    JOIN lineage_tree lt ON dl.target_table_id = lt.id
                    WHERE lt.level > -2  -- Limit depth
                    
                    UNION ALL
                    
                    -- Get downstream tables (targets)
                    SELECT tm.id, tm.name, tm.schema_name, tm.connection_id,
                           dc.name as connection_name, lt.level + 1 as level, 'target' as node_type
                    FROM data_lineage dl
                    JOIN table_metadata tm ON dl.target_table_id = tm.id
                    JOIN data_connections dc ON tm.connection_id = dc.id
                    JOIN lineage_tree lt ON dl.source_table_id = lt.id
                    WHERE lt.level < 2  -- Limit depth
                )
                SELECT DISTINCT * FROM lineage_tree ORDER BY level, name
            """, table_id)
            
            # Get edges/relationships
            edges_data = await conn.fetch("""
                SELECT dl.*, 
                       tm1.name as source_table_name,
                       tm2.name as target_table_name
                FROM data_lineage dl
                JOIN table_metadata tm1 ON dl.source_table_id = tm1.id
                JOIN table_metadata tm2 ON dl.target_table_id = tm2.id
                WHERE dl.source_table_id = $1 OR dl.target_table_id = $1
            """, table_id)
            
            # Format nodes
            nodes = []
            for row in lineage_data:
                nodes.append({
                    'id': row['id'],
                    'name': f"{row['schema_name']}.{row['name']}",
                    'table_name': row['name'],
                    'schema_name': row['schema_name'],
                    'connection_name': row['connection_name'],
                    'level': row['level'],
                    'type': row['node_type']
                })
            
            # Format edges
            edges = []
            for row in edges_data:
                edges.append({
                    'source': row['source_table_id'],
                    'target': row['target_table_id'],
                    'type': row['transformation_type'],
                    'description': row['transformation_logic']
                })
            
            return {"nodes": nodes, "edges": edges}
    
    async def get_overall_lineage(self):
        """Get complete lineage graph for all connected databases - FIXED"""
        async with self.metadata_pool.acquire() as conn:
            # Get all tables from connected databases
            nodes_data = await conn.fetch("""
                SELECT tm.id, tm.name, tm.schema_name, tm.connection_id,
                       dc.name as connection_name, tm.record_count
                FROM table_metadata tm
                JOIN data_connections dc ON tm.connection_id = dc.id
                WHERE dc.status = 'connected'
                ORDER BY tm.name
            """)
            
            # Get all lineage relationships
            edges_data = await conn.fetch("""
                SELECT dl.*, 
                       tm1.name as source_table_name,
                       tm1.schema_name as source_schema,
                       tm2.name as target_table_name,
                       tm2.schema_name as target_schema
                FROM data_lineage dl
                JOIN table_metadata tm1 ON dl.source_table_id = tm1.id
                JOIN table_metadata tm2 ON dl.target_table_id = tm2.id
                JOIN data_connections dc1 ON tm1.connection_id = dc1.id
                JOIN data_connections dc2 ON tm2.connection_id = dc2.id
                WHERE dc1.status = 'connected' AND dc2.status = 'connected'
            """)
            
            # Format nodes
            nodes = []
            for row in nodes_data:
                node_type = 'source'
                if 'summary' in row['name'] or 'analytics' in row['name']:
                    node_type = 'target'
                elif not row['name'].startswith('raw_'):
                    node_type = 'transform'
                
                nodes.append({
                    'id': row['id'],
                    'name': f"{row['schema_name']}.{row['name']}",
                    'table_name': row['name'],
                    'schema_name': row['schema_name'],
                    'connection_name': row['connection_name'],
                    'record_count': row['record_count'],
                    'type': node_type
                })
            
            # Format edges
            edges = []
            for row in edges_data:
                edges.append({
                    'source': row['source_table_id'],
                    'target': row['target_table_id'],
                    'type': row['transformation_type'],
                    'description': row['transformation_logic']
                })
            
            return {"nodes": nodes, "edges": edges}
    
    async def get_upstream_tables(self, table_id: int):
        """Get tables that feed into this table"""
        async with self.metadata_pool.acquire() as conn:
            upstream = await conn.fetch("""
                SELECT tm.*, dc.name as connection_name, dl.transformation_type, dl.transformation_logic
                FROM data_lineage dl
                JOIN table_metadata tm ON dl.source_table_id = tm.id
                JOIN data_connections dc ON tm.connection_id = dc.id
                WHERE dl.target_table_id = $1
            """, table_id)
            
            return [dict(row) for row in upstream]
    
    async def get_downstream_tables(self, table_id: int):
        """Get tables that consume data from this table"""
        async with self.metadata_pool.acquire() as conn:
            downstream = await conn.fetch("""
                SELECT tm.*, dc.name as connection_name, dl.transformation_type, dl.transformation_logic
                FROM data_lineage dl
                JOIN table_metadata tm ON dl.target_table_id = tm.id
                JOIN data_connections dc ON tm.connection_id = dc.id
                WHERE dl.source_table_id = $1
            """, table_id)
            
            return [dict(row) for row in downstream]
    
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
    
    async def get_table_columns(self, connection_id: int, schema_name: str, table_name: str) -> List[dict]:
        """Get detailed column information for a table"""
        try:
            # Get connection details
            connection_info = await self.get_connection(connection_id)
            if not connection_info:
                raise HTTPException(status_code=404, detail="Connection not found")
            
            # Connect to the target database
            target_conn = await asyncpg.connect(
                host=connection_info['host'],
                port=connection_info['port'],
                database=connection_info['database_name'],
                user=connection_info['username'],
                password=connection_info['password']
            )
            
            try:
                # Get column information from information_schema
                columns = await target_conn.fetch("""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        character_maximum_length,
                        numeric_precision,
                        numeric_scale,
                        ordinal_position,
                        udt_name,
                        CASE WHEN is_nullable = 'YES' THEN true ELSE false END as nullable
                    FROM information_schema.columns 
                    WHERE table_schema = $1 AND table_name = $2
                    ORDER BY ordinal_position
                """, schema_name, table_name)
                
                # Get primary key information
                primary_keys = await target_conn.fetch("""
                    SELECT column_name 
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    WHERE tc.table_schema = $1 
                        AND tc.table_name = $2 
                        AND tc.constraint_type = 'PRIMARY KEY'
                """, schema_name, table_name)
                
                pk_columns = {row['column_name'] for row in primary_keys}
                
                # Get foreign key information  
                foreign_keys = await target_conn.fetch("""
                    SELECT 
                        kcu.column_name,
                        ccu.table_schema as foreign_table_schema,
                        ccu.table_name as foreign_table_name,
                        ccu.column_name as foreign_column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage ccu
                        ON ccu.constraint_name = tc.constraint_name
                        AND ccu.table_schema = tc.table_schema
                    WHERE tc.table_schema = $1 
                        AND tc.table_name = $2 
                        AND tc.constraint_type = 'FOREIGN KEY'
                """, schema_name, table_name)
                
                fk_info = {}
                for row in foreign_keys:
                    fk_info[row['column_name']] = {
                        'references_table': f"{row['foreign_table_schema']}.{row['foreign_table_name']}",
                        'references_column': row['foreign_column_name']
                    }
                
                # Format column information
                formatted_columns = []
                for col in columns:
                    column_info = {
                        'name': col['column_name'],
                        'data_type': col['data_type'],
                        'nullable': col['nullable'],
                        'default_value': col['column_default'],
                        'max_length': col['character_maximum_length'],
                        'precision': col['numeric_precision'],
                        'scale': col['numeric_scale'],
                        'position': col['ordinal_position'],
                        'udt_name': col['udt_name'],
                        'is_primary_key': col['column_name'] in pk_columns,
                        'foreign_key': fk_info.get(col['column_name'])
                    }
                    formatted_columns.append(column_info)
                
                return formatted_columns
                
            finally:
                await target_conn.close()
                
        except Exception as e:
            logger.error(f"Failed to get columns for {schema_name}.{table_name}: {e}")
            raise
    
    async def get_table_column_names(self, table_id: int):
        """Get simple column names for a table"""
        async with self.metadata_pool.acquire() as conn:
            # Get table info first
            table_info = await conn.fetchrow("""
                SELECT tm.*, dc.host, dc.port, dc.database_name, dc.username, dc.password_hash
                FROM table_metadata tm
                JOIN data_connections dc ON tm.connection_id = dc.id
                WHERE tm.id = $1 AND dc.status = 'connected'
            """, table_id)
            
            if not table_info:
                return []
            
            try:
                # Connect to target database
                target_conn = await asyncpg.connect(
                    host=table_info['host'],
                    port=table_info['port'],
                    database=table_info['database_name'],
                    user=table_info['username'],
                    password=table_info['password_hash']
                )
                
                # Get column names
                columns = await target_conn.fetch("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name = $2
                    ORDER BY ordinal_position
                """, table_info['schema_name'], table_info['name'])
                
                await target_conn.close()
                return [col['column_name'] for col in columns]
                
            except Exception as e:
                logger.error(f"Failed to get column names for table {table_id}: {e}")
                return []
    
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