# quality_calculator.py
import asyncpg
import logging
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, timedelta
import statistics

logger = logging.getLogger(__name__)

class QualityCalculator:
    """Calculate real data quality scores based on various metrics"""
    
    @staticmethod
    async def calculate_table_quality_score(
        connection_pool: asyncpg.Pool, 
        connection_id: int, 
        schema_name: str, 
        table_name: str
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate comprehensive quality score for a table
        Returns: (quality_score, detailed_metrics)
        """
        try:
            async with connection_pool.acquire() as conn:
                # Get basic table stats
                basic_stats = await QualityCalculator._get_basic_table_stats(
                    conn, schema_name, table_name
                )
                
                if basic_stats['record_count'] == 0:
                    return 0.0, {'error': 'Empty table', 'record_count': 0}
                
                # Get column quality metrics
                column_metrics = await QualityCalculator._get_column_quality_metrics(
                    conn, schema_name, table_name
                )
                
                # Calculate individual quality components (0-100 scale)
                completeness_score = QualityCalculator._calculate_completeness_score(column_metrics)
                consistency_score = QualityCalculator._calculate_consistency_score(column_metrics)
                validity_score = QualityCalculator._calculate_validity_score(column_metrics)
                freshness_score = await QualityCalculator._calculate_freshness_score(
                    conn, schema_name, table_name
                )
                uniqueness_score = QualityCalculator._calculate_uniqueness_score(column_metrics)
                
                # Weighted average of quality dimensions
                weights = {
                    'completeness': 0.3,   # 30% - NULL values impact
                    'consistency': 0.25,   # 25% - Data type consistency
                    'validity': 0.25,      # 25% - Valid data patterns
                    'freshness': 0.1,      # 10% - Data recency
                    'uniqueness': 0.1      # 10% - Duplicate detection
                }
                
                overall_score = (
                    completeness_score * weights['completeness'] +
                    consistency_score * weights['consistency'] +
                    validity_score * weights['validity'] +
                    freshness_score * weights['freshness'] +
                    uniqueness_score * weights['uniqueness']
                )
                
                detailed_metrics = {
                    'record_count': basic_stats['record_count'],
                    'column_count': len(column_metrics),
                    'completeness_score': round(completeness_score, 1),
                    'consistency_score': round(consistency_score, 1),
                    'validity_score': round(validity_score, 1),
                    'freshness_score': round(freshness_score, 1),
                    'uniqueness_score': round(uniqueness_score, 1),
                    'overall_score': round(overall_score, 1),
                    'column_metrics': column_metrics,
                    'calculated_at': datetime.now().isoformat()
                }
                
                return round(overall_score, 1), detailed_metrics
                
        except Exception as e:
            logger.error(f"Error calculating quality score for {schema_name}.{table_name}: {e}")
            return 50.0, {'error': str(e), 'calculated_at': datetime.now().isoformat()}
    
    @staticmethod
    async def _get_basic_table_stats(conn: asyncpg.Connection, schema_name: str, table_name: str) -> Dict[str, Any]:
        """Get basic table statistics"""
        try:
            # Get record count
            record_count = await conn.fetchval(
                f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"'
            )
            
            return {
                'record_count': record_count,
                'table_size': record_count  # Could be enhanced with actual size in bytes
            }
        except Exception as e:
            logger.warning(f"Could not get basic stats for {schema_name}.{table_name}: {e}")
            return {'record_count': 0, 'table_size': 0}
    
    @staticmethod
    async def _get_column_quality_metrics(conn: asyncpg.Connection, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Analyze each column for quality metrics"""
        try:
            # Get column information
            columns_info = await conn.fetch(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
            """, schema_name, table_name)
            
            column_metrics = []
            total_records = await conn.fetchval(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
            
            if total_records == 0:
                return []
            
            for col in columns_info:
                col_name = col['column_name']
                data_type = col['data_type']
                
                # Calculate NULL percentage
                null_count = await conn.fetchval(
                    f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}" WHERE "{col_name}" IS NULL'
                )
                null_percentage = (null_count / total_records) * 100 if total_records > 0 else 0
                
                # Calculate unique percentage
                unique_count = await conn.fetchval(
                    f'SELECT COUNT(DISTINCT "{col_name}") FROM "{schema_name}"."{table_name}" WHERE "{col_name}" IS NOT NULL'
                )
                non_null_records = total_records - null_count
                unique_percentage = (unique_count / non_null_records) * 100 if non_null_records > 0 else 0
                
                # Sample values for pattern analysis
                sample_values = []
                try:
                    sample_rows = await conn.fetch(f'''
                        SELECT DISTINCT "{col_name}"::text as value
                        FROM "{schema_name}"."{table_name}" 
                        WHERE "{col_name}" IS NOT NULL 
                        LIMIT 10
                    ''')
                    sample_values = [row['value'] for row in sample_rows]
                except:
                    sample_values = []
                
                column_metrics.append({
                    'column_name': col_name,
                    'data_type': data_type,
                    'is_nullable': col['is_nullable'],
                    'null_percentage': round(null_percentage, 2),
                    'unique_percentage': round(unique_percentage, 2),
                    'total_values': total_records,
                    'null_count': null_count,
                    'unique_count': unique_count,
                    'sample_values': sample_values[:5]  # Keep only first 5 for storage
                })
            
            return column_metrics
            
        except Exception as e:
            logger.warning(f"Could not analyze columns for {schema_name}.{table_name}: {e}")
            return []
    
    @staticmethod
    def _calculate_completeness_score(column_metrics: List[Dict[str, Any]]) -> float:
        """Calculate completeness score based on NULL percentages"""
        if not column_metrics:
            return 100.0
        
        # Average completeness across all columns
        null_percentages = [col['null_percentage'] for col in column_metrics]
        avg_null_percentage = statistics.mean(null_percentages)
        
        # Convert to completeness score (0-100)
        completeness_score = 100.0 - avg_null_percentage
        return max(0.0, min(100.0, completeness_score))
    
    @staticmethod
    def _calculate_consistency_score(column_metrics: List[Dict[str, Any]]) -> float:
        """Calculate consistency score based on data patterns"""
        if not column_metrics:
            return 100.0
        
        consistency_scores = []
        
        for col in column_metrics:
            # Penalize columns with very inconsistent patterns
            data_type = col['data_type'].lower()
            null_pct = col['null_percentage']
            
            # Base consistency score
            col_score = 100.0
            
            # Penalize high NULL percentage for non-nullable columns
            if not col['is_nullable'] and null_pct > 0:
                col_score -= null_pct * 2  # Double penalty for required fields
            
            # Text fields with very low uniqueness might indicate inconsistent formatting
            if 'text' in data_type or 'varchar' in data_type:
                if col['unique_percentage'] < 10 and col['null_percentage'] < 50:
                    # Might be categorical data, which is fine
                    pass
                elif col['unique_percentage'] < 5:
                    col_score -= 10  # Possible data quality issue
            
            consistency_scores.append(max(0.0, col_score))
        
        return statistics.mean(consistency_scores) if consistency_scores else 100.0
    
    @staticmethod
    def _calculate_validity_score(column_metrics: List[Dict[str, Any]]) -> float:
        """Calculate validity score based on data patterns and types"""
        if not column_metrics:
            return 100.0
        
        validity_scores = []
        
        for col in column_metrics:
            col_score = 100.0
            data_type = col['data_type'].lower()
            sample_values = col.get('sample_values', [])
            
            # Check for obvious data quality issues in sample values
            if sample_values:
                issues = 0
                total_samples = len(sample_values)
                
                for value in sample_values:
                    if value is None:
                        continue
                    
                    # Check for placeholder values
                    placeholder_values = ['n/a', 'null', 'none', 'unknown', '', '0', 'test']
                    if str(value).lower().strip() in placeholder_values:
                        issues += 1
                    
                    # Check for obvious format issues
                    if 'date' in data_type:
                        # Could add date format validation
                        pass
                    elif 'numeric' in data_type or 'int' in data_type:
                        # Could add numeric validation
                        pass
                
                if total_samples > 0:
                    issue_percentage = (issues / total_samples) * 100
                    col_score -= issue_percentage
            
            validity_scores.append(max(0.0, col_score))
        
        return statistics.mean(validity_scores) if validity_scores else 100.0
    
    @staticmethod
    async def _calculate_freshness_score(conn: asyncpg.Connection, schema_name: str, table_name: str) -> float:
        """Calculate freshness score based on recent data updates"""
        try:
            # Look for common timestamp columns
            timestamp_columns = await conn.fetch(f"""
                SELECT column_name 
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                AND (data_type LIKE '%timestamp%' OR data_type LIKE '%date%')
                AND (column_name ILIKE '%updated%' OR column_name ILIKE '%created%' 
                     OR column_name ILIKE '%modified%' OR column_name ILIKE '%time%')
                ORDER BY 
                    CASE 
                        WHEN column_name ILIKE '%updated%' THEN 1
                        WHEN column_name ILIKE '%modified%' THEN 2
                        WHEN column_name ILIKE '%created%' THEN 3
                        ELSE 4
                    END
                LIMIT 1
            """, schema_name, table_name)
            
            if not timestamp_columns:
                return 80.0  # Neutral score if no timestamp columns found
            
            col_name = timestamp_columns[0]['column_name']
            
            # Get the most recent timestamp
            latest_timestamp = await conn.fetchval(f'''
                SELECT MAX("{col_name}")
                FROM "{schema_name}"."{table_name}"
                WHERE "{col_name}" IS NOT NULL
            ''')
            
            if not latest_timestamp:
                return 60.0  # Lower score if no valid timestamps
            
            # Calculate days since last update
            now = datetime.now()
            if isinstance(latest_timestamp, datetime):
                days_since_update = (now - latest_timestamp).days
            else:
                return 70.0  # Neutral score if can't parse timestamp
            
            # Scoring based on recency (exponential decay)
            if days_since_update <= 1:
                return 100.0
            elif days_since_update <= 7:
                return 95.0 - (days_since_update * 2)  # Linear decay for first week
            elif days_since_update <= 30:
                return 80.0 - ((days_since_update - 7) * 1.5)  # Slower decay for first month
            elif days_since_update <= 90:
                return 50.0 - ((days_since_update - 30) * 0.5)  # Very slow decay up to 3 months
            else:
                return 20.0  # Very old data
            
        except Exception as e:
            logger.warning(f"Could not calculate freshness for {schema_name}.{table_name}: {e}")
            return 75.0  # Neutral score if calculation fails
    
    @staticmethod
    def _calculate_uniqueness_score(column_metrics: List[Dict[str, Any]]) -> float:
        """Calculate uniqueness score to detect duplicate records"""
        if not column_metrics:
            return 100.0
        
        # Focus on columns that should be unique (like IDs)
        id_columns = [col for col in column_metrics 
                     if 'id' in col['column_name'].lower() or col['column_name'].lower().endswith('_pk')]
        
        if id_columns:
            # For ID columns, high uniqueness is expected
            id_uniqueness = [col['unique_percentage'] for col in id_columns]
            return statistics.mean(id_uniqueness)
        else:
            # For non-ID tables, look at overall uniqueness patterns
            # Tables with all columns having very low uniqueness might have duplicate issues
            avg_uniqueness = statistics.mean([col['unique_percentage'] for col in column_metrics])
            
            # If average uniqueness is too low, it might indicate duplication issues
            if avg_uniqueness < 20:
                return avg_uniqueness * 2  # Boost the score a bit as low uniqueness can be normal
            else:
                return min(100.0, avg_uniqueness + 20)  # Add bonus for diverse data

    @staticmethod
    async def detect_quality_issues(
        connection_pool: asyncpg.Pool,
        connection_id: int, 
        schema_name: str, 
        table_name: str,
        table_id: int
    ) -> List[Dict[str, Any]]:
        """Detect specific quality issues in a table"""
        issues = []
        
        try:
            async with connection_pool.acquire() as conn:
                # Get column metrics first
                column_metrics = await QualityCalculator._get_column_quality_metrics(
                    conn, schema_name, table_name
                )
                
                for col in column_metrics:
                    # High NULL percentage in non-nullable columns
                    if not col['is_nullable'] and col['null_percentage'] > 5:
                        issues.append({
                            'table_id': table_id,
                            'table_name': f"{schema_name}.{table_name}",
                            'issue_type': 'high_nulls',
                            'severity': 'high' if col['null_percentage'] > 20 else 'medium',
                            'description': f"Column '{col['column_name']}' has {col['null_percentage']:.1f}% NULL values but is marked as NOT NULL",
                            'affected_records': col['null_count'],
                            'column_name': col['column_name']
                        })
                    
                    # Very low uniqueness in potential ID columns
                    if 'id' in col['column_name'].lower() and col['unique_percentage'] < 95:
                        issues.append({
                            'table_id': table_id,
                            'table_name': f"{schema_name}.{table_name}",
                            'issue_type': 'duplicate_ids',
                            'severity': 'high',
                            'description': f"ID column '{col['column_name']}' has only {col['unique_percentage']:.1f}% unique values",
                            'affected_records': col['total_values'] - col['unique_count'],
                            'column_name': col['column_name']
                        })
                    
                    # High NULL percentage in any column
                    if col['null_percentage'] > 50:
                        issues.append({
                            'table_id': table_id,
                            'table_name': f"{schema_name}.{table_name}",
                            'issue_type': 'mostly_null',
                            'severity': 'medium',
                            'description': f"Column '{col['column_name']}' is {col['null_percentage']:.1f}% NULL",
                            'affected_records': col['null_count'],
                            'column_name': col['column_name']
                        })
                
                # Check for empty table
                total_records = column_metrics[0]['total_values'] if column_metrics else 0
                if total_records == 0:
                    issues.append({
                        'table_id': table_id,
                        'table_name': f"{schema_name}.{table_name}",
                        'issue_type': 'empty_table',
                        'severity': 'high',
                        'description': f"Table '{schema_name}.{table_name}' contains no records",
                        'affected_records': 0,
                        'column_name': None
                    })
                
                return issues
                
        except Exception as e:
            logger.error(f"Error detecting quality issues for {schema_name}.{table_name}: {e}")
            return []

    @staticmethod
    def calculate_trend_percentage(current_score: float, previous_score: Optional[float]) -> float:
        """Calculate trend percentage change"""
        if previous_score is None or previous_score == 0:
            return 0.0
        
        return round(((current_score - previous_score) / previous_score) * 100, 2)