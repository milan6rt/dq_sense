# DataIQ Platform - User Guide

## Overview

DataIQ Platform is a comprehensive multi-agent data quality management system that provides automated data profiling, quality monitoring, lineage tracking, and governance for your PostgreSQL databases.

### Key Features
- 🔗 **Database Connection Management** - Connect to multiple PostgreSQL databases
- 📊 **Automated Data Profiling** - Discover and analyze your data assets
- 🎯 **Data Quality Monitoring** - Real-time quality checks and issue detection
- 🌐 **Data Lineage Visualization** - Interactive relationship mapping
- 🤖 **Multi-Agent System** - AI-powered continuous monitoring
- 🛡️ **Data Governance** - Policy enforcement and compliance tracking

---

## Getting Started

### Prerequisites
- Python 3.9+ installed
- PostgreSQL database(s) to connect to
- Node.js 14+ for the frontend interface

### Installation & Setup

1. **Install Backend Dependencies**
   ```bash
   cd backend
   pip3 install -r requirements.txt
   ```

2. **Install Frontend Dependencies**
   ```bash
   cd frontend
   npm install
   ```

3. **Start the Backend API**
   ```bash
   cd backend
   python3 -m uvicorn main:app --host 0.0.0.0 --port 8001 --reload
   ```

4. **Start the Frontend Interface**
   ```bash
   cd frontend
   PORT=3001 npm start
   ```

5. **Access the Application**
   - Open your browser and go to: `http://localhost:3001`
   - The backend API will be running at: `http://localhost:8001`

---

## User Interface Guide

### Navigation Menu

The left sidebar provides access to all major features:

- **🏠 Dashboard** - Overview and key metrics
- **📚 Data Catalog** - Browse and explore data assets
- **🎯 Data Quality** - Quality monitoring and issues
- **🌐 Lineage** - Data relationship visualization
- **🤖 AI Agents** - Agent management and monitoring
- **🛡️ Governance** - Policies and compliance
- **🔗 Connections** - Database connection management

---

## Step-by-Step Usage Guide

### 1. Connecting to Your Database

#### Add a New Database Connection

1. **Navigate to Connections**
   - Click on **"Connections"** in the sidebar
   - Click the **"New Connection"** button

2. **Fill in Connection Details**
   ```
   Connection Name: My PostgreSQL DB
   Host: localhost (or your database host)
   Port: 5432
   Database Name: your_database_name
   Username: your_username
   Password: your_password (optional)
   Connection Type: postgresql
   ```

3. **Test & Save**
   - Click **"Test & Save"** button
   - The system will validate the connection
   - On success, the connection will be saved and marked as "connected"

#### Managing Connections

- **Refresh**: Update connection status and rediscover tables
- **Disconnect**: Safely disconnect and clear cached data
- **Status Indicators**: 
  - 🟢 Green dot = Connected
  - 🔴 Red dot = Disconnected

### 2. Exploring Your Data Catalog

#### Viewing Tables

1. **Go to Data Catalog**
   - Click **"Data Catalog"** in the sidebar
   - All discovered tables will be displayed as cards

2. **Table Information**
   Each table card shows:
   - **Table Name** (schema.table_name)
   - **Description** (if available)
   - **Record Count**
   - **Quality Score** (0-100%)
   - **Owner Information**
   - **Tags** for categorization
   - **Popularity Rating** (1-5 stars)

#### Table Details

1. **Click on Any Table Card**
   - Opens detailed view modal
   - Shows comprehensive metadata
   - Displays column information
   - Provides quality metrics

2. **Column Information**
   - Column names and data types
   - Nullable/Primary Key indicators
   - Quality scores per column
   - Sample values (when available)

### 3. Monitoring Data Quality

#### Quality Overview

1. **Navigate to Data Quality**
   - Click **"Data Quality"** in the sidebar
   - View overall quality metrics

2. **Key Metrics**
   - **Overall Quality Score**: Platform-wide quality percentage
   - **Issues Found**: Categorized by severity (Critical/Warning/Info)
   - **Trend Analysis**: Quality improvements over time

#### Quality Issues Management

1. **Review Issues**
   - Issues are listed by table and severity
   - Each issue shows:
     - Table name
     - Issue type (Missing Values, Data Drift, Duplicates, etc.)
     - Severity level
     - Number of affected records
     - Detailed description

2. **Take Action**
   - **Fix**: Address the issue directly
   - **Ignore**: Mark as acceptable/expected

#### Quality Rules

The platform includes pre-configured quality rules:
- **Null Value Check**: Ensures critical fields aren't empty
- **Data Freshness**: Monitors data update frequency
- **Duplicate Detection**: Identifies duplicate records
- **Format Validation**: Validates data format patterns

### 4. Visualizing Data Lineage

#### Interactive Lineage Graph

1. **Go to Lineage**
   - Click **"Lineage"** in the sidebar
   - View interactive data relationship visualization

2. **Navigation Controls**
   - **Pan**: Click and drag to move around
   - **Zoom**: Use mouse wheel or zoom buttons
   - **Fit to View**: Reset to optimal viewing position

3. **Table Relationships**
   - **Source Tables**: Data origins (left side)
   - **Intermediate Tables**: Processing layers (center)
   - **Target Tables**: Final outputs (right side)
   - **Connection Lines**: Show data flow direction

#### Understanding Relationships

- **Hover over tables** to see detailed information
- **Follow connection lines** to understand data flow
- **Expand/collapse** table details for better visibility

### 5. AI Agents Management

#### Agent Status Monitoring

1. **Navigate to AI Agents**
   - Click **"AI Agents"** in the sidebar
   - View all 4 AI agents and their status

2. **Available Agents**
   - **Data Profiler**: Analyzes table statistics and patterns
   - **Quality Validator**: Runs data quality checks
   - **Lineage Tracker**: Discovers data relationships
   - **Anomaly Detector**: Identifies unusual patterns

#### Agent Controls

1. **Start/Stop Agents**
   - Use play/pause buttons to control individual agents
   - Monitor agent status (Active/Inactive/Error)

2. **Activity Monitoring**
   - **Real-time Logs**: Live terminal showing agent activity
   - **Execution History**: Past tasks and completion status
   - **Error Tracking**: Failed tasks and error messages

### 6. Data Governance

#### Policy Management

1. **Access Governance**
   - Click **"Governance"** in the sidebar
   - View compliance overview

2. **Key Metrics**
   - **Active Policies**: Number of enforced policies
   - **Compliance Score**: Overall compliance percentage
   - **Policy Violations**: Issues requiring attention

#### Data Classification

Tables are automatically classified by sensitivity:
- **🔴 Highly Sensitive**: Requires maximum protection
- **🟠 Sensitive**: Contains personal or confidential data
- **🟡 Internal**: Internal business data
- **🟢 Public**: Publicly available information

---

## Dashboard Overview

### Key Performance Indicators

The main dashboard provides at-a-glance insights:

1. **Data Sources**
   - Total number of connected databases
   - Connection health status

2. **Data Quality Score**
   - Overall platform quality percentage
   - Week-over-week trend

3. **Active Agents**
   - Number of running AI agents
   - Continuous monitoring status

4. **Issues Detected**
   - Total quality issues requiring attention
   - Categorized by severity

### Recent Activity

- **Agent Activity Feed**: Real-time agent operations
- **Quality Issues**: Latest detected problems
- **Recently Profiled Tables**: Newest data analysis

---

## Best Practices

### Database Connections

1. **Use Descriptive Names**: Name connections clearly (e.g., "Production DB", "Analytics Warehouse")
2. **Test Regularly**: Use the refresh button to ensure connection health
3. **Monitor Status**: Check connection dashboard regularly

### Data Quality

1. **Review Issues Promptly**: Address critical issues immediately
2. **Set Up Monitoring**: Let AI agents run continuously
3. **Track Trends**: Monitor quality improvements over time

### Data Governance

1. **Classify Sensitivity**: Ensure proper data classification
2. **Review Policies**: Regularly check compliance status
3. **Address Violations**: Handle policy violations promptly

---

## Troubleshooting

### Common Issues

#### Connection Problems

**Issue**: "Connection test failed"
- **Solution**: Verify database credentials and network connectivity
- **Check**: Firewall settings and database permissions

#### Missing Tables

**Issue**: Tables not showing in catalog
- **Solution**: Click "Refresh" on the connection
- **Check**: Database permissions for table discovery

#### Quality Issues

**Issue**: Quality scores seem incorrect
- **Solution**: Let agents complete their profiling cycle
- **Wait**: Initial profiling may take a few minutes

#### Frontend Not Loading

**Issue**: UI not accessible at localhost:3001
- **Solution**: Ensure both backend (port 8001) and frontend (port 3001) are running
- **Check**: No port conflicts with other applications

---

## API Reference

### Base URL
```
http://localhost:8001/api
```

### Key Endpoints

#### Connections
- `GET /connections` - List all connections
- `POST /connections` - Create new connection
- `POST /connections/{id}/refresh` - Refresh connection

#### Data Catalog
- `GET /catalog/tables` - Get all tables
- `GET /catalog/tables/{id}` - Get table details

#### Data Quality
- `GET /quality/overview` - Quality metrics
- `GET /quality/issues` - List quality issues

#### Lineage
- `GET /lineage/alation-style` - Get lineage visualization data

---

## Support & Configuration

### Configuration Options

The platform can be customized through environment variables:

```bash
# Database Settings
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_NAME=postgres
DATABASE_USER=postgres
DATABASE_PASSWORD=password

# Agent Settings
AGENT_CHECK_INTERVAL=5
QUALITY_CHECK_INTERVAL=3600

# API Settings
API_PORT=8001
```

### Getting Help

1. **Check Logs**: Monitor agent logs for detailed information
2. **API Documentation**: Visit `http://localhost:8001/docs` for API docs
3. **Database Permissions**: Ensure proper read permissions on target databases

---

## Advanced Features

### Custom Quality Rules

The platform supports extensible quality rules that can be configured for specific use cases.

### Batch Operations

Multiple tables can be profiled or monitored simultaneously for efficiency.

### Export Capabilities

Data quality reports and lineage information can be exported for external analysis.

---

*DataIQ Platform - Comprehensive Data Quality Management Made Simple*