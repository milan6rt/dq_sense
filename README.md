# DataIQ Platform

🎯 **Multi-Agent Data Quality Management System** - Comprehensive data profiling, quality monitoring, lineage tracking, and governance for PostgreSQL databases.

![DataIQ Platform](https://img.shields.io/badge/DataIQ-Platform-blue?style=for-the-badge)
![Python](https://img.shields.io/badge/Python-3.9+-blue?style=flat-square&logo=python)
![React](https://img.shields.io/badge/React-18+-blue?style=flat-square&logo=react)
![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green?style=flat-square&logo=fastapi)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-blue?style=flat-square&logo=postgresql)

---

## ✨ Features

### 🔗 **Database Connection Management**
- Connect to multiple PostgreSQL databases
- Real-time connection health monitoring
- Secure credential management

### 📊 **Automated Data Profiling**
- Automatic table and column discovery
- Statistical analysis and data profiling
- Metadata extraction and cataloging

### 🎯 **Data Quality Monitoring**
- Real-time quality scoring (0-100%)
- Automated issue detection and categorization
- Configurable quality rules and thresholds
- Trend analysis and reporting

### 🌐 **Interactive Data Lineage**
- Visual relationship mapping between tables
- Alation-style lineage visualization with D3.js
- Impact analysis for upstream/downstream dependencies
- Column-level lineage tracking

### 🤖 **Multi-Agent AI System**
- **Data Profiler**: Analyzes patterns and statistics
- **Quality Validator**: Runs continuous quality checks
- **Lineage Tracker**: Discovers data relationships
- **Anomaly Detector**: Identifies unusual patterns

### 🛡️ **Data Governance**
- Data classification (Public, Internal, Sensitive, Highly Sensitive)
- Policy creation and enforcement
- Compliance scoring and violation tracking
- Governance activity monitoring

---

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Node.js 14+
- PostgreSQL database(s) to connect to

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/dataiq-platform.git
   cd dataiq-platform
   ```

2. **Backend Setup**
   ```bash
   cd backend
   pip3 install -r requirements.txt
   ```

3. **Frontend Setup**
   ```bash
   cd frontend
   npm install
   ```

4. **Start the Application**
   
   **Terminal 1 - Backend:**
   ```bash
   cd backend
   python3 -m uvicorn main:app --host 0.0.0.0 --port 8001 --reload
   ```
   
   **Terminal 2 - Frontend:**
   ```bash
   cd frontend
   PORT=3001 npm start
   ```

5. **Access the Application**
   - **Frontend UI**: http://localhost:3001
   - **Backend API**: http://localhost:8001
   - **API Documentation**: http://localhost:8001/docs

---

## 📚 Documentation

### 📖 **User Guide**
Comprehensive user guide available in [USER_GUIDE.md](USER_GUIDE.md) covering:
- Step-by-step setup instructions
- Feature walkthrough with screenshots
- Best practices and troubleshooting
- API reference and configuration options

### 🏗️ **Architecture Overview**

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend UI   │    │   Backend API   │    │   PostgreSQL    │
│   (React)       │◄──►│   (FastAPI)     │◄──►│   Databases     │
│   Port: 3001    │    │   Port: 8001    │    │   Port: 5432    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                               │
                       ┌───────▼───────┐
                       │  Multi-Agent  │
                       │    System     │
                       │ ┌───┐ ┌───┐   │
                       │ │ P │ │ Q │   │
                       │ │ r │ │ u │   │
                       │ │ o │ │ a │   │
                       │ └───┘ └───┘   │
                       │ ┌───┐ ┌───┐   │
                       │ │ L │ │ A │   │
                       │ │ i │ │ n │   │
                       │ │ n │ │ o │   │
                       │ └───┘ └───┘   │
                       └───────────────┘
```

### 🎯 **Core Components**
- **FastAPI Backend**: RESTful API with async PostgreSQL connections
- **React Frontend**: Modern SPA with Tailwind CSS styling
- **Multi-Agent System**: AI-powered continuous monitoring
- **PostgreSQL Integration**: Native asyncpg for optimal performance
- **D3.js Visualizations**: Interactive data lineage graphs

---

## 📊 Screenshots

### Dashboard Overview
![Dashboard](screenshots/dashboard.png)

### Data Catalog
![Data Catalog](screenshots/catalog.png)

### Data Lineage
![Lineage](screenshots/lineage.png)

### Quality Monitoring
![Quality](screenshots/quality.png)

---

## 🛠️ Development

### Backend Development
```bash
cd backend
python3 -m uvicorn main:app --reload --port 8001
```

### Frontend Development
```bash
cd frontend
npm start
```

### Running Tests
```bash
# Backend tests
cd backend
pytest

# Frontend tests
cd frontend
npm test
```

---

## 📈 API Endpoints

### Core APIs
- `GET /api/dashboard/metrics` - Dashboard KPIs
- `POST /api/connections` - Create database connection
- `GET /api/catalog/tables` - Browse data catalog
- `GET /api/quality/overview` - Quality metrics
- `GET /api/lineage/alation-style` - Lineage visualization data
- `GET /api/agents` - Agent status and management

### Full API Documentation
Interactive API documentation available at `http://localhost:8001/docs` when running.

---

## 🔧 Configuration

### Environment Variables
```bash
# Database Settings
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_USER=postgres
DATABASE_PASSWORD=password

# API Settings
API_HOST=0.0.0.0
API_PORT=8001

# Agent Settings
AGENT_CHECK_INTERVAL=5
QUALITY_CHECK_INTERVAL=3600
```

### Custom Configuration
See [config.py](backend/config.py) for all available configuration options.

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🆘 Support

### Getting Help
- 📖 Read the [User Guide](USER_GUIDE.md)
- 🐛 Report issues on [GitHub Issues](https://github.com/yourusername/dataiq-platform/issues)
- 💬 Join discussions in [GitHub Discussions](https://github.com/yourusername/dataiq-platform/discussions)

### Common Issues
- **Connection Failed**: Check database credentials and network connectivity
- **Tables Not Discovered**: Verify database permissions for schema access
- **Port Conflicts**: Ensure ports 3001 and 8001 are available

---

## 🎖️ Acknowledgments

- Built with [FastAPI](https://fastapi.tiangolo.com/) for high-performance APIs
- UI powered by [React](https://reactjs.org/) and [Tailwind CSS](https://tailwindcss.com/)
- Data visualizations using [D3.js](https://d3js.org/)
- Database connectivity via [asyncpg](https://github.com/MagicStack/asyncpg)

---

<div align="center">

**DataIQ Platform** - *Making Data Quality Management Simple and Powerful*

[![GitHub stars](https://img.shields.io/github/stars/yourusername/dataiq-platform?style=social)](https://github.com/yourusername/dataiq-platform/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/yourusername/dataiq-platform?style=social)](https://github.com/yourusername/dataiq-platform/network/members)

</div>