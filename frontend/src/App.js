import React, { useState, useEffect, useRef } from 'react';
import { 
  Search, Database, Users, Activity, BarChart3, Network, Settings, 
  Plus, Star, MessageCircle, Eye, GitBranch, CheckCircle, AlertTriangle,
  Clock, TrendingUp, Zap, Bot, Shield, FileText, Filter, ChevronDown,
  ChevronRight, Play, Pause, RefreshCw, Download, Upload, Share2,
  Bell, User, Menu, X, Home, Layers, Target, Workflow
} from 'lucide-react';

// Mock data for demonstration
const mockConnections = [
  { id: 1, name: 'Production DB', type: 'PostgreSQL', status: 'connected', host: 'localhost:5432', lastSync: '2 mins ago' },
  { id: 2, name: 'Analytics DB', type: 'PostgreSQL', status: 'connected', host: 'analytics.local:5432', lastSync: '5 mins ago' },
  { id: 3, name: 'Staging DB', type: 'PostgreSQL', status: 'disconnected', host: 'staging.local:5432', lastSync: '2 hours ago' }
];

const mockTables = [
  { 
    id: 1, 
    name: 'users', 
    schema: 'public',
    connection: 'Production DB',
    records: 150000, 
    quality: 95, 
    lastProfiled: '10 mins ago',
    description: 'Core user information table',
    owner: 'Data Team',
    tags: ['PII', 'Core'],
    popularity: 4.5
  },
  {
    id: 2,
    name: 'orders',
    schema: 'public', 
    connection: 'Production DB',
    records: 2500000,
    quality: 87,
    lastProfiled: '15 mins ago',
    description: 'Customer order transactions',
    owner: 'Sales Team',
    tags: ['Financial', 'Transactional'],
    popularity: 4.8
  },
  {
    id: 3,
    name: 'products',
    schema: 'inventory',
    connection: 'Production DB', 
    records: 50000,
    quality: 92,
    lastProfiled: '20 mins ago',
    description: 'Product catalog and inventory',
    owner: 'Product Team',
    tags: ['Catalog', 'Inventory'],
    popularity: 4.2
  }
];

const mockAgents = [
  { id: 1, name: 'Data Profiler', type: 'profiling', status: 'active', lastRun: '2 mins ago', tasksCompleted: 45 },
  { id: 2, name: 'Quality Validator', type: 'validation', status: 'active', lastRun: '1 min ago', tasksCompleted: 23 },
  { id: 3, name: 'Lineage Tracker', type: 'lineage', status: 'active', lastRun: '3 mins ago', tasksCompleted: 12 },
  { id: 4, name: 'Anomaly Detector', type: 'monitoring', status: 'idle', lastRun: '15 mins ago', tasksCompleted: 8 }
];

const mockIssues = [
  { id: 1, table: 'users', type: 'Missing Values', severity: 'high', count: 1250, description: 'email field has null values' },
  { id: 2, table: 'orders', type: 'Data Drift', severity: 'medium', count: 500, description: 'Order amounts showing unusual distribution' },
  { id: 3, table: 'products', type: 'Duplicate Records', severity: 'low', count: 45, description: 'SKU duplicates detected' }
];

const DataIntelligencePlatform = () => {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedTable, setSelectedTable] = useState(null);
  const [agentLogs, setAgentLogs] = useState([]);
  
  // Real API data state
  const [connections, setConnections] = useState([]);
  const [agents, setAgents] = useState([]);
  const [dashboardMetrics, setDashboardMetrics] = useState(null);
  const [loading, setLoading] = useState(true);
  
  // Form state for new connection
  const [showConnectionForm, setShowConnectionForm] = useState(false);
  const [connectionForm, setConnectionForm] = useState({
    name: '',
    host: 'localhost',
    port: '5432',
    database: '',
    username: '',
    password: '',
    connection_type: 'postgresql'
  });
  const [formLoading, setFormLoading] = useState(false);
  const [formError, setFormError] = useState('');

  // API base URL
  const API_BASE = 'http://localhost:8000/api';

  // Fetch data from backend API
  const fetchData = async () => {
    try {
      setLoading(true);
      
      // Fetch connections
      const connectionsResponse = await fetch(`${API_BASE}/connections`);
      if (connectionsResponse.ok) {
        const connectionsData = await connectionsResponse.json();
        setConnections(connectionsData);
      }

      // Fetch agents
      const agentsResponse = await fetch(`${API_BASE}/agents`);
      if (agentsResponse.ok) {
        const agentsData = await agentsResponse.json();
        setAgents(agentsData);
      }

      // Fetch dashboard metrics
      const metricsResponse = await fetch(`${API_BASE}/dashboard/metrics`);
      if (metricsResponse.ok) {
        const metricsData = await metricsResponse.json();
        setDashboardMetrics(metricsData);
      }

    } catch (error) {
      console.error('Error fetching data:', error);
      // Fallback to mock data if API fails
      setConnections(mockConnections);
      setAgents(mockAgents);
    } finally {
      setLoading(false);
    }
  };

  // Handle form input changes
  const handleFormChange = (field, value) => {
    setConnectionForm(prev => ({
      ...prev,
      [field]: value
    }));
    setFormError(''); // Clear error when user starts typing
  };

  // Handle form submission
  const handleSubmitConnection = async (e) => {
    e.preventDefault();
    
    // Basic validation
    if (!connectionForm.name || !connectionForm.database || !connectionForm.username) {
      setFormError('Please fill in all required fields (Name, Database, Username)');
      return;
    }

    try {
      setFormLoading(true);
      setFormError('');

      const response = await fetch(`${API_BASE}/connections`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(connectionForm)
      });

      if (response.ok) {
        // Success - refresh connections and reset form
        await fetchData();
        setConnectionForm({
          name: '',
          host: 'localhost',
          port: '5432',
          database: '',
          username: '',
          password: '',
          connection_type: 'postgresql'
        });
        setShowConnectionForm(false);
      } else {
        const errorData = await response.json();
        setFormError(errorData.detail || 'Failed to create connection');
      }
    } catch (error) {
      setFormError('Network error. Please check if the backend is running.');
    } finally {
      setFormLoading(false);
    }
  };

  // Initial data fetch
  useEffect(() => {
    fetchData();
    
    // Refresh data every 30 seconds
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, []);

  // Simulate agent activity
  useEffect(() => {
    const interval = setInterval(() => {
      const agentNames = ['Data Profiler', 'Quality Validator', 'Lineage Tracker'];
      const activities = ['Profiling table', 'Validating data', 'Tracking lineage', 'Detecting anomalies'];
      
      setAgentLogs(prev => [
        ...prev.slice(-9),
        {
          id: Date.now(),
          agent: agentNames[Math.floor(Math.random() * agentNames.length)],
          activity: activities[Math.floor(Math.random() * activities.length)],
          timestamp: new Date().toLocaleTimeString()
        }
      ]);
    }, 3000);

    return () => clearInterval(interval);
  }, []);

  const Sidebar = () => (
    <div className="w-72 bg-slate-800 text-white flex flex-col shadow-xl">
      <div className="p-6 border-b border-slate-700">
        <div className="flex items-center space-x-3">
          <div className="w-10 h-10 bg-blue-600 rounded-lg flex items-center justify-center">
            <Database className="w-6 h-6 text-white" />
          </div>
          <div>
            <h1 className="text-lg font-bold text-white">DataIQ Platform</h1>
            <p className="text-xs text-slate-400">Multi-Agent Data Quality</p>
          </div>
        </div>
      </div>
      
      <nav className="flex-1 px-4 py-6 space-y-1">
        {[
          { id: 'dashboard', icon: Home, label: 'Dashboard' },
          { id: 'catalog', icon: Layers, label: 'Data Catalog' },
          { id: 'quality', icon: Target, label: 'Data Quality' },
          { id: 'lineage', icon: Network, label: 'Lineage' },
          { id: 'agents', icon: Bot, label: 'AI Agents' },
          { id: 'governance', icon: Shield, label: 'Governance' },
          { id: 'connections', icon: Database, label: 'Connections' }
        ].map(({ id, icon: Icon, label }) => (
          <button
            key={id}
            onClick={() => setActiveTab(id)}
            className={`w-full flex items-center space-x-3 px-4 py-3 rounded-lg text-left transition-all duration-200 ${
              activeTab === id 
                ? 'bg-blue-600 text-white shadow-md' 
                : 'text-slate-300 hover:bg-slate-700 hover:text-white'
            }`}
          >
            <Icon className="w-5 h-5 flex-shrink-0" />
            <span className="font-medium">{label}</span>
          </button>
        ))}
      </nav>
    </div>
  );

  const Header = () => (
    <div className="bg-white border-b border-slate-200 px-8 py-4 flex items-center justify-between shadow-sm">
      <div className="flex items-center space-x-4">
        <div className="relative">
          <Search className="w-5 h-5 absolute left-3 top-3 text-slate-400" />
          <input
            type="text"
            placeholder="Search tables, columns, and data assets..."
            className="pl-10 pr-4 py-2 w-96 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
        </div>
      </div>
      
      <div className="flex items-center space-x-4">
        <button className="p-2 hover:bg-slate-100 rounded-lg relative">
          <Bell className="w-5 h-5" />
          <span className="absolute -top-1 -right-1 w-3 h-3 bg-red-500 rounded-full"></span>
        </button>
        <button className="flex items-center space-x-2 p-2 hover:bg-slate-100 rounded-lg">
          <User className="w-5 h-5" />
          <span>Admin User</span>
        </button>
      </div>
    </div>
  );

  const Dashboard = () => (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-slate-800">Data Intelligence Dashboard</h2>
          <p className="text-slate-600">Real-time insights from your multi-agent data quality system</p>
        </div>
        <div className="flex space-x-3">
          <button className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center space-x-2">
            <RefreshCw className="w-4 h-4" />
            <span>Refresh</span>
          </button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-slate-600 text-sm">Data Sources</p>
              <p className="text-2xl font-bold text-slate-800">{connections.length}</p>
              <p className="text-green-600 text-sm">↑ {connections.filter(c => c.status === 'connected').length} connected</p>
            </div>
            <Database className="w-10 h-10 text-blue-500" />
          </div>
        </div>

        <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-slate-600 text-sm">Data Quality Score</p>
              <p className="text-2xl font-bold text-slate-800">91.3%</p>
              <p className="text-green-600 text-sm">↑ 2.1% from last week</p>
            </div>
            <Target className="w-10 h-10 text-green-500" />
          </div>
        </div>

        <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-slate-600 text-sm">Active Agents</p>
              <p className="text-2xl font-bold text-slate-800">{mockAgents.filter(a => a.status === 'active').length}</p>
              <p className="text-blue-600 text-sm">Running continuously</p>
            </div>
            <Bot className="w-10 h-10 text-purple-500" />
          </div>
        </div>

        <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-slate-600 text-sm">Issues Detected</p>
              <p className="text-2xl font-bold text-slate-800">{mockIssues.length}</p>
              <p className="text-orange-600 text-sm">Requires attention</p>
            </div>
            <AlertTriangle className="w-10 h-10 text-orange-500" />
          </div>
        </div>
      </div>

      {/* Agent Activity & Data Quality */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-slate-800">Agent Activity Feed</h3>
            <Activity className="w-5 h-5 text-slate-400" />
          </div>
          <div className="space-y-3 max-h-64 overflow-y-auto">
            {agentLogs.map(log => (
              <div key={log.id} className="flex items-center space-x-3 p-3 bg-slate-50 rounded-lg">
                <Bot className="w-4 h-4 text-blue-500 flex-shrink-0" />
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-slate-800 truncate">{log.agent}</p>
                  <p className="text-xs text-slate-600">{log.activity}</p>
                </div>
                <span className="text-xs text-slate-400">{log.timestamp}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-slate-800">Data Quality Issues</h3>
            <AlertTriangle className="w-5 h-5 text-orange-500" />
          </div>
          <div className="space-y-3">
            {mockIssues.map(issue => (
              <div key={issue.id} className="flex items-center justify-between p-3 border border-slate-200 rounded-lg">
                <div className="flex-1">
                  <div className="flex items-center space-x-2 mb-1">
                    <span className="text-sm font-medium text-slate-800">{issue.table}</span>
                    <span className={`px-2 py-1 text-xs rounded-full ${
                      issue.severity === 'high' ? 'bg-red-100 text-red-800' :
                      issue.severity === 'medium' ? 'bg-orange-100 text-orange-800' :
                      'bg-yellow-100 text-yellow-800'
                    }`}>
                      {issue.severity}
                    </span>
                  </div>
                  <p className="text-xs text-slate-600">{issue.description}</p>
                </div>
                <div className="text-right">
                  <p className="text-sm font-medium text-slate-800">{issue.count}</p>
                  <p className="text-xs text-slate-600">records</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Recent Tables */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
        <h3 className="text-lg font-semibold text-slate-800 mb-4">Recently Profiled Tables</h3>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-slate-200">
                <th className="text-left py-3 px-4 font-medium text-slate-600">Table</th>
                <th className="text-left py-3 px-4 font-medium text-slate-600">Connection</th>
                <th className="text-left py-3 px-4 font-medium text-slate-600">Records</th>
                <th className="text-left py-3 px-4 font-medium text-slate-600">Quality Score</th>
                <th className="text-left py-3 px-4 font-medium text-slate-600">Last Profiled</th>
                <th className="text-left py-3 px-4 font-medium text-slate-600">Popularity</th>
              </tr>
            </thead>
            <tbody>
              {mockTables.map(table => (
                <tr key={table.id} className="border-b border-slate-100 hover:bg-slate-50">
                  <td className="py-3 px-4">
                    <div>
                      <p className="font-medium text-slate-800">{table.schema}.{table.name}</p>
                      <p className="text-sm text-slate-600">{table.description}</p>
                    </div>
                  </td>
                  <td className="py-3 px-4 text-slate-600">{table.connection}</td>
                  <td className="py-3 px-4 text-slate-600">{table.records.toLocaleString()}</td>
                  <td className="py-3 px-4">
                    <div className="flex items-center space-x-2">
                      <div className="w-12 bg-slate-200 rounded-full h-2">
                        <div 
                          className={`h-2 rounded-full ${table.quality >= 90 ? 'bg-green-500' : table.quality >= 80 ? 'bg-yellow-500' : 'bg-red-500'}`}
                          style={{ width: `${table.quality}%` }}
                        ></div>
                      </div>
                      <span className="text-sm text-slate-600">{table.quality}%</span>
                    </div>
                  </td>
                  <td className="py-3 px-4 text-slate-600">{table.lastProfiled}</td>
                  <td className="py-3 px-4">
                    <div className="flex items-center space-x-1">
                      <Star className="w-4 h-4 text-yellow-500 fill-current" />
                      <span className="text-sm text-slate-600">{table.popularity}</span>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );

  const DataCatalog = () => (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-slate-800">Data Catalog</h2>
        <div className="flex space-x-3">
          <button className="px-4 py-2 border border-slate-200 rounded-lg hover:bg-slate-50 flex items-center space-x-2">
            <Filter className="w-4 h-4" />
            <span>Filter</span>
          </button>
          <button className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center space-x-2">
            <Plus className="w-4 h-4" />
            <span>Add Asset</span>
          </button>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {mockTables.map(table => (
          <div key={table.id} className="bg-white p-6 rounded-xl shadow-sm border border-slate-200 hover:shadow-md transition-shadow cursor-pointer"
               onClick={() => setSelectedTable(table)}>
            <div className="flex items-start justify-between mb-3">
              <div className="flex items-center space-x-2">
                <Database className="w-5 h-5 text-blue-500" />
                <h3 className="font-semibold text-slate-800">{table.schema}.{table.name}</h3>
              </div>
              <div className="flex space-x-1">
                {table.tags.map(tag => (
                  <span key={tag} className="px-2 py-1 bg-blue-100 text-blue-800 text-xs rounded-full">
                    {tag}
                  </span>
                ))}
              </div>
            </div>
            
            <p className="text-slate-600 text-sm mb-4">{table.description}</p>
            
            <div className="space-y-2">
              <div className="flex items-center justify-between text-sm">
                <span className="text-slate-600">Records:</span>
                <span className="font-medium">{table.records.toLocaleString()}</span>
              </div>
              <div className="flex items-center justify-between text-sm">
                <span className="text-slate-600">Quality:</span>
                <div className="flex items-center space-x-2">
                  <div className="w-16 bg-slate-200 rounded-full h-1.5">
                    <div 
                      className={`h-1.5 rounded-full ${table.quality >= 90 ? 'bg-green-500' : table.quality >= 80 ? 'bg-yellow-500' : 'bg-red-500'}`}
                      style={{ width: `${table.quality}%` }}
                    ></div>
                  </div>
                  <span className="font-medium">{table.quality}%</span>
                </div>
              </div>
              <div className="flex items-center justify-between text-sm">
                <span className="text-slate-600">Owner:</span>
                <span className="font-medium">{table.owner}</span>
              </div>
              <div className="flex items-center justify-between text-sm">
                <span className="text-slate-600">Popularity:</span>
                <div className="flex items-center space-x-1">
                  <Star className="w-3 h-3 text-yellow-500 fill-current" />
                  <span className="font-medium">{table.popularity}</span>
                </div>
              </div>
            </div>
          </div>
        ))}
      </div>

      {selectedTable && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl p-6 max-w-4xl w-full mx-4 max-h-[90vh] overflow-y-auto">
            <div className="flex items-center justify-between mb-6">
              <h2 className="text-xl font-bold text-slate-800">{selectedTable.schema}.{selectedTable.name}</h2>
              <button onClick={() => setSelectedTable(null)}>
                <X className="w-6 h-6 text-slate-400" />
              </button>
            </div>
            
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div className="space-y-4">
                <div>
                  <h3 className="font-semibold text-slate-800 mb-2">Overview</h3>
                  <p className="text-slate-600">{selectedTable.description}</p>
                </div>
                
                <div>
                  <h3 className="font-semibold text-slate-800 mb-2">Metadata</h3>
                  <div className="space-y-2">
                    <div className="flex justify-between">
                      <span className="text-slate-600">Connection:</span>
                      <span>{selectedTable.connection}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-slate-600">Records:</span>
                      <span>{selectedTable.records.toLocaleString()}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-slate-600">Owner:</span>
                      <span>{selectedTable.owner}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-slate-600">Last Profiled:</span>
                      <span>{selectedTable.lastProfiled}</span>
                    </div>
                  </div>
                </div>
              </div>
              
              <div className="space-y-4">
                <div>
                  <h3 className="font-semibold text-slate-800 mb-2">Data Quality</h3>
                  <div className="flex items-center space-x-4">
                    <div className="flex-1 bg-slate-200 rounded-full h-3">
                      <div 
                        className={`h-3 rounded-full ${selectedTable.quality >= 90 ? 'bg-green-500' : selectedTable.quality >= 80 ? 'bg-yellow-500' : 'bg-red-500'}`}
                        style={{ width: `${selectedTable.quality}%` }}
                      ></div>
                    </div>
                    <span className="font-semibold text-lg">{selectedTable.quality}%</span>
                  </div>
                </div>
                
                <div>
                  <h3 className="font-semibold text-slate-800 mb-2">Tags</h3>
                  <div className="flex flex-wrap gap-2">
                    {selectedTable.tags.map(tag => (
                      <span key={tag} className="px-3 py-1 bg-blue-100 text-blue-800 text-sm rounded-full">
                        {tag}
                      </span>
                    ))}
                  </div>
                </div>
                
                <div>
                  <h3 className="font-semibold text-slate-800 mb-2">Popularity</h3>
                  <div className="flex items-center space-x-2">
                    {[1, 2, 3, 4, 5].map(star => (
                      <Star key={star} className={`w-5 h-5 ${star <= selectedTable.popularity ? 'text-yellow-500 fill-current' : 'text-slate-300'}`} />
                    ))}
                    <span className="text-slate-600 ml-2">{selectedTable.popularity}/5</span>
                  </div>
                </div>
              </div>
            </div>
            
            <div className="mt-6 pt-6 border-t border-slate-200">
              <h3 className="font-semibold text-slate-800 mb-3">Sample Columns</h3>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-slate-200">
                      <th className="text-left py-2 font-medium text-slate-600">Column</th>
                      <th className="text-left py-2 font-medium text-slate-600">Type</th>
                      <th className="text-left py-2 font-medium text-slate-600">Nullable</th>
                      <th className="text-left py-2 font-medium text-slate-600">Quality</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr className="border-b border-slate-100">
                      <td className="py-2">id</td>
                      <td className="py-2">INTEGER</td>
                      <td className="py-2">NO</td>
                      <td className="py-2">
                        <span className="px-2 py-1 bg-green-100 text-green-800 text-xs rounded-full">100%</span>
                      </td>
                    </tr>
                    <tr className="border-b border-slate-100">
                      <td className="py-2">email</td>
                      <td className="py-2">VARCHAR(255)</td>
                      <td className="py-2">YES</td>
                      <td className="py-2">
                        <span className="px-2 py-1 bg-orange-100 text-orange-800 text-xs rounded-full">92%</span>
                      </td>
                    </tr>
                    <tr className="border-b border-slate-100">
                      <td className="py-2">created_at</td>
                      <td className="py-2">TIMESTAMP</td>
                      <td className="py-2">NO</td>
                      <td className="py-2">
                        <span className="px-2 py-1 bg-green-100 text-green-800 text-xs rounded-full">100%</span>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );

  const QualityTab = () => (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-slate-800">Data Quality Dashboard</h2>
        <button className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center space-x-2">
          <Zap className="w-4 h-4" />
          <span>Run Quality Check</span>
        </button>
      </div>

      {/* Quality Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-slate-800">Overall Quality</h3>
            <Target className="w-6 h-6 text-green-500" />
          </div>
          <div className="text-center">
            <div className="text-4xl font-bold text-slate-800 mb-2">91.3%</div>
            <p className="text-slate-600">Across all tables</p>
            <div className="w-full bg-slate-200 rounded-full h-2 mt-3">
              <div className="bg-green-500 h-2 rounded-full" style={{ width: '91.3%' }}></div>
            </div>
          </div>
        </div>

        <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-slate-800">Issues Found</h3>
            <AlertTriangle className="w-6 h-6 text-orange-500" />
          </div>
          <div className="space-y-2">
            <div className="flex justify-between">
              <span className="text-red-600">Critical:</span>
              <span className="font-bold">2</span>
            </div>
            <div className="flex justify-between">
              <span className="text-orange-600">Warning:</span>
              <span className="font-bold">7</span>
            </div>
            <div className="flex justify-between">
              <span className="text-yellow-600">Info:</span>
              <span className="font-bold">12</span>
            </div>
          </div>
        </div>

        <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-slate-800">Trend</h3>
            <TrendingUp className="w-6 h-6 text-blue-500" />
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-green-600 mb-2">+2.1%</div>
            <p className="text-slate-600">vs last week</p>
            <p className="text-sm text-green-600 mt-1">Quality improving</p>
          </div>
        </div>
      </div>

      {/* Detailed Issues */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
        <h3 className="text-lg font-semibold text-slate-800 mb-4">Quality Issues by Table</h3>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-slate-200">
                <th className="text-left py-3 px-4 font-medium text-slate-600">Table</th>
                <th className="text-left py-3 px-4 font-medium text-slate-600">Issue Type</th>
                <th className="text-left py-3 px-4 font-medium text-slate-600">Severity</th>
                <th className="text-left py-3 px-4 font-medium text-slate-600">Records Affected</th>
                <th className="text-left py-3 px-4 font-medium text-slate-600">Actions</th>
              </tr>
            </thead>
            <tbody>
              {mockIssues.map(issue => (
                <tr key={issue.id} className="border-b border-slate-100 hover:bg-slate-50">
                  <td className="py-3 px-4 font-medium text-slate-800">{issue.table}</td>
                  <td className="py-3 px-4 text-slate-600">{issue.type}</td>
                  <td className="py-3 px-4">
                    <span className={`px-2 py-1 text-xs rounded-full ${
                      issue.severity === 'high' ? 'bg-red-100 text-red-800' :
                      issue.severity === 'medium' ? 'bg-orange-100 text-orange-800' :
                      'bg-yellow-100 text-yellow-800'
                    }`}>
                      {issue.severity}
                    </span>
                  </td>
                  <td className="py-3 px-4 text-slate-600">{issue.count.toLocaleString()}</td>
                  <td className="py-3 px-4">
                    <div className="flex space-x-2">
                      <button className="px-3 py-1 bg-blue-100 text-blue-700 rounded text-sm hover:bg-blue-200">
                        Fix
                      </button>
                      <button className="px-3 py-1 bg-slate-100 text-slate-700 rounded text-sm hover:bg-slate-200">
                        Ignore
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Quality Rules */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-slate-800">Active Quality Rules</h3>
          <button className="px-4 py-2 border border-slate-200 rounded-lg hover:bg-slate-50">
            Add Rule
          </button>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="p-4 border border-slate-200 rounded-lg">
            <div className="flex items-center justify-between mb-2">
              <h4 className="font-medium text-slate-800">Null Value Check</h4>
              <CheckCircle className="w-5 h-5 text-green-500" />
            </div>
            <p className="text-sm text-slate-600">Ensures critical fields are not null</p>
            <p className="text-xs text-slate-500 mt-1">Applied to 15 tables</p>
          </div>
          
          <div className="p-4 border border-slate-200 rounded-lg">
            <div className="flex items-center justify-between mb-2">
              <h4 className="font-medium text-slate-800">Data Freshness</h4>
              <CheckCircle className="w-5 h-5 text-green-500" />
            </div>
            <p className="text-sm text-slate-600">Monitors data update frequency</p>
            <p className="text-xs text-slate-500 mt-1">Applied to 8 tables</p>
          </div>
          
          <div className="p-4 border border-slate-200 rounded-lg">
            <div className="flex items-center justify-between mb-2">
              <h4 className="font-medium text-slate-800">Duplicate Detection</h4>
              <AlertTriangle className="w-5 h-5 text-orange-500" />
            </div>
            <p className="text-sm text-slate-600">Identifies duplicate records</p>
            <p className="text-xs text-slate-500 mt-1">Applied to 12 tables</p>
          </div>
          
          <div className="p-4 border border-slate-200 rounded-lg">
            <div className="flex items-center justify-between mb-2">
              <h4 className="font-medium text-slate-800">Format Validation</h4>
              <CheckCircle className="w-5 h-5 text-green-500" />
            </div>
            <p className="text-sm text-slate-600">Validates data format patterns</p>
            <p className="text-xs text-slate-500 mt-1">Applied to 20 tables</p>
          </div>
        </div>
      </div>
    </div>
  );

  const LineageTab = () => {
    const canvasRef = useRef(null);
    
    useEffect(() => {
      if (canvasRef.current) {
        const canvas = canvasRef.current;
        const ctx = canvas.getContext('2d');
        canvas.width = canvas.offsetWidth;
        canvas.height = canvas.offsetHeight;
        
        // Simple lineage visualization
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        
        // Draw connections
        ctx.strokeStyle = '#3b82f6';
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.moveTo(150, 100);
        ctx.lineTo(250, 100);
        ctx.moveTo(350, 100);
        ctx.lineTo(450, 100);
        ctx.moveTo(300, 150);
        ctx.lineTo(300, 180);
        ctx.stroke();
        
        // Draw nodes
        const nodes = [
          { x: 100, y: 100, label: 'users', color: '#10b981' },
          { x: 300, y: 100, label: 'user_profile', color: '#f59e0b' },
          { x: 500, y: 100, label: 'analytics_users', color: '#ef4444' },
          { x: 300, y: 200, label: 'user_events', color: '#f59e0b' }
        ];
        
        nodes.forEach(node => {
          ctx.fillStyle = node.color;
          ctx.beginPath();
          ctx.arc(node.x, node.y, 20, 0, 2 * Math.PI);
          ctx.fill();
          
          ctx.fillStyle = '#1f2937';
          ctx.font = '12px sans-serif';
          ctx.textAlign = 'center';
          ctx.fillText(node.label, node.x, node.y + 35);
        });
      }
    }, []);
    
    return (
      <div className="p-6 space-y-6">
        <div className="flex items-center justify-between">
          <h2 className="text-2xl font-bold text-slate-800">Data Lineage</h2>
          <div className="flex space-x-3">
            <button className="px-4 py-2 border border-slate-200 rounded-lg hover:bg-slate-50">
              Export
            </button>
            <button className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700">
              Trace Impact
            </button>
          </div>
        </div>
        
        <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-slate-800">Lineage Graph</h3>
            <div className="flex items-center space-x-4">
              <div className="flex items-center space-x-2">
                <div className="w-4 h-4 bg-green-500 rounded-full"></div>
                <span className="text-sm text-slate-600">Source</span>
              </div>
              <div className="flex items-center space-x-2">
                <div className="w-4 h-4 bg-yellow-500 rounded-full"></div>
                <span className="text-sm text-slate-600">Transform</span>
              </div>
              <div className="flex items-center space-x-2">
                <div className="w-4 h-4 bg-red-500 rounded-full"></div>
                <span className="text-sm text-slate-600">Target</span>
              </div>
            </div>
          </div>
          <canvas 
            ref={canvasRef}
            className="w-full h-64 border border-slate-200 rounded-lg"
          />
        </div>
        
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
            <h3 className="text-lg font-semibold text-slate-800 mb-4">Upstream Dependencies</h3>
            <div className="space-y-3">
              {['users', 'user_profiles', 'user_sessions'].map(table => (
                <div key={table} className="flex items-center space-x-3 p-3 border border-slate-200 rounded-lg">
                  <Database className="w-5 h-5 text-green-500" />
                  <div className="flex-1">
                    <p className="font-medium text-slate-800">{table}</p>
                    <p className="text-sm text-slate-600">Source table</p>
                  </div>
                  <GitBranch className="w-4 h-4 text-slate-400" />
                </div>
              ))}
            </div>
          </div>
          
          <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
            <h3 className="text-lg font-semibold text-slate-800 mb-4">Downstream Impact</h3>
            <div className="space-y-3">
              {['analytics_users', 'user_reports', 'dashboard_metrics'].map(table => (
                <div key={table} className="flex items-center space-x-3 p-3 border border-slate-200 rounded-lg">
                  <Database className="w-5 h-5 text-red-500" />
                  <div className="flex-1">
                    <p className="font-medium text-slate-800">{table}</p>
                    <p className="text-sm text-slate-600">Target table</p>
                  </div>
                  <GitBranch className="w-4 h-4 text-slate-400" />
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    );
  };

  const AgentsTab = () => (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-slate-800">AI Agents Management</h2>
        <button className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center space-x-2">
          <Plus className="w-4 h-4" />
          <span>Deploy Agent</span>
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {mockAgents.map(agent => (
          <div key={agent.id} className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center space-x-3">
                <div className={`w-3 h-3 rounded-full ${agent.status === 'active' ? 'bg-green-500' : 'bg-slate-400'}`}></div>
                <h3 className="font-semibold text-slate-800">{agent.name}</h3>
              </div>
              <div className="flex space-x-2">
                <button className="p-2 hover:bg-slate-100 rounded">
                  {agent.status === 'active' ? <Pause className="w-4 h-4" /> : <Play className="w-4 h-4" />}
                </button>
                <button className="p-2 hover:bg-slate-100 rounded">
                  <Settings className="w-4 h-4" />
                </button>
              </div>
            </div>
            
            <div className="space-y-3">
              <div className="flex justify-between">
                <span className="text-slate-600">Type:</span>
                <span className="capitalize">{agent.type}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-slate-600">Status:</span>
                <span className={`capitalize ${agent.status === 'active' ? 'text-green-600' : 'text-slate-600'}`}>
                  {agent.status}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-slate-600">Last Run:</span>
                <span>{agent.lastRun}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-slate-600">Tasks Completed:</span>
                <span className="font-medium">{agent.tasksCompleted}</span>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Agent Logs */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-slate-800">Agent Execution Logs</h3>
          <div className="flex items-center space-x-2">
            <div className="flex space-x-1">
              <div className="w-3 h-3 bg-red-500 rounded-full"></div>
              <div className="w-3 h-3 bg-yellow-500 rounded-full"></div>
              <div className="w-3 h-3 bg-green-500 rounded-full"></div>
            </div>
          </div>
        </div>
        
        {/* Terminal Window */}
        <div className="bg-black rounded-lg overflow-hidden border border-slate-700">
          {/* Terminal Header */}
          <div className="bg-slate-800 px-4 py-2 flex items-center justify-between">
            <div className="flex items-center space-x-2">
              <span className="text-slate-300 text-sm font-mono">bash-3.2$</span>
            </div>
            <div className="text-slate-400 text-xs">
              DataIQ Agent Monitor
            </div>
          </div>
          
          {/* Terminal Content */}
          <div className="p-4 max-h-64 overflow-y-auto">
            <div className="font-mono text-sm space-y-1">
              <div className="text-green-400">
                <span className="text-slate-500">$</span> tail -f /var/log/dataiq/agents.log
              </div>
              <div className="text-slate-400 mb-2">
                Monitoring agent activity...
              </div>
              
              {agentLogs.length === 0 ? (
                <div className="text-green-400">
                  <span className="text-slate-500">[{new Date().toLocaleTimeString()}]</span> 
                  <span className="text-yellow-400"> INFO</span> Waiting for agent activity...
                </div>
              ) : (
                agentLogs.map((log, index) => (
                  <div key={log.id} className="text-green-400">
                    <span className="text-slate-500">[{log.timestamp}]</span> 
                    <span className="text-blue-400"> {log.agent}</span>
                    <span className="text-white">:</span> 
                    <span className="text-green-300"> {log.activity}</span>
                    {index === agentLogs.length - 1 && (
                      <span className="animate-pulse text-green-400">_</span>
                    )}
                  </div>
                ))
              )}
              
              {/* Blinking cursor */}
              <div className="flex items-center">
                <span className="text-slate-500">$</span>
                <span className="ml-1 animate-pulse text-green-400">█</span>
              </div>
            </div>
          </div>
        </div>
        
        <div className="mt-3 flex items-center justify-between text-sm text-slate-600">
          <div className="flex items-center space-x-4">
            <span>Live monitoring enabled</span>
            <div className="flex items-center space-x-1">
              <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
              <span>Connected</span>
            </div>
          </div>
          <div className="flex space-x-2">
            <button className="px-3 py-1 bg-slate-100 text-slate-700 rounded text-xs hover:bg-slate-200">
              Clear
            </button>
            <button className="px-3 py-1 bg-slate-100 text-slate-700 rounded text-xs hover:bg-slate-200">
              Download
            </button>
          </div>
        </div>
      </div>
    </div>
  );

  const ConnectionsTab = () => (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-slate-800">Data Connections</h2>
        <button 
          onClick={() => setShowConnectionForm(!showConnectionForm)}
          className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center space-x-2"
        >
          <Plus className="w-4 h-4" />
          <span>{showConnectionForm ? 'Cancel' : 'New Connection'}</span>
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {loading ? (
          <div className="col-span-full text-center py-8">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto"></div>
            <p className="text-slate-600 mt-2">Loading connections...</p>
          </div>
        ) : connections.length === 0 ? (
          <div className="col-span-full text-center py-8">
            <Database className="w-12 h-12 text-slate-400 mx-auto mb-4" />
            <p className="text-slate-600">No database connections found</p>
            <p className="text-slate-500 text-sm">Add a connection to get started</p>
          </div>
        ) : (
          connections.map(conn => (
            <div key={conn.id} className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center space-x-3">
                <Database className="w-8 h-8 text-blue-500" />
                <div>
                  <h3 className="font-semibold text-slate-800">{conn.name}</h3>
                  <p className="text-sm text-slate-600">{conn.connection_type}</p>
                </div>
              </div>
              <div className={`w-3 h-3 rounded-full ${conn.status === 'connected' ? 'bg-green-500' : 'bg-red-500'}`}></div>
            </div>
            
            <div className="space-y-2">
              <div className="flex justify-between">
                <span className="text-slate-600">Database:</span>
                <span className="text-sm">{conn.database}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-slate-600">Host:</span>
                <span className="text-sm">{conn.host}:{conn.port}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-slate-600">Status:</span>
                <span className={`capitalize ${conn.status === 'connected' ? 'text-green-600' : 'text-red-600'}`}>
                  {conn.status}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-slate-600">Created:</span>
                <span className="text-sm">{new Date(conn.created_at).toLocaleDateString()}</span>
              </div>
            </div>
            
            <div className="flex space-x-2 mt-4">
              <button className="flex-1 px-3 py-2 bg-blue-100 text-blue-700 rounded-lg hover:bg-blue-200 text-sm">
                Test
              </button>
              <button className="flex-1 px-3 py-2 bg-slate-100 text-slate-700 rounded-lg hover:bg-slate-200 text-sm">
                Configure
              </button>
            </div>
            </div>
          ))
        )}
      </div>

      {/* Connection Form */}
      {showConnectionForm && (
        <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
          <h3 className="text-lg font-semibold text-slate-800 mb-4">Add PostgreSQL Connection</h3>
          
          {formError && (
            <div className="mb-4 p-3 bg-red-100 border border-red-200 text-red-700 rounded-lg">
              {formError}
            </div>
          )}
          
          <form onSubmit={handleSubmitConnection}>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <input
                type="text"
                placeholder="Connection Name *"
                value={connectionForm.name}
                onChange={(e) => handleFormChange('name', e.target.value)}
                className="px-4 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
              <input
                type="text"
                placeholder="Host"
                value={connectionForm.host}
                onChange={(e) => handleFormChange('host', e.target.value)}
                className="px-4 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <input
                type="text"
                placeholder="Port"
                value={connectionForm.port}
                onChange={(e) => handleFormChange('port', e.target.value)}
                className="px-4 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <input
                type="text"
                placeholder="Database Name *"
                value={connectionForm.database}
                onChange={(e) => handleFormChange('database', e.target.value)}
                className="px-4 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
              <input
                type="text"
                placeholder="Username *"
                value={connectionForm.username}
                onChange={(e) => handleFormChange('username', e.target.value)}
                className="px-4 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
              <input
                type="password"
                placeholder="Password (optional)"
                value={connectionForm.password}
                onChange={(e) => handleFormChange('password', e.target.value)}
                className="px-4 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div className="flex space-x-3 mt-4">
              <button 
                type="submit" 
                disabled={formLoading}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {formLoading ? 'Testing & Saving...' : 'Test & Save'}
              </button>
              <button 
                type="button"
                onClick={() => setShowConnectionForm(false)}
                className="px-4 py-2 border border-slate-200 rounded-lg hover:bg-slate-50"
              >
                Cancel
              </button>
            </div>
          </form>
        </div>
      )}
    </div>
  );

  const GovernanceTab = () => (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-slate-800">Data Governance</h2>
        <button className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center space-x-2">
          <Shield className="w-4 h-4" />
          <span>Create Policy</span>
        </button>
      </div>

      {/* Governance Overview */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
          <div className="flex items-center justify-between mb-4">
            <h3 className="font-semibold text-slate-800">Active Policies</h3>
            <Shield className="w-6 h-6 text-blue-500" />
          </div>
          <div className="text-2xl font-bold text-slate-800 mb-2">24</div>
          <p className="text-slate-600">Enforced across platform</p>
        </div>

        <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
          <div className="flex items-center justify-between mb-4">
            <h3 className="font-semibold text-slate-800">Compliance Score</h3>
            <CheckCircle className="w-6 h-6 text-green-500" />
          </div>
          <div className="text-2xl font-bold text-slate-800 mb-2">96.5%</div>
          <p className="text-slate-600">Across all data assets</p>
        </div>

        <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
          <div className="flex items-center justify-between mb-4">
            <h3 className="font-semibold text-slate-800">Policy Violations</h3>
            <AlertTriangle className="w-6 h-6 text-orange-500" />
          </div>
          <div className="text-2xl font-bold text-slate-800 mb-2">3</div>
          <p className="text-slate-600">Require immediate action</p>
        </div>
      </div>

      {/* Data Classification */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
        <h3 className="text-lg font-semibold text-slate-800 mb-4">Data Classification</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <div className="p-4 bg-red-50 border border-red-200 rounded-lg">
            <div className="flex items-center space-x-2 mb-2">
              <div className="w-3 h-3 bg-red-500 rounded-full"></div>
              <span className="font-medium text-slate-800">Highly Sensitive</span>
            </div>
            <p className="text-2xl font-bold text-slate-800">15</p>
            <p className="text-sm text-slate-600">tables</p>
          </div>
          
          <div className="p-4 bg-orange-50 border border-orange-200 rounded-lg">
            <div className="flex items-center space-x-2 mb-2">
              <div className="w-3 h-3 bg-orange-500 rounded-full"></div>
              <span className="font-medium text-slate-800">Sensitive</span>
            </div>
            <p className="text-2xl font-bold text-slate-800">32</p>
            <p className="text-sm text-slate-600">tables</p>
          </div>
          
          <div className="p-4 bg-yellow-50 border border-yellow-200 rounded-lg">
            <div className="flex items-center space-x-2 mb-2">
              <div className="w-3 h-3 bg-yellow-500 rounded-full"></div>
              <span className="font-medium text-slate-800">Internal</span>
            </div>
            <p className="text-2xl font-bold text-slate-800">87</p>
            <p className="text-sm text-slate-600">tables</p>
          </div>
          
          <div className="p-4 bg-green-50 border border-green-200 rounded-lg">
            <div className="flex items-center space-x-2 mb-2">
              <div className="w-3 h-3 bg-green-500 rounded-full"></div>
              <span className="font-medium text-slate-800">Public</span>
            </div>
            <p className="text-2xl font-bold text-slate-800">23</p>
            <p className="text-sm text-slate-600">tables</p>
          </div>
        </div>
      </div>

      {/* Recent Governance Activity */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
        <h3 className="text-lg font-semibold text-slate-800 mb-4">Recent Governance Activity</h3>
        <div className="space-y-4">
          <div className="flex items-center space-x-4 p-4 bg-slate-50 rounded-lg">
            <Shield className="w-5 h-5 text-blue-500" />
            <div className="flex-1">
              <p className="font-medium text-slate-800">New PII protection policy applied</p>
              <p className="text-sm text-slate-600">Applied to users and customer_details tables</p>
            </div>
            <span className="text-sm text-slate-500">2 hours ago</span>
          </div>
          
          <div className="flex items-center space-x-4 p-4 bg-slate-50 rounded-lg">
            <AlertTriangle className="w-5 h-5 text-orange-500" />
            <div className="flex-1">
              <p className="font-medium text-slate-800">Data retention policy violation detected</p>
              <p className="text-sm text-slate-600">audit_logs table exceeds 7-year retention limit</p>
            </div>
            <span className="text-sm text-slate-500">1 day ago</span>
          </div>
          
          <div className="flex items-center space-x-4 p-4 bg-slate-50 rounded-lg">
            <CheckCircle className="w-5 h-5 text-green-500" />
            <div className="flex-1">
              <p className="font-medium text-slate-800">Compliance audit completed</p>
              <p className="text-sm text-slate-600">All financial data tables passed GDPR compliance check</p>
            </div>
            <span className="text-sm text-slate-500">2 days ago</span>
          </div>
        </div>
      </div>
    </div>
  );

  const renderActiveTab = () => {
    switch (activeTab) {
      case 'dashboard':
        return <Dashboard />;
      case 'catalog':
        return <DataCatalog />;
      case 'quality':
        return <QualityTab />;
      case 'lineage':
        return <LineageTab />;
      case 'agents':
        return <AgentsTab />;
      case 'governance':
        return <GovernanceTab />;
      case 'connections':
        return <ConnectionsTab />;
      default:
        return <Dashboard />;
    }
  };

  return (
    <div className="min-h-screen bg-slate-50 flex">
      <Sidebar />
      <div className="flex-1 flex flex-col">
        <Header />
        <main className="flex-1 overflow-y-auto">
          {renderActiveTab()}
        </main>
      </div>
    </div>
  );
};

export default DataIntelligencePlatform;