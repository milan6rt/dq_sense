import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Filter, Settings, Plus, Minus, Maximize2, Eye, BarChart3, Database, Move, ZoomIn, ZoomOut, RotateCcw } from 'lucide-react';

const AlationStyleLineage = () => {
  const [selectedColumn, setSelectedColumn] = useState('customer_id');
  const [useCompoundLayout, setUseCompoundLayout] = useState(false);
  const [expandedTables, setExpandedTables] = useState([]);
  const [lineageData, setLineageData] = useState({ tables: [], connections: [] });
  const [loading, setLoading] = useState(true);
  
  // Simple pan and zoom state
  const [transform, setTransform] = useState({
    x: 0,
    y: 0,
    scale: 1
  });
  const [isDragging, setIsDragging] = useState(false);
  const [dragStart, setDragStart] = useState({ x: 0, y: 0 });
  const [transformStart, setTransformStart] = useState({ x: 0, y: 0 });
  
  const svgRef = useRef();
  const containerRef = useRef();

  // Fetch lineage data
  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        const response = await fetch('http://localhost:8000/api/lineage/alation-style');
        if (response.ok) {
          const data = await response.json();
          setLineageData(data);
          
          // Auto-expand all tables
          if (data.tables && data.tables.length > 0) {
            setExpandedTables(data.tables.map(table => table.id));
            
            // Set default selected column
            const firstTable = data.tables[0];
            if (firstTable && firstTable.columns && firstTable.columns.length > 0) {
              setSelectedColumn(firstTable.columns[0]);
            }
            
            // Auto-fit to view with delay to ensure DOM is fully rendered
            setTimeout(() => fitToView(data.tables), 500);
          }
        }
      } catch (error) {
        console.error('Error fetching lineage data:', error);
      } finally {
        setLoading(false);
      }
    };
    
    fetchData();
  }, []);

  // Mouse event handlers
  const handleMouseDown = useCallback((e) => {
    if (e.button === 0) {
      setIsDragging(true);
      const rect = svgRef.current.getBoundingClientRect();
      setDragStart({ 
        x: e.clientX - rect.left, 
        y: e.clientY - rect.top 
      });
      setTransformStart({ x: transform.x, y: transform.y });
      e.preventDefault();
    }
  }, [transform.x, transform.y]);

  const handleMouseMove = useCallback((e) => {
    if (isDragging && svgRef.current) {
      const rect = svgRef.current.getBoundingClientRect();
      const currentX = e.clientX - rect.left;
      const currentY = e.clientY - rect.top;
      
      const deltaX = currentX - dragStart.x;
      const deltaY = currentY - dragStart.y;
      
      setTransform(prev => ({
        ...prev,
        x: transformStart.x + deltaX,
        y: transformStart.y + deltaY
      }));
    }
  }, [isDragging, dragStart, transformStart]);

  const handleMouseUp = useCallback(() => {
    setIsDragging(false);
  }, []);

  // Global mouse events
  useEffect(() => {
    if (isDragging) {
      const handleGlobalMouseMove = (e) => handleMouseMove(e);
      const handleGlobalMouseUp = () => handleMouseUp();
      
      document.addEventListener('mousemove', handleGlobalMouseMove);
      document.addEventListener('mouseup', handleGlobalMouseUp);
      
      return () => {
        document.removeEventListener('mousemove', handleGlobalMouseMove);
        document.removeEventListener('mouseup', handleGlobalMouseUp);
      };
    }
  }, [isDragging, handleMouseMove, handleMouseUp]);

  // Zoom functionality
  const handleWheel = useCallback((e) => {
    e.preventDefault();
    
    const rect = svgRef.current.getBoundingClientRect();
    const mouseX = e.clientX - rect.left;
    const mouseY = e.clientY - rect.top;
    
    const delta = -e.deltaY / 1000;
    const scaleFactor = Math.exp(delta);
    const newScale = Math.max(0.1, Math.min(3, transform.scale * scaleFactor));
    
    // Zoom towards mouse position
    const scaleRatio = newScale / transform.scale;
    const newX = transform.x - (mouseX - transform.x) * (scaleRatio - 1);
    const newY = transform.y - (mouseY - transform.y) * (scaleRatio - 1);
    
    setTransform({
      x: newX,
      y: newY,
      scale: newScale
    });
  }, [transform]);

  // Zoom controls
  const zoomIn = useCallback(() => {
    const newScale = Math.min(3, transform.scale * 1.3);
    const rect = svgRef.current?.getBoundingClientRect();
    if (rect) {
      const centerX = rect.width / 2;
      const centerY = rect.height / 2;
      const scaleRatio = newScale / transform.scale;
      
      setTransform({
        x: transform.x - (centerX - transform.x) * (scaleRatio - 1),
        y: transform.y - (centerY - transform.y) * (scaleRatio - 1),
        scale: newScale
      });
    }
  }, [transform]);

  const zoomOut = useCallback(() => {
    const newScale = Math.max(0.1, transform.scale / 1.3);
    const rect = svgRef.current?.getBoundingClientRect();
    if (rect) {
      const centerX = rect.width / 2;
      const centerY = rect.height / 2;
      const scaleRatio = newScale / transform.scale;
      
      setTransform({
        x: transform.x - (centerX - transform.x) * (scaleRatio - 1),
        y: transform.y - (centerY - transform.y) * (scaleRatio - 1),
        scale: newScale
      });
    }
  }, [transform]);

  // Calculate content bounds for dynamic sizing
  const getContentBounds = useCallback((tables = lineageData.tables) => {
    if (!tables || tables.length === 0) return { width: 800, height: 600, minX: 0, minY: 0, maxX: 800, maxY: 600 };
    
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    
    tables.forEach(table => {
      if (!table?.position) return;
      
      const x = table.position.x;
      const y = table.position.y;
      const width = 240;
      const height = expandedTables.includes(table.id) 
        ? Math.max(120, (table.columns?.length || 0) * 20 + 60) 
        : 80;
      
      minX = Math.min(minX, x);
      minY = Math.min(minY, y);
      maxX = Math.max(maxX, x + width);
      maxY = Math.max(maxY, y + height);
    });
    
    const padding = 100;
    return {
      minX: minX - padding,
      minY: minY - padding,
      maxX: maxX + padding,
      maxY: maxY + padding,
      width: maxX - minX + (padding * 2),
      height: maxY - minY + (padding * 2)
    };
  }, [lineageData.tables, expandedTables]);

  // Fit to view - ensure all content is visible
  const fitToView = useCallback((tables = lineageData.tables) => {
    if (!tables || tables.length === 0 || !svgRef.current) return;
    
    const rect = svgRef.current.getBoundingClientRect();
    const containerWidth = rect.width;
    const containerHeight = rect.height;
    
    if (containerWidth === 0 || containerHeight === 0) return;
    
    // Calculate actual content bounds
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    
    tables.forEach(table => {
      if (!table?.position) return;
      
      const x = table.position.x;
      const y = table.position.y;
      const width = 240;
      const height = expandedTables.includes(table.id) 
        ? Math.max(120, (table.columns?.length || 0) * 20 + 60) 
        : 80;
      
      minX = Math.min(minX, x);
      minY = Math.min(minY, y);
      maxX = Math.max(maxX, x + width);
      maxY = Math.max(maxY, y + height);
    });
    
    if (minX === Infinity) return;
    
    const contentWidth = maxX - minX;
    const contentHeight = maxY - minY;
    
    // Use more generous padding to ensure nothing is cut off
    const padding = 60;
    const availableWidth = containerWidth - padding * 2;
    const availableHeight = containerHeight - padding * 2;
    
    // Calculate scale to fit all content within available space
    const scaleX = availableWidth / contentWidth;
    const scaleY = availableHeight / contentHeight;
    let optimalScale = Math.min(scaleX, scaleY);
    
    // Ensure reasonable zoom levels (between 15% and 90%)
    optimalScale = Math.max(0.15, Math.min(optimalScale, 0.9));
    
    // Calculate scaled dimensions
    const scaledContentWidth = contentWidth * optimalScale;
    const scaledContentHeight = contentHeight * optimalScale;
    
    // Center horizontally and vertically with padding
    const x = (containerWidth - scaledContentWidth) / 2 - minX * optimalScale;
    const y = (containerHeight - scaledContentHeight) / 2 - minY * optimalScale;
    
    // Ensure minimum padding on all sides
    const finalX = Math.max(x, padding - minX * optimalScale);
    const finalY = Math.max(y, padding - minY * optimalScale);
    
    setTransform({
      x: finalX,
      y: finalY,
      scale: optimalScale
    });
  }, [lineageData.tables, expandedTables]);

  const resetView = useCallback(() => {
    setTransform({ x: 0, y: 0, scale: 1 });
  }, []);

  const toggleTable = useCallback((tableId) => {
    setExpandedTables(prev => {
      const isExpanded = prev.includes(tableId);
      const newExpanded = isExpanded 
        ? prev.filter(id => id !== tableId)
        : [...prev, tableId];
      
      // Refit after expansion changes
      setTimeout(() => fitToView(), 50);
      
      return newExpanded;
    });
  }, [fitToView]);

  // Helper functions
  const isColumnHighlighted = (tableId, column) => {
    return lineageData.connections?.some(conn => 
      (conn?.from?.table === tableId && conn?.from?.column === column) ||
      (conn?.to?.table === tableId && conn?.to?.column === column)
    ) && (column === selectedColumn);
  };

  const getConnectionPath = (fromTable, toTable) => {
    const fromX = fromTable.position.x + 240;
    const fromY = fromTable.position.y + 40;
    const toX = toTable.position.x;
    const toY = toTable.position.y + 40;
    
    const distance = Math.abs(toX - fromX);
    const controlOffset = Math.min(Math.max(distance * 0.4, 80), 200);
    
    const controlX1 = fromX + controlOffset;
    const controlX2 = toX - controlOffset;
    
    return `M ${fromX} ${fromY} C ${controlX1} ${fromY} ${controlX2} ${toY} ${toX} ${toY}`;
  };

  if (loading) {
    return (
      <div className="h-full bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading data lineage...</p>
        </div>
      </div>
    );
  }

  if (!lineageData.tables || lineageData.tables.length === 0) {
    return (
      <div className="h-full bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <Database className="w-16 h-16 text-gray-400 mx-auto mb-4" />
          <h3 className="text-lg font-semibold text-gray-800 mb-2">No Data Lineage Found</h3>
          <p className="text-gray-600 mb-4">Connect to a database to see lineage visualization.</p>
        </div>
      </div>
    );
  }

  const transformString = `translate(${transform.x}, ${transform.y}) scale(${transform.scale})`;

  return (
    <div className="h-screen bg-gray-50 flex flex-col">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <h2 className="text-xl font-semibold text-gray-800">Data Lineage</h2>
            <div className="flex items-center space-x-2 text-sm">
              <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
              <span className="text-gray-600">Live View • {lineageData.tables?.length || 0} tables</span>
            </div>
          </div>
          
          <div className="flex items-center space-x-4">
            <div className="flex items-center space-x-2">
              <span className="text-sm text-gray-600">Compound Layout</span>
              <button
                onClick={() => setUseCompoundLayout(!useCompoundLayout)}
                className={`w-10 h-6 rounded-full transition-colors ${
                  useCompoundLayout ? 'bg-blue-500' : 'bg-gray-300'
                }`}
              >
                <div className={`w-4 h-4 bg-white rounded-full transform transition-transform ${
                  useCompoundLayout ? 'translate-x-5' : 'translate-x-1'
                } mt-1 shadow-sm`}></div>
              </button>
            </div>
            
            <button className="text-blue-600 font-medium hover:bg-blue-50 px-3 py-2 rounded-lg">
              Impact Analysis
            </button>
          </div>
        </div>
      </div>

      {/* Controls */}
      <div className="bg-white border-b border-gray-200 px-6 py-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-6">
            <div className="flex items-center space-x-2">
              <Filter className="w-4 h-4 text-gray-500" />
              <span className="text-sm text-gray-700 font-medium">Filters</span>
            </div>
            <div className="text-sm text-gray-600">
              Showing <span className="font-semibold text-gray-800">{selectedColumn}</span> relationships
            </div>
          </div>
          
          <div className="flex items-center space-x-2">
            <div className="flex items-center space-x-1 bg-gray-100 rounded-lg p-1">
              <button 
                className="p-2 hover:bg-white rounded" 
                onClick={zoomOut}
                disabled={transform.scale <= 0.1}
              >
                <ZoomOut className="w-4 h-4 text-gray-600" />
              </button>
              <div className="px-3 py-1 text-sm font-medium text-gray-700 min-w-[60px] text-center">
                {Math.round(transform.scale * 100)}%
              </div>
              <button 
                className="p-2 hover:bg-white rounded" 
                onClick={zoomIn}
                disabled={transform.scale >= 3}
              >
                <ZoomIn className="w-4 h-4 text-gray-600" />
              </button>
            </div>
            
            <button 
              className="flex items-center space-x-1 text-sm text-gray-600 hover:bg-gray-100 px-3 py-2 rounded-lg"
              onClick={fitToView}
            >
              <Maximize2 className="w-4 h-4" />
              <span>Fit to View</span>
            </button>
            
            <button 
              className="flex items-center space-x-1 text-sm text-gray-600 hover:bg-gray-100 px-3 py-2 rounded-lg"
              onClick={resetView}
            >
              <RotateCcw className="w-4 h-4" />
              <span>Reset</span>
            </button>
          </div>
        </div>
      </div>

      {/* Main Visualization */}
      <div className="flex-1 p-6" style={{ minHeight: 0 }}>
        <div 
          ref={containerRef}
          className="relative w-full h-full bg-white rounded-lg border border-gray-200 shadow-sm"
          style={{ minHeight: '500px', overflow: 'visible' }}
        >
          <svg 
            ref={svgRef}
            className="w-full h-full select-none"
            style={{ 
              cursor: isDragging ? 'grabbing' : 'grab'
            }}
            onMouseDown={handleMouseDown}
            onWheel={handleWheel}
          >
            {/* Background */}
            <defs>
              <pattern id="grid" width="20" height="20" patternUnits="userSpaceOnUse">
                <path d="M 20 0 L 0 0 0 20" fill="none" stroke="#f3f4f6" strokeWidth="1"/>
              </pattern>
            </defs>
            <rect width="100%" height="100%" fill="url(#grid)" />
            
            <g transform={transformString}>
              {/* Connections */}
              {lineageData.connections?.map((conn, index) => {
                const fromTable = lineageData.tables?.find(t => t?.id === conn?.from?.table);
                const toTable = lineageData.tables?.find(t => t?.id === conn?.to?.table);
                
                if (!fromTable || !toTable) return null;
                
                const isHighlighted = conn.from.column === selectedColumn || conn.to.column === selectedColumn;
                const path = getConnectionPath(fromTable, toTable);
                
                return (
                  <g key={`conn-${index}`}>
                    <path
                      d={path}
                      fill="none"
                      stroke={isHighlighted ? "#3b82f6" : "#d1d5db"}
                      strokeWidth={isHighlighted ? "3" : "2"}
                      className="transition-all duration-200"
                    />
                    {isHighlighted && (
                      <circle
                        cx={(fromTable.position.x + 240 + toTable.position.x) / 2}
                        cy={(fromTable.position.y + 40 + toTable.position.y + 40) / 2}
                        r="4"
                        fill="#3b82f6"
                        className="animate-pulse"
                      />
                    )}
                  </g>
                );
              })}
              
              {/* Tables */}
              {lineageData.tables?.map((table) => {
                if (!table?.position) return null;
                
                const isExpanded = expandedTables.includes(table.id);
                const tableHeight = isExpanded ? Math.max(120, (table.columns?.length || 0) * 20 + 60) : 80;
                
                return (
                  <g key={table.id}>
                    {/* Table container */}
                    <rect
                      x={table.position.x}
                      y={table.position.y}
                      width="240"
                      height={tableHeight}
                      fill="white"
                      stroke="#e2e8f0"
                      strokeWidth="1"
                      rx="8"
                      className="drop-shadow-sm hover:stroke-blue-300 transition-colors"
                    />
                    
                    {/* Header */}
                    <rect
                      x={table.position.x}
                      y={table.position.y}
                      width="240"
                      height="40"
                      fill="#f8fafc"
                      stroke="#e2e8f0"
                      strokeWidth="1"
                      rx="8"
                    />
                    <rect
                      x={table.position.x}
                      y={table.position.y + 32}
                      width="240"
                      height="8"
                      fill="#f8fafc"
                    />
                    
                    {/* Type indicator */}
                    <circle
                      cx={table.position.x + 16}
                      cy={table.position.y + 20}
                      r="6"
                      fill={
                        table.type === 'source' ? '#10b981' :
                        table.type === 'target' ? '#f59e0b' : '#6366f1'
                      }
                    />
                    
                    {/* Table name */}
                    <text
                      x={table.position.x + 30}
                      y={table.position.y + 25}
                      fontSize="13"
                      fontWeight="600"
                      fill="#1e293b"
                    >
                      {table.name}
                    </text>
                    
                    {/* Expand button */}
                    <g 
                      className="cursor-pointer"
                      onClick={() => toggleTable(table.id)}
                    >
                      <circle
                        cx={table.position.x + 215}
                        cy={table.position.y + 20}
                        r="10"
                        fill={isExpanded ? "#3b82f6" : "#94a3b8"}
                        className="hover:opacity-80 transition-opacity"
                      />
                      <text
                        x={table.position.x + 215}
                        y={table.position.y + 25}
                        fontSize="12"
                        textAnchor="middle"
                        fill="white"
                        fontWeight="bold"
                      >
                        {isExpanded ? '−' : '+'}
                      </text>
                    </g>
                    
                    {/* Columns */}
                    {isExpanded && (
                      <g>
                        {(table.columns || []).map((column, colIndex) => {
                          const isHighlighted = isColumnHighlighted(table.id, column);
                          const y = table.position.y + 55 + colIndex * 20;
                          
                          return (
                            <g key={`${table.id}-${column}`}>
                              {isHighlighted && (
                                <rect
                                  x={table.position.x + 4}
                                  y={y - 10}
                                  width="232"
                                  height="18"
                                  fill="#dbeafe"
                                  rx="4"
                                />
                              )}
                              <circle
                                cx={table.position.x + 18}
                                cy={y - 1}
                                r="3"
                                fill={isHighlighted ? "#3b82f6" : "#94a3b8"}
                              />
                              <text
                                x={table.position.x + 28}
                                y={y + 2}
                                fontSize="12"
                                fill={isHighlighted ? "#1d4ed8" : "#64748b"}
                                className="cursor-pointer hover:fill-blue-600"
                                onClick={() => setSelectedColumn(column)}
                              >
                                {column}
                              </text>
                            </g>
                          );
                        })}
                      </g>
                    )}
                    
                    {/* Connection points */}
                    {table.type !== 'source' && (
                      <circle
                        cx={table.position.x - 5}
                        cy={table.position.y + 40}
                        r="4"
                        fill="#3b82f6"
                        stroke="white"
                        strokeWidth="2"
                      />
                    )}
                    {table.type !== 'target' && (
                      <circle
                        cx={table.position.x + 245}
                        cy={table.position.y + 40}
                        r="4"
                        fill="#3b82f6"
                        stroke="white"
                        strokeWidth="2"
                      />
                    )}
                  </g>
                );
              })}
            </g>
          </svg>
          
          {/* Instructions */}
          <div className="absolute bottom-4 left-4 bg-white/95 border border-gray-200 rounded-lg px-3 py-2 text-xs text-gray-600 shadow">
            <div className="flex items-center space-x-4">
              <span>• Drag to pan</span>
              <span>• Scroll to zoom</span>
              <span>• Click columns to highlight</span>
            </div>
          </div>

          {/* Performance indicator */}
          {isDragging && (
            <div className="absolute top-4 right-4 bg-blue-100 text-blue-800 px-3 py-1 rounded-full text-xs font-medium">
              Moving...
            </div>
          )}
        </div>
        
        {/* Legend */}
        <div className="mt-4 flex items-center justify-center space-x-8 text-sm">
          <div className="flex items-center space-x-2">
            <div className="w-4 h-2 bg-blue-500 rounded-full"></div>
            <span className="text-gray-700">Active Flow</span>
          </div>
          <div className="flex items-center space-x-2">
            <div className="w-4 h-2 bg-gray-300 rounded-full"></div>
            <span className="text-gray-700">Other Connections</span>
          </div>
          <div className="flex items-center space-x-2">
            <div className="w-3 h-3 bg-green-500 rounded-full"></div>
            <span className="text-gray-700">Source</span>
          </div>
          <div className="flex items-center space-x-2">
            <div className="w-3 h-3 bg-indigo-500 rounded-full"></div>
            <span className="text-gray-700">Transform</span>
          </div>
          <div className="flex items-center space-x-2">
            <div className="w-3 h-3 bg-amber-500 rounded-full"></div>
            <span className="text-gray-700">Target</span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default AlationStyleLineage;