/**
 * Join Optimization Visualizer
 * Handles visualization of the join optimization process
 */

class JoinOptimizationVisualizer {
    constructor(container) {
        this.container = container;
        this.originalGraph = null;
        this.optimizedGraph = null;
        this.initContainer();
    }
    
    /**
     * Initialize the container with necessary elements
     */
    initContainer() {
        // Clear any existing content
        this.container.innerHTML = '';
        
        // Create container for visualization
        const visualizationContainer = document.createElement('div');
        visualizationContainer.className = 'optimization-visualization';
        
        // Create side-by-side layout for original and optimized query plans
        const rowDiv = document.createElement('div');
        rowDiv.className = 'row optimization-row';
        
        // Original plan container
        const originalDiv = document.createElement('div');
        originalDiv.className = 'col-md-6 optimization-col';
        originalDiv.innerHTML = `
            <div class="optimization-header">
                <h4>After Predicate Pushdown</h4>
            </div>
            <div id="join-original-graph" class="graph-container"></div>
        `;
        
        // Optimized plan container
        const optimizedDiv = document.createElement('div');
        optimizedDiv.className = 'col-md-6 optimization-col';
        optimizedDiv.innerHTML = `
            <div class="optimization-header">
                <h4>After Join Optimization</h4>
            </div>
            <div id="join-optimized-graph" class="graph-container"></div>
        `;
        
        // Add columns to row
        rowDiv.appendChild(originalDiv);
        rowDiv.appendChild(optimizedDiv);
        
        // Add all elements to container
        visualizationContainer.appendChild(rowDiv);
        this.container.appendChild(visualizationContainer);
    }
    
    /**
     * Process optimization data from backend
     */
    processOptimizationData(data) {
        if (!data) {
            this.showError("No optimization data received");
            return;
        }
        
        try {
            // Create containers for original and optimized graphs
            this.createOptimizationGraphs(data.original_plan_json, data.optimized_plan_json);
        } catch (error) {
            console.error("Error processing join optimization data:", error);
            this.showError("Failed to process join optimization data: " + error.message);
        }
    }
    
    /**
     * Create side-by-side graph visualizations
     */
    createOptimizationGraphs(originalPlan, optimizedPlan) {
        console.log("Creating join optimization graphs with:", originalPlan, optimizedPlan);
        
        // Create specialized graph class for optimization view
        class OptimizationGraph {
            constructor(containerId, data, title) {
                this.container = document.getElementById(containerId);
                this.data = data;
                this.title = title;
                this.nodes = [];
                this.edges = [];
                this.init();
            }
            
            init() {
                // Clear container
                this.container.innerHTML = '';
                
                // Add title if provided
                if (this.title) {
                    const titleDiv = document.createElement('div');
                    titleDiv.className = 'graph-title';
                    titleDiv.textContent = this.title;
                    this.container.appendChild(titleDiv);
                }
                
                // Create the graph canvas
                const graphCanvas = document.createElement('div');
                graphCanvas.className = 'graph-canvas';
                graphCanvas.style.width = '100%';
                graphCanvas.style.height = '450px';
                graphCanvas.style.position = 'relative';
                this.container.appendChild(graphCanvas);
                
                // Create tooltip
                this.tooltip = document.createElement('div');
                this.tooltip.className = 'graph-tooltip';
                this.tooltip.style.opacity = '0';
                graphCanvas.appendChild(this.tooltip);
                
                // Process the data
                this.processData(this.data);
                
                // Calculate node positions
                this.calculateNodePositions();
                
                // Render the graph
                this.renderGraph(graphCanvas);
            }
            
            processData(node, parentId = null, depth = 0) {
                if (!node || typeof node !== 'object') return null;
                
                // Create node ID
                const nodeId = `${this.container.id}_node_${this.nodes.length}`;
                
                // Determine node type and style
                let nodeType = node.type || 'unknown';
                let nodeLabel = this.getNodeLabel(node);
                let nodeClass = this.getNodeClass(nodeType);
                let nodeSize = this.getNodeSize(nodeType);
                
                // Create node object
                const nodeObj = {
                    id: nodeId,
                    type: nodeType,
                    label: nodeLabel,
                    className: nodeClass,
                    size: nodeSize,
                    data: node,
                    depth: depth,
                    x: 0,
                    y: 0
                };
                
                // Add to nodes array
                this.nodes.push(nodeObj);
                
                // Connect to parent
                if (parentId) {
                    this.edges.push({
                        from: parentId,
                        to: nodeId
                    });
                }
                
                // Process children
                if (node.input) {
                    this.processData(node.input, nodeId, depth + 1);
                }
                
                if (node.left) {
                    this.processData(node.left, nodeId, depth + 1);
                }
                
                if (node.right) {
                    this.processData(node.right, nodeId, depth + 1);
                }
                
                // Handle subqueries
                if (node.type === 'subquery' && node.query) {
                    this.processData(node.query, nodeId, depth + 1);
                }
                
                return nodeId;
            }
            
            getNodeLabel(node) {
                switch (node.type) {
                    case 'select':
                        return 'SELECT';
                    case 'project':
                        return 'PROJECT';
                    case 'join':
                        return 'JOIN';
                    case 'base_relation':
                        if (node.tables && node.tables.length > 0) {
                            return `TABLE: ${node.tables[0].name}`;
                        }
                        return 'TABLE';
                    case 'subquery':
                        return `SUBQUERY: ${node.alias || ''}`;
                    default:
                        return node.type ? node.type.toUpperCase() : 'OP';
                }
            }
            
            getNodeClass(type) {
                const classMap = {
                    'select': 'graph-node-select',
                    'project': 'graph-node-project',
                    'join': 'graph-node-join',
                    'base_relation': 'graph-node-base',
                    'subquery': 'graph-node-subquery'
                };
                
                return classMap[type] || '';
            }
            
            getNodeSize(type) {
                const sizeMap = {
                    'select': 60,
                    'project': 60,
                    'join': 60,
                    'base_relation': 60,
                    'subquery': 70
                };
                
                return sizeMap[type] || 60;
            }
            
            calculateNodePositions() {
                // Group nodes by depth
                const nodesByDepth = {};
                let maxDepth = 0;
                
                this.nodes.forEach(node => {
                    if (!nodesByDepth[node.depth]) {
                        nodesByDepth[node.depth] = [];
                    }
                    nodesByDepth[node.depth].push(node);
                    maxDepth = Math.max(maxDepth, node.depth);
                });
                
                // Get canvas dimensions
                const canvasWidth = this.container.clientWidth;
                const canvasHeight = 450; // Match the height of the canvas
                const padding = 60;
                
                // Position nodes level by level
                Object.keys(nodesByDepth).forEach(depth => {
                    const nodesAtDepth = nodesByDepth[depth];
                    const depthInt = parseInt(depth);
                    
                    // Calculate vertical position
                    const verticalStep = (canvasHeight - 2 * padding) / (maxDepth + 1);
                    const y = padding + depthInt * verticalStep;
                    
                    // Calculate horizontal positions
                    const horizontalSpacing = Math.max(
                        100,
                        (canvasWidth - 2 * padding) / (nodesAtDepth.length + 1)
                    );
                    
                    nodesAtDepth.forEach((node, index) => {
                        node.y = y;
                        node.x = padding + (index + 1) * horizontalSpacing;
                    });
                });
                
                // Center parent nodes over their children
                this.nodes.forEach(node => {
                    const children = this.nodes.filter(n => {
                        return this.edges.some(e => e.from === node.id && e.to === n.id);
                    });
                    
                    if (children.length > 0) {
                        const avgX = children.reduce((sum, child) => sum + child.x, 0) / children.length;
                        node.x = avgX;
                    }
                });
            }

            renderGraph(canvas) {
                // Render edges first
                this.edges.forEach((edge, index) => {
                    const fromNode = this.nodes.find(n => n.id === edge.from);
                    const toNode = this.nodes.find(n => n.id === edge.to);
                    
                    if (!fromNode || !toNode) return;
                    
                    const edgeElement = document.createElement('div');
                    edgeElement.className = 'graph-edge';
                    edgeElement.id = `${this.container.id}_edge_${index}`;
                    
                    // Calculate node radii
                    const fromRadius = fromNode.size / 2;
                    const toRadius = toNode.size / 2;
                    
                    // Calculate edge start and end points
                    const angle = Math.atan2(toNode.y - fromNode.y, toNode.x - fromNode.x);
                    
                    // Start point - at the edge of the from node
                    const startX = fromNode.x + Math.cos(angle) * fromRadius;
                    const startY = fromNode.y + Math.sin(angle) * fromRadius;
                    
                    // End point - at the edge of the to node
                    const endX = toNode.x - Math.cos(angle) * toRadius;
                    const endY = toNode.y - Math.sin(angle) * toRadius;
                    
                    // Calculate the length
                    const length = Math.sqrt(
                        Math.pow(endX - startX, 2) + Math.pow(endY - startY, 2)
                    );
                    
                    // Set edge styles
                    edgeElement.style.width = `${length}px`;
                    edgeElement.style.height = '2px';
                    edgeElement.style.position = 'absolute';
                    edgeElement.style.left = `${startX}px`;
                    edgeElement.style.top = `${startY}px`;
                    edgeElement.style.transformOrigin = '0 0';
                    edgeElement.style.transform = `rotate(${angle}rad)`;
                    edgeElement.style.backgroundColor = '#adb5bd';
                    edgeElement.style.zIndex = '1';
                    
                    canvas.appendChild(edgeElement);
                });
                
                // Render nodes
                this.nodes.forEach(node => {
                    const nodeElement = document.createElement('div');
                    nodeElement.className = `graph-node ${node.className}`;
                    nodeElement.id = node.id;
                    
                    // Set node styles
                    nodeElement.style.width = `${node.size}px`;
                    nodeElement.style.height = `${node.size}px`;
                    nodeElement.style.position = 'absolute';
                    nodeElement.style.left = `${node.x - node.size / 2}px`;
                    nodeElement.style.top = `${node.y - node.size / 2}px`;
                    nodeElement.style.display = 'flex';
                    nodeElement.style.alignItems = 'center';
                    nodeElement.style.justifyContent = 'center';
                    nodeElement.style.borderRadius = '50%';
                    nodeElement.style.border = '2px solid';
                    nodeElement.style.fontSize = `${Math.max(9, node.size / 7)}px`;
                    nodeElement.style.fontWeight = 'bold';
                    nodeElement.style.zIndex = '2';
                    nodeElement.style.cursor = 'pointer';
                    
                    // Set text content
                    nodeElement.textContent = node.label;
                    
                    // Add hover effect for tooltip
                    nodeElement.addEventListener('mouseenter', e => {
                        this.showTooltip(e, node);
                    });
                    
                    nodeElement.addEventListener('mouseleave', () => {
                        this.hideTooltip();
                    });
                    
                    nodeElement.addEventListener('mousemove', e => {
                        this.updateTooltipPosition(e);
                    });
                    
                    canvas.appendChild(nodeElement);
                });
            }
            
            showTooltip(e, node) {
                let tooltipContent = '';
                
                switch (node.type) {
                    case 'select':
                        if (node.data.condition) {
                            tooltipContent = `WHERE: ${this.formatCondition(node.data.condition)}`;
                        }
                        break;
                    case 'project':
                        if (node.data.columns && node.data.columns.length > 0) {
                            tooltipContent = `COLUMNS: ${node.data.columns.map(col => 
                                col.table ? `${col.table}.${col.attr}` : col.attr).join(', ')}`;
                        }
                        break;
                    case 'join':
                        if (node.data.condition) {
                            tooltipContent = `ON: ${this.formatCondition(node.data.condition)}`;
                        }
                        // Add cost information if available
                        if (node.data.cost !== undefined) {
                            tooltipContent += ` (Cost: ${node.data.cost})`;
                        }
                        break;
                    case 'base_relation':
                        if (node.data.tables && node.data.tables.length > 0) {
                            tooltipContent = node.data.tables.map(t => 
                                t.alias ? `${t.name} AS ${t.alias}` : t.name).join(', ');
                        }
                        // Add cost information if available
                        if (node.data.cost !== undefined) {
                            tooltipContent += ` (Cost: ${node.data.cost})`;
                        }
                        break;
                    case 'subquery':
                        tooltipContent = `Alias: ${node.data.alias || 'unnamed'}`;
                        break;
                }
                
                // Set tooltip content
                this.tooltip.textContent = tooltipContent;
                this.tooltip.style.opacity = '1';
                
                // Position tooltip
                this.updateTooltipPosition(e);
            }
            
            updateTooltipPosition(e) {
                const rect = this.container.getBoundingClientRect();
                const x = e.clientX - rect.left;
                const y = e.clientY - rect.top;
                
                this.tooltip.style.left = `${x + 15}px`;
                this.tooltip.style.top = `${y - 30}px`;
            }
            
            hideTooltip() {
                this.tooltip.style.opacity = '0';
            }
            
            formatCondition(condition) {
                if (!condition) return '';
                
                if (condition.table && condition.attr) {
                    return `${condition.table}.${condition.attr}`;
                }
                
                if (condition.type === 'column') {
                    return condition.table ? `${condition.table}.${condition.attr}` : condition.attr;
                }
                
                if (['int', 'float', 'string'].includes(condition.type)) {
                    return condition.type === 'string' ? `'${condition.value}'` : condition.value;
                }
                
                // Handle operators
                const operators = {
                    "EQ": "=",
                    "NEQ": "≠",
                    "GT": ">",
                    "GTE": "≥",
                    "LT": "<",
                    "LTE": "≤",
                    "AND": "AND",
                    "OR": "OR",
                    "NOT": "NOT"
                };
                
                if (condition.left && condition.right) {
                    const operator = operators[condition.type] || condition.type;
                    return `${this.formatCondition(condition.left)} ${operator} ${this.formatCondition(condition.right)}`;
                }
                
                return JSON.stringify(condition);
            }
        }
        
        // Create optimized and original graph visualizations
        new OptimizationGraph('join-original-graph', originalPlan, null);
        new OptimizationGraph('join-optimized-graph', optimizedPlan, null);
    }
    
    /**
     * Display an error message in the container
     */
    showError(message) {
        const errorDiv = document.createElement('div');
        errorDiv.className = 'alert alert-danger';
        errorDiv.textContent = `Join Optimization Error: ${message}`;
        
        // Clear container and show error
        this.container.innerHTML = '';
        this.container.appendChild(errorDiv);
    }
}

// Function to run join optimization
function runJoinOptimization() {
    console.log("Running join optimization...");
    
    // Get the optimized plan from the predicate pushdown step
    if (!window.lastOptimizedPlan) {
        showJoinOptimizationError('Please run predicate pushdown optimization first');
        return;
    }
    
    // Show loading indicator
    document.getElementById('join-optimization-loading').classList.remove('d-none');
    document.getElementById('join-optimization-error').classList.add('d-none');
    
    console.log("Sending join optimization request...");
    
    // Send the optimized plan to the join optimizer
    fetch('/optimize/join/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            'relational_algebra': window.lastOptimizedPlan
        })
    })
    .then(response => response.json())
    .then(data => {
        console.log("Join optimization response:", data);
        
        // Hide loading indicator
        document.getElementById('join-optimization-loading').classList.add('d-none');
        
        if (data.success) {
            try {
                // Create the container for the visualizer
                const container = document.getElementById('join-optimization-result-container');
                container.innerHTML = '<div id="join-optimization-visualizer" style="height:700px;width:100%;"></div>';
                
                // Create the visualizer
                const visualizer = new JoinOptimizationVisualizer(
                    document.getElementById('join-optimization-visualizer')
                );
                
                // Process the data
                visualizer.processOptimizationData(data);
                
                // Store the optimized plan for any further steps
                window.joinOptimizedPlan = data.optimized_plan_json;
                
            } catch (error) {
                console.error("Error creating join visualization:", error);
                showJoinOptimizationError('Error creating visualization: ' + error.message);
            }
        } else {
            showJoinOptimizationError(data.error || 'An unknown error occurred during join optimization');
        }
    })
    .catch(error => {
        console.error("Fetch error:", error);
        document.getElementById('join-optimization-loading').classList.add('d-none');
        showJoinOptimizationError('Network error: ' + error.message);
    });
}

// Helper function to show join optimization error messages
function showJoinOptimizationError(message) {
    const errorElement = document.getElementById('join-optimization-error');
    errorElement.textContent = message;
    errorElement.classList.remove('d-none');
}