/**
 * Query Optimization Visualizer
 * Handles visualization of the optimization process
 */

class QueryOptimizationVisualizer {
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
                <h4>Original Query Plan</h4>
            </div>
            <div id="original-graph" class="graph-container"></div>
        `;
        
        // Optimized plan container
        const optimizedDiv = document.createElement('div');
        optimizedDiv.className = 'col-md-6 optimization-col';
        optimizedDiv.innerHTML = `
            <div class="optimization-header">
                <h4>Optimized Query Plan (Predicate Pushdown)</h4>
            </div>
            <div id="optimized-graph" class="graph-container"></div>
        `;
        
        // Add columns to row
        rowDiv.appendChild(originalDiv);
        rowDiv.appendChild(optimizedDiv);
        
        // Add explanation section
        const explanationDiv = document.createElement('div');
        explanationDiv.className = 'optimization-explanation mt-3';
        explanationDiv.innerHTML = `
            <h5>Predicate Pushdown Optimization</h5>
            <p>
                Predicate Pushdown is a query optimization technique that moves filtering operations (WHERE clauses) 
                as close as possible to the data sources. This reduces the amount of data that needs to be processed 
                by subsequent operations, improving query performance.
            </p>
            <div class="optimization-comparison">
                <div class="original-explanation">
                    <h6>Original Plan</h6>
                    <pre>
                        In the original plan, the filter (SELECT) is applied <strong>after</strong> the join operations.
                        This means that we must first join all tables and then filter the results.
                    </pre>
                </div>
                <div class="optimized-explanation">
                    <h6>Optimized Plan</h6>
                    <pre>
                        In the optimized plan, the filter (SELECT) is "pushed down" to be applied <strong>before</strong> 
                        the join operations, directly to the base table. This reduces the number of rows that need to be 
                        processed in the subsequent join operations.
                    </pre>
                </div>
            </div>
            <div class="optimization-benefits">
                <h6>Benefits</h6>
                <ul>
                    <li>Reduces the amount of data processed during joins</li>
                    <li>Decreases intermediate result sizes</li>
                    <li>Improves overall query execution time</li>
                </ul>
            </div>
        `;
        
        // Add all elements to container
        visualizationContainer.appendChild(rowDiv);
        visualizationContainer.appendChild(explanationDiv);
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

            if(data.original_plan_str){
                const highlighted = data.original_plan_str
                    .replace(/\b(FILTER|SCAN)\b/g, '<b>$1</b>');

                const originalPlanStrDiv = this.container.querySelector('.original-explanation pre');
                if (originalPlanStrDiv) {
                    originalPlanStrDiv.innerHTML = highlighted;
                }
            }

            if (data.optimized_plan_str) {
                const highlighted = data.optimized_plan_str
                    .replace(/\b(FILTER|SCAN)\b/g, '<b>$1</b>');
            
                const optimizedPlanStrDiv = this.container.querySelector('.optimized-explanation pre');
                if (optimizedPlanStrDiv) {
                    optimizedPlanStrDiv.innerHTML = highlighted;
                }
            }
            
        } catch (error) {
            console.error("Error processing optimization data:", error);
            this.showError("Failed to process optimization data: " + error.message);
        }
    }
    
    /**
     * Create side-by-side graph visualizations
     */
    createOptimizationGraphs(originalPlan, optimizedPlan) {
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
                const canvasHeight = this.container.clientHeight || 700; // Use default if height not set
                const padding = 60; // Increased padding for better spacing
                
                // Position nodes level by level
                Object.keys(nodesByDepth).forEach(depth => {
                    const nodesAtDepth = nodesByDepth[depth];
                    const depthInt = parseInt(depth);
                    
                    // Calculate vertical position with more space between levels
                    const verticalStep = (canvasHeight - 2 * padding) / (maxDepth + 1);
                    const y = padding + depthInt * verticalStep;
                    
                    // Calculate horizontal positions with more spacing
                    const horizontalSpacing = Math.max(
                        100, // Minimum spacing
                        (canvasWidth - 2 * padding) / (nodesAtDepth.length + 1)
                    );
                    
                    nodesAtDepth.forEach((node, index) => {
                        node.y = y;
                        node.x = padding + (index + 1) * horizontalSpacing;
                    });
                });
                
                // Additional spacing for parent nodes to make the tree more readable
                this.nodes.forEach(node => {
                    // Find all children of this node
                    const children = this.nodes.filter(n => {
                        return this.edges.some(e => e.from === node.id && e.to === n.id);
                    });
                    
                    if (children.length > 0) {
                        // Calculate the average x-position of children
                        const avgX = children.reduce((sum, child) => sum + child.x, 0) / children.length;
                        
                        // Move the parent node to be above its children
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
                    
                    // Calculate edge start and end points accounting for node radii
                    const angle = Math.atan2(toNode.y - fromNode.y, toNode.x - fromNode.x);
                    
                    // Start point - at the edge of the from node
                    const startX = fromNode.x + Math.cos(angle) * fromRadius;
                    const startY = fromNode.y + Math.sin(angle) * fromRadius;
                    
                    // End point - at the edge of the to node
                    const endX = toNode.x - Math.cos(angle) * toRadius;
                    const endY = toNode.y - Math.sin(angle) * toRadius;
                    
                    // Calculate the length between the adjusted points
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
                        break;
                    case 'base_relation':
                        if (node.data.tables && node.data.tables.length > 0) {
                            tooltipContent = node.data.tables.map(t => 
                                t.alias ? `${t.name} AS ${t.alias}` : t.name).join(', ');
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
        new OptimizationGraph('original-graph', originalPlan, null);
        new OptimizationGraph('optimized-graph', optimizedPlan, null);
    }
    
    /**
     * Display an error message in the container
     */
    showError(message) {
        const errorDiv = document.createElement('div');
        errorDiv.className = 'alert alert-danger';
        errorDiv.textContent = `Optimization Error: ${message}`;
        
        // Clear container and show error
        this.container.innerHTML = '';
        this.container.appendChild(errorDiv);
    }
}

// Function to run optimization
function runOptimization() {
    console.log("Running optimization...");
    
    const rawContent = document.getElementById('json-raw-content').textContent;
    
    if (!rawContent) {
        showOptimizationError('No query plan to optimize');
        return;
    }
    
    // Show loading indicator
    document.getElementById('optimization-loading').classList.remove('d-none');
    document.getElementById('optimization-error').classList.add('d-none');
    
    console.log("Sending optimization request...");
    
    // Log what we're sending
    const requestData = {
        'relational_algebra': JSON.parse(rawContent)
    };
    console.log("Request data:", requestData);
    
    // Send the parsed relational algebra to the optimizer
    fetch('/optimize/pred_push/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestData)
    })
    .then(response => {
        console.log("Response status:", response.status);
        return response.json();
    })
    .then(data => {
        console.log("Response data:", data);
        // Hide loading indicator
        document.getElementById('optimization-loading').classList.add('d-none');
        
        if (data.success) {
            try {
                // Initialize the optimization visualizer
                const container = document.getElementById('optimization-result-container');
                container.innerHTML = '<div id="optimization-visualizer"></div>';
                
                console.log("Creating QueryOptimizationVisualizer instance");
                const optimizationVisualizer = new QueryOptimizationVisualizer(
                    document.getElementById('optimization-visualizer')
                );
                
                // Process and display the optimization data
                console.log("Processing optimization data");
                optimizationVisualizer.processOptimizationData(data);
                
                // Store the optimized plan for the next step
                window.lastOptimizedPlan = data.optimized_plan_json;
                
                // Enable the join optimization button
                document.getElementById('join-optimization-btn').disabled = false;
                
            } catch (error) {
                console.error("Error initializing visualizer:", error);
                showOptimizationError('Error initializing visualizer: ' + error.message);
            }
        } else {
            showOptimizationError(data.error || 'An unknown error occurred during optimization');
        }
    })
    .catch(error => {
        console.error("Fetch error:", error);
        document.getElementById('optimization-loading').classList.add('d-none');
        showOptimizationError('Network error: ' + error.message);
    });
}

// Helper function to show optimization error messages
function showOptimizationError(message) {
    const errorElement = document.getElementById('optimization-error');
    errorElement.textContent = message;
    errorElement.classList.remove('d-none');
}