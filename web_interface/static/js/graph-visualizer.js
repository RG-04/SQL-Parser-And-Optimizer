/**
 * SQL to Relational Algebra Graph Visualizer
 * Creates a stick-and-ball representation of relational algebra operations
 */

class RelationalAlgebraGraph {
    constructor(container) {
        this.container = container;
        this.nodes = [];
        this.edges = [];
        this.tooltipElement = null;
        
        // Node types and their visual attributes
        this.nodeTypes = {
            "select": { className: "graph-node-select", label: "SELECT", size: 80 },
            "project": { className: "graph-node-project", label: "PROJECT", size: 80 },
            "join": { className: "graph-node-join", label: "JOIN", size: 80 },
            "base_relation": { className: "graph-node-base", label: "TABLE", size: 70 },
            "subquery": { className: "graph-node-subquery", label: "SUBQUERY", size: 90 },
            "default": { className: "", label: "OP", size: 70 }
        };
        
        this.initContainer();
    }
    
    /**
     * Initialize the container with necessary elements
     */
    initContainer() {
        // Clear any existing content
        this.container.innerHTML = '';
        
        // Create tooltip element
        this.tooltipElement = document.createElement('div');
        this.tooltipElement.className = 'graph-tooltip';
        this.container.appendChild(this.tooltipElement);
    }
    
    /**
     * Renders the graph from a relational algebra JSON structure
     */
    render(data) {
        try {
            // Reset collections
            this.nodes = [];
            this.edges = [];
            
            // Process the data to create nodes and edges
            this.processData(data);
            
            // Calculate initial positions for nodes
            this.calculateInitialPositions();
            
            // Create all visual elements
            this.createVisualElements();
            
            // Update positions
            this.updatePositions();
        } catch (error) {
            console.error("Error rendering graph:", error);
            this.showError("Failed to render graph: " + error.message);
        }
    }
    
    /**
     * Display an error message in the container
     */
    showError(message) {
        this.container.innerHTML = `
            <div class="alert alert-danger" role="alert">
                <strong>Graph Visualization Error:</strong> ${message}
            </div>
        `;
    }
    
    /**
     * Process the relational algebra data to extract nodes and edges
     */
    processData(node, parentId = null, depth = 0) {
        if (!node || typeof node !== 'object') return null;
        
        // Create a unique ID for this node
        const nodeId = 'node_' + this.nodes.length;
        
        // Get node type - properly handle subqueries
        let nodeType = node.type;
        let nodeLabel = '';
        
        if (nodeType === 'subquery') {
            // For subqueries, extract more meaningful information
            nodeType = 'subquery';
            nodeLabel = node.alias ? `SUBQUERY (${node.alias})` : 'SUBQUERY';
        } else {
            // Get node configuration based on type
            const nodeConfig = this.nodeTypes[node.type] || this.nodeTypes.default;
            nodeLabel = nodeConfig.label;
        }
        
        // Create node object with all necessary data
        const nodeObj = {
            id: nodeId,
            type: nodeType,
            label: nodeLabel,
            className: this.getNodeClassName(nodeType),
            size: this.getNodeSize(nodeType),
            data: node,
            depth: depth,
            x: 0,
            y: 0
        };
        
        this.nodes.push(nodeObj);
        
        // Connect to parent if this is not the root node
        if (parentId) {
            this.edges.push({
                from: parentId,
                to: nodeId
            });
        }
        
        // Process children recursively
        if (node.input) {
            this.processData(node.input, nodeId, depth + 1);
        }
        
        if (node.left) {
            this.processData(node.left, nodeId, depth + 1);
        }
        
        if (node.right) {
            this.processData(node.right, nodeId, depth + 1);
        }
        
        // Process subquery if present
        if (node.type === 'subquery' && node.query) {
            this.processData(node.query, nodeId, depth + 1);
        }
        
        return nodeId;
    }
    
    /**
     * Get appropriate CSS class for a node type
     */
    getNodeClassName(type) {
        const classMap = {
            "select": "graph-node-select",
            "project": "graph-node-project",
            "join": "graph-node-join",
            "base_relation": "graph-node-base",
            "subquery": "graph-node-subquery"
        };
        
        return classMap[type] || "";
    }
    
    /**
     * Get appropriate size for a node type
     */
    getNodeSize(type) {
        const sizeMap = {
            "select": 70,
            "project": 70, 
            "join": 70,
            "base_relation": 65,
            "subquery": 75
        };
        
        return sizeMap[type] || 65;
    }
    
    /**
     * Calculate initial positions for all nodes
     */
    calculateInitialPositions() {
        // First pass: Organize nodes by depth levels
        const depthToNodes = {};
        
        this.nodes.forEach(node => {
            if (!depthToNodes[node.depth]) {
                depthToNodes[node.depth] = [];
            }
            depthToNodes[node.depth].push(node);
        });
        
        // Track parent-child relationships
        const childrenMap = {};
        this.edges.forEach(edge => {
            if (!childrenMap[edge.from]) {
                childrenMap[edge.from] = [];
            }
            childrenMap[edge.from].push(edge.to);
        });
        
        // Get container dimensions
        const containerWidth = this.container.clientWidth;
        const containerHeight = this.container.clientHeight;
        const padding = 50; // Padding from container edges
        
        // Find the max depth
        const maxDepth = Math.max(...Object.keys(depthToNodes).map(Number), 0);
        
        // Calculate vertical spacing based on depth, with more space for larger trees
        const minVerticalSpacing = 120; // Minimum space between levels
        let verticalSpacing = Math.max(minVerticalSpacing, (containerHeight - 2 * padding) / (maxDepth + 1));
        
        // First, position the root node (depth 0) at the center top
        if (depthToNodes[0] && depthToNodes[0].length > 0) {
            const rootNode = depthToNodes[0][0];
            rootNode.x = containerWidth / 2;
            rootNode.y = padding + rootNode.size / 2;
        }
        
        // Then position all other nodes level by level
        for (let depth = 1; depth <= maxDepth; depth++) {
            const nodesAtDepth = depthToNodes[depth] || [];
            const nodeCount = nodesAtDepth.length;
            
            if (nodeCount === 0) continue;
            
            // Group nodes by their parent to keep siblings together
            const nodesByParent = {};
            nodesAtDepth.forEach(node => {
                // Find parent of this node
                let parentId = null;
                this.edges.forEach(edge => {
                    if (edge.to === node.id) {
                        parentId = edge.from;
                    }
                });
                
                if (!nodesByParent[parentId]) {
                    nodesByParent[parentId] = [];
                }
                nodesByParent[parentId].push(node);
            });
            
            // Position all nodes at this level
            const horizontalSpacing = Math.max(100, containerWidth / (nodeCount + 1)); // Minimum horizontal spacing
            
            // Calculate how to distribute parent segments
            const parents = Object.keys(nodesByParent);
            const parentSpacing = containerWidth / (parents.length + 1);
            
            parents.forEach((parentId, parentIndex) => {
                const children = nodesByParent[parentId];
                const parentX = (parentIndex + 1) * parentSpacing;
                
                // Find parent node to center children below it
                const parentNode = this.nodes.find(n => n.id === parentId);
                let parentCenterX = parentX; // Default if parent not found
                
                if (parentNode) {
                    parentCenterX = parentNode.x;
                }
                
                // Position children centered under parent with adequate spacing
                const childrenCount = children.length;
                const childGroupWidth = childrenCount * horizontalSpacing;
                const startX = parentCenterX - childGroupWidth / 2 + horizontalSpacing / 2;
                
                children.forEach((node, index) => {
                    node.x = Math.max(padding, Math.min(containerWidth - padding, startX + index * horizontalSpacing));
                    node.y = padding + depth * verticalSpacing;
                });
            });
        }
    }
    
    /**
     * Center and scale the graph to fit nicely in the container
     */
    centerAndScaleGraph() {
        // Find the bounds of the graph
        let minX = Infinity, maxX = -Infinity;
        let minY = Infinity, maxY = -Infinity;
        
        this.nodes.forEach(node => {
            minX = Math.min(minX, node.x - node.size / 2);
            maxX = Math.max(maxX, node.x + node.size / 2);
            minY = Math.min(minY, node.y - node.size / 2);
            maxY = Math.max(maxY, node.y + node.size / 2);
        });
        
        // Calculate width and height of the graph
        const graphWidth = maxX - minX;
        const graphHeight = maxY - minY;
        
        // Calculate the container's usable dimensions
        const containerWidth = this.container.clientWidth;
        const containerHeight = this.container.clientHeight;
        const padding = 50; // Padding from container edges
        const usableWidth = containerWidth - 2 * padding;
        const usableHeight = containerHeight - 2 * padding;
        
        // Calculate scaling factors
        const scaleX = graphWidth > 0 ? usableWidth / graphWidth : 1;
        const scaleY = graphHeight > 0 ? usableHeight / graphHeight : 1;
        const scale = Math.min(scaleX, scaleY, 1); // Only scale down, never up
        
        // Calculate center points
        const graphCenterX = (minX + maxX) / 2;
        const graphCenterY = (minY + maxY) / 2;
        const containerCenterX = containerWidth / 2;
        const containerCenterY = containerHeight / 2;
        
        // Apply the transformations to center and scale
        if (scale < 1 || graphCenterX !== containerCenterX || graphCenterY !== containerCenterY) {
            this.nodes.forEach(node => {
                // Center the graph
                const relativeX = node.x - graphCenterX;
                const relativeY = node.y - graphCenterY;
                
                // Scale and position
                node.x = containerCenterX + relativeX * scale;
                node.y = containerCenterY + relativeY * scale;
            });
        }
    }
    
    /**
     * Create all visual elements for nodes and edges
     */
    createVisualElements() {
        // Clear existing elements
        const existingNodes = this.container.querySelectorAll('.graph-node, .graph-edge, .graph-edge-label');
        existingNodes.forEach(node => node.remove());
        
        // Create edges first (so they appear behind nodes)
        this.edges.forEach((edge, index) => {
            const fromNode = this.nodes.find(n => n.id === edge.from);
            const toNode = this.nodes.find(n => n.id === edge.to);
            
            if (!fromNode || !toNode) return;
            
            // Create the edge element
            const edgeElem = document.createElement('div');
            edgeElem.className = 'graph-edge';
            edgeElem.id = `edge_${index}`;
            this.container.appendChild(edgeElem);
        });
        
        // Create nodes
        this.nodes.forEach(node => {
            const nodeElem = document.createElement('div');
            nodeElem.className = `graph-node ${node.className}`;
            nodeElem.id = node.id;
            nodeElem.style.width = `${node.size}px`;
            nodeElem.style.height = `${node.size}px`;
            nodeElem.style.fontSize = `${Math.max(10, node.size / 8)}px`;
            nodeElem.innerText = node.label;
            
            // Add tooltip event handlers
            nodeElem.addEventListener('mouseenter', (e) => {
                this.showTooltip(e, node);
            });
            
            nodeElem.addEventListener('mousemove', (e) => {
                this.updateTooltipPosition(e);
            });
            
            nodeElem.addEventListener('mouseleave', () => {
                this.hideTooltip();
            });
            
            this.container.appendChild(nodeElem);
        });
    }
    
    /**
     * Update positions of all nodes and edges based on current state
     */
    updatePositions() {
        // Update nodes
        this.nodes.forEach(node => {
            const nodeElem = document.getElementById(node.id);
            if (nodeElem) {
                const x = node.x;
                const y = node.y;
                
                // Calculate position accounting for node size to center it
                const offsetX = node.size / 2;
                const offsetY = node.size / 2;
                
                nodeElem.style.left = `${x - offsetX}px`;
                nodeElem.style.top = `${y - offsetY}px`;
                nodeElem.style.width = `${node.size}px`;
                nodeElem.style.height = `${node.size}px`;
                nodeElem.style.fontSize = `${Math.max(10, node.size / 6)}px`;
            }
        });
        
        // Update edges
        this.edges.forEach((edge, index) => {
            const fromNode = this.nodes.find(n => n.id === edge.from);
            const toNode = this.nodes.find(n => n.id === edge.to);
            
            if (!fromNode || !toNode) return;
            
            const fromX = fromNode.x;
            const fromY = fromNode.y;
            const toX = toNode.x;
            const toY = toNode.y;
            
            // Calculate the angle of the line
            const angle = Math.atan2(toY - fromY, toX - fromX);
            
            // Calculate the length of the line
            const length = Math.sqrt((toX - fromX) ** 2 + (toY - fromY) ** 2);
            
            // Get edge element
            const edgeElem = document.getElementById(`edge_${index}`);
            if (edgeElem) {
                // Position and rotate the edge
                edgeElem.style.width = `${length}px`;
                edgeElem.style.height = '2px';
                edgeElem.style.left = `${fromX}px`;
                edgeElem.style.top = `${fromY}px`;
                edgeElem.style.transform = `rotate(${angle}rad)`;
            }
        });
    }
    
    /**
     * Show tooltip with node details
     */
    showTooltip(e, node) {
        // Build tooltip content based on node type and data
        let tooltipContent = '';
        
        switch (node.type) {
            case 'base_relation':
                if (node.data.tables && node.data.tables.length > 0) {
                    tooltipContent += node.data.tables.map(t => 
                        t.alias ? `${t.name} AS ${t.alias}` : t.name).join(', ');
                }
                break;
                
            case 'select':
                if (node.data.condition) {
                    tooltipContent += `WHERE ${this.formatCondition(node.data.condition)}`;
                }
                break;
                
            case 'project':
                if (node.data.columns && node.data.columns.length > 0) {
                    tooltipContent += node.data.columns.map(c => 
                        c.table ? `${c.table}.${c.attr}` : c.attr).join(', ');
                }
                break;
                
            case 'join':
                if (node.data.condition) {
                    tooltipContent += `ON ${this.formatCondition(node.data.condition)}`;
                }
                break;

            case 'subquery':
                tooltipContent += `${node.data.alias || 'unnamed'}`;
                
                if (node.data.query && node.data.query.type) {
                    tooltipContent += ` (${node.data.query.type})`;
                }
                break;
                
            default:
                // For other node types, just show the type
                tooltipContent += node.type.toUpperCase();
        }
        
        // Set tooltip content and show
        this.tooltipElement.innerHTML = tooltipContent;
        this.tooltipElement.style.opacity = '1';
        
        // Position the tooltip
        this.updateTooltipPosition(e);
    }
    
    /**
     * Update tooltip position to follow cursor
     */
    updateTooltipPosition(e) {
        const rect = this.container.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        
        // Position tooltip with offset from cursor
        this.tooltipElement.style.left = `${x + 15}px`;
        this.tooltipElement.style.top = `${y + 15}px`;
        
        // Check if tooltip is going beyond container bounds
        const tooltipRect = this.tooltipElement.getBoundingClientRect();
        const containerRight = rect.left + rect.width;
        const containerBottom = rect.top + rect.height;
        
        // Adjust if beyond right edge
        if (tooltipRect.right > containerRight) {
            this.tooltipElement.style.left = `${x - tooltipRect.width - 10}px`;
        }
        
        // Adjust if beyond bottom edge
        if (tooltipRect.bottom > containerBottom) {
            this.tooltipElement.style.top = `${y - tooltipRect.height - 10}px`;
        }
    }
    
    /**
     * Hide the tooltip
     */
    hideTooltip() {
        this.tooltipElement.style.opacity = '0';
    }
    
    /**
     * Get a more detailed label for the node
     */
    getDetailedNodeLabel(node) {
        switch (node.type) {
            case 'base_relation':
                if (node.data.tables && node.data.tables.length > 0) {
                    const tableName = node.data.tables[0].name;
                    const alias = node.data.tables[0].alias;
                    return `TABLE: ${tableName}${alias ? ` (${alias})` : ''}`;
                }
                return 'TABLE';
                
            case 'select':
                return 'SELECT (σ)';
                
            case 'project':
                return 'PROJECT (π)';
                
            case 'join':
                // Show the tables being joined if we can determine them
                let joinStr = 'JOIN (⋈)';
                if (node.data.condition && node.data.condition.left && node.data.condition.right) {
                    const leftTable = node.data.condition.left.table || 
                                     (node.data.condition.left.type === 'column' ? node.data.condition.left.table : null);
                    const rightTable = node.data.condition.right.table || 
                                      (node.data.condition.right.type === 'column' ? node.data.condition.right.table : null);
                    
                    if (leftTable && rightTable) {
                        joinStr = `JOIN ${leftTable} ⋈ ${rightTable}`;
                    }
                }
                return joinStr;
                
            case 'subquery':
                return `SUBQUERY: ${node.data.alias || 'unnamed'}`;
                
            default:
                return node.label || node.type.toUpperCase();
        }
    }
    
    /**
     * Format a condition expression for readable display
     */
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
        
        // Operators
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