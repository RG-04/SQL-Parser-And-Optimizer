/**
 * SQL to Relational Algebra Tree Visualizer
 * Creates a hierarchical tree visualization of relational algebra operations
 */

class RelationalAlgebraTree {
    constructor(container) {
        this.container = container;
        this.nodeTypes = {
            // Basic relation operations
            "base_relation": { label: "BASE RELATION", icon: "📋" },
            "select": { label: "SELECT", icon: "🔍" },
            "project": { label: "PROJECT", icon: "📏" },
            "join": { label: "JOIN", icon: "🔗" },
            "cross_join": { label: "CROSS JOIN", icon: "✖️" },
            "natural_join": { label: "NATURAL JOIN", icon: "🧩" },
            "left_join": { label: "LEFT JOIN", icon: "👈" },
            "right_join": { label: "RIGHT JOIN", icon: "👉" },
            "full_join": { label: "FULL JOIN", icon: "👐" },
            "union": { label: "UNION", icon: "∪" },
            "intersect": { label: "INTERSECT", icon: "∩" },
            "except": { label: "EXCEPT", icon: "−" },
            "aggregate": { label: "AGGREGATE", icon: "∑" },
            "sort": { label: "SORT", icon: "↕️" },
            "limit": { label: "LIMIT", icon: "🔢" },
            "group": { label: "GROUP", icon: "📊" },
            "distinct": { label: "DISTINCT", icon: "🎯" },
            // Default for any unrecognized types
            "default": { label: "OPERATION", icon: "⚙️" }
        };
        
        // Condition operators
        this.operators = {
            "EQ": "=",
            "NEQ": "≠",
            "GT": ">",
            "GTE": "≥",
            "LT": "<",
            "LTE": "≤",
            "AND": "AND",
            "OR": "OR",
            "NOT": "NOT",
            "IN": "IN",
            "LIKE": "LIKE",
            "IS": "IS",
            "BETWEEN": "BETWEEN"
        };
    }
    
    /**
     * Renders the tree from a relational algebra JSON structure
     */
    render(data) {
        this.container.innerHTML = '';
        const treeRoot = document.createElement('ul');
        treeRoot.className = 'tree';
        
        const rootNode = this.createTreeNodeElement(data);
        treeRoot.appendChild(rootNode);
        
        this.container.appendChild(treeRoot);
        this.addToggleListeners();
    }
    
    /**
     * Creates a tree node element based on the operation type
     */
    createTreeNodeElement(node) {
        const li = document.createElement('li');
        li.className = 'expanded';
        
        // Add toggle button for expandable nodes
        if (this.hasChildren(node)) {
            const toggleBtn = document.createElement('span');
            toggleBtn.className = 'toggle-btn';
            li.appendChild(toggleBtn);
        }
        
        // Create the node container
        const nodeElem = document.createElement('div');
        nodeElem.className = 'tree-node';
        
        // Get the display details for this node type
        const typeInfo = this.getNodeTypeInfo(node.type);
        
        // Create the node header with type and icon
        const nodeHeader = document.createElement('span');
        nodeHeader.className = 'tree-node-type';
        nodeHeader.innerHTML = `${typeInfo.icon} ${typeInfo.label}`;
        nodeElem.appendChild(nodeHeader);
        
        // Add node-specific details based on the operation type
        this.addNodeDetails(nodeElem, node);
        
        li.appendChild(nodeElem);
        
        // If the node has children, render them recursively
        if (this.hasChildren(node)) {
            const childrenUl = document.createElement('ul');
            this.appendChildNodes(childrenUl, node);
            li.appendChild(childrenUl);
        }
        
        return li;
    }
    
    /**
     * Determines if a node has child operations
     */
    hasChildren(node) {
        // Check for common child patterns in relational algebra operations
        return node.input || 
               node.left || 
               (node.tables && node.tables.length > 0) ||
               (node.columns && node.columns.length > 0);
    }
    
    /**
     * Gets the display information for a node type
     */
    getNodeTypeInfo(type) {
        return this.nodeTypes[type] || this.nodeTypes.default;
    }
    
    /**
     * Adds node-specific details based on the operation type
     */
    addNodeDetails(nodeElem, node) {
        const detailSpan = document.createElement('span');
        detailSpan.className = 'tree-node-detail';
        
        switch (node.type) {
            case 'base_relation':
                if (node.tables && node.tables.length > 0) {
                    detailSpan.innerHTML = ': ' + node.tables.map(table => {
                        let tableName = `<span class="tree-table">${this.escapeHtml(table.name)}</span>`;
                        if (table.alias) {
                            tableName += ` AS <span class="tree-table">${this.escapeHtml(table.alias)}</span>`;
                        }
                        return tableName;
                    }).join(', ');
                }
                break;
                
            case 'select':
                if (node.condition) {
                    detailSpan.innerHTML = ': <span class="tree-node-condition">WHERE ' + 
                        this.formatCondition(node.condition) + '</span>';
                }
                break;
                
            case 'project':
                if (node.columns && node.columns.length > 0) {
                    detailSpan.innerHTML = ': ' + node.columns.map(col => {
                        let colStr = '';
                        if (col.table) {
                            colStr += `<span class="tree-table">${this.escapeHtml(col.table)}</span>.`;
                        }
                        colStr += `<span class="tree-column">${this.escapeHtml(col.attr)}</span>`;
                        return colStr;
                    }).join(', ');
                }
                break;
                
            case 'join':
            case 'left_join':
            case 'right_join':
            case 'full_join':
                if (node.condition) {
                    detailSpan.innerHTML = ': <span class="tree-node-condition">ON ' + 
                        this.formatCondition(node.condition) + '</span>';
                }
                break;
                
            case 'aggregate':
                if (node.aggregates && node.aggregates.length > 0) {
                    detailSpan.innerHTML = ': ' + node.aggregates.map(agg => {
                        return `${agg.op}(${agg.column})`;
                    }).join(', ');
                }
                break;
                
            case 'sort':
                if (node.sort_keys && node.sort_keys.length > 0) {
                    detailSpan.innerHTML = ': ' + node.sort_keys.map(key => {
                        let direction = key.direction === 'DESC' ? '↓' : '↑';
                        return `${this.formatColumn(key.column)} ${direction}`;
                    }).join(', ');
                }
                break;
                
            case 'limit':
                if (node.limit !== undefined) {
                    detailSpan.innerHTML = `: ${node.limit}`;
                    if (node.offset !== undefined) {
                        detailSpan.innerHTML += ` OFFSET ${node.offset}`;
                    }
                }
                break;
                
            case 'group':
                if (node.group_by && node.group_by.length > 0) {
                    detailSpan.innerHTML = ': BY ' + node.group_by.map(col => {
                        return this.formatColumn(col);
                    }).join(', ');
                }
                break;
        }
        
        if (detailSpan.innerHTML) {
            nodeElem.appendChild(detailSpan);
        }
    }
    
    /**
     * Appends child nodes to a parent UL element
     */
    appendChildNodes(parentUl, node) {
        // Handle node with a single input
        if (node.input) {
            parentUl.appendChild(this.createTreeNodeElement(node.input));
        }
        
        // Handle nodes with left/right children (joins, union, etc.)
        if (node.left) {
            parentUl.appendChild(this.createTreeNodeElement(node.left));
        }
        
        if (node.right) {
            parentUl.appendChild(this.createTreeNodeElement(node.right));
        }
        
        // For base_relation with tables
        if (node.tables && node.tables.length > 0 && node.type === 'base_relation') {
            // Tables are already displayed in the node details, not as children
        }
        
        // For nodes with columns list (usually doesn't need child nodes for columns)
        if (node.columns && node.columns.length > 0 && node.type === 'project') {
            // Columns are already displayed in the node details, not as children
        }
    }
    
    /**
     * Formats a condition expression recursively
     */
    formatCondition(condition) {
        if (!condition) return '';
        
        // Handle leaf nodes (column references or literals)
        if (condition.table && condition.attr) {
            return `<span class="tree-table">${this.escapeHtml(condition.table)}</span>.` +
                   `<span class="tree-column">${this.escapeHtml(condition.attr)}</span>`;
        }
        
        // Handle literal values
        if (condition.type === 'int' || condition.type === 'float') {
            return `<span class="tree-literal">${condition.value}</span>`;
        }
        
        if (condition.type === 'string') {
            return `<span class="tree-literal">'${this.escapeHtml(condition.value)}'</span>`;
        }
        
        if (condition.type === 'column') {
            let result = '';
            if (condition.table) {
                result += `<span class="tree-table">${this.escapeHtml(condition.table)}</span>.`;
            }
            result += `<span class="tree-column">${this.escapeHtml(condition.attr)}</span>`;
            return result;
        }
        
        // Handle binary operations
        if (condition.left && condition.right) {
            const operator = this.operators[condition.type] || condition.type;
            
            // Special handling for AND/OR to add parentheses
            if (condition.type === 'AND' || condition.type === 'OR') {
                return `(${this.formatCondition(condition.left)} <span class="tree-operator">${operator}</span> ${this.formatCondition(condition.right)})`;
            }
            
            return `${this.formatCondition(condition.left)} <span class="tree-operator">${operator}</span> ${this.formatCondition(condition.right)}`;
        }
        
        // Handle unary operations (like NOT)
        if (condition.type === 'NOT' && condition.expr) {
            return `<span class="tree-operator">NOT</span> ${this.formatCondition(condition.expr)}`;
        }
        
        // Default case
        return JSON.stringify(condition);
    }
    
    /**
     * Formats a column reference
     */
    formatColumn(column) {
        if (typeof column === 'string') {
            return `<span class="tree-column">${this.escapeHtml(column)}</span>`;
        }
        
        if (column.table && column.attr) {
            return `<span class="tree-table">${this.escapeHtml(column.table)}</span>.` +
                   `<span class="tree-column">${this.escapeHtml(column.attr)}</span>`;
        }
        
        return JSON.stringify(column);
    }
    
    /**
     * Adds click event listeners to all toggle buttons
     */
    addToggleListeners() {
        const toggleButtons = this.container.querySelectorAll('.toggle-btn');
        toggleButtons.forEach(btn => {
            btn.addEventListener('click', () => {
                const li = btn.parentElement;
                if (li.classList.contains('expanded')) {
                    li.classList.remove('expanded');
                    li.classList.add('collapsed');
                } else {
                    li.classList.remove('collapsed');
                    li.classList.add('expanded');
                }
            });
        });
    }
    
    /**
     * Helper to escape HTML special characters
     */
    escapeHtml(str) {
        if (!str) return '';
        
        const escapeMap = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#39;'
        };
        
        return String(str).replace(/[&<>"']/g, s => escapeMap[s]);
    }
}