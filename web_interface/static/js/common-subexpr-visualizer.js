/**
 * Common Subexpression Elimination Visualizer
 * Handles visualization of the common subexpression elimination process
 * using pre-generated images instead of creating graphs
 */

class CommonSubexprVisualizer {
    constructor(container) {
        this.container = container;
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
                <div id="original-cost-new" class="plan-cost"></div>
            </div>
            <div id="subexpr-original-img" class="graph-image-container"></div>
        `;
        
        // Optimized plan container
        const optimizedDiv = document.createElement('div');
        optimizedDiv.className = 'col-md-6 optimization-col';
        optimizedDiv.innerHTML = `
            <div class="optimization-header">
                <h4>After Common Subexpression Elimination</h4>
                <div id="optimized-cost-new" class="plan-cost"></div>
            </div>
            <div id="subexpr-optimized-img" class="graph-image-container"></div>
        `;
        
        // Add columns to row
        rowDiv.appendChild(originalDiv);
        rowDiv.appendChild(optimizedDiv);
        
        // Add all elements to container
        visualizationContainer.appendChild(rowDiv);
        
        // Add common expressions section if available
        const commonExprsDiv = document.createElement('div');
        commonExprsDiv.id = 'common-expressions-section';
        commonExprsDiv.className = 'mt-4 p-3 border rounded bg-light';
        commonExprsDiv.innerHTML = `
            <h5>Identified Common Expressions</h5>
            <div id="common-expressions-list"></div>
        `;
        visualizationContainer.appendChild(commonExprsDiv);
        
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
            console.log("Processing optimization data:", data);
            if (data.original_cost !== undefined) {
                const originalCostElement = document.getElementById('original-cost-new');
                originalCostElement.textContent = `Total Cost: ${data.original_cost}`;
            }
            
            if (data.optimized_cost !== undefined) {
                const optimizedCostElement = document.getElementById('optimized-cost-new');
                optimizedCostElement.textContent = `Total Cost: ${data.optimized_cost}`;
                
            }

            // Display the images for original and optimized plans
            this.displayPlanImages(data.original_plan_svg, data.optimized_plan_svg);
            
            // Display common expressions if available
            this.displayCommonExpressions(data.optimized_plan_json);
        } catch (error) {
            console.error("Error processing common subexpression data:", error);
            this.showError("Failed to process common subexpression data: " + error.message);
        }
    }
    
    /**
     * Display plan images
     */
    displayPlanImages(originalPlanSVG, optimizedPlanSVG) {
        // Convert SVG content to data URLs
        const originalDataUrl = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(originalPlanSVG)}`;
        const optimizedDataUrl = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(optimizedPlanSVG)}`;
        
        // For the original plan image
        const originalContainer = document.getElementById('subexpr-original-img');
        if (originalContainer) {
            originalContainer.innerHTML = `
                <div class="text-center">
                    <img src="${originalDataUrl}" alt="Original Plan" class="img-fluid plan-image" />
                    <p class="mt-2"><small class="text-muted">Original plan after predicate pushdown</small></p>
                </div>
            `;
        }
        
        // For the optimized plan image
        const optimizedContainer = document.getElementById('subexpr-optimized-img');
        if (optimizedContainer) {
            optimizedContainer.innerHTML = `
                <div class="text-center">
                    <img src="${optimizedDataUrl}" alt="Optimized Plan" class="img-fluid plan-image" />
                    <p class="mt-2"><small class="text-muted">Plan after common subexpression elimination</small></p>
                </div>
            `;
        }
    }
    
    /**
     * Display identified common expressions
     */
    displayCommonExpressions(optimizedPlan) {
        const container = document.getElementById('common-expressions-list');
        if (!container) return;
        
        // Clear container
        container.innerHTML = '';
        
        // Check if the optimized plan has common expressions
        if (optimizedPlan && optimizedPlan.common_expressions) {
            const exprCount = Object.keys(optimizedPlan.common_expressions).length;
            
            if (exprCount > 0) {
                // Create a list to display common expressions
                const exprList = document.createElement('div');
                exprList.className = 'list-group';
                
                for (const [exprId, expr] of Object.entries(optimizedPlan.common_expressions)) {
                    const exprItem = document.createElement('div');
                    exprItem.className = 'list-group-item';
                    
                    // Create header for the expression
                    const exprHeader = document.createElement('h6');
                    exprHeader.textContent = `Expression ID: ${exprId}`;
                    exprHeader.className = 'mb-2 text-primary';
                    exprItem.appendChild(exprHeader);
                    
                    // Add description based on expression type
                    const exprDesc = document.createElement('p');
                    exprDesc.className = 'mb-1';
                    
                    switch (expr.type) {
                        case 'project':
                            exprDesc.textContent = `Projection of columns: ${expr.columns.map(col => 
                                col.table ? `${col.table}.${col.attr}` : col.attr).join(', ')}`;
                            break;
                        case 'base_relation':
                            exprDesc.textContent = `Base relation: ${expr.tables.map(t => t.name).join(', ')}`;
                            break;
                        default:
                            exprDesc.textContent = `Operation type: ${expr.type}`;
                    }
                    
                    exprItem.appendChild(exprDesc);
                    
                    // Add a code preview
                    const exprCode = document.createElement('pre');
                    exprCode.className = 'bg-light p-2 mt-2 border rounded';
                    exprCode.style.fontSize = '0.85rem';
                    exprCode.textContent = JSON.stringify(expr, null, 2);
                    exprItem.appendChild(exprCode);
                    
                    exprList.appendChild(exprItem);
                }
                
                container.appendChild(exprList);
            } else {
                container.innerHTML = '<p class="text-muted">No common expressions identified in this query.</p>';
            }
        } else {
            container.innerHTML = '<p class="text-muted">No common expressions data available.</p>';
        }
    }
    
    /**
     * Display an error message in the container
     */
    showError(message) {
        const errorDiv = document.createElement('div');
        errorDiv.className = 'alert alert-danger';
        errorDiv.textContent = `Common Subexpression Error: ${message}`;
        
        // Clear container and show error
        this.container.innerHTML = '';
        this.container.appendChild(errorDiv);
    }
}

// Function to run common subexpression elimination
function runCommonSubexprElimination() {
    console.log("Running common subexpression elimination...");
    
    // Get the optimized plan from the predicate pushdown step
    if (!window.lastOptimizedPlan) {
        showCommonSubexprError('Please run predicate pushdown optimization first');
        return;
    }
    
    // Show loading indicator
    document.getElementById('common-subexpr-loading').classList.remove('d-none');
    document.getElementById('common-subexpr-error').classList.add('d-none');
    
    console.log("Sending common subexpression elimination request...");
    
    // Send the optimized plan to the common subexpression eliminator
    fetch('/optimize/common_subexpr/', {
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
        console.log("Common subexpression elimination response:", data);
        
        // Hide loading indicator
        document.getElementById('common-subexpr-loading').classList.add('d-none');
        
        if (data.success) {
            try {
                // Create the container for the visualizer
                const container = document.getElementById('common-subexpr-result-container');
                container.innerHTML = '<div id="common-subexpr-visualizer" style="height:700px;width:100%;"></div>';
                
                // Create the visualizer
                const visualizer = new CommonSubexprVisualizer(
                    document.getElementById('common-subexpr-visualizer')
                );
                
                // Process the data
                visualizer.processOptimizationData(data);
                
                // Store the optimized plan for any further steps
                window.commonSubexprOptimizedPlan = data.optimized_plan_json;
                
                // Enable the join optimization button
                document.getElementById('join-optimization-btn').disabled = false;
                
            } catch (error) {
                console.error("Error creating common subexpression visualization:", error);
                showCommonSubexprError('Error creating visualization: ' + error.message);
            }
        } else {
            showCommonSubexprError(data.error || 'An unknown error occurred during common subexpression elimination');
        }
    })
    .catch(error => {
        console.error("Fetch error:", error);
        document.getElementById('common-subexpr-loading').classList.add('d-none');
        showCommonSubexprError('Network error: ' + error.message);
    });
}

// Helper function to show common subexpression elimination error messages
function showCommonSubexprError(message) {
    const errorElement = document.getElementById('common-subexpr-error');
    errorElement.textContent = message;
    errorElement.classList.remove('d-none');
}