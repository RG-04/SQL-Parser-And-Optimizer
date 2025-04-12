#!/usr/bin/env python3
"""
Query Tree Visualizer

This program visualizes both the original JSON query tree and the optimized query tree,
making it easier to understand the optimizations made by the common subexpression eliminator.
"""

import json
import argparse
import os
import sys
from graphviz import Digraph

def truncate_text(text, max_length=40):
    """Truncate text to a maximum length."""
    if len(text) > max_length:
        return text[:max_length-3] + "..."
    return text

def get_node_label(node, max_length=40):
    """Create a label for a node based on its content."""
    if not isinstance(node, dict):
        return truncate_text(str(node), max_length)
    
    # Handle expression references
    if node.get("type") == "expr_ref":
        return f"expr_ref: {node.get('id', 'unknown')}"
    
    # Handle column references
    if node.get("type") == "col_ref":
        return f"col_ref: {node.get('id', 'unknown')}"
    
    # Handle table.attr references
    if "table" in node and "attr" in node:
        return f"{node['table']}.{node['attr']}"
    
    # Handle normal expressions
    type_str = node.get("type", "unknown")
    if "value" in node:
        return f"{type_str}: {truncate_text(str(node['value']), max_length)}"
    
    return type_str

def get_node_color(node):
    """Determine color for a node."""
    if not isinstance(node, dict):
        return "gray"
    
    # Color by node type
    node_type = node.get("type", "")
    
    if node_type == "expr_ref":
        return "orange"
    elif node_type == "col_ref":
        return "blue"
    elif node_type == "select":
        return "red"
    elif node_type == "project":
        return "green"
    elif node_type == "join":
        return "purple"
    elif node_type == "base_relation":
        return "brown"
    elif node_type == "AND" or node_type == "OR":
        return "pink"
    elif node_type in ["GT", "LT", "GE", "LE", "EQ", "NE"]:
        return "cyan"
    elif "table" in node and "attr" in node:
        return "blue"
    else:
        return "gray"

def add_node_to_graph(graph, node, node_id, parent_id=None, edge_label=None):
    """Add a node to the graph with an edge from its parent."""
    label = get_node_label(node)
    color = get_node_color(node)
    
    # Add the node
    graph.node(str(node_id), label=label, style="filled", fillcolor=color, shape="box")
    
    # Add edge from parent if applicable
    if parent_id is not None:
        graph.edge(str(parent_id), str(node_id), label=edge_label if edge_label else "")
    
    return node_id

def build_tree_graph(graph, data, node_id=0, parent_id=None, edge_label=None):
    """Recursively build a graph from a JSON tree."""
    # Add current node
    current_id = add_node_to_graph(graph, data, node_id, parent_id, edge_label)
    next_id = node_id + 1
    
    # Process children if this is a dictionary
    if isinstance(data, dict):
        for key, value in data.items():
            # Skip metadata fields
            if key.startswith("_"):
                continue
                
            if isinstance(value, (dict, list)):
                child_id, next_id = build_tree_graph(graph, value, next_id, current_id, key)
    
    # Process items if this is a list
    elif isinstance(data, list):
        for i, item in enumerate(data):
            if isinstance(item, (dict, list)):
                child_id, next_id = build_tree_graph(graph, item, next_id, current_id, str(i))
    
    return current_id, next_id

def visualize_expression_defs(graph, expressions, start_id=0):
    """Visualize the common expression definitions."""
    next_id = start_id
    
    # Add header node
    graph.node(str(next_id), label="Common Expressions", style="filled", 
               fillcolor="gold", shape="box")
    header_id = next_id
    next_id += 1
    
    # Add each expression
    for expr_id, expr in expressions.items():
        # Create a label for this expression
        expr_label = f"{expr_id}\n({get_node_label(expr)})"
        
        # Add node for this expression ID
        graph.node(str(next_id), label=expr_label, style="filled", 
                   fillcolor="orange", shape="box")
        graph.edge(str(header_id), str(next_id))
        expr_node_id = next_id
        next_id += 1
        
        # Add the expression tree
        _, next_id = build_tree_graph(graph, expr, next_id, expr_node_id)
    
    return next_id

def visualize_query_tree(original_tree, optimized_tree=None, output_file="query_tree_viz"):
    """
    Create a visualization of the query tree.
    
    Args:
        original_tree: The original query tree
        optimized_tree: The optimized query tree (optional)
        output_file: Base name for the output file
    
    Returns:
        Path to the generated visualization file
    """
    # Create the graph
    if optimized_tree:
        # Side by side comparison
        graph = Digraph("query_trees", filename=output_file, format="pdf")
        graph.attr(rankdir="LR")
        
        # Create two clusters for the trees
        with graph.subgraph(name="cluster_original") as c1:
            c1.attr(label="Original Query Tree", style="filled", color="lightgrey")
            build_tree_graph(c1, original_tree, node_id=0)
        
        with graph.subgraph(name="cluster_optimized") as c2:
            c2.attr(label="Optimized Query Tree", style="filled", color="lightblue")
            
            # Start with expression defs if available
            next_id = 100  # Offset to avoid ID conflicts
            
            if "common_expressions" in optimized_tree:
                next_id = visualize_expression_defs(c2, optimized_tree["common_expressions"], next_id)
            
            build_tree_graph(c2, optimized_tree.get("query", optimized_tree), next_id)
    else:
        # Single tree visualization
        graph = Digraph("query_tree", filename=output_file, format="pdf")
        graph.attr(rankdir="TB")
        build_tree_graph(graph, original_tree, node_id=0)
    
    # Render the graph
    graph.render()
    
    # Return the path to the generated file
    return f"{output_file}.pdf"

def visualize_from_json_files(original_file, optimized_file=None, output_file="query_tree_viz"):
    """Load JSON from files and visualize the trees."""
    with open(original_file, 'r') as f:
        original_tree = json.load(f)
    
    optimized_tree = None
    if optimized_file:
        with open(optimized_file, 'r') as f:
            optimized_tree = json.load(f)
    
    return visualize_query_tree(original_tree, optimized_tree, output_file)

def main():
    parser = argparse.ArgumentParser(description="Visualize query trees from JSON.")
    parser.add_argument("original", help="Path to the original query tree JSON file")
    parser.add_argument("-o", "--optimized", help="Path to the optimized query tree JSON file")
    parser.add_argument("-f", "--output-file", default="query_tree_viz", 
                        help="Base name for the output file (without extension)")
    args = parser.parse_args()
    
    if not os.path.exists(args.original):
        print(f"Error: Original file '{args.original}' not found")
        sys.exit(1)
    
    if args.optimized and not os.path.exists(args.optimized):
        print(f"Error: Optimized file '{args.optimized}' not found")
        sys.exit(1)
    
    output_path = visualize_from_json_files(args.original, args.optimized, args.output_file)
    print(f"Visualization created: {output_path}")

if __name__ == "__main__":
    main()