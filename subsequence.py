#!/usr/bin/env python3
"""
Query Tree Optimizer - Common Expressions Only

This program takes a JSON query tree, identifies common subexpressions,
and produces an optimized representation with eliminated redundancies.
It focuses only on common expressions and does not use column_refs.
"""

import json
import copy
import sys
import argparse


class QueryTreeOptimizer:
    """Query Tree Optimizer that eliminates common subexpressions."""
    
    def __init__(self):
        self.common_expressions = {}  # Maps expr_id to expression definition
        self.next_expr_id = 0
        
    def optimize(self, query_tree):
        """
        Main optimization function that processes the query tree and returns
        an optimized representation.
        """
        # Make a deep copy of the query tree to avoid modifying the original
        tree_copy = copy.deepcopy(query_tree)
        
        # Identify common subexpressions
        common_exprs = self._identify_common_exprs(tree_copy)
        
        # Replace common subexpressions with references
        optimized_tree = self._replace_common_exprs(tree_copy, common_exprs)
        
        # Build the final optimized representation
        result = {
            "metadata": {
                "version": "1.0",
                "optimization_level": "expressions_only"
            },
            "common_expressions": self.common_expressions,
            "query": optimized_tree
        }
        
        return result
    
    def _identify_common_exprs(self, tree):
        """
        Identify common subexpressions.
        Returns a mapping from serialized expression to list of node references.
        """
        expr_map = {}  # Maps serialized expression to list of (node, path) tuples
        
        def is_significant_expr(node):
            """Check if a node is significant enough to be considered a common subexpression"""
            # Consider column references as valid expressions
            if isinstance(node, dict) and 'table' in node and 'attr' in node:
                # Only consider column references significant if they're part of a larger expression
                return False  # We're skipping column_refs in this version
            
            return (isinstance(node, dict) and 
                    'type' in node and 
                    len(node) >= 2)  # Consider any expression with at least 2 attributes
        
        def serialize_for_comparison(node):
            """Serialize a node for comparison, ignoring metadata fields"""
            if not isinstance(node, dict):
                return json.dumps(node)
            
            clean_node = {k: v for k, v in node.items() if not k.startswith('_')}
            return json.dumps(clean_node, sort_keys=True)
        
        def traverse(node, path=None):
            if path is None:
                path = []
                
            if not isinstance(node, dict):
                return
            
            # Process children first (post-order traversal for bottom-up optimization)
            for k, v in list(node.items()):
                if isinstance(v, dict):
                    traverse(v, path + [k])
                elif isinstance(v, list):
                    for i, item in enumerate(v):
                        if isinstance(item, dict):
                            traverse(item, path + [k, i])
            
            # Check if this is a significant expression
            if is_significant_expr(node):
                expr_str = serialize_for_comparison(node)
                if expr_str not in expr_map:
                    expr_map[expr_str] = []
                expr_map[expr_str].append((node, path))
        
        traverse(tree)
        
        # Filter to only include expressions that appear multiple times
        # Also filter by size to prioritize larger expressions
        return {k: v for k, v in expr_map.items() if len(v) > 1 and len(k) > 20}
    
    def _replace_common_exprs(self, tree, common_exprs):
        """
        Replace common subexpressions with expr_ref nodes.
        """
        # Sort expressions by size (descending) to handle larger expressions first
        sorted_exprs = sorted(common_exprs.items(), key=lambda x: -len(x[0]))
        
        # Track which nodes have been replaced
        processed_nodes = set()
        
        # Create expression definitions and replace occurrences
        for expr_str, references in sorted_exprs:
            # Skip if references are less than 2 (should not happen due to filtering)
            if len(references) < 2:
                continue
            
            # Use the first occurrence as the definition
            node_to_extract, _ = references[0]
            
            # Skip if this node has already been processed
            # (could happen if it's part of another larger common expression)
            node_id = id(node_to_extract)
            if node_id in processed_nodes:
                continue
            
            # Create expression definition
            expr_id = f"expr_{self.next_expr_id}"
            self.next_expr_id += 1
            self.common_expressions[expr_id] = copy.deepcopy(node_to_extract)
            
            # Replace all occurrences with expr_ref
            for node, _ in references:
                # Skip if this node has already been processed
                node_id = id(node)
                if node_id in processed_nodes:
                    continue
                
                # Keep a copy of the original for debugging
                node["_original"] = copy.deepcopy(node)
                
                # Replace with expr_ref
                for k in list(node.keys()):
                    if not k.startswith('_'):
                        node.pop(k)
                
                node["type"] = "expr_ref"
                node["id"] = expr_id
                
                processed_nodes.add(node_id)
        
        return tree
    
    def cleanup_tree(self, tree):
        """
        Remove internal metadata fields (starting with _) from the final representation.
        """
        if not isinstance(tree, dict):
            return tree
            
        result = {}
        for k, v in tree.items():
            if not k.startswith('_'):
                if isinstance(v, dict):
                    result[k] = self.cleanup_tree(v)
                elif isinstance(v, list):
                    result[k] = [self.cleanup_tree(item) if isinstance(item, dict) else item for item in v]
                else:
                    result[k] = v
                    
        return result
    
    def optimize_and_cleanup(self, query_tree):
        """
        Optimize the query tree and remove internal metadata fields.
        """
        optimized = self.optimize(query_tree)
        optimized["query"] = self.cleanup_tree(optimized["query"])
        
        # Clean up the common expressions too
        cleaned_exprs = {}
        for expr_id, expr in optimized["common_expressions"].items():
            cleaned_exprs[expr_id] = self.cleanup_tree(expr)
        optimized["common_expressions"] = cleaned_exprs
        
        return optimized


def main():
    parser = argparse.ArgumentParser(description="Optimize a query tree by identifying and eliminating common subexpressions.")
    parser.add_argument('-i', '--input', type=str, help='Input JSON file (default: stdin)')
    parser.add_argument('-o', '--output', type=str, help='Output JSON file (default: stdout)')
    parser.add_argument('--pretty', action='store_true', help='Pretty-print the output JSON')
    parser.add_argument('--stats', action='store_true', help='Print optimization statistics')
    parser.add_argument('--min-size', type=int, default=20, help='Minimum size in characters for common expressions')
    args = parser.parse_args()

    # Read input
    if args.input:
        with open(args.input, 'r') as f:
            query_tree = json.load(f)
    else:
        try:
            query_tree = json.load(sys.stdin)
        except json.JSONDecodeError:
            print("Error: Invalid JSON input", file=sys.stderr)
            sys.exit(1)

    # Run the optimizer
    optimizer = QueryTreeOptimizer()
    optimized_tree = optimizer.optimize_and_cleanup(query_tree)

    # Print statistics if requested
    if args.stats:
        original_size = len(json.dumps(query_tree))
        optimized_size = len(json.dumps(optimized_tree))
        size_difference = original_size - optimized_size
        percentage = (size_difference / original_size) * 100 if original_size > 0 else 0

        print(f"Original query tree size: {original_size} characters", file=sys.stderr)
        print(f"Optimized query tree size: {optimized_size} characters", file=sys.stderr)
        print(f"Size difference: {size_difference} characters ({percentage:.2f}%)", file=sys.stderr)
        print(f"Common expressions extracted: {len(optimized_tree['common_expressions'])}", file=sys.stderr)
        
        # Show expression details
        if optimized_tree['common_expressions']:
            print("\nExtracted expressions:", file=sys.stderr)
            for expr_id, expr in optimized_tree['common_expressions'].items():
                size = len(json.dumps(expr))
                type_str = expr.get('type', 'unknown')
                print(f"- {expr_id}: {type_str} expression ({size} characters)", file=sys.stderr)

    # Write output
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(optimized_tree, f, indent=2 if args.pretty else None)
    else:
        json.dump(optimized_tree, sys.stdout, indent=2 if args.pretty else None)
        print()  # Add newline

if __name__ == "__main__":
    main()