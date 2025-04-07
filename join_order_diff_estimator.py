import itertools
import numpy as np
import psycopg2
import re
import time
from typing import List, Dict, Tuple, Set, Callable, Union, FrozenSet
import argparse
from getpass import getpass

class Table:
    def __init__(self, name: str, row_count: int, columns: List[str], 
                 column_ndvs: Dict[str, int] = None,
                 column_mcvs: Dict[str, List[Tuple[any, float]]] = None,
                 width: int = None):
        """
        Initialize a table with its properties
        
        Args:
            name: Table name
            row_count: Number of rows in the table
            columns: List of column names
            column_ndvs: Dictionary mapping column names to number of distinct values
            column_mcvs: Dictionary mapping column names to list of (value, frequency) tuples
            width: Average row width in bytes
        """
        self.name = name
        self.row_count = row_count
        self.columns = columns
        self.column_ndvs = column_ndvs or {col: row_count // 10 for col in columns}
        self.column_mcvs = column_mcvs or {}
        # Estimate width if not provided (assume 20 bytes per column as default)
        self.width = width or len(columns) * 20

class JoinCondition:
    def __init__(self, left_table: str, left_column: str, right_table: str, right_column: str):
        """
        Initialize a join condition
        """
        self.left_table = left_table
        self.left_column = left_column
        self.right_table = right_table
        self.right_column = right_column
    
    def __str__(self):
        return f"{self.left_table}.{self.left_column} = {self.right_table}.{self.right_column}"
    
    def involves_tables(self, tables: Set[str]) -> bool:
        """Check if this join condition involves any of the given tables"""
        return self.left_table in tables or self.right_table in tables
    
    def connects_table_sets(self, set1: Set[str], set2: Set[str]) -> bool:
        """Check if this join condition connects the two sets of tables"""
        return (self.left_table in set1 and self.right_table in set2) or \
               (self.left_table in set2 and self.right_table in set1)

# Representation of a join tree node
class JoinTreeNode:
    def __init__(self, tables: Union[str, Set[str], Tuple['JoinTreeNode', 'JoinTreeNode']], 
                 estimated_rows: int = None, 
                 cost: float = 0.0,
                 join_method: str = None,
                 width: int = None):
        """
        Initialize a join tree node
        """
        # Convert single table to a set
        if isinstance(tables, str):
            self.tables = {tables}
            self.left_child = None
            self.right_child = None
        # If we got a tuple of child nodes, merge their tables
        elif isinstance(tables, tuple) and all(isinstance(t, JoinTreeNode) for t in tables):
            self.left_child, self.right_child = tables
            self.tables = self.left_child.tables.union(self.right_child.tables)
        # Otherwise it should be a set of tables
        else:
            self.tables = set(tables)
            self.left_child = None
            self.right_child = None
            
        self.estimated_rows = estimated_rows
        self.cost = cost
        self.join_method = join_method
        self.width = width
        
    def __str__(self):
        if self.left_child and self.right_child:
            method = f"[{self.join_method}]" if self.join_method else ""
            return f"({self.left_child}) ⋈{method} ({self.right_child})"
        return " ⋈ ".join(self.tables)

class JoinQuery:
    def __init__(self, tables: List[Table], join_conditions: List[JoinCondition]):
        """
        Initialize a join query
        """
        self.tables = {table.name: table for table in tables}
        self.join_conditions = join_conditions
        
        # Build a graph of table connections
        self.graph = self._build_graph()
    
    def _build_graph(self) -> Dict[str, List[Tuple[str, JoinCondition]]]:
        """
        Build an adjacency graph representing the join connections
        """
        graph = {table: [] for table in self.tables}
        
        for cond in self.join_conditions:
            graph[cond.left_table].append((cond.right_table, cond))
            graph[cond.right_table].append((cond.left_table, cond))
            
        return graph
    
    def get_possible_join_orders(self) -> List[Tuple[str, ...]]:
        """
        Generate all possible linear join orders
        """
        return list(itertools.permutations(self.tables.keys()))
    
    def find_relevant_join_conditions(self, tables_set: Set[str]) -> List[JoinCondition]:
        """
        Find all join conditions that involve the given tables
        """
        return [cond for cond in self.join_conditions if cond.involves_tables(tables_set)]
    
    def find_connecting_join_conditions(self, set1: Set[str], set2: Set[str]) -> List[JoinCondition]:
        """
        Find all join conditions that connect two sets of tables
        """
        return [cond for cond in self.join_conditions if cond.connects_table_sets(set1, set2)]

# ======== Different Join Methods ========

class JoinMethod:
    """Base class for different join methods"""
    @staticmethod
    def estimate_cost(outer_rows: int, outer_width: int, 
                      inner_rows: int, inner_width: int,
                      output_rows: int, output_width: int) -> float:
        """Estimate the cost of the join"""
        raise NotImplementedError("Subclasses must implement this")

class NestedLoopJoin(JoinMethod):
    """Nested loop join (expensive for large tables)"""
    @staticmethod
    def estimate_cost(outer_rows: int, outer_width: int, 
                      inner_rows: int, inner_width: int,
                      output_rows: int, output_width: int) -> float:
        # Cost is proportional to outer rows * inner rows
        # as we scan the inner table once for each outer row
        cpu_cost = outer_rows * inner_rows * 0.2  # CPU cost of comparison
        io_cost = outer_rows + (outer_rows * inner_rows * 0.01)  # I/O cost
        return cpu_cost + io_cost

class HashJoin(JoinMethod):
    """Hash join (good for large tables, equal joins)"""
    @staticmethod
    def estimate_cost(outer_rows: int, outer_width: int, 
                      inner_rows: int, inner_width: int,
                      output_rows: int, output_width: int) -> float:
        # Cost is building hash table for smaller table + scanning larger
        build_cost = min(outer_rows, inner_rows) * 1.2  # Build hash table
        probe_cost = max(outer_rows, inner_rows) * 0.1  # Probe hash table
        return build_cost + probe_cost

class MergeJoin(JoinMethod):
    """Merge join (good when inputs are already sorted)"""
    @staticmethod
    def estimate_cost(outer_rows: int, outer_width: int, 
                      inner_rows: int, inner_width: int,
                      output_rows: int, output_width: int) -> float:
        # Cost is sorting (if needed) + merge step
        # Simplifying by assuming data requires sorting
        sort_cost = (outer_rows * np.log2(max(outer_rows, 2)) + 
                     inner_rows * np.log2(max(inner_rows, 2))) * 0.05
        merge_cost = (outer_rows + inner_rows) * 0.02
        return sort_cost + merge_cost

# Map of join methods
JOIN_METHODS = {
    "nested_loop": NestedLoopJoin,
    "hash_join": HashJoin,
    "merge_join": MergeJoin
}

# ======== Cost Estimator Functions ========

def fixed_selectivity_estimator(query: JoinQuery, 
                                left_tables: Union[Set[str], JoinTreeNode], 
                                right_tables: Union[Set[str], JoinTreeNode]) -> Tuple[int, float]:
    """
    Estimate join output size using fixed selectivity
    
    Returns:
        Tuple of (estimated_rows, selectivity)
    """
    # Convert JoinTreeNodes to sets if needed
    if isinstance(left_tables, JoinTreeNode):
        left_set = left_tables.tables
        left_rows = left_tables.estimated_rows
    else:
        left_set = left_tables
        left_rows = sum(query.tables[t].row_count for t in left_set)
    
    if isinstance(right_tables, JoinTreeNode):
        right_set = right_tables.tables
        right_rows = right_tables.estimated_rows
    else:
        right_set = right_tables
        right_rows = sum(query.tables[t].row_count for t in right_set)
    
    # Find connecting join conditions
    join_conditions = query.find_connecting_join_conditions(left_set, right_set)
    
    if not join_conditions:
        # Cross join
        return left_rows * right_rows, 1.0
    
    # Fixed selectivity factor (10%)
    selectivity = 0.1
    
    # Apply selectivity
    return int(left_rows * right_rows * selectivity), selectivity

def ndv_based_estimator(query: JoinQuery, 
                         left_tables: Union[Set[str], JoinTreeNode], 
                         right_tables: Union[Set[str], JoinTreeNode]) -> Tuple[int, float]:
    """
    Estimate join output size using NDV statistics
    
    Returns:
        Tuple of (estimated_rows, selectivity)
    """
    # Convert JoinTreeNodes to sets if needed
    if isinstance(left_tables, JoinTreeNode):
        left_set = left_tables.tables
        left_rows = left_tables.estimated_rows
    else:
        left_set = left_tables
        left_rows = sum(query.tables[t].row_count for t in left_set)
    
    if isinstance(right_tables, JoinTreeNode):
        right_set = right_tables.tables
        right_rows = right_tables.estimated_rows
    else:
        right_set = right_tables
        right_rows = sum(query.tables[t].row_count for t in right_set)
    
    # Find connecting join conditions
    join_conditions = query.find_connecting_join_conditions(left_set, right_set)
    
    if not join_conditions:
        # Cross join
        return left_rows * right_rows, 1.0
    
    # Helper function to find column case-insensitively
    def find_column(table_name, column_name):
        table = query.tables[table_name]
        column_ndvs = table.column_ndvs
        # Try exact match first
        if column_name in column_ndvs:
            return column_name
        # Try case-insensitive match
        column_lower = column_name.lower()
        for col in column_ndvs:
            if col.lower() == column_lower:
                return col
        # If still not found, return None
        return None
    
    # Apply selectivity based on join conditions
    selectivity = 1.0
    for cond in join_conditions:
        # Get columns (handling case differences)
        left_col = find_column(cond.left_table, cond.left_column)
        right_col = find_column(cond.right_table, cond.right_column)
        
        # Get NDVs, using defaults if column wasn't found
        if left_col:
            left_ndv = query.tables[cond.left_table].column_ndvs[left_col]
        else:
            left_ndv = query.tables[cond.left_table].row_count // 10
            
        if right_col:
            right_ndv = query.tables[cond.right_table].column_ndvs[right_col]
        else:
            right_ndv = query.tables[cond.right_table].row_count // 10
        
        # Selectivity is 1/max(NDV)
        cond_selectivity = 1.0 / max(left_ndv, right_ndv)
        selectivity = min(selectivity, cond_selectivity)  # Take the most selective join
    
    # Calculate estimated output rows
    return int(left_rows * right_rows * selectivity), selectivity

def mcv_aware_estimator(query: JoinQuery, 
                         left_tables: Union[Set[str], JoinTreeNode], 
                         right_tables: Union[Set[str], JoinTreeNode]) -> Tuple[int, float]:
    """
    Estimate join output size using MCV statistics
    
    Returns:
        Tuple of (estimated_rows, selectivity)
    """
    # Convert JoinTreeNodes to sets if needed
    if isinstance(left_tables, JoinTreeNode):
        left_set = left_tables.tables
        left_rows = left_tables.estimated_rows
    else:
        left_set = left_tables
        left_rows = sum(query.tables[t].row_count for t in left_set)
    
    if isinstance(right_tables, JoinTreeNode):
        right_set = right_tables.tables
        right_rows = right_tables.estimated_rows
    else:
        right_set = right_tables
        right_rows = sum(query.tables[t].row_count for t in right_set)
    
    # Find connecting join conditions
    join_conditions = query.find_connecting_join_conditions(left_set, right_set)
    
    if not join_conditions:
        # Cross join
        return left_rows * right_rows, 1.0
    
    # Helper function to find column case-insensitively
    def find_column(table_name, column_name, column_dict):
        # Try exact match first
        if column_name in column_dict:
            return column_name
        # Try case-insensitive match
        column_lower = column_name.lower()
        for col in column_dict:
            if col.lower() == column_lower:
                return col
        # If still not found, return None
        return None
    
    # Apply selectivity based on join conditions
    selectivity = 1.0
    for cond in join_conditions:
        # Try to use MCV-based selectivity
        left_mcvs_dict = query.tables[cond.left_table].column_mcvs
        left_col_mcv = find_column(cond.left_table, cond.left_column, left_mcvs_dict)
        
        right_mcvs_dict = query.tables[cond.right_table].column_mcvs
        right_col_mcv = find_column(cond.right_table, cond.right_column, right_mcvs_dict)
        
        if left_col_mcv and right_col_mcv:
            left_mcvs = left_mcvs_dict[left_col_mcv]
            right_mcvs = right_mcvs_dict[right_col_mcv]
            
            if left_mcvs and right_mcvs:
                # Create dictionaries for quick lookup
                left_values = {val: freq for val, freq in left_mcvs}
                right_values = {val: freq for val, freq in right_mcvs}
                
                # Find overlap of common values
                overlap_selectivity = 0.0
                for val in set(left_values.keys()) & set(right_values.keys()):
                    overlap_selectivity += left_values[val] * right_values[val]
                
                if overlap_selectivity > 0:
                    cond_selectivity = overlap_selectivity
                    selectivity = min(selectivity, cond_selectivity)
                    continue
        
        # Fall back to NDV approach if MCV didn't work
        column_ndvs = query.tables[cond.left_table].column_ndvs
        left_col_ndv = find_column(cond.left_table, cond.left_column, column_ndvs)
        
        column_ndvs = query.tables[cond.right_table].column_ndvs
        right_col_ndv = find_column(cond.right_table, cond.right_column, column_ndvs)
        
        # Get NDVs, using defaults if column wasn't found
        if left_col_ndv:
            left_ndv = query.tables[cond.left_table].column_ndvs[left_col_ndv]
        else:
            left_ndv = query.tables[cond.left_table].row_count // 10
            
        if right_col_ndv:
            right_ndv = query.tables[cond.right_table].column_ndvs[right_col_ndv]
        else:
            right_ndv = query.tables[cond.right_table].row_count // 10
        
        # Selectivity is 1/max(NDV)
        cond_selectivity = 1.0 / max(left_ndv, right_ndv)
        selectivity = min(selectivity, cond_selectivity)  # Take the most selective join
    
    # Calculate estimated output rows
    return int(left_rows * right_rows * selectivity), selectivity

# Map estimator names to functions
ESTIMATORS = {
    "fixed": fixed_selectivity_estimator,
    "ndv": ndv_based_estimator,
    "mcv": mcv_aware_estimator
}

# ======== Join Strategy Selection ========

def select_join_method(left_rows: int, right_rows: int, 
                      join_conditions: List[JoinCondition]) -> str:
    """
    Select the appropriate physical join method
    """
    if left_rows < 1000 and right_rows < 1000:
        # For small tables, nested loop is fine
        return "nested_loop"
    elif len(join_conditions) > 0:
        # For equi-joins with larger tables, hash join is usually better
        if min(left_rows, right_rows) < max(left_rows, right_rows) / 10:
            # If tables are very different in size, hash join is much better
            return "hash_join"
        else:
            # For similarly sized tables, merge join can be competitive
            return "merge_join"
    else:
        # For non-equi-joins, might need nested loop
        return "nested_loop"

# ======== Heuristic Join Plan Generators ========

def generate_left_deep_plans(query: JoinQuery, 
                           estimator: str = "ndv",
                           max_plans: int = 10) -> List[JoinTreeNode]:
    """
    Generate left-deep join trees with heuristic pruning
    """
    # Verify estimator is valid
    if estimator not in ESTIMATORS:
        raise ValueError(f"Unknown estimator: {estimator}")
    
    estimator_fn = ESTIMATORS[estimator]
    all_tables = set(query.tables.keys())
    plans = []
    
    # Start with single tables
    base_plans = {}
    for table_name, table in query.tables.items():
        base_plans[table_name] = JoinTreeNode(
            table_name, table.row_count, 0, width=table.width)
    
    # Try different starting points (tables with high selectivity are good starts)
    for start_table in query.tables:
        # Initialize plan with the starting table
        current_plan = base_plans[start_table]
        remaining_tables = all_tables - {start_table}
        
        # Build the plan by adding one table at a time
        while remaining_tables:
            best_next_table = None
            best_cost = float('inf')
            best_plan = None
            
            # Try each remaining table
            for next_table in remaining_tables:
                # Create a node for this table
                next_node = base_plans[next_table]
                
                # Estimate output and cost
                output_rows, _ = estimator_fn(query, current_plan, next_node)
                
                # Select join method
                joining_conditions = query.find_connecting_join_conditions(
                    current_plan.tables, {next_table})
                join_method = select_join_method(
                    current_plan.estimated_rows, next_node.estimated_rows, joining_conditions)
                
                # Estimate output width (simplified)
                output_width = current_plan.width + next_node.width
                
                # Calculate join cost
                join_cost = JOIN_METHODS[join_method].estimate_cost(
                    current_plan.estimated_rows, current_plan.width,
                    next_node.estimated_rows, next_node.width,
                    output_rows, output_width
                )
                
                total_cost = current_plan.cost + join_cost
                
                # Keep track of the best next table
                if total_cost < best_cost:
                    best_cost = total_cost
                    best_next_table = next_table
                    best_plan = JoinTreeNode(
                        (current_plan, next_node), output_rows, 
                        total_cost, join_method, width=output_width)
            
            if best_next_table:
                current_plan = best_plan
                remaining_tables.remove(best_next_table)
            else:
                break  # Shouldn't happen if the join graph is connected
        
        plans.append(current_plan)
    
    # Sort by cost and return the top plans
    plans.sort(key=lambda p: p.cost)
    return plans[:max_plans]

def generate_bushy_plans_with_heuristics(query: JoinQuery, 
                                       estimator: str = "ndv",
                                       max_plans: int = 10,
                                       max_bushy_size: int = 3) -> List[JoinTreeNode]:
    """
    Generate bushy join plans with heuristic pruning
    
    Args:
        query: The join query
        estimator: Name of the estimator to use
        max_plans: Maximum number of plans to return
        max_bushy_size: Maximum number of tables in a bushy subtree
                       (to control combinatorial explosion)
    """
    # Verify estimator is valid
    if estimator not in ESTIMATORS:
        raise ValueError(f"Unknown estimator: {estimator}")
    
    estimator_fn = ESTIMATORS[estimator]
    all_tables = list(query.tables.keys())
    table_count = len(all_tables)
    
    # Start with left-deep plans as a base
    base_plans = generate_left_deep_plans(query, estimator, max_plans=3)
    if not base_plans:
        return []
    
    all_plans = list(base_plans)  # Make a copy to extend
    
    # Use a greedy approach to "bushify" the base plans
    for base_plan in base_plans:
        # Try to find opportunities to convert linear segments to bushy trees
        candidates = []
        
        # Extract linear chain segments from the plan
        def extract_linear_chains(node, chain=None):
            if chain is None:
                chain = []
            
            if node.left_child and node.right_child:
                # For each node with two children
                if len(node.right_child.tables) == 1:
                    # Right child is a single table, so this follows left-deep pattern
                    # Add the right child's table to our chain
                    new_chain = chain + [list(node.right_child.tables)[0]]
                    # Continue down the left branch
                    extract_linear_chains(node.left_child, new_chain)
                    
                    # If chain is long enough, consider it as a candidate for bushification
                    if len(new_chain) >= 3:
                        candidates.append((node, new_chain))
                else:
                    # Not a clear linear chain, recurse both sides
                    extract_linear_chains(node.left_child)
                    extract_linear_chains(node.right_child)
        
        extract_linear_chains(base_plan)
        
        # For each candidate chain, try to bushify it
        for node, chain in candidates:
            # Only process chains of manageable size to avoid combinatorial explosion
            if len(chain) > max_bushy_size:
                chain = chain[:max_bushy_size]
            
            # Generate all possible ways to split this chain
            for split_point in range(1, len(chain)):
                left_tables = set(chain[:split_point])
                right_tables = set(chain[split_point:])
                
                # Skip if there are no join conditions between these table sets
                if not query.find_connecting_join_conditions(left_tables, right_tables):
                    continue
                
                # Create nodes for left and right sides
                left_rows = sum(query.tables[t].row_count for t in left_tables)
                left_width = sum(query.tables[t].width for t in left_tables)
                left_node = JoinTreeNode(left_tables, left_rows, 0, width=left_width)
                
                right_rows = sum(query.tables[t].row_count for t in right_tables)
                right_width = sum(query.tables[t].width for t in right_tables)
                right_node = JoinTreeNode(right_tables, right_rows, 0, width=right_width)
                
                # Estimate the bushy subtree
                output_rows, _ = estimator_fn(query, left_node, right_node)
                
                # Select join method
                joining_conditions = query.find_connecting_join_conditions(
                    left_tables, right_tables)
                join_method = select_join_method(
                    left_rows, right_rows, joining_conditions)
                
                # Calculate join cost
                output_width = left_width + right_width
                join_cost = JOIN_METHODS[join_method].estimate_cost(
                    left_rows, left_width,
                    right_rows, right_width,
                    output_rows, output_width
                )
                
                bushy_node = JoinTreeNode(
                    (left_node, right_node), output_rows, 
                    join_cost, join_method, width=output_width)
                
                # Create a new plan by replacing the linear chain with the bushy subtree
                # (simplified - in a real implementation, would need to modify the tree)
                all_plans.append(bushy_node)
    
    # Sort by cost and return the top plans
    all_plans.sort(key=lambda p: p.cost)
    return all_plans[:max_plans]

# ======== Visualization Functions ========

def print_join_tree(node: JoinTreeNode, indent: int = 0):
    """
    Pretty-print a join tree
    """
    if node.left_child and node.right_child:
        join_method = f" [{node.join_method}]" if node.join_method else ""
        print(f"{' ' * indent}⋈{join_method} ({node.estimated_rows} rows, cost: {node.cost:.2f})")
        print(f"{' ' * (indent+2)}Left:")
        print_join_tree(node.left_child, indent + 4)
        print(f"{' ' * (indent+2)}Right:")
        print_join_tree(node.right_child, indent + 4)
    else:
        tables_str = ", ".join(node.tables)
        print(f"{' ' * indent}Scan: {tables_str} ({node.estimated_rows} rows, cost: {node.cost:.2f})")

def visualize_query_plans(query: JoinQuery, 
                        plan_generators: List[Tuple[str, Callable]], 
                        estimators: List[str],
                        max_plans: int = 2):
    """
    Generate and visualize query plans for different combinations
    
    Args:
        query: The join query
        plan_generators: List of (name, generator_function) tuples
        estimators: List of estimator names to try
        max_plans: Maximum number of plans to show per category
    """
    print("\n=== Comparing Join Plan Optimization Strategies ===")
    results = []
    
    for gen_name, generator in plan_generators:
        for est_name in estimators:
            print(f"\nGenerating {gen_name} plans with {est_name} estimator...")
            
            try:
                start_time = time.time()
                plans = generator(query, est_name, max_plans=max_plans)
                end_time = time.time()
                
                if not plans:
                    print("No valid plans generated!")
                    continue
                
                print(f"Generated {len(plans)} plans in {end_time - start_time:.2f} seconds")
                print(f"\nBest Plan (cost: {plans[0].cost:.2f}):")
                print_join_tree(plans[0])
                
                # Save result
                results.append({
                    'generator': gen_name,
                    'estimator': est_name,
                    'plan': plans[0],
                    'cost': plans[0].cost,
                    'time': end_time - start_time
                })
                
                if len(plans) > 1 and max_plans > 1:
                    print(f"\nSecond Best Plan (cost: {plans[1].cost:.2f}):")
                    print_join_tree(plans[1])
            except Exception as e:
                print(f"Error generating plans: {e}")
    
    # Print summary comparison
    print("\n=== Summary of Optimization Strategies ===")
    print(f"{'Strategy':<20} {'Estimator':<10} {'Cost':<12} {'Time (s)':<10}")
    print("-" * 52)
    
    # Sort by cost
    results.sort(key=lambda x: x['cost'])
    
    for result in results:
        strategy = result['generator']
        estimator = result['estimator']
        cost = result['cost']
        timing = result['time']
        print(f"{strategy:<20} {estimator:<10} {cost:<12.2f} {timing:<10.2f}")
    
    if results:
        best_result = results[0]
        print(f"\nBest overall strategy: {best_result['generator']} with {best_result['estimator']} estimator")
        print(f"Cost: {best_result['cost']:.2f}")
        print("\nPlan details:")
        print_join_tree(best_result['plan'])

# ======== PostgreSQL-related Functions ========

def parse_join_condition(condition_str: str) -> JoinCondition:
    """
    Parse a join condition string of the form "table1.col1 = table2.col2"
    
    Args:
        condition_str: The join condition string
    
    Returns:
        JoinCondition object
    """
    # Strip whitespace and split by equals
    parts = condition_str.strip().split('=')
    if len(parts) != 2:
        raise ValueError(f"Invalid join condition: {condition_str}. Expected format: table1.col1 = table2.col2")
    
    # Parse left and right sides
    left_parts = parts[0].strip().split('.')
    right_parts = parts[1].strip().split('.')
    
    if len(left_parts) != 2 or len(right_parts) != 2:
        raise ValueError(f"Invalid join condition: {condition_str}. Expected format: table1.col1 = table2.col2")
    
    return JoinCondition(left_parts[0], left_parts[1], right_parts[0], right_parts[1])

def extract_tables_from_sql(sql: str) -> List[str]:
    """
    Extract table names from a SQL query
    
    Args:
        sql: SQL query string
    
    Returns:
        List of table names
    """
    # This is a simple regex-based approach; real-world SQL parsing would be more complex
    from_regex = r'FROM\s+([a-zA-Z0-9_,\s]+)(?:WHERE|JOIN|ORDER BY|GROUP BY|$)'
    join_regex = r'JOIN\s+([a-zA-Z0-9_]+)'
    
    tables = []
    
    # Look for tables in FROM clause
    from_matches = re.search(from_regex, sql, re.IGNORECASE)

    if from_matches:
        from_tables = from_matches.group(1).split(',')
        for table in from_tables:
            table_name = table.strip()
            if table_name:  # Skip empty strings
                tables.append(table_name)

    # Look for tables in JOIN clauses
    join_matches = re.findall(join_regex, sql, re.IGNORECASE)

    tables.extend(join_matches)
    
    return tables

def extract_join_conditions_from_sql(sql: str) -> List[str]:
    """
    Extract join conditions from a SQL query
    
    Args:
        sql: SQL query string
    
    Returns:
        List of join condition strings
    """
    # Look for join conditions in ON clauses or WHERE clause
    on_regex = r'ON\s+([a-zA-Z0-9_\.]+\s*=\s*[a-zA-Z0-9_\.]+)'
    where_regex = r'WHERE\s+(.+?)(?:ORDER BY|GROUP BY|LIMIT|$)'
    
    conditions = []
    
    # Extract ON conditions
    on_matches = re.findall(on_regex, sql, re.IGNORECASE)
    conditions.extend(on_matches)
    
    # Extract WHERE conditions
    where_matches = re.search(where_regex, sql, re.IGNORECASE)
    if where_matches:
        where_clause = where_matches.group(1)
        # Split by AND and look for equi-joins (table1.col1 = table2.col2)
        where_parts = where_clause.split('AND')
        for part in where_parts:
            if '=' in part and '.' in part:
                # Check if this is likely a join condition (table1.col1 = table2.col2)
                # by ensuring both sides have a table reference (contains a dot)
                left_right = part.split('=')
                if len(left_right) == 2 and '.' in left_right[0] and '.' in left_right[1]:
                    # Ensure it's not comparing with a string literal (contains quotes)
                    if "'" not in part and '"' not in part:
                        conditions.append(part.strip())
    
    return conditions

def get_table_stats_from_postgres(conn, table_name: str) -> Table:
    """
    Get table statistics from PostgreSQL
    
    Args:
        conn: PostgreSQL connection
        table_name: Name of the table
    
    Returns:
        Table object with statistics
    """
    cursor = conn.cursor()
    
    # Get row count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cursor.fetchone()[0]
    
    # Get columns
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
    """)
    columns = [row[0] for row in cursor.fetchall()]
    
    # Get average row width
    try:
        cursor.execute(f"""
            SELECT avg_width
            FROM pg_stats
            WHERE tablename = '{table_name}'
            LIMIT 1
        """)
        row = cursor.fetchone()
        if row and row[0]:
            width = row[0] * len(columns)  # Rough approximation
        else:
            width = len(columns) * 20  # Default approximation
    except Exception:
        width = len(columns) * 20  # Default if error
    
    # Get NDV (number of distinct values) for each column
    column_ndvs = {}
    for column in columns:
        try:
            cursor.execute(f"SELECT COUNT(DISTINCT {column}) FROM {table_name}")
            ndv = cursor.fetchone()[0]
            column_ndvs[column] = max(1, ndv)  # Avoid division by zero
        except Exception:
            # Default if error
            column_ndvs[column] = max(1, row_count // 10)
    
    # Get MCV (most common values) for each column
    # PostgreSQL provides this through pg_stats view
    column_mcvs = {}
    for column in columns:
        try:
            cursor.execute(f"""
                SELECT most_common_vals, most_common_freqs
                FROM pg_stats
                WHERE tablename = '{table_name}' AND attname = '{column}'
            """)
            stats = cursor.fetchone()
            
            if stats and stats[0] is not None and stats[1] is not None:
                # Handle different data types returned by PostgreSQL
                vals = []
                freqs = []
                
                # Handle array literal strings
                if isinstance(stats[0], str):
                    vals = stats[0].strip('{}').split(',')
                # Handle actual arrays
                elif isinstance(stats[0], list):
                    vals = stats[0]
                
                if isinstance(stats[1], str):
                    freqs = stats[1].strip('{}').split(',')
                elif isinstance(stats[1], list):
                    freqs = stats[1]
                
                mcvs = []
                for i in range(min(len(vals), len(freqs))):
                    try:
                        if isinstance(vals[i], str):
                            val = vals[i].strip('"\'')
                        else:
                            val = vals[i]
                            
                        if isinstance(freqs[i], str):
                            freq = float(freqs[i])
                        else:
                            freq = float(freqs[i])
                            
                        mcvs.append((val, freq))
                    except (ValueError, IndexError) as e:
                        pass
                
                if mcvs:
                    column_mcvs[column] = mcvs
        except Exception as e:
            pass  # Skip if can't get MCV stats
    
    cursor.close()
    return Table(table_name, row_count, columns, column_ndvs, column_mcvs, width)

def main():
    parser = argparse.ArgumentParser(description='Advanced Join Order Optimizer for PostgreSQL')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--user', default='postgres', help='PostgreSQL user')
    parser.add_argument('--database', required=True, help='PostgreSQL database name')
    parser.add_argument('--query-file', help='File containing SQL query (optional)')
    parser.add_argument('--estimator', choices=['fixed', 'ndv', 'mcv', 'all'], default='all',
                        help='Estimator to use (default: try all)')
    parser.add_argument('--strategy', choices=['left-deep', 'bushy', 'all'], default='all',
                        help='Join tree strategy (default: try all)')
    
    args = parser.parse_args()
    
    # Get password
    password = getpass(f"Enter password for user {args.user}: ")
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            password=password,
            database=args.database
        )
        print(f"Connected to PostgreSQL database {args.database} on {args.host}:{args.port}")
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return
    
    # Get SQL query
    sql_query = None
    if args.query_file:
        try:
            with open(args.query_file, 'r') as f:
                sql_query = f.read()
        except Exception as e:
            print(f"Error reading query file: {e}")
    
    if not sql_query:
        print("\nEnter your SQL join query (press Ctrl+D or type 'END' on a new line to finish):")
        sql_lines = []
        while True:
            try:
                line = input()
                if line.strip() == 'END':
                    break
                sql_lines.append(line)
            except EOFError:
                break
        sql_query = '\n'.join(sql_lines)
    
    print(f"\nAnalyzing query:\n{sql_query}\n")
    
    # Extract tables and join conditions
    table_names = extract_tables_from_sql(sql_query)
    join_condition_strs = extract_join_conditions_from_sql(sql_query)
    
    print(f"Found tables: {', '.join(table_names)}")
    print(f"Found join conditions: {', '.join(join_condition_strs)}")
    
    # Get table statistics from PostgreSQL
    tables = []
    for table_name in table_names:
        try:
            table = get_table_stats_from_postgres(conn, table_name)
            tables.append(table)
            print(f"Loaded statistics for {table_name}: {table.row_count} rows, {len(table.columns)} columns")
        except Exception as e:
            print(f"Error getting statistics for table {table_name}: {e}")
            return
    
    # Parse join conditions
    join_conditions = []
    for condition_str in join_condition_strs:
        try:
            condition = parse_join_condition(condition_str)
            join_conditions.append(condition)
        except Exception as e:
            print(f"Error parsing join condition '{condition_str}': {e}")
    
    if not join_conditions:
        print("Warning: No valid join conditions found. This may result in cross joins.")
    
    # Create query object
    query = JoinQuery(tables, join_conditions)
    
    # Determine which estimators to use
    estimators_to_use = ['fixed', 'ndv', 'mcv'] if args.estimator == 'all' else [args.estimator]
    
    # Determine which strategies to use
    strategies = []
    if args.strategy == 'all' or args.strategy == 'left-deep':
        strategies.append(('Left-Deep', generate_left_deep_plans))
    if args.strategy == 'all' or args.strategy == 'bushy':
        strategies.append(('Bushy (Heuristic)', generate_bushy_plans_with_heuristics))
    
    print("\nEvaluating join orders...")
    # Limit to 8 tables maximum to avoid factorial explosion
    if len(tables) > 8:
        print(f"Warning: {len(tables)} tables detected. Using heuristic approaches to avoid combinatorial explosion.")
    
    # Visualize and compare different strategies
    visualize_query_plans(query, strategies, estimators_to_use)
    
    # Close connection
    conn.close()

if __name__ == "__main__":
    main()