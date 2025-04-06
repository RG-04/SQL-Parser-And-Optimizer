import itertools
import numpy as np
from typing import List, Dict, Tuple, Set, Callable, Union, FrozenSet

class Table:
    def __init__(self, name: str, row_count: int, columns: List[str], 
                 column_ndvs: Dict[str, int] = None,
                 column_mcvs: Dict[str, List[Tuple[any, float]]] = None):
        """
        Initialize a table with its properties
        """
        self.name = name
        self.row_count = row_count
        self.columns = columns
        self.column_ndvs = column_ndvs or {col: row_count // 10 for col in columns}
        self.column_mcvs = column_mcvs or {}

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

# New class to represent a join tree node
class JoinTreeNode:
    def __init__(self, tables: Union[str, Set[str], 'JoinTreeNode', Tuple['JoinTreeNode', 'JoinTreeNode']], 
                 estimated_rows: int = None, 
                 cost: float = 0.0,
                 join_method: str = None):
        """
        Initialize a join tree node
        
        Args:
            tables: Either a single table name, a set of table names, or child nodes
            estimated_rows: Estimated number of rows in the result
            cost: Cost to produce this node
            join_method: Physical join algorithm to use
        """
        # Convert single table to a set
        if isinstance(tables, str):
            self.tables = {tables}
        # If we got a tuple of child nodes, merge their tables
        elif isinstance(tables, tuple) and all(isinstance(t, JoinTreeNode) for t in tables):
            self.left_child, self.right_child = tables
            self.tables = self.left_child.tables.union(self.right_child.tables)
        # Otherwise it should be a set of tables
        else:
            self.tables = set(tables)
            
        self.estimated_rows = estimated_rows
        self.cost = cost
        self.join_method = join_method
        
    def __str__(self):
        if hasattr(self, 'left_child') and hasattr(self, 'right_child'):
            return f"({self.left_child}) ⋈ ({self.right_child})"
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
        sort_cost = (outer_rows * np.log2(outer_rows) + inner_rows * np.log2(inner_rows)) * 0.05
        merge_cost = (outer_rows + inner_rows) * 0.02
        return sort_cost + merge_cost

# Map of join methods
JOIN_METHODS = {
    "nested_loop": NestedLoopJoin,
    "hash_join": HashJoin,
    "merge_join": MergeJoin
}

# ======== Cost Estimator Functions ========

def estimate_output_rows(query: JoinQuery, 
                        left_tables: Union[Set[str], JoinTreeNode], 
                        right_tables: Union[Set[str], JoinTreeNode],
                        method: str = "ndv") -> int:
    """
    Estimate the number of rows in the join output
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
        return left_rows * right_rows
    
    # Apply selectivity based on join conditions
    selectivity = 1.0
    for cond in join_conditions:
        if method == "fixed":
            # Simple fixed selectivity
            cond_selectivity = 0.1
        elif method == "ndv":
            # Use NDV-based selectivity
            left_ndv = query.tables[cond.left_table].column_ndvs.get(
                cond.left_column, query.tables[cond.left_table].row_count // 10)
            right_ndv = query.tables[cond.right_table].column_ndvs.get(
                cond.right_column, query.tables[cond.right_table].row_count // 10)
            cond_selectivity = 1.0 / max(left_ndv, right_ndv)
        elif method == "mcv":
            # Try to use MCV-based selectivity
            left_mcvs = query.tables[cond.left_table].column_mcvs.get(cond.left_column, [])
            right_mcvs = query.tables[cond.right_table].column_mcvs.get(cond.right_column, [])
            
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
                else:
                    # Fall back to NDV approach
                    left_ndv = query.tables[cond.left_table].column_ndvs.get(
                        cond.left_column, query.tables[cond.left_table].row_count // 10)
                    right_ndv = query.tables[cond.right_table].column_ndvs.get(
                        cond.right_column, query.tables[cond.right_table].row_count // 10)
                    cond_selectivity = 1.0 / max(left_ndv, right_ndv)
            else:
                # Fall back to NDV approach
                left_ndv = query.tables[cond.left_table].column_ndvs.get(
                    cond.left_column, query.tables[cond.left_table].row_count // 10)
                right_ndv = query.tables[cond.right_table].column_ndvs.get(
                    cond.right_column, query.tables[cond.right_table].row_count // 10)
                cond_selectivity = 1.0 / max(left_ndv, right_ndv)
        
        selectivity = min(selectivity, cond_selectivity)
    
    # Calculate estimated output rows
    return int(left_rows * right_rows * selectivity)

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
        return "hash_join"
    else:
        # For non-equi-joins, might need nested loop
        return "nested_loop"

# ======== Different Join Plan Generators ========

def generate_left_deep_plans(query: JoinQuery, 
                           estimator_method: str = "ndv") -> List[JoinTreeNode]:
    """
    Generate all possible left-deep join trees
    """
    plans = []
    
    for join_order in query.get_possible_join_orders():
        root = JoinTreeNode(join_order[0], query.tables[join_order[0]].row_count, 0)
        
        for next_table in join_order[1:]:
            # Estimate join output
            next_node = JoinTreeNode(next_table, query.tables[next_table].row_count, 0)
            output_rows = estimate_output_rows(query, root, next_node, estimator_method)
            
            # Select join method
            joining_conditions = query.find_connecting_join_conditions(root.tables, {next_table})
            join_method = select_join_method(root.estimated_rows, next_node.estimated_rows, joining_conditions)
            
            # Calculate join cost
            join_cost = JOIN_METHODS[join_method].estimate_cost(
                root.estimated_rows, 1,  # Simplifying width to 1
                next_node.estimated_rows, 1,
                output_rows, 1
            )
            
            # Create new node
            new_root = JoinTreeNode((root, next_node), output_rows, root.cost + next_node.cost + join_cost, join_method)
            root = new_root
        
        plans.append(root)
    
    # Sort by cost
    plans.sort(key=lambda p: p.cost)
    return plans

def generate_bushy_plans(query: JoinQuery, 
                        estimator_method: str = "ndv",
                        max_size: int = 8) -> List[JoinTreeNode]:
    """
    Generate bushy join plans (allowing joins between intermediate results)
    Limited to max_size tables to prevent combinatorial explosion
    """
    if len(query.tables) > max_size:
        print(f"Warning: Limiting bushy plan generation to {max_size} tables to avoid combinatorial explosion")
        # Fall back to left-deep plans for large queries
        return generate_left_deep_plans(query, estimator_method)
    
    # Start with base table nodes
    table_nodes = {}
    for table_name, table in query.tables.items():
        table_nodes[frozenset([table_name])] = JoinTreeNode(
            table_name, table.row_count, 0)
    
    # Initialize best plans
    best_plans = {fs: node for fs, node in table_nodes.items()}
    
    # Bottom-up dynamic programming to build optimal plans
    for size in range(2, len(query.tables) + 1):
        for tables in itertools.combinations(query.tables.keys(), size):
            tables_set = frozenset(tables)
            best_cost = float('inf')
            best_plan = None
            
            # Try all ways to split this set
            for i in range(1, size):
                for left_tables in itertools.combinations(tables, i):
                    left_set = frozenset(left_tables)
                    right_set = tables_set - left_set
                    
                    # Skip if there are no join conditions between these sets
                    if not query.find_connecting_join_conditions(left_set, right_set):
                        continue
                    
                    left_plan = best_plans[left_set]
                    right_plan = best_plans[right_set]
                    
                    # Estimate output and cost
                    output_rows = estimate_output_rows(query, left_plan, right_plan, estimator_method)
                    
                    # Select join method
                    joining_conditions = query.find_connecting_join_conditions(left_set, right_set)
                    join_method = select_join_method(left_plan.estimated_rows, right_plan.estimated_rows, joining_conditions)
                    
                    # Calculate join cost
                    join_cost = JOIN_METHODS[join_method].estimate_cost(
                        left_plan.estimated_rows, 1,  # Simplifying width to 1
                        right_plan.estimated_rows, 1,
                        output_rows, 1
                    )
                    
                    total_cost = left_plan.cost + right_plan.cost + join_cost
                    
                    if total_cost < best_cost:
                        best_cost = total_cost
                        best_plan = JoinTreeNode(
                            (left_plan, right_plan), output_rows, total_cost, join_method)
            
            if best_plan:
                best_plans[tables_set] = best_plan
    
    # Return all plans found, sorted by cost
    result_plans = list(best_plans.values())
    result_plans.sort(key=lambda p: p.cost)
    
    # Filter to keep only full joins (joining all tables)
    return [p for p in result_plans if len(p.tables) == len(query.tables)]

# ======== Visualization Functions ========

def print_join_tree(node: JoinTreeNode, indent: int = 0):
    """
    Pretty-print a join tree
    """
    if hasattr(node, 'left_child') and hasattr(node, 'right_child'):
        join_method = f" [{node.join_method}]" if node.join_method else ""
        print(f"{' ' * indent}⋈{join_method} ({node.estimated_rows} rows, cost: {node.cost:.2f})")
        print(f"{' ' * (indent+2)}Left:")
        print_join_tree(node.left_child, indent + 4)
        print(f"{' ' * (indent+2)}Right:")
        print_join_tree(node.right_child, indent + 4)
    else:
        tables_str = ", ".join(node.tables)
        print(f"{' ' * indent}Scan: {tables_str} ({node.estimated_rows} rows, cost: {node.cost:.2f})")

def visualize_query_plan(query: JoinQuery, plan_generator: Callable, estimator_method: str):
    """
    Generate and visualize query plans
    """
    print(f"\n=== {plan_generator.__name__} with {estimator_method} estimator ===")
    plans = plan_generator(query, estimator_method)
    
    if not plans:
        print("No valid plans generated!")
        return
    
    print(f"\nBest Plan (cost: {plans[0].cost:.2f}):")
    print_join_tree(plans[0])
    
    if len(plans) > 1:
        print(f"\nSecond Best Plan (cost: {plans[1].cost:.2f}):")
        print_join_tree(plans[1])

# ======== Example Usage ========

def main():
    # Create sample tables
    tables = [
        Table("A", 1000, ["id", "a1", "a2"], 
              column_ndvs={"id": 1000, "a1": 100, "a2": 50}),
        
        Table("B", 5000, ["id", "b1", "b2", "a_id"], 
              column_ndvs={"id": 5000, "b1": 500, "b2": 200, "a_id": 800}),
        
        Table("C", 20000, ["id", "c1", "c2", "b_id"], 
              column_ndvs={"id": 20000, "c1": 2000, "c2": 500, "b_id": 4000})
    ]
    
    # Create join conditions
    join_conditions = [
        JoinCondition("A", "id", "B", "a_id"),
        JoinCondition("B", "id", "C", "b_id")
    ]
    
    # Create query
    query = JoinQuery(tables, join_conditions)
    
    # Generate and visualize different types of plans
    for estimator in ["fixed", "ndv", "mcv"]:
        visualize_query_plan(query, generate_left_deep_plans, estimator)
        visualize_query_plan(query, generate_bushy_plans, estimator)

if __name__ == "__main__":
    main()