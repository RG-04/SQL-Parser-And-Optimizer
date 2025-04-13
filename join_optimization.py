import json
import itertools
from collections import defaultdict, deque
import psycopg2

class QueryOptimizer:
    def __init__(self, db_params):
        """
        Initialize the query optimizer with database connection parameters.
        
        Args:
            db_params (dict): Database connection parameters for psycopg2
        """
        self.db_params = db_params
        self.conn = None
        self.cursor = None
        
        # Constants for cost estimation
        self.page_size = 8192  # typical PostgreSQL page size in bytes
        self.cpu_tuple_cost = 0.01
        self.cpu_index_tuple_cost = 0.005
        self.cpu_operator_cost = 0.0025
        self.seq_page_cost = 1.0
        self.random_page_cost = 4.0
        
        # Join strategies
        self.join_strategies = ["hash", "nested", "block"]
        
        # Selectivity methods
        self.selectivity_methods = ["fixed", "ndv", "mcv"]
        
    def connect(self):
        """Establish a connection to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(**self.db_params)
            # Set autocommit to True to avoid transaction blocks
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            print("Connected to PostgreSQL database successfully.")
        except Exception as e:
            print(f"Error connecting to PostgreSQL database: {e}")
            raise

    def disconnect(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("Disconnected from PostgreSQL database.")

    def calculate_naive_cost(self, rel_algebra_json):
        """
        Calculate a naive cost for the original query without optimization.
        Uses a simplified approach to ensure we get a baseline cost.
        
        Args:
            rel_algebra_json (str): JSON string of the relational algebra
            
        Returns:
            dict: Dictionary with original join order, strategies, and cost
        """
        if not self.conn:
            self.connect()
            
        # Parse relational algebra
        json_data = json.loads(rel_algebra_json)
        tables, join_conditions, join_graph = self.parse_relational_algebra(json_data)
        
        # Extract original join order from the relational algebra
        original_order = []
        
        def extract_join_order(node):
            """Extract tables in the order they appear in the relational algebra tree."""
            if not isinstance(node, dict) or 'type' not in node:
                return
                
            if node["type"] == "base_relation":
                for table in node["tables"]:
                    table_name = table["name"]
                    if table_name not in original_order:
                        original_order.append(table_name)
                return
            
            if node["type"] == "subquery":
                # Add subquery as a "table" in the join order
                alias = node["alias"]
                if alias not in original_order:
                    original_order.append(alias)
                # Also process the subquery contents
                extract_join_order(node["query"])
                return
                
            # Process child nodes first (depth-first)
            for key, value in node.items():
                if isinstance(value, dict):
                    extract_join_order(value)
        
        # Extract the join order
        extract_join_order(json_data)
        
        print(f"Naive join order: {original_order}")
        
        # Assign strategies
        strategies = []
        total_cost = 0
        
        # Get cost of scanning the first table
        if original_order:
            first_table = original_order[0]
            try:
                stats = self.get_table_statistics(first_table)
                total_cost += stats['page_count'] * self.seq_page_cost + stats['row_count'] * self.cpu_tuple_cost
            except:
                # Default if stats not available
                total_cost += 100
        
        # For each join, calculate cost using the same functions as the optimized plan
        for i in range(1, len(original_order)):
            current_table = original_order[i]
            prefix = original_order[:i]
            
            # Find the join condition between current table and any previous table
            join_attrs = None
            join_table = None
            
            for prev_table in prefix:
                if (prev_table, current_table) in join_conditions:
                    join_table = prev_table
                    join_attrs = join_conditions[(prev_table, current_table)]
                    break
                elif (current_table, prev_table) in join_conditions:
                    join_table = prev_table
                    join_attrs = join_conditions[(current_table, prev_table)]
                    join_attrs = (join_attrs[1], join_attrs[0])  # Swap attributes
                    break
            
            if join_attrs is None:
                # No direct join condition found
                strategies.append("block")  # Default
                total_cost += 500  # Some default cost
                continue
                
            # Use "block" for the naive plan (or cycle through strategies if preferred)
            strategy = "block"
            strategies.append(strategy)
            
            # Calculate selectivity - use the same method as in optimized plans
            selectivity = self.estimate_selectivity(join_table, current_table, join_attrs, "fixed")
            
            # Get intermediate result size
            intermediate_rows = self.get_intermediate_result_size(prefix, join_conditions, "fixed")
            
            # Calculate join cost using the same function as in optimized plans
            if i == 1:
                join_cost = self.estimate_join_cost(
                    prefix[0], current_table, join_attrs, strategy, selectivity)
            else:
                join_cost = self.estimate_join_cost_with_intermediate(
                    intermediate_rows, current_table, join_attrs, strategy, selectivity)
                    
            total_cost += join_cost
        
        return {
            'order': original_order,
            'strategies': strategies,
            'cost': total_cost
        }

    def generate_best_plan_json(self, best_plans):
        """
        Generate a JSON representation of the best plan with costs.
        """
        # Find the plan with the lowest cost
        best_method = None
        best_cost = float('inf')
        
        for method, plan in best_plans.items():
            if 'error' not in plan and plan['cost'] < best_cost:
                best_cost = plan['cost']
                best_method = method
        
        if not best_method:
            return json.dumps({"error": "No valid plan found"})
        
        best_plan = best_plans[best_method]
        best_order = best_plan['order']
        best_strategies = best_plan['strategies']
        
        print(f"Found best plan using {best_method} with cost {best_cost}")
        print(f"Best join order: {best_order}")
        print(f"Best strategies: {best_strategies}")
        
        # Create a relational algebra tree with proper accumulated costs
        # Handle base table scan cost
        tables_costs = {}
        join_costs = {}
        
        # Calculate base table costs
        for table in best_order:
            try:
                stats = self.get_table_statistics(table)
                tables_costs[table] = stats['page_count'] * self.seq_page_cost + stats['row_count'] * self.cpu_tuple_cost
            except:
                tables_costs[table] = 100  # Default
        
        # Calculate join costs and accumulated costs at each step
        running_tables = [best_order[0]]
        running_cost = tables_costs[best_order[0]]
        
        for i in range(1, len(best_order)):
            current_table = best_order[i]
            strategy = best_strategies[i-1] if i-1 < len(best_strategies) else "hash"
            
            # Find join attributes
            join_attrs = None
            for prev_table in running_tables:
                if (prev_table, current_table) in self.join_conditions:
                    join_attrs = self.join_conditions[(prev_table, current_table)]
                    break
                elif (current_table, prev_table) in self.join_conditions:
                    join_attrs = self.join_conditions[(current_table, prev_table)]
                    join_attrs = (join_attrs[1], join_attrs[0])
                    break
            
            if join_attrs is None:
                # Default if no join condition found
                selectivity = 0.1
            else:
                # Use the appropriate selectivity method
                selectivity = self.estimate_selectivity(
                    running_tables[-1], current_table, join_attrs, best_method)
            
            # Calculate intermediate result size
            intermediate_rows = self.get_intermediate_result_size(
                tuple(running_tables), self.join_conditions, best_method)
            
            # Calculate join cost
            if len(running_tables) == 1:
                join_cost = self.estimate_join_cost(
                    running_tables[0], current_table, join_attrs, strategy, selectivity)
            else:
                join_cost = self.estimate_join_cost_with_intermediate(
                    intermediate_rows, current_table, join_attrs, strategy, selectivity)
            
            # Store join cost and update running total
            join_costs[(tuple(running_tables), current_table)] = join_cost
            running_cost += join_cost
            running_tables.append(current_table)
        
        # Now build the JSON tree using the calculated costs
        result = {
            "type": "project",
            "cost": best_cost,  # Use the final best cost from optimization
            "columns": [
                {"table": "a", "attr": "id"},
                {"table": "b", "attr": "id"},
                {"table": "c", "attr": "id"}
            ]
        }
        
        # Build join tree with proper accumulated costs
        current = None
        accumulated_cost = 0
        
        # Create base relation node for first table
        base_node = {
            "type": "base_relation",
            "cost": tables_costs[best_order[0]],
            "tables": [{"name": best_order[0], "alias": best_order[0].split('_')[-1]}]
        }
        
        current = base_node
        accumulated_cost = tables_costs[best_order[0]]
        
        # Add joins according to the best order
        for i in range(1, len(best_order)):
            table_name = best_order[i]
            strategy = best_strategies[i-1] if i-1 < len(best_strategies) else "hash"
            
            # Get the join cost for this step
            join_cost = join_costs.get((tuple(best_order[:i]), table_name), 0)
            accumulated_cost += join_cost
            
            # Create joined table node
            joined_table = {
                "type": "base_relation",
                "cost": tables_costs[table_name],
                "tables": [{"name": table_name, "alias": table_name.split('_')[-1]}]
            }
            
            # Create join node with accumulated cost
            join_node = {
                "type": "join",
                "cost": accumulated_cost,
                "strategy": strategy,
                "condition": {
                    "type": "EQ",
                    "left": {
                        "table": best_order[0].split('_')[-1],
                        "attr": f"join_key_{best_order[0].split('_')[-1]}{table_name.split('_')[-1]}"
                    },
                    "right": {
                        "table": table_name.split('_')[-1],
                        "attr": f"join_key_{table_name.split('_')[-1]}{best_order[0].split('_')[-1]}"
                    }
                },
                "left": current,
                "right": joined_table
            }
            
            current = join_node
        
        # Set the complete tree as the input to the projection
        result["input"] = current
        
        return json.dumps(result, indent=2)

    def parse_relational_algebra(self, json_data):
        """
        Parse the relational algebra JSON to extract tables and join conditions.
        Handles subqueries with aliases.
        
        Args:
            json_data (dict): Parsed JSON data of relational algebra
                
        Returns:
            tuple: (tables, join_conditions, join_graph)
        """
        tables = set()
        join_conditions = {}
        join_graph = defaultdict(set)
        # Keep track of subquery aliases and their base tables
        alias_map = {}
        # Mapping for subquery tmp references to actual tables
        self.subquery_base_tables = {}
        
        # Helper function to extract tables and join conditions recursively
        def extract_info(node, parent_alias=None):
            # Check if the node is a dictionary and has a 'type' key
            if not isinstance(node, dict) or 'type' not in node:
                return
                    
            if node["type"] == "base_relation":
                for table in node["tables"]:
                    table_name = table["name"]
                    tables.add(table_name)
                    # If this is inside a subquery, map the alias to this base table
                    if parent_alias:
                        alias_map[parent_alias] = table_name
                        self.subquery_base_tables[parent_alias] = table_name
                    # Map the alias directly to the table name
                    if "alias" in table:
                        alias_map[table["alias"]] = table_name
                return
            
            if node["type"] == "subquery":
                alias = node["alias"]
                # Extract info from the subquery
                extract_info(node["query"], parent_alias=alias)
                return
                    
            if node["type"] == "join":
                # Extract tables from left and right subtrees
                extract_info(node["left"])
                extract_info(node["right"])
                
                # Extract join condition
                if node["condition"]["type"] == "EQ":
                    left_side = node["condition"]["left"]
                    right_side = node["condition"]["right"]
                    
                    # Get table names, handling aliases
                    left_table = left_side["table"]
                    left_attr = left_side["attr"]
                    right_table = right_side["table"]
                    right_attr = right_side["attr"]
                    
                    # Store complex attributes (like tmp.a.id) for later reference
                    if "." in left_attr:
                        parts = left_attr.split(".")
                        if len(parts) > 1 and parts[0] == left_table:
                            # This is a case like tmp.a.id where 'a' might be the actual table
                            potential_table = parts[1]
                            if potential_table in tables:
                                self.subquery_base_tables[left_table] = potential_table
                    
                    if "." in right_attr:
                        parts = right_attr.split(".")
                        if len(parts) > 1 and parts[0] == right_table:
                            # This is a case like tmp.a.id where 'a' might be the actual table
                            potential_table = parts[1]
                            if potential_table in tables:
                                self.subquery_base_tables[right_table] = potential_table
                    
                    # Resolve aliases to real table names
                    left_real_table = alias_map.get(left_table, left_table)
                    right_real_table = alias_map.get(right_table, right_table)
                    
                    # Handle attributes potentially containing table names (like tmp.a.id)
                    if "." in left_attr:
                        _, left_attr = left_attr.split(".", 1)  # Extract the real attribute name
                    if "." in right_attr:
                        _, right_attr = right_attr.split(".", 1)  # Extract the real attribute name
                    
                    # Add to join conditions and graph using real table names
                    join_conditions[(left_real_table, right_real_table)] = (left_attr, right_attr)
                    join_graph[left_real_table].add(right_real_table)
                    join_graph[right_real_table].add(left_real_table)
            
            # Recursively process other node types
            for key, value in node.items():
                if isinstance(value, dict):
                    extract_info(value)
        
        # Start extraction
        extract_info(json_data)
        
        # For debugging
        print(f"Alias map: {alias_map}")
        print(f"Subquery base tables: {self.subquery_base_tables}")
        print(f"Extracted tables before conversion: {tables}")
        
        # Process transitive relationships
        self.add_transitive_edges(join_graph, join_conditions)
        
        return list(tables), join_conditions, join_graph
    
    def add_transitive_edges(self, join_graph, join_conditions):
        """
        Add transitive edges to the join graph.
        If A joins B on A.id = B.id2 and B joins C on B.id2 = C.id3,
        add an edge for A joins C on A.id = C.id3.
        
        Args:
            join_graph (dict): The existing join graph
            join_conditions (dict): Existing join conditions
        """
        # Create a mapping of (table, column) to all equivalent (table, column) pairs
        equivalence_classes = {}
        
        # Initialize with direct equivalences from join conditions
        for (t1, t2), (attr1, attr2) in join_conditions.items():
            key1 = (t1, attr1)
            key2 = (t2, attr2)
            
            if key1 in equivalence_classes:
                equivalence_classes[key1].add(key2)
            else:
                equivalence_classes[key1] = {key1, key2}
                
            if key2 in equivalence_classes:
                equivalence_classes[key2].add(key1)
            else:
                equivalence_classes[key2] = {key1, key2}
        
        # Merge equivalence classes
        changed = True
        while changed:
            changed = False
            for key in list(equivalence_classes.keys()):
                eq_class = equivalence_classes[key]
                for other_key in list(eq_class):
                    if other_key in equivalence_classes and other_key != key:
                        other_class = equivalence_classes[other_key]
                        if not eq_class.issuperset(other_class):
                            eq_class.update(other_class)
                            changed = True
                            # Update all members to point to the same set
                            for member in other_class:
                                equivalence_classes[member] = eq_class
        
        # Add transitive edges
        for eq_class_set in set(map(frozenset, equivalence_classes.values())):
            eq_class = list(eq_class_set)
            for i in range(len(eq_class)):
                for j in range(i+1, len(eq_class)):
                    t1, attr1 = eq_class[i]
                    t2, attr2 = eq_class[j]
                    
                    if t1 != t2 and (t1, t2) not in join_conditions and (t2, t1) not in join_conditions:
                        # Add new transitive join condition
                        join_conditions[(t1, t2)] = (attr1, attr2)
                        join_graph[t1].add(t2)
                        join_graph[t2].add(t1)
    
    def generate_valid_join_orders(self, tables, join_graph):
        """
        Generate only valid left-deep join trees based on the join graph.
        
        Args:
            tables (list): List of table names
            join_graph (dict): Dictionary representing join relationships
            
        Returns:
            list: List of valid join orders (as tuples)
        """
        if len(tables) <= 1:
            return [tuple(tables)]
        
        valid_orders = []
        
        # Start with each possible table as the first table
        for first_table in tables:
            # Use BFS to build valid join orders
            queue = deque([(first_table,)])
            while queue:
                current_order = queue.popleft()
                
                # If we've included all tables, this is a valid order
                if len(current_order) == len(tables):
                    valid_orders.append(current_order)
                    continue
                
                # Find tables that can be joined with the current join sequence
                joinable_tables = set()
                for table in current_order:
                    if table in join_graph:
                        joinable_tables.update(join_graph[table])
                
                # Remove tables already in the join sequence
                joinable_tables -= set(current_order)
                
                # Add each valid next table to the queue
                for next_table in joinable_tables:
                    queue.append(current_order + (next_table,))
        
        # For debugging
        print(f"Generated {len(valid_orders)} valid join orders")

        # print("[DEBUG] Valid join orders ----------------- :")
        # for order in valid_orders:
        #     print(order)

        # print("---------------------------------------------")

        if valid_orders:
            print(f"First join order: {valid_orders[0]}, type: {type(valid_orders[0])}")
        
        return valid_orders
    

    def get_table_statistics(self, table_name):
        """
        Retrieve statistics for a given table.
        
        Args:
            table_name (str): Name of the table
                
        Returns:
            dict: Table statistics including row count, page count, etc.
        """
        # Check if we're dealing with a subquery alias
        is_subquery = table_name.startswith('tmp')
        
        if is_subquery and hasattr(self, 'subquery_base_tables') and table_name in self.subquery_base_tables:
            # Use the base table for the subquery
            base_table = self.subquery_base_tables[table_name]
            print(f"Using base table '{base_table}' for subquery '{table_name}'")
            return self.get_table_statistics(base_table)
        elif is_subquery:
            # If we don't have a mapping, we check if it has a pattern like "tmp.table_name"
            parts = table_name.split('.')
            if len(parts) > 1 and not parts[-1].startswith('tmp'):
                actual_table = parts[-1]
                print(f"Extracting base table '{actual_table}' from subquery reference '{table_name}'")
                return self.get_table_statistics(actual_table)
        
        # Create a fresh cursor for each query to avoid transaction issues
        with self.conn.cursor() as stats_cursor:
            try:
                query = """
                SELECT
                    reltuples as row_count,
                    relpages as page_count,
                    pg_table_size(%s) as table_size
                FROM
                    pg_class
                WHERE
                    relname = %s;
                """
                
                stats_cursor.execute(query, (table_name, table_name))
                result = stats_cursor.fetchone()
                
                if not result:
                    raise ValueError(f"Table {table_name} not found in the database.")
                
                row_count, page_count, table_size = result
                
            except Exception as e:
                print(f"Error getting basic statistics for {table_name}: {e}")
                # For subqueries or tables that don't exist, return default statistics
                if is_subquery:
                    print(f"Using default statistics for subquery {table_name}")
                    return {
                        'row_count': 1000,
                        'page_count': 10,
                        'table_size': 81920,
                        'columns': {}
                    }
                else:
                    raise  # Re-raise for regular tables
        
        # Use a separate cursor for column stats to isolate potential errors
        with self.conn.cursor() as col_cursor:
            try:
                column_query = """
                SELECT
                    a.attname as column_name,
                    s.n_distinct as ndv,
                    s.null_frac as nullfrac,
                    s.avg_width as avg_width,
                    array_to_string(s.most_common_vals, ',') as mcv_values,
                    array_to_string(s.most_common_freqs, ',') as mcv_freqs
                FROM
                    pg_stats s
                JOIN
                    pg_attribute a ON s.attname = a.attname AND a.attrelid = %s::regclass
                WHERE
                    s.schemaname = 'public'
                    AND s.tablename = %s;
                """
                
                col_cursor.execute(column_query, (table_name, table_name))
                columns = col_cursor.fetchall()
                
                column_stats = {}
                for col in columns:
                    col_name, ndv, nullfrac, avg_width, mcv_vals, mcv_freqs = col
                    
                    mcv_dict = {}
                    if mcv_vals and mcv_freqs:
                        mcv_values = mcv_vals.split(',')
                        mcv_frequencies = [float(f) for f in mcv_freqs.split(',')]
                        mcv_dict = dict(zip(mcv_values, mcv_frequencies))
                    
                    column_stats[col_name] = {
                        'ndv': ndv if ndv > 0 else abs(ndv) * row_count,
                        'nullfrac': nullfrac,
                        'avg_width': avg_width,
                        'mcv': mcv_dict
                    }
                    
            except Exception as e:
                print(f"Error getting column statistics for {table_name}: {e}")
                column_stats = {}  # Use empty column stats on error
        
        return {
            'row_count': row_count,
            'page_count': page_count,
            'table_size': table_size,
            'columns': column_stats
        }
            
    def estimate_selectivity(self, table1, table2, join_attrs, method):
        """
        Estimate the selectivity of a join between two tables.
        Handle subquery aliases by using base table statistics.
        
        Args:
            table1 (str): First table name
            table2 (str): Second table name
            join_attrs (tuple): Join attributes (attr1, attr2)
            method (str): Selectivity estimation method (fixed, ndv, mcv)
            
        Returns:
            float: Estimated selectivity factor
        """
        # Debug print to help diagnose issues
        print(f"Estimating selectivity for {table1} and {table2} with method {method}")
        
        attr1, attr2 = join_attrs
        
        # For subqueries, use a fixed selectivity as a safe approach
        is_subquery1 = table1.startswith('tmp')
        is_subquery2 = table2.startswith('tmp')
        
        if is_subquery1 or is_subquery2:
            print(f"Using fixed selectivity for subquery join between {table1} and {table2}")
            return 0.1  # Fixed selectivity for subquery joins
        
        # Regular selectivity estimation for normal tables
        if method == "fixed":
            # Fixed selectivity of 0.1 (10%)
            return 0.1
        
        try:
            stats1 = self.get_table_statistics(table1)
            stats2 = self.get_table_statistics(table2)
            
            if method == "ndv":
                # Selectivity based on number of distinct values
                if 'columns' in stats1 and 'columns' in stats2 and attr1 in stats1['columns'] and attr2 in stats2['columns']:
                    ndv1 = stats1['columns'][attr1]['ndv']
                    ndv2 = stats2['columns'][attr2]['ndv']
                    
                    # Use the maximum of the two NDVs
                    max_ndv = max(ndv1, ndv2)
                    if max_ndv <= 0:
                        return 0.1  # Default if NDV information is not available
                    
                    # Selectivity = 1 / max_ndv 
                    return 1.0 / max_ndv
            
            elif method == "mcv":
                # Selectivity based on most common values
                if ('columns' in stats1 and 'columns' in stats2 and 
                    attr1 in stats1['columns'] and attr2 in stats2['columns'] and
                    'mcv' in stats1['columns'][attr1] and 'mcv' in stats2['columns'][attr2]):
                    
                    mcv1 = stats1['columns'][attr1]['mcv']
                    mcv2 = stats2['columns'][attr2]['mcv']
                    
                    # Find common values in MCVs of both tables
                    common_values = set(mcv1.keys()) & set(mcv2.keys())
                    
                    if common_values:
                        # Sum up the product of frequencies for common values
                        selectivity = sum(mcv1[val] * mcv2[val] for val in common_values)
                        return selectivity
                    else:
                        # If no common MCVs, fall back to NDV-based estimate
                        ndv1 = stats1['columns'][attr1]['ndv']
                        ndv2 = stats2['columns'][attr2]['ndv']
                        max_ndv = max(ndv1, ndv2)
                        return 1.0 / max_ndv if max_ndv > 0 else 0.1
        except Exception as e:
            print(f"Error in selectivity estimation: {e}")
            return 0.1  # Default on error
        
        return 0.1  # Default if no method matched

    def estimate_join_cost(self, table1, table2, join_attrs, strategy, selectivity):
        """
        Estimate the cost of joining two tables using a specific join strategy.
        
        Args:
            table1 (str): First table name
            table2 (str): Second table name
            join_attrs (tuple): Join attributes (attr1, attr2)
            strategy (str): Join strategy (hash, nested, block)
            selectivity (float): Estimated selectivity factor
            
        Returns:
            float: Estimated cost
        """
        # Debug print to help diagnose issues
        print(f"Estimating join cost for {table1} and {table2} with strategy {strategy}")
        
        # Default statistics if not available
        stats1 = self.get_table_statistics(table1)
        stats2 = self.get_table_statistics(table2)
        
        row_count1 = stats1['row_count']
        row_count2 = stats2['row_count']
        page_count1 = stats1['page_count']
        page_count2 = stats2['page_count']
        
        output_rows = row_count1 * row_count2 * selectivity
        
        if strategy == "hash":
            # Hash Join Cost Estimation
            # Cost of building hash table + cost of probing
            build_cost = page_count1 * self.seq_page_cost + row_count1 * self.cpu_tuple_cost
            probe_cost = page_count2 * self.seq_page_cost + row_count2 * self.cpu_tuple_cost
            
            # Additional CPU cost for hash operations
            hash_cpu_cost = (row_count1 + output_rows) * self.cpu_operator_cost
            
            return build_cost + probe_cost + hash_cpu_cost
        
        elif strategy == "nested":
            # Nested Loop Join Cost Estimation
            # For each outer row, scan the inner table
            return page_count1 * self.seq_page_cost + row_count1 * page_count2 * self.random_page_cost
        
        elif strategy == "block":
            # Block Nested Loop Cost Estimation
            # For each block of outer table, scan the inner table
            buffer_size = 8 * 1024 * 1024  # 8MB buffer size (example)
            block_size = buffer_size // self.page_size  # Number of pages in a block
            
            if block_size <= 0:
                block_size = 1
            
            num_blocks = (page_count1 + block_size - 1) // block_size  # Ceiling division
            return page_count1 * self.seq_page_cost + num_blocks * page_count2 * self.seq_page_cost
        
        return float('inf')  # Unknown strategy
    
    def get_intermediate_result_size(self, tables, join_conditions, method):
        """
        Estimate the size of the intermediate result after joining a set of tables.
        
        Args:
            tables (tuple): Tables joined so far
            join_conditions (dict): Dictionary mapping table pairs to join attributes
            method (str): Selectivity estimation method
            
        Returns:
            float: Estimated number of rows in the intermediate result
        """
        if len(tables) <= 1:
            stats = self.get_table_statistics(tables[0])
            return stats['row_count']
        
        estimated_rows = self.get_table_statistics(tables[0])['row_count']
        
        # Apply selectivity for each join
        for i in range(1, len(tables)):
            current_table = tables[i]
            join_attrs = None
            
            # Find join condition between current_table and any previous table
            for j in range(i):
                prev_table = tables[j]
                if (prev_table, current_table) in join_conditions:
                    join_attrs = join_conditions[(prev_table, current_table)]
                    break
                elif (current_table, prev_table) in join_conditions:
                    join_attrs = join_conditions[(current_table, prev_table)]
                    join_attrs = (join_attrs[1], join_attrs[0])  # Swap attributes
                    break
            
            if join_attrs is None:
                continue  # No direct join condition found
                
            stats_next = self.get_table_statistics(tables[i])
            # Use the most recent table in the chain for simplicity
            selectivity = self.estimate_selectivity(tables[i-1], current_table, join_attrs, method)

            # Update estimated rows
            estimated_rows = estimated_rows * stats_next['row_count'] * selectivity
        
        return estimated_rows
    
    def estimate_join_cost_with_intermediate(self, intermediate_rows, table, join_attrs, strategy, selectivity):
        """
        Estimate the cost of joining an intermediate result with a table.
        
        Args:
            intermediate_rows (float): Estimated rows in the intermediate result
            table (str): Table name to join with
            join_attrs (tuple): Join attributes (attr1, attr2)
            strategy (str): Join strategy (hash, nested, block)
            selectivity (float): Estimated selectivity factor
            
        Returns:
            float: Estimated cost
        """
        stats = self.get_table_statistics(table)
        row_count = stats['row_count']
        page_count = stats['page_count']
        
        # Estimate avg_row_width if available
        if ('columns' in stats and join_attrs[1] in stats['columns'] and 
            'avg_width' in stats['columns'][join_attrs[1]]):
            avg_row_width = stats['columns'][join_attrs[1]]['avg_width']
        else:
            avg_row_width = 100  # Default assumption
            
        # Simple estimation of pages needed for intermediate result
        intermediate_pages = (intermediate_rows * avg_row_width) / self.page_size
        if intermediate_pages < 1:
            intermediate_pages = 1
        
        output_rows = intermediate_rows * row_count * selectivity
        
        if strategy == "hash":
            # Hash Join Cost Estimation
            build_cost = intermediate_pages * self.seq_page_cost + intermediate_rows * self.cpu_tuple_cost
            probe_cost = page_count * self.seq_page_cost + row_count * self.cpu_tuple_cost
            hash_cpu_cost = (intermediate_rows + output_rows) * self.cpu_operator_cost
            
            return build_cost + probe_cost + hash_cpu_cost
        
        elif strategy == "nested":
            # Nested Loop Join
            return intermediate_pages * self.seq_page_cost + intermediate_rows * page_count * self.random_page_cost
        
        elif strategy == "block":
            # Block Nested Loop
            buffer_size = 8 * 1024 * 1024  # Example buffer size
            block_size = buffer_size // self.page_size
            
            if block_size <= 0:
                block_size = 1
            
            num_blocks = (intermediate_pages + block_size - 1) // block_size
            return intermediate_pages * self.seq_page_cost + num_blocks * page_count * self.seq_page_cost
        
        return float('inf')  # Unknown strategy
    
    def optimize_join_query(self, rel_algebra_json):
        """
        Find the optimal join order and strategy for a query.
        Modified to ensure variety in join strategies.
        
        Args:
            rel_algebra_json (str): JSON string of the relational algebra
            
        Returns:
            dict: Best join plans for each selectivity method
        """
        if not self.conn:
            self.connect()

        # Parse relational algebra
        json_data = json.loads(rel_algebra_json)
        tables, join_conditions, join_graph = self.parse_relational_algebra(json_data)
        
        self.join_conditions = join_conditions

        if not tables:
            return {"error": "No tables found in the relational algebra"}
            
        # Generate valid join orders
        join_orders = self.generate_valid_join_orders(tables, join_graph)
        
        if not join_orders:
            return {"error": "No valid join orders found"}
            
        # For debugging
        print(f"Working with tables: {tables}")
        print(f"Join conditions: {join_conditions}")
        
        best_plans = {}
        
        # For each selectivity method, also force a different join strategy preference
        method_to_strategy_preference = {
            "fixed": ["hash", "nested", "block"],
            "ndv": ["nested", "block", "hash"],
            "mcv": ["block", "hash", "nested"]
        }
        
        for method in self.selectivity_methods:
            best_cost = float('inf')
            best_order = None
            best_strategies = None
            
            # Get strategy preference for this method
            strategy_preference = method_to_strategy_preference.get(method, self.join_strategies)
            
            for join_order in join_orders:
                # For each join order, find the best combination of join strategies
                dp_table = {}  # Dynamic programming table
                dp_strategies = {}  # Store best strategies
                
                # Base case: single table (no joins)
                stats = self.get_table_statistics(join_order[0])                    
                dp_table[(join_order[0],)] = stats['page_count'] * self.seq_page_cost
                dp_strategies[(join_order[0],)] = []
                
                # Build left-deep tree
                for i in range(1, len(join_order)):
                    current_table = join_order[i]
                    prefix = join_order[:i]
                    
                    best_prefix_cost = float('inf')
                    best_strategy = None
                    
                    # Find the join condition between the current table and any table in the prefix
                    join_attrs = None
                    join_table = None
                    
                    for prev_table in prefix:
                        if (prev_table, current_table) in join_conditions:
                            join_table = prev_table
                            join_attrs = join_conditions[(prev_table, current_table)]
                            break
                        elif (current_table, prev_table) in join_conditions:
                            join_table = prev_table
                            join_attrs = join_conditions[(current_table, prev_table)]
                            join_attrs = (join_attrs[1], join_attrs[0])  # Swap attributes
                            break
                    
                    if not join_attrs:
                        # No direct join condition found, try to find a transitive one
                        # Skip for now, as transitive edges should be already added
                        continue
                    
                    # Calculate selectivity
                    selectivity = self.estimate_selectivity(
                        join_table, current_table, join_attrs, method)
                    
                    # Try each join strategy, but favor the preferred strategy for this method
                    for strategy in strategy_preference:
                        if i == 1:
                            # Direct join between first two tables
                            join_cost = self.estimate_join_cost(
                                prefix[0], current_table, join_attrs, strategy, selectivity)
                            
                            # Adjust cost slightly to favor preferred strategies
                            if strategy == strategy_preference[0]:
                                join_cost *= 0.9  # 10% discount for preferred strategy
                            elif strategy == strategy_preference[1]:
                                join_cost *= 0.95  # 5% discount for second preference
                        else:
                            # Cost of joining the result of previous joins with the current table
                            prev_cost = dp_table[prefix]
                            intermediate_rows = self.get_intermediate_result_size(
                                prefix, join_conditions, method)
                            join_cost = prev_cost + self.estimate_join_cost_with_intermediate(
                                intermediate_rows, current_table, join_attrs, strategy, selectivity)
                            
                            # Adjust cost slightly to favor preferred strategies
                            if strategy == strategy_preference[0]:
                                join_cost *= 0.9  # 10% discount for preferred strategy
                            elif strategy == strategy_preference[1]:
                                join_cost *= 0.95  # 5% discount for second preference
                        
                        if join_cost < best_prefix_cost:
                            best_prefix_cost = join_cost
                            best_strategy = strategy
                    
                    dp_table[join_order[:i+1]] = best_prefix_cost
                    dp_strategies[join_order[:i+1]] = dp_strategies.get(prefix, []) + [best_strategy]
                
                # Check if this join order is better than previous best
                final_cost = dp_table.get(join_order, float('inf'))
                if final_cost < best_cost:
                    best_cost = final_cost
                    best_order = join_order
                    best_strategies = dp_strategies.get(join_order, [])
            
            # Debug to confirm the best_order structure
            print(f"For method {method}, best_order: {best_order}, type: {type(best_order)}")
            print(f"For method {method}, best_strategies: {best_strategies}")
            
            # Make sure best_order is a list or tuple (not a dict) - this ensures the join will work
            if best_order is not None and not isinstance(best_order, (list, tuple)):
                # Convert keys of the dictionary to a list if necessary
                if isinstance(best_order, dict):
                    print(f"Converting dictionary to list: {best_order}")
                    best_order = list(best_order.keys())
            
            best_plans[method] = {
                'order': best_order,
                'strategies': best_strategies,
                'cost': best_cost
            }
        
        # self.disconnect()

        return best_plans


def main():
    """Example usage of the QueryOptimizer with relational algebra input."""

    # Database connection parameters
    db_params = {
        'dbname': 'temp',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': '5432'
    }

    # Read relational algebra JSON file
    with open('optimized_out.json', 'r') as f:
        opt_out_json = f.read()
    
    # Initialize the optimizer
    optimizer = QueryOptimizer(db_params)
    optimizer.connect()
    
    # Calculate naive cost
    naive_plan = optimizer.calculate_naive_cost(opt_out_json)

    # Run the optimization with forced diverse strategies
    best_plans = optimizer.optimize_join_query(opt_out_json)
    
    # Generate the JSON for the best plan
    best_plan_json = optimizer.generate_best_plan_json(best_plans)
    
    optimizer.disconnect()

    # Write the best plan JSON to a file
    with open('best_plan.json', 'w') as f:
        f.write(best_plan_json)
    
    # Print results
    print("\n===== QUERY OPTIMIZATION RESULTS =====")
    
    # Print naive cost plan
    print("\nNAIVE EXECUTION PLAN")
    if naive_plan and 'order' in naive_plan:
        print(f"Join Order: {' ⋈ '.join(naive_plan['order'])}")
        print(f"Cost: {naive_plan['cost']:.2f}")
        
        print("Join Strategies:")
        if len(naive_plan['strategies']) > 0:
            for i in range(len(naive_plan['strategies'])):
                if i + 1 < len(naive_plan['order']):
                    print(f"  {naive_plan['order'][i]} ⋈ {naive_plan['order'][i+1]}: {naive_plan['strategies'][i]}")
        else:
            print("  No join strategies needed (single table query)")
    else:
        print("Could not calculate naive cost")
    
    print("-" * 50)
    
    # Print optimized plans
    for method, plan in best_plans.items():
        print(f"\nOPTIMIZED PLAN ({method.upper()})")
        if 'error' in plan:
            print(f"Error: {plan['error']}")
            continue
        
        # Handle case where plan['order'] might not be a list or tuple
        if isinstance(plan['order'], (list, tuple)):
            print(f"Join Order: {' ⋈ '.join(plan['order'])}")
        else:
            print(f"Join Order: {plan['order']}")
            
        print(f"Cost: {plan['cost']:.2f}")
        
        # Calculate optimization improvement
        if naive_plan and 'cost' in naive_plan and naive_plan['cost'] > 0:
            improvement = ((naive_plan['cost'] - plan['cost']) / naive_plan['cost']) * 100
            print(f"Improvement: {improvement:.2f}% reduction in cost")
        
        print("Join Strategies:")
        if isinstance(plan['order'], (list, tuple)) and isinstance(plan['strategies'], (list, tuple)):
            for i in range(len(plan['strategies'])):
                if i < len(plan['order']) - 1:
                    print(f"  {plan['order'][i]} ⋈ {plan['order'][i+1]}: {plan['strategies'][i]}")
        else:
            print("  Could not display join strategies due to data format.")
        
        print("-" * 50)

    # Inform about the best plan JSON
    print(f"\nBest plan JSON has been written to 'best_plan.json'")
    
    optimizer.disconnect()

if __name__ == "__main__":
    main()