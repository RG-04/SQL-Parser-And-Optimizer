import psycopg2
import itertools
from collections import defaultdict

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
    
    def get_table_statistics(self, table_name):
        """
        Retrieve statistics for a given table.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            dict: Table statistics including row count, page count, etc.
        """
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
        
        self.cursor.execute(query, (table_name, table_name))
        result = self.cursor.fetchone()
        
        if not result:
            raise ValueError(f"Table {table_name} not found in the database.")
        
        row_count, page_count, table_size = result
        
        # Get column statistics
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
        
        self.cursor.execute(column_query, (table_name, table_name))
        columns = self.cursor.fetchall()
        
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
        
        return {
            'row_count': row_count,
            'page_count': page_count,
            'table_size': table_size,
            'columns': column_stats
        }
    
    def estimate_selectivity(self, table1, table2, join_col, method):
        """
        Estimate the selectivity of a join between two tables.
        
        Args:
            table1 (str): First table name
            table2 (str): Second table name
            join_col (str): Join column name
            method (str): Selectivity estimation method (fixed, ndv, mcv)
            
        Returns:
            float: Estimated selectivity factor
        """
        if method == "fixed":
            # Fixed selectivity of 0.1 (10%)
            return 0.1
        
        stats1 = self.get_table_statistics(table1)
        stats2 = self.get_table_statistics(table2)
        
        if method == "ndv":
            # Selectivity based on number of distinct values
            if join_col in stats1['columns'] and join_col in stats2['columns']:
                ndv1 = stats1['columns'][join_col]['ndv']
                ndv2 = stats2['columns'][join_col]['ndv']
                
                # Use the maximum of the two NDVs
                max_ndv = max(ndv1, ndv2)
                if max_ndv <= 0:
                    return 0.1  # Default if NDV information is not available
                
                # Selectivity = 1 / max_ndv 
                return 1.0 / max_ndv
            else:
                return 0.1  # Default if column stats not available
        
        elif method == "mcv":
            # Selectivity based on most common values
            if (join_col in stats1['columns'] and join_col in stats2['columns'] and 
                stats1['columns'][join_col]['mcv'] and stats2['columns'][join_col]['mcv']):
                
                mcv1 = stats1['columns'][join_col]['mcv']
                mcv2 = stats2['columns'][join_col]['mcv']
                
                # Find common values in MCVs of both tables
                common_values = set(mcv1.keys()) & set(mcv2.keys())
                
                if common_values:
                    # Sum up the product of frequencies for common values
                    selectivity = sum(mcv1[val] * mcv2[val] for val in common_values)
                    return selectivity
                else:
                    # If no common MCVs, fall back to NDV-based estimate
                    ndv1 = stats1['columns'][join_col]['ndv']
                    ndv2 = stats2['columns'][join_col]['ndv']
                    max_ndv = max(ndv1, ndv2)
                    return 1.0 / max_ndv if max_ndv > 0 else 0.1
            else:
                # Fall back to fixed selectivity
                return 0.1
        
        return 0.1  # Default
    
    def estimate_join_cost(self, table1, table2, join_col, strategy, selectivity):
        """
        Estimate the cost of joining two tables using a specific join strategy.
        
        Args:
            table1 (str): First table name
            table2 (str): Second table name
            join_col (str): Join column name
            strategy (str): Join strategy (hash, nested, block)
            selectivity (float): Estimated selectivity factor
            
        Returns:
            float: Estimated cost
        """
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
    
    def generate_join_orders(self, tables):
        """
        Generate all possible left-deep join trees.
        
        Args:
            tables (list): List of table names
            
        Returns:
            list: List of possible join orders (as tuples)
        """
        if len(tables) <= 1:
            return [tables]
        
        join_orders = []
        for i in range(len(tables)):
            # Fix the first table
            first_table = tables[i]
            remaining_tables = tables[:i] + tables[i+1:]
            
            # Generate all permutations of the remaining tables
            for perm in itertools.permutations(remaining_tables):
                # Create a left-deep tree
                join_orders.append((first_table,) + perm)
        
        return join_orders
    
    def optimize_join_query(self, tables, join_conditions):
        """
        Find the optimal join order and strategy for a set of tables.
        
        Args:
            tables (list): List of table names
            join_conditions (dict): Dictionary mapping table pairs to join columns
            
        Returns:
            dict: Best join plans for each selectivity method
        """
        if not self.conn:
            self.connect()
        
        join_orders = self.generate_join_orders(tables)
        best_plans = {}
        
        for method in self.selectivity_methods:
            best_cost = float('inf')
            best_order = None
            best_strategies = None
            
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
                    
                    # Find the join condition
                    join_col = None
                    for (t1, t2), col in join_conditions.items():
                        if (t1 in prefix and t2 == current_table) or (t2 in prefix and t1 == current_table):
                            join_col = col
                            break
                    
                    if not join_col:
                        continue  # Skip if no join condition found
                    
                    # Calculate selectivity
                    if i == 1:
                        # Direct join between first two tables
                        selectivity = self.estimate_selectivity(prefix[0], current_table, join_col, method)
                    else:
                        # Join between prefix and current table
                        # Simplified: use the selectivity of the last table in prefix with current table
                        selectivity = self.estimate_selectivity(prefix[-1], current_table, join_col, method)
                    
                    # Try each join strategy
                    for strategy in self.join_strategies:
                        if i == 1:
                            # Direct join between first two tables
                            join_cost = self.estimate_join_cost(prefix[0], current_table, join_col, strategy, selectivity)
                        else:
                            # Cost of joining the result of previous joins with the current table
                            prev_cost = dp_table[prefix]
                            intermediate_rows = self.get_intermediate_result_size(prefix, join_conditions, method)
                            join_cost = prev_cost + self.estimate_join_cost_with_intermediate(
                                intermediate_rows, current_table, join_col, strategy, selectivity)
                        
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
            
            best_plans[method] = {
                'order': best_order,
                'strategies': best_strategies,
                'cost': best_cost
            }
        
        return best_plans
    
    def get_intermediate_result_size(self, tables, join_conditions, method):
        """
        Estimate the size of the intermediate result after joining a set of tables.
        
        Args:
            tables (tuple): Tables joined so far
            join_conditions (dict): Dictionary mapping table pairs to join columns
            method (str): Selectivity estimation method
            
        Returns:
            float: Estimated number of rows in the intermediate result
        """
        if len(tables) <= 1:
            stats = self.get_table_statistics(tables[0])
            return stats['row_count']
        
        # Start with the row count of the first table
        stats = self.get_table_statistics(tables[0])
        estimated_rows = stats['row_count']
        
        # Apply selectivity for each join
        for i in range(1, len(tables)):
            join_col = None
            
            # Find join condition
            for (t1, t2), col in join_conditions.items():
                if (t1 == tables[i-1] and t2 == tables[i]) or (t2 == tables[i-1] and t1 == tables[i]):
                    join_col = col
                    break
            
            if not join_col:
                continue
            
            stats_next = self.get_table_statistics(tables[i])
            selectivity = self.estimate_selectivity(tables[i-1], tables[i], join_col, method)
            
            # Update estimated rows
            estimated_rows = estimated_rows * stats_next['row_count'] * selectivity
        
        return estimated_rows
    
    def estimate_join_cost_with_intermediate(self, intermediate_rows, table, join_col, strategy, selectivity):
        """
        Estimate the cost of joining an intermediate result with a table.
        
        Args:
            intermediate_rows (float): Estimated rows in the intermediate result
            table (str): Table name to join with
            join_col (str): Join column name
            strategy (str): Join strategy (hash, nested, block)
            selectivity (float): Estimated selectivity factor
            
        Returns:
            float: Estimated cost
        """
        stats = self.get_table_statistics(table)
        row_count = stats['row_count']
        page_count = stats['page_count']
        
        # Estimate pages for intermediate result
        if join_col in stats['columns']:
            avg_row_width = stats['columns'][join_col]['avg_width']
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


def main():
    """Example usage of the QueryOptimizer."""
    # Database connection parameters
    db_params = {
        'dbname': 'temp',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': '5432'
    }
    
    # Initialize the optimizer
    optimizer = QueryOptimizer(db_params)
    
    try:
        # Connect to the database
        optimizer.connect()
        
        # Define tables and join conditions
        tables = ["table_a", "table_b", "table_c"]
        join_conditions = {
            ('table_a', 'table_b'): 'join_key_ab',
            ('table_a', 'table_c'): 'join_key_ac',
            ('table_b', 'table_c'): 'join_key_bc'
        }
        
        # Run the optimization
        best_plans = optimizer.optimize_join_query(tables, join_conditions)
        
        # Print results
        print("\n===== QUERY OPTIMIZATION RESULTS =====")
        for method, plan in best_plans.items():
            print(f"\nSelectivity Method: {method.upper()}")
            print(f"Best Join Order: {' ⋈ '.join(plan['order'])}")
            print(f"Estimated Cost: {plan['cost']:.2f}")
            
            print("Join Strategies:")
            for i in range(len(plan['strategies'])):
                if i < len(plan['order']) - 1:
                    print(f"  {plan['order'][i]} ⋈ {plan['order'][i+1]}: {plan['strategies'][i]}")
            
            print("-" * 50)
    
    finally:
        # Disconnect from the database
        optimizer.disconnect()


if __name__ == "__main__":
    main()