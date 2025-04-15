import json
import psycopg2

predicate_selectivity = {
    'GT': 0.5,  # e.g., id > 1
    'LT': 0.5,
    'EQ': 0.1
}

tuple_io_cost = 1



class CostCalculator:
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

    def get_table_statistics(self, table_name: str):
        """
        Retrieve statistics for a given table.
        
        Args:
            table_name (str): Name of the table
                
        Returns:
            dict: Table statistics including row count, page count, etc.
        """
        # Check if we're dealing with a subquery alias
        table_name = table_name.lower()
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
    
    def calculate_cost(self, node):

        node_type = node["type"]

        if node_type == "base_relation":
            table = node["tables"][0]
            table_name = table["name"]
            stats = self.get_table_statistics(table_name)
            row_size = stats["row_count"]
            page_size = stats["page_count"]
            cost = row_size * self.cpu_tuple_cost + page_size * self.seq_page_cost

            node["cost"] = cost
            node["cardinality"] = row_size

            return cost, row_size

        elif node_type == "select":
            input_cost, input_size = self.calculate_cost(node["input"])
            pred_type = node["condition"]["type"]
            selectivity = predicate_selectivity.get(pred_type, 0.5)
            output_size = input_size * selectivity  

            node["cost"] = input_cost + (input_size * self.cpu_operator_cost)
            node["cardinality"] = output_size

            return input_cost + input_size, output_size

        elif node_type == "project":
            input_cost, input_size = self.calculate_cost(node["input"])

            node["cost"] = input_cost
            node["cardinality"] = input_size

            return input_cost, input_size

        elif node_type == "join":
            left_cost, left_size = self.calculate_cost(node["left"])
            right_cost, right_size = self.calculate_cost(node["right"])
            join_cost = left_size * right_size / max(left_size, right_size)
            output_size = join_cost / max(left_size, right_size)

            node["cost"] = left_cost + right_cost + join_cost
            node["cardinality"] = output_size

            return left_cost + right_cost + join_cost, output_size

        elif node_type == "subquery":
            # Subqueries can have an alias, which we map to the result of the subquery
            sub_cost, sub_size = self.calculate_cost(node["query"])

            node["cost"] = sub_cost        

            return sub_cost, sub_size
        
        elif node_type == "expr_ref":
            # For expr_ref, we assume the cost is already calculated
            expr_id = node["id"]
            if expr_id in self.exprs:
                expr = self.exprs[expr_id]
                self.expr_occ[expr_id] = self.expr_occ.get(expr_id, 0) + 1
                try:
                    node["cost"] = expr["cost"]
                except:
                    print(f"Error: Expression {expr_id} not found in common expressions.")
                node["cardinality"] = expr["cardinality"]
                return expr["cost"], expr["cardinality"]
            else:
                raise ValueError(f"Expression ID {expr_id} not found in common expressions.")

        else:
            raise ValueError(f"Unsupported node type: {node_type}")
        

    def calc_subseq_cost(self, subseq_json: dict):
        query = subseq_json["query"]
        common_expressions = subseq_json["common_expressions"]
        for expr in common_expressions:
            # calc the cost of this expression
            node = common_expressions[expr]
            types = ["select", "project", "join", "base_relation", "subquery"]
            if "type" not in node or node["type"] not in types:
                continue
            print("Costing")
            cost, cardinality = self.calculate_cost(node)
            # add the cost to the expression in the common_expressions dict
            common_expressions[expr]["cost"] = cost
            common_expressions[expr]["cardinality"] = cardinality

        self.exprs = subseq_json["common_expressions"]
        print(f"Expressions: {self.exprs}")
        self.expr_occ = {}

        # Calculate the cost of the main query
        cost, cardinality = self.calculate_cost(query)

        total_cost = cost
        for expr_id, expr in self.expr_occ.items():
            total_cost -= common_expressions[expr_id]["cost"] * (self.expr_occ[expr_id] - 1)

        print("Net benefit: ", cost - total_cost, cost, total_cost)
        return total_cost, cardinality
    
    def scale_costs(self, node, factor = 0.8):
        """
        Scale the costs of the nodes in the query plan by a given factor.
        
        Args:
            node (dict): The node to scale.
            factor (float): The scaling factor.
        """
        if "cost" in node:
            node["cost"] *= factor
        if "cardinality" in node:
            node["cardinality"] *= factor
        for child in ["left", "right", "input", "query"]:
            if child in node:
                self.scale_costs(node[child], factor)

if __name__ == "__main__":
    # Database connection parameters
    db_params = {
        'dbname': 'temp',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': '5432'
    }

    with open('optimized_out.json', 'r') as f:
        opt_out_json = f.read()

    opt_out_json = json.loads(opt_out_json) 
    
    # Initialize the optimizer
    optimizer = CostCalculator(db_params)
    optimizer.connect()

    # calculate cost of input plan:
    cost, cardinality = optimizer.calculate_cost(opt_out_json)

    with open('optimized_out_with_cost.json', 'w') as f:
        json.dump(opt_out_json, f, indent=4)
    print(f"Cost: {cost}, Cardinality: {cardinality}")
    print("JSON cost: ", opt_out_json["cost"])

    SUBSEQ_FILE = "subseq_plan.json"
    with open(SUBSEQ_FILE, 'r') as f:
        subseq_json = f.read()

    subseq_json = json.loads(subseq_json)

    # calculate cost of subsequence plan:
    subseq_cost, subseq_cardinality = optimizer.calc_subseq_cost(subseq_json)
    print(f"Subsequence Cost: {subseq_cost}, Cardinality: {subseq_cardinality}")


