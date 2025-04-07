import json
import sys

# ------------------ Logical Plan Nodes ------------------ #
class LogicalPlanNode:
    def __init__(self, node_type, children=None, predicate=None, table=None, alias=None, columns=None):
        self.node_type = node_type
        self.children = children or []
        self.predicate = predicate
        self.table = table
        self.alias = alias
        self.columns = columns

    def __str__(self, level=0):
        indent = "  " * level
        s = f"{indent}{self.node_type}"
        if self.table:
            s += f"({self.table}" + (f" AS {self.alias}" if self.alias and self.alias != self.table else "") + ")"
        if self.predicate:
            s += f" [{self.predicate}]"
        if self.columns:
            s += f" ⮕ {self.columns}"
        s += "\n"
        for child in self.children:
            s += child.__str__(level + 1)
        return s

# ------------------ Logical Plan Builder ------------------ #
def build_logical_plan_from_json(json_obj):
    """
    Build a logical plan from the provided JSON structure
    """
    if json_obj['type'] == 'select':
        # Handle SELECT with WHERE clause (filter)
        condition = format_condition_from_json(json_obj['condition'])
        child_plan = build_logical_plan_from_json(json_obj['input'])
        return LogicalPlanNode('FILTER', children=[child_plan], predicate=condition)
    
    elif json_obj['type'] == 'project':
        # Handle PROJECT (SELECT columns)
        columns = format_columns_from_json(json_obj['columns'])
        child_plan = build_logical_plan_from_json(json_obj['input'])
        return LogicalPlanNode('PROJECT', children=[child_plan], columns=columns)
    
    elif json_obj['type'] == 'join':
        # Handle JOIN
        left_plan = build_logical_plan_from_json(json_obj['left'])
        right_plan = build_logical_plan_from_json(json_obj['right'])
        condition = format_condition_from_json(json_obj['condition'])
        return LogicalPlanNode('JOIN', children=[left_plan, right_plan], predicate=condition)
    
    elif json_obj['type'] == 'base_relation':
        # Handle base table scan
        tables = json_obj['tables']
        if len(tables) == 1:
            table_info = tables[0]
            table_name = table_info['name']
            alias = table_info.get('alias', table_name)
            return LogicalPlanNode('SCAN', table=table_name, alias=alias)
        else:
            # Handle multiple tables if needed
            raise ValueError("Multiple tables in base_relation not supported yet")
    
    elif json_obj['type'] == 'rename':
        # Handle rename operation
        input_plan = build_logical_plan_from_json(json_obj['input'])
        # In our simplified model, we can treat it as adding an alias to a scan or join
        input_plan.alias = json_obj['new_name']
        return input_plan
    
    else:
        raise ValueError(f"Unknown node type: {json_obj['type']}")

def format_columns_from_json(columns):
    """
    Format columns from JSON representation to string representation
    """
    formatted = []
    for col in columns:
        table = col.get('table', '')
        attr = col.get('attr', '')
        
        if table and attr:
            column_str = f"{table}.{attr}"
        else:
            column_str = attr
            
        formatted.append(column_str)
    
    return formatted

def format_condition_from_json(condition):
    """
    Format condition from JSON representation to string representation
    """
    if not condition or 'type' not in condition:
        return str(condition)
    
    condition_type = condition['type']
    
    # Map common condition types to operators
    op_map = {
        'EQ': '=',
        'LT': '<',
        'GT': '>',
        'LE': '<=',
        'GE': '>=',
        'NE': '<>',
        'AND': 'AND',
        'OR': 'OR',
        'NOT': 'NOT'
    }
    
    if condition_type in ('AND', 'OR'):
        left = format_condition_from_json(condition['left'])
        right = format_condition_from_json(condition['right'])
        operator = op_map.get(condition_type, condition_type)
        return f"({left}) {operator} ({right})"
    
    elif condition_type == 'NOT':
        operand = format_condition_from_json(condition.get('cond', {}))
        return f"NOT ({operand})"
    
    elif condition_type in op_map:
        left = format_operand(condition['left'])
        right = format_operand(condition['right'])
        operator = op_map.get(condition_type, condition_type)
        return f"{left} {operator} {right}"
    
    else:
        # If it's a custom or unknown condition type
        return str(condition)

def format_operand(operand):
    """
    Format an operand which could be a column reference or a literal
    """
    if isinstance(operand, dict):
        if operand.get('type') == 'column':
            # It's a column reference with explicit type
            table = operand.get('table', '')
            attr = operand.get('attr', '')
            if table:
                return f"{table}.{attr}"
            return attr
        elif operand.get('type') == 'int':
            # It's an integer literal
            return str(operand.get('value', 0))
        elif operand.get('type') == 'float':
            # It's a float literal
            return str(operand.get('value', 0.0))
        elif operand.get('type') == 'string':
            # It's a string literal with explicit type
            return f"'{operand.get('value', '')}'"
        elif 'table' in operand and 'attr' in operand:
            # It's a column reference without explicit type
            return f"{operand['table']}.{operand['attr']}"
        else:
            # Some other structure
            return str(operand)
    elif isinstance(operand, str):
        # It's a string literal
        return f"'{operand}'"
    elif isinstance(operand, (int, float)):
        # It's a numeric literal
        return str(operand)
    else:
        # It's some other literal
        return str(operand)

# ------------------ Predicate Pushdown ------------------ #
def predicate_pushdown(plan):
    """
    Optimize a logical plan by pushing predicates down closer to the tables they apply to.
    This makes joins more efficient by filtering data earlier.
    """
    if not plan:
        return plan
        
    # Base case: no children to optimize
    if not hasattr(plan, 'children') or not plan.children:
        return plan

    # Recursively optimize all children first
    plan.children = [predicate_pushdown(child) for child in plan.children]

    # If this is a FILTER node, try to push it down
    if plan.node_type == 'FILTER':
        child = plan.children[0]
        predicate = plan.predicate
        
        # Debug information
        print(f"Attempting to push down predicate: {predicate}")
        
        # Helper function to extract table references from a predicate
        def extract_table_references(pred_str):
            references = []
            # Split on spaces and parentheses to better handle expressions
            parts = pred_str.replace('(', ' ').replace(')', ' ').split()
            for part in parts:
                if '.' in part:
                    table = part.split('.')[0]
                    references.append(table)
            return set(references)
        
        # Helper function to find and wrap scan nodes with filters
        def push_filter_to_scan(node, table_name, predicate):
            if node.node_type == 'SCAN' and (node.table == table_name or node.alias == table_name):
                # We found the scan, wrap it with a filter
                print(f"Found SCAN for {table_name}, applying filter directly")
                return LogicalPlanNode('FILTER', children=[node], predicate=predicate)
            
            if not hasattr(node, 'children') or not node.children:
                return node
            
            # Find matching table in children
            for i, child in enumerate(node.children):
                if find_table_in_subtree(child, table_name):
                    node.children[i] = push_filter_to_scan(child, table_name, predicate)
                    return node
            
            # If not found in any child, return unchanged
            return node
        
        # Helper to find a table in a subtree
        def find_table_in_subtree(node, table_name):
            if node.node_type == 'SCAN':
                return node.table == table_name or node.alias == table_name
            
            if not hasattr(node, 'children') or not node.children:
                return False
                
            return any(find_table_in_subtree(child, table_name) for child in node.children)
        
        # Helper to find table alias mappings
        def get_table_aliases(node, aliases=None):
            if aliases is None:
                aliases = {}
                
            if node.node_type == 'SCAN' and node.alias and node.alias != node.table:
                aliases[node.alias] = node.table
                aliases[node.table] = node.table  # Also map table to itself
            
            if hasattr(node, 'children') and node.children:
                for child in node.children:
                    get_table_aliases(child, aliases)
                    
            return aliases
        
        # Check if this is an AND condition which we can split
        if ' AND ' in predicate:
            print("AND condition detected, splitting into separate predicates")
            conditions = []
            
            # Improved AND condition splitting that properly handles parentheses
            def split_and_conditions(predicate_str):
                # Helper function to check if parentheses are balanced in a string segment
                def are_parentheses_balanced(s, start, end):
                    count = 0
                    for i in range(start, end + 1):
                        if s[i] == '(':
                            count += 1
                        elif s[i] == ')':
                            count -= 1
                        if count < 0:  # Unbalanced
                            return False
                    return count == 0
                
                # Helper function to properly split on top-level AND operators
                def proper_split(s):
                    print(f"Processing: {s}")
                    
                    # Remove outer parentheses if they enclose the entire string
                    if s.startswith('(') and s.endswith(')') and are_parentheses_balanced(s, 1, len(s) - 2):
                        return proper_split(s[1:-1].strip())
                    
                    # Find top-level AND operators (not inside parentheses)
                    level = 0
                    and_positions = []
                    
                    for i in range(len(s) - 4):  # -4 to leave room for " AND"
                        if s[i] == '(':
                            level += 1
                        elif s[i] == ')':
                            level -= 1
                        
                        # Check for AND at level 0 (not inside parentheses)
                        if level == 0 and i + 5 <= len(s) and s[i:i+5] == ' AND ':
                            and_positions.append(i)
                    
                    if and_positions:
                        # Split the string at each AND position
                        last_pos = 0
                        for pos in and_positions:
                            part = s[last_pos:pos].strip()
                            if part:
                                proper_split(part)
                            last_pos = pos + 5  # Skip " AND "
                        
                        # Don't forget the last part
                        last_part = s[last_pos:].strip()
                        if last_part:
                            proper_split(last_part)
                    else:
                        # No top-level AND, add the whole condition
                        conditions.append(s)
                
                proper_split(predicate_str)
                return conditions
            
            conditions = split_and_conditions(predicate)
            print(f"Split into {len(conditions)} separate conditions: {conditions}")
            
            # Get table aliases
            aliases = get_table_aliases(plan)
            print(f"Table aliases: {aliases}")
                
            # Process the project node case
            if child.node_type == 'PROJECT':
                # Apply filters below the projection
                filtered_child = child.children[0]
                for cond in conditions:
                    filtered_child = LogicalPlanNode('FILTER', children=[filtered_child], predicate=cond)
                child.children[0] = predicate_pushdown(filtered_child)
                return child
                
            # Map table names to their conditions
            table_conditions = {}
            multi_table_conditions = []
            
            for cond in conditions:
                tables = extract_table_references(cond)
                if len(tables) == 1:
                    table_name = list(tables)[0]
                    print(f"Condition '{cond}' references single table: {table_name}")
                    
                    # Resolve table name if it's an alias
                    actual_table = aliases.get(table_name, table_name)
                    
                    # Add to table_conditions dictionary
                    if actual_table not in table_conditions:
                        table_conditions[actual_table] = []
                    table_conditions[actual_table].append(cond)
                else:
                    # This condition references multiple tables or no tables
                    multi_table_conditions.append(cond)
            
            print(f"Table conditions: {table_conditions}")
            print(f"Multi-table conditions: {multi_table_conditions}")
            
            # First apply single-table conditions
            result = child
            for table_name, conds in table_conditions.items():
                for cond in conds:
                    # Push this condition to the appropriate scan
                    result = push_filter_to_scan(result, table_name, cond)
            
            # Then apply multi-table conditions at the top level
            for cond in multi_table_conditions:
                result = LogicalPlanNode('FILTER', children=[result], predicate=cond)
            
            return result
            
        # For non-AND conditions
        referenced_tables = extract_table_references(predicate)
        print(f"Tables referenced in predicate: {referenced_tables}")
        
        if len(referenced_tables) == 1:
            # The predicate only references a single table, we can push it down
            table_name = list(referenced_tables)[0]
            print(f"Trying to push predicate down to table: {table_name}")
            
            # Handle the PROJECT node case - push the filter below the projection
            if child.node_type == 'PROJECT':
                child.children[0] = predicate_pushdown(
                    LogicalPlanNode('FILTER', children=[child.children[0]], predicate=predicate)
                )
                return child
            
            # Apply the recursive filter pushing
            if child.node_type in ['JOIN', 'FILTER']:
                modified_child = push_filter_to_scan(child, table_name, predicate)
                return modified_child
            elif child.node_type == 'SCAN' and (child.table == table_name or child.alias == table_name):
                # Direct filter on scan
                return LogicalPlanNode('FILTER', children=[child], predicate=predicate)
        
        # If we can't determine which table the predicate belongs to, or it spans
        # multiple tables, keep the filter where it is
        return plan
            
    return plan

# ------------------ Convert Logical Plan back to JSON ------------------ #
def logical_plan_to_json(plan):
    """
    Convert a logical plan back to JSON format
    """
    if plan.node_type == 'PROJECT':
        columns = parse_columns_to_json(plan.columns)
        input_json = logical_plan_to_json(plan.children[0])
        
        return {
            "type": "project",
            "columns": columns,
            "input": input_json
        }
    
    elif plan.node_type == 'FILTER':
        condition = parse_condition_to_json(plan.predicate)
        input_json = logical_plan_to_json(plan.children[0])
        
        # Generate select-type syntax
        if input_json.get("type") == "project":
            return {
                "type": "select",
                "condition": condition,
                "input": input_json
            }
        else:
            # If not directly above a project, keep the filter type
            return {
                "type": "select",
                "condition": condition,
                "input": input_json
            }
    
    elif plan.node_type == 'JOIN':
        left_json = logical_plan_to_json(plan.children[0])
        right_json = logical_plan_to_json(plan.children[1])
        condition = parse_condition_to_json(plan.predicate)
        
        return {
            "type": "join",
            "condition": condition,
            "left": left_json,
            "right": right_json
        }
    
    elif plan.node_type == 'SCAN':
        return {
            "type": "base_relation",
            "tables": [
                {
                    "name": plan.table,
                    **({"alias": plan.alias} if plan.alias and plan.alias != plan.table else {})
                }
            ]
        }
    
    else:
        raise ValueError(f"Unknown node type for JSON conversion: {plan.node_type}")

def parse_columns_to_json(columns_str):
    """
    Parse columns from string format back to JSON
    """
    result = []
    for col_str in columns_str:
        # Check if it's a table.column reference
        if '.' in col_str:
            table, attr = col_str.split('.')
            column_obj = {"table": table, "attr": attr}
        else:
            column_obj = {"attr": col_str}
            
        result.append(column_obj)
    
    return result

def parse_condition_to_json(condition_str):
    """
    Parse a condition from string format back to JSON
    """
    # Handle parentheses
    condition_str = condition_str.strip()
    if condition_str.startswith('(') and condition_str.endswith(')'):
        # Remove outer parentheses
        condition_str = condition_str[1:-1].strip()
    
    if ' AND ' in condition_str:
        left_str, right_str = condition_str.split(' AND ', 1)
        return {
            "type": "AND",
            "left": parse_condition_to_json(left_str),
            "right": parse_condition_to_json(right_str)
        }
    elif ' OR ' in condition_str:
        left_str, right_str = condition_str.split(' OR ', 1)
        return {
            "type": "OR",
            "left": parse_condition_to_json(left_str),
            "right": parse_condition_to_json(right_str)
        }
    elif condition_str.startswith('NOT '):
        operand_str = condition_str[4:].strip()
        if operand_str.startswith('(') and operand_str.endswith(')'):
            operand_str = operand_str[1:-1].strip()
        return {
            "type": "NOT",
            "cond": parse_condition_to_json(operand_str)
        }
    
    # Handle basic comparisons
    for op_str, op_type in [('=', 'EQ'), ('<', 'LT'), ('>', 'GT'), 
                           ('<=', 'LE'), ('>=', 'GE'), ('<>', 'NE')]:
        if op_str in condition_str:
            left_str, right_str = condition_str.split(op_str, 1)
            left = parse_operand_to_json(left_str.strip())
            right = parse_operand_to_json(right_str.strip())
            return {"type": op_type, "left": left, "right": right}
    
    # If none of the above, return the condition as is
    return condition_str

def parse_operand_to_json(operand_str):
    """
    Parse an operand from string format back to JSON
    """
    if '.' in operand_str:
        # It's a table.column reference
        table, attr = operand_str.split('.')
        return {"table": table, "attr": attr}
    elif operand_str.startswith("'") and operand_str.endswith("'"):
        # It's a string literal
        return {"type": "string", "value": operand_str[1:-1]}
    elif operand_str.isdigit():
        # It's an integer
        return {"type": "int", "value": int(operand_str)}
    elif operand_str.replace('.', '', 1).isdigit():
        # It's a float
        return {"type": "float", "value": float(operand_str)}
    else:
        # It's some other value
        return operand_str

# ------------------ Entry Point ------------------ #
def optimize_query_plan(json_str):
    """
    Takes a JSON string representing a SQL query plan,
    applies predicate pushdown optimization, and returns the optimized plan.
    """
    try:
        # Parse JSON to dict
        query_json = json.loads(json_str)
        
        # Build logical plan from JSON
        logical_plan = build_logical_plan_from_json(query_json)
        
        print("Original Logical Plan:")
        print(logical_plan)
        
        # Apply predicate pushdown
        optimized_plan = predicate_pushdown(logical_plan)
        
        print("\nOptimized Logical Plan (with predicate pushdown):")
        print(optimized_plan)
        
        # Convert optimized plan back to JSON
        optimized_json = logical_plan_to_json(optimized_plan)
        
        return json.dumps(optimized_json, indent=2)
    
    except Exception as e:
        print(f"Error during optimization: {e}")
        import traceback
        traceback.print_exc()
        return None

# Test with an example
if __name__ == "__main__":
    # Example JSON with AND condition
    example_and_filter = """
    {"type": "select", "condition": {"type": "AND", "left": {"type": "AND", "left": {"type": "GT", "left": {"table": "customers", "attr": "age"}, "right": {"type": "int", "value": 30}}, "right": {"type": "GT", "left": {"table": "orders", "attr": "amount"}, "right": {"type": "int", "value": 100}}}, "right": {"type": "EQ", "left": {"table": "customers", "attr": "city"}, "right": {"type": "string", "value": "New York"}}}, "input": {"type": "project", "columns": [{"table": "customers", "attr": "id"}, {"table": "customers", "attr": "name"}, {"table": "orders", "attr": "order_id"}, {"table": "orders", "attr": "amount"}], "input": {"type": "join", "condition": {"type": "EQ", "left": {"table": "temp", "attr": "id"}, "right": {"type": "column", "table": "customers", "attr": "id"}}, "left": {"type": "join", "condition": {"type": "EQ", "left": {"table": "customers", "attr": "id"}, "right": {"type": "column", "table": "o", "attr": "customer_id"}}, "left": {"type": "base_relation", "tables": [{"name": "customers"}]}, "right": {"type": "base_relation", "tables": [{"name": "orders", "alias": "o"}]}}, "right": {"type": "base_relation", "tables": [{"name": "temp"}]}}}}
    """
    
    # Example JSON with OR condition
    example_or_filter = """
    {"type": "select", "condition": {"type": "OR", "left": {"type": "GT", "left": {"table": "customers", "attr": "age"}, "right": {"type": "int", "value": 30}}, "right": {"type": "GT", "left": {"table": "orders", "attr": "amount"}, "right": {"type": "int", "value": 100}}}, "input": {"type": "project", "columns": [{"table": "customers", "attr": "id"}, {"table": "customers", "attr": "name"}, {"table": "orders", "attr": "order_id"}, {"table": "orders", "attr": "amount"}], "input": {"type": "join", "condition": {"type": "EQ", "left": {"table": "temp", "attr": "id"}, "right": {"type": "column", "table": "customers", "attr": "id"}}, "left": {"type": "join", "condition": {"type": "EQ", "left": {"table": "customers", "attr": "id"}, "right": {"type": "column", "table": "o", "attr": "customer_id"}}, "left": {"type": "base_relation", "tables": [{"name": "customers"}]}, "right": {"type": "base_relation", "tables": [{"name": "orders", "alias": "o"}]}}, "right": {"type": "base_relation", "tables": [{"name": "temp"}]}}}}
    """
    
    print("\nOptimizing query with OR condition:")
    optimized = optimize_query_plan(example_or_filter)

    if optimized:
        with open ('optimized_query_OR.json', 'w') as f:
            f.write(optimized)

    print("\nOptimizing query with AND condition:")
    optimized = optimize_query_plan(example_and_filter)

    if optimized:
        with open ('optimized_query_AND.json', 'w') as f:
            f.write(optimized)
    