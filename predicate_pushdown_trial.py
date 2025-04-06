import ply.lex as lex
import ply.yacc as yacc
import sys

# ------------------ Lexer ------------------ #
tokens = (
    'SELECT', 'FROM', 'WHERE', 'JOIN', 'ON',
    'AS', 'AND', 'OR', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS',
    'DOT', 'COMMA', 'EQUALS', 'LT', 'GT', 'LE', 'GE', 'NE',
    'LPAREN', 'RPAREN',
    'IDENTIFIER', 'STRING', 'INTEGER', 'FLOAT',
    'ASTERISK',
)

# Regular expressions for simple tokens
t_COMMA = r','
t_DOT = r'\.'
t_EQUALS = r'='
t_LT = r'<'
t_GT = r'>'
t_LE = r'<='
t_GE = r'>='
t_NE = r'<>|!='
t_LPAREN = r'\('
t_RPAREN = r'\)'
t_ASTERISK = r'\*'
t_ignore = ' \t\n'

# Define reserved words
reserved = {
    'select': 'SELECT',
    'from': 'FROM',
    'where': 'WHERE',
    'join': 'JOIN',
    'on': 'ON',
    'as': 'AS',
    'and': 'AND',
    'or': 'OR',
    'inner': 'INNER',
    'left': 'LEFT',
    'right': 'RIGHT',
    'outer': 'OUTER',
    'cross': 'CROSS',
}

# Case-insensitive SQL keywords
def t_SELECT(t):
    r'[Ss][Ee][Ll][Ee][Cc][Tt]'
    return t

def t_FROM(t):
    r'[Ff][Rr][Oo][Mm]'
    return t

def t_WHERE(t):
    r'[Ww][Hh][Ee][Rr][Ee]'
    return t

def t_JOIN(t):
    r'[Jj][Oo][Ii][Nn]'
    return t

def t_ON(t):
    r'[Oo][Nn]'
    return t

def t_AS(t):
    r'[Aa][Ss]'
    return t

def t_AND(t):
    r'[Aa][Nn][Dd]'
    return t

def t_OR(t):
    r'[Oo][Rr]'
    return t

def t_INNER(t):
    r'[Ii][Nn][Nn][Ee][Rr]'
    return t

def t_LEFT(t):
    r'[Ll][Ee][Ff][Tt]'
    return t

def t_RIGHT(t):
    r'[Rr][Ii][Gg][Hh][Tt]'
    return t

def t_OUTER(t):
    r'[Oo][Uu][Tt][Ee][Rr]'
    return t

def t_CROSS(t):
    r'[Cc][Rr][Oo][Ss][Ss]'
    return t

def t_FLOAT(t):
    r'\d+\.\d+'
    t.value = float(t.value)
    return t

def t_INTEGER(t):
    r'\d+'
    t.value = int(t.value)
    return t

def t_STRING(t):
    r'\"[^\"]*\"|\'[^\']*\''
    # Remove the quotes
    t.value = t.value[1:-1]
    return t

def t_IDENTIFIER(t):
    r'[a-zA-Z_][a-zA-Z0-9_]*'
    t.type = reserved.get(t.value.lower(), 'IDENTIFIER')
    return t

def t_error(t):
    print(f"Illegal character '{t.value[0]}'")
    t.lexer.skip(1)

lexer = lex.lex()

# ------------------ Parser ------------------ #
# Define precedence rules
precedence = (
    ('left', 'OR'),
    ('left', 'AND'),
    ('nonassoc', 'EQUALS', 'LT', 'GT', 'LE', 'GE', 'NE'),
)

def p_query(p):
    '''query : SELECT select_list FROM table_expr where_clause'''
    p[0] = {
        'select': p[2],
        'from': p[4],
        'where': p[5]
    }

def p_select_list(p):
    '''select_list : ASTERISK
                   | column_list'''
    if p[1] == '*':
        p[0] = ['*']
    else:
        p[0] = p[1]

def p_column_list(p):
    '''column_list : column
                   | column_list COMMA column'''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1] + [p[3]]

def p_column(p):
    '''column : column_reference
              | column_reference AS IDENTIFIER
              | column_reference IDENTIFIER'''
    if len(p) == 2:
        p[0] = p[1]
    elif p[2].upper() == 'AS':
        # SELECT col AS alias
        p[0] = {'column': p[1], 'alias': p[3]}
    else:
        # SELECT col alias
        p[0] = {'column': p[1], 'alias': p[2]}

def p_column_reference(p):
    '''column_reference : IDENTIFIER
                        | IDENTIFIER DOT IDENTIFIER
                        | IDENTIFIER DOT ASTERISK'''
    if len(p) == 2:
        p[0] = p[1]
    elif p[3] == '*':
        p[0] = f"{p[1]}.*"
    else:
        p[0] = f"{p[1]}.{p[3]}"

def p_table_expr(p):
    '''table_expr : table_primary
                  | joined_table'''
    p[0] = p[1]

def p_table_primary(p):
    '''table_primary : IDENTIFIER
                     | IDENTIFIER AS IDENTIFIER
                     | IDENTIFIER IDENTIFIER'''
    if len(p) == 2:
        p[0] = {'table': p[1], 'alias': p[1]}
    elif p[2].upper() == 'AS':
        # FROM table AS alias
        p[0] = {'table': p[1], 'alias': p[3]}
    else:
        # FROM table alias
        p[0] = {'table': p[1], 'alias': p[2]}

def p_joined_table(p):
    '''joined_table : table_expr join_type JOIN table_primary ON join_condition'''
    p[0] = {
        'join': {
            'left': p[1],
            'type': p[2],
            'right': p[4],
            'condition': p[6]
        }
    }

def p_join_type(p):
    '''join_type : INNER
                 | LEFT
                 | RIGHT
                 | CROSS
                 | '''
    if len(p) == 1:
        # Default is inner join
        p[0] = 'INNER'
    else:
        p[0] = p[1].upper()

def p_join_condition(p):
    '''join_condition : comparison_expr'''
    p[0] = p[1]

def p_where_clause(p):
    '''where_clause : WHERE search_condition
                    | '''
    if len(p) == 1:
        p[0] = None
    else:
        p[0] = p[2]

def p_search_condition(p):
    '''search_condition : boolean_term
                        | search_condition OR boolean_term'''
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = {'operator': 'OR', 'left': p[1], 'right': p[3]}

def p_boolean_term(p):
    '''boolean_term : boolean_factor
                    | boolean_term AND boolean_factor'''
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = {'operator': 'AND', 'left': p[1], 'right': p[3]}

def p_boolean_factor(p):
    '''boolean_factor : comparison_expr
                      | LPAREN search_condition RPAREN'''
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = p[2]

def p_comparison_expr(p):
    '''comparison_expr : column_reference EQUALS column_reference
                       | column_reference EQUALS STRING
                       | column_reference EQUALS INTEGER
                       | column_reference EQUALS FLOAT
                       | column_reference LT column_reference
                       | column_reference LT STRING
                       | column_reference LT INTEGER
                       | column_reference LT FLOAT
                       | column_reference GT column_reference
                       | column_reference GT STRING
                       | column_reference GT INTEGER
                       | column_reference GT FLOAT
                       | column_reference LE column_reference
                       | column_reference LE STRING
                       | column_reference LE INTEGER
                       | column_reference LE FLOAT
                       | column_reference GE column_reference
                       | column_reference GE STRING
                       | column_reference GE INTEGER
                       | column_reference GE FLOAT
                       | column_reference NE column_reference
                       | column_reference NE STRING
                       | column_reference NE INTEGER
                       | column_reference NE FLOAT'''
    
    # Map tokens to operators
    op_map = {
        '=': 'EQUALS',
        '<': 'LT',
        '>': 'GT',
        '<=': 'LE',
        '>=': 'GE',
        '<>': 'NE',
        '!=': 'NE'
    }
    
    op = p[2]
    if isinstance(op, str) and op in op_map:
        op = op_map[op]
    
    p[0] = {'operator': op, 'left': p[1], 'right': p[3]}

def p_error(p):
    if p:
        print(f"Syntax error at '{p.value}'")
    else:
        print("Syntax error at EOF")

parser = yacc.yacc()

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
            s += f"({self.table}" + (f" AS {self.alias}" if self.alias != self.table else "") + ")"
        if self.predicate:
            s += f" [{self.predicate}]"
        if self.columns:
            s += f" ⮕ {self.columns}"
        s += "\n"
        for child in self.children:
            s += child.__str__(level + 1)
        return s

# ------------------ Logical Plan Builder ------------------ #
def build_logical_plan(parsed_query):
    from_clause = parsed_query['from']
    where_clause = parsed_query.get('where')
    select_cols = parsed_query.get('select', [])

    # Create the logical plan based on the query structure
    if 'join' in from_clause:
        # Handle JOIN
        join_info = from_clause['join']
        left = build_table_reference(join_info['left'])
        right = build_table_reference(join_info['right'])
        
        join_condition = format_condition(join_info['condition'])
        current = LogicalPlanNode('JOIN', children=[left, right], predicate=join_condition)
    else:
        # Handle single table
        current = build_table_reference(from_clause)

    # Add WHERE clause if present
    if where_clause:
        condition = format_condition(where_clause)
        current = LogicalPlanNode('FILTER', children=[current], predicate=condition)

    # Add PROJECT (SELECT) node
    formatted_cols = format_columns(select_cols)
    current = LogicalPlanNode('PROJECT', children=[current], columns=formatted_cols)

    return current

def build_table_reference(table_ref):
    if 'join' in table_ref:
        # Recursive case for nested joins
        join_info = table_ref['join']
        left = build_table_reference(join_info['left'])
        right = build_table_reference(join_info['right'])
        
        join_condition = format_condition(join_info['condition'])
        return LogicalPlanNode('JOIN', children=[left, right], predicate=join_condition)
    else:
        # Base case: simple table
        return LogicalPlanNode('SCAN', table=table_ref['table'], alias=table_ref['alias'])

def format_condition(condition):
    if isinstance(condition, dict):
        if 'operator' in condition:
            if condition['operator'] in ('AND', 'OR'):
                left = format_condition(condition['left'])
                right = format_condition(condition['right'])
                return f"({left}) {condition['operator']} ({right})"
            elif condition['operator'] == 'NOT':
                operand = format_condition(condition['operand'])
                return f"NOT ({operand})"
            else:
                # Binary comparison
                left = condition['left']
                right = condition['right']
                
                # Handle different right operand types
                if isinstance(right, str) and not '.' in right:
                    # It's a string literal
                    right = f"'{right}'"
                
                return f"{left} {condition['operator']} {right}"
    
    # If it's not a dict or doesn't have the expected structure
    return str(condition)

def format_columns(columns):
    formatted = []
    for col in columns:
        if isinstance(col, dict) and 'column' in col:
            # Column with alias
            formatted.append(f"{col['column']} AS {col['alias']}")
        else:
            # Simple column
            formatted.append(str(col))
    
    return formatted

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
        
        if child.node_type == 'JOIN':
            # Check if the predicate can be pushed to left or right child
            left_child = child.children[0]
            right_child = child.children[1]
            
            # Print debugging info
            print(f"JOIN detected: Left alias = {left_child.alias}, Right alias = {right_child.alias}")
            
            # Check if predicate mentions the left child's alias
            if left_child.alias and f"{left_child.alias}." in predicate:
                print(f"Pushing predicate to left child (table: {left_child.table}, alias: {left_child.alias})")
                left_with_filter = LogicalPlanNode('FILTER', 
                                                  children=[left_child], 
                                                  predicate=predicate)
                child.children[0] = left_with_filter
                # Return the JOIN node with the filter pushed down
                return child
            # Check if predicate mentions the right child's alias
            elif right_child.alias and f"{right_child.alias}." in predicate:
                print(f"Pushing predicate to right child (table: {right_child.table}, alias: {right_child.alias})")
                right_with_filter = LogicalPlanNode('FILTER', 
                                                   children=[right_child], 
                                                   predicate=predicate)
                child.children[1] = right_with_filter
                # Return the JOIN node with the filter pushed down
                return child
            else:
                print(f"Could not push down predicate: {predicate} - no matching alias found")
                # Cannot push down, keep filter above join
                return plan
        elif child.node_type == 'SCAN':
            # If child is a SCAN, we can merge the filter with it
            print(f"Merging filter with SCAN on table {child.table}")
            child.predicate = predicate
            return child
            
    return plan

# ------------------ Entry Point ------------------ #
def parse_sql(query):
    try:
        # Parse the query
        parsed_query = parser.parse(query)
        if not parsed_query:
            print("Parsing failed.")
            return None, None
        

        print("Parsed Query:")
        print(parsed_query)

        # Build and optimize the plan
        logical_plan = build_logical_plan(parsed_query)
        optimized_plan = predicate_pushdown(logical_plan)
        
        return logical_plan, optimized_plan
    
    except Exception as e:
        print(f"Error during parsing or optimization: {e}")
        return None, None

# Test case to demonstrate predicate pushdown
if __name__ == "__main__":
    # Create a complex query with pushable predicates
    query = """
    SELECT c.name, o.product 
    FROM customers c 
    JOIN ders o ON c.id = o.customer_id 
    WHERE c.country = 'USA' AND o.amount > 1000
    """
    
    print(f"Testing query: {query}")
    
    # Parse the query
    import ply.lex as lex
    import ply.yacc as yacc
    
    # Build lexer and parser
    lexer = lex.lex()
    parser = yacc.yacc()
    
    # Parse the query
    parsed_query = parser.parse(query, lexer=lexer)
    
    if not parsed_query:
        print("Parsing failed.")
    else:
        print("\nParsed query successfully!")
        print(f"Parsed structure: {parsed_query}")
        
        # Build logical plan
        logical_plan = build_logical_plan(parsed_query)
        
        print("\nOriginal Logical Plan:")
        print(logical_plan)
        
        # Apply predicate pushdown
        optimized_plan = predicate_pushdown(logical_plan)
        
        print("\nOptimized Logical Plan (with predicate pushdown):")
        print(optimized_plan)
        
        # Test with a simpler query (single predicate)
        simple_query = """
        SELECT c.name
        FROM customers c
        JOIN ders o ON c.id = o.customer_id
        WHERE c.country = 'USA'
        """
        
        print(f"\n\nTesting simpler query: {simple_query}")
        
        # Parse the simpler query
        parsed_simple = parser.parse(simple_query, lexer=lexer)
        
        if not parsed_simple:
            print("Parsing failed for simple query.")
        else:
            # Build logical plan for simple query
            simple_plan = build_logical_plan(parsed_simple)
            
            print("\nOriginal Logical Plan (simple query):")
            print(simple_plan)
            
            # Apply predicate pushdown to simple query
            optimized_simple = predicate_pushdown(simple_plan)
            
            print("\nOptimized Logical Plan (simple query):")
            print(optimized_simple)
