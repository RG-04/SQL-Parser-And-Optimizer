import ply.lex as lex
import ply.yacc as yacc

# ------------------ Lexer ------------------ #
tokens = (
    'SELECT', 'FROM', 'WHERE', 'JOIN', 'ON',
    'AS', 'DOT', 'COMMA', 'EQUALS',
    'IDENTIFIER', 'STRING',
)

# Define token patterns
t_DOT = r'\.'
t_COMMA = r','
t_EQUALS = r'='
t_ignore = ' \t\n'  # Ignore whitespace and newlines

# Define tokens with specific function to handle case-insensitivity
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

def t_STRING(t):
    r'"[^"]*"|\'[^\']*\''
    # Handle both single and double quotes
    if t.value.startswith('"'):
        t.value = t.value.strip('"')
    else:
        t.value = t.value.strip("'")
    return t

def t_IDENTIFIER(t):
    r'[a-zA-Z_][a-zA-Z0-9_]*'
    # Check for keywords (case insensitive)
    if t.value.upper() in ('SELECT', 'FROM', 'WHERE', 'JOIN', 'ON', 'AS'):
        t.type = t.value.upper()
    return t

def t_error(t):
    print(f"Lexer Error: Illegal character '{t.value[0]}'")
    t.lexer.skip(1)

lexer = lex.lex(debug=False)

# ------------------ Parser ------------------ #
def p_query(p):
    '''query : SELECT select_list FROM table_reference where_clause'''
    p[0] = {
        'select': p[2],
        'from': p[4],
        'where': p[5]
    }

def p_select_list(p):
    '''select_list : column
                   | column COMMA select_list'''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = [p[1]] + p[3]

def p_column(p):
    '''column : IDENTIFIER DOT IDENTIFIER
              | IDENTIFIER'''
    if len(p) == 4:
        p[0] = f"{p[1]}.{p[3]}"
    else:
        p[0] = p[1]

def p_table_reference(p):
    '''table_reference : table_alias
                      | join_clause'''
    p[0] = p[1]

def p_table_alias(p):
    '''table_alias : IDENTIFIER 
                  | IDENTIFIER IDENTIFIER
                  | IDENTIFIER AS IDENTIFIER'''
    if len(p) == 2:
        # Just the table name
        p[0] = {'table': p[1], 'alias': p[1]}
    elif len(p) == 3:
        # Table and alias without AS
        p[0] = {'table': p[1], 'alias': p[2]}
    else:
        # Table AS alias
        p[0] = {'table': p[1], 'alias': p[3]}

def p_join_clause(p):
    '''join_clause : table_alias JOIN table_alias ON join_condition'''
    p[0] = {
        'join': {
            'left': p[1],
            'right': p[3],
            'on': p[5]
        }
    }

def p_join_condition(p):
    '''join_condition : column EQUALS column'''
    p[0] = f"{p[1]} = {p[3]}"

def p_where_clause(p):
    '''where_clause : WHERE column EQUALS STRING
                    | WHERE column EQUALS column
                    | '''
    if len(p) == 5:
        if p[3] == '=':
            if isinstance(p[4], str):
                p[0] = f"{p[2]} = \"{p[4]}\""
            else:
                p[0] = f"{p[2]} = {p[4]}"
    else:
        p[0] = None

def p_error(p):
    if p:
        print(f"Parser Error: Syntax error at token {p.type} with value '{p.value}'")
    else:
        print("Parser Error: Syntax error at EOF (unexpected end of input)")

parser = yacc.yacc(debug=False)

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

    print("UNDERSTAND: ")
    print(from_clause)
    print("----")
    print(where_clause)
    print("----")
    print(select_cols)

    # Handle simple table case
    if 'table' in from_clause:
        table_info = from_clause
        scan = LogicalPlanNode('SCAN', table=table_info['table'], alias=table_info['alias'])
        current = scan
    else:
        # Handle join case
        join_info = from_clause['join']
        left = join_info['left']
        right = join_info['right']

        scan_left = LogicalPlanNode('SCAN', table=left['table'], alias=left['alias'])
        scan_right = LogicalPlanNode('SCAN', table=right['table'], alias=right['alias'])
        current = LogicalPlanNode('JOIN', children=[scan_left, scan_right],
                               predicate=join_info['on'])

    if where_clause:
        current = LogicalPlanNode('FILTER', predicate=where_clause, children=[current])

    if select_cols:
        current = LogicalPlanNode('PROJECT', columns=select_cols, children=[current])

    return current

# ------------------ Predicate Pushdown ------------------ #
def predicate_pushdown(plan):
    if not plan.children:
        return plan

    plan.children = [predicate_pushdown(child) for child in plan.children]

    if plan.node_type == 'FILTER':
        child = plan.children[0]
        predicate = plan.predicate
        
        if child.node_type == 'JOIN':
            # Check which side of the join the predicate refers to
            left_alias = child.children[0].alias
            right_alias = child.children[1].alias
            
            # Simple heuristic to determine which table the predicate belongs to
            if left_alias and left_alias in predicate:
                child.children[0] = LogicalPlanNode('FILTER', predicate=predicate, children=[child.children[0]])
                return child
            elif right_alias and right_alias in predicate:
                child.children[1] = LogicalPlanNode('FILTER', predicate=predicate, children=[child.children[1]])
                return child
            else:
                # Can't push down, keep filter above join
                return plan
        elif child.node_type == 'SCAN':
            # Simplify tree by merging filter with scan
            child.predicate = predicate
            return child
            
    return plan

# ------------------ Entry Point ------------------ #
if __name__ == "__main__":
    try:
        query = input("Enter SQL Query: ")
        print(f"\nAttempting to parse: {query}")
        
        # Show tokens
        lexer.input(query)
        print("\nTokens found:")
        for tok in lexer:
            print(f"  {tok.type}: '{tok.value}'")
        
        # Reset lexer and parse
        lexer.input(query)
        parsed = parser.parse(query, lexer=lexer)

        if parsed is None:
            print("\nParsing failed. Please check your SQL syntax.")
        else:
            print("\nParsing successful!")
            print(f"Parsed query: {parsed}")
            
            logical_plan = build_logical_plan(parsed)
            optimized_plan = predicate_pushdown(logical_plan)

            print("\nOriginal Logical Plan:")
            print(logical_plan)
            print("Optimized Logical Plan:")
            print(optimized_plan)
            
    except Exception as e:
        print(f"Error: {e}")