import re
import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Where
from sqlparse.tokens import Keyword, DML

class PlanNode:
    def __init__(self, op, children=None, expr=None):
        self.op = op                  # e.g. "SCAN", "JOIN", "FILTER", "SELECT"
        self.children = children or []  # list of PlanNode objects
        self.expr = expr              # details like table name, predicate, join condition, etc.

    def signature(self):
        """
        Generate a tuple signature that uniquely represents this node and its subtree.
        """
        return (self.op, self.expr, tuple(child.signature() for child in self.children))

    def __hash__(self):
        return hash(self.signature())

    def __eq__(self, other):
        if not isinstance(other, PlanNode):
            return False
        return self.signature() == other.signature()

    def __repr__(self):
        return f"{self.op}({self.expr})"


def deduplicate_plan(node, memo=None):
    """
    Recursively deduplicate common subexpressions.
    If an identical subtree is found, reuse the same node (thus converting the tree into a DAG).
    """
    if memo is None:
        memo = {}
    # Process children first (postorder traversal)
    new_children = []
    for child in node.children:
        dedup_child = deduplicate_plan(child, memo)
        new_children.append(dedup_child)
    node.children = new_children

    # Generate a unique signature for the node (including its children)
    sig = node.signature()
    if sig in memo:
        return memo[sig]
    else:
        memo[sig] = node
        return node


def print_dag(node, visited=None, indent=0):
    """
    Print the plan in a hierarchical form.
    If a node is visited again, indicate a shared reference.
    """
    if visited is None:
        visited = set()

    prefix = "  " * indent
    if id(node) in visited:
        print(f"{prefix}{node} (shared)")
        return
    print(f"{prefix}{node}")
    visited.add(id(node))
    for child in node.children:
        print_dag(child, visited, indent + 1)


def is_subquery(token):
    """Check if a token is a subquery by looking for a SELECT within a group."""
    if token.is_group:
        for t in token.tokens:
            if t.ttype is DML and t.value.upper() == "SELECT":
                return True
    return False


def parse_sql_to_plan(sql):
    """
    Parse a simple SELECT SQL statement to generate a query plan.
    
    Supported structure (roughly):
      SELECT <columns> 
      FROM table [AS alias] 
         [JOIN table [AS alias] ON <condition> ...]
      [WHERE <condition with possible nested subqueries>];
    
    Nested subqueries (within parentheses) are detected in the WHERE clause.
    """
    parsed = sqlparse.parse(sql)
    if not parsed:
        raise ValueError("Invalid SQL statement")
    stmt = parsed[0]
    if stmt.get_type() != 'SELECT':
        raise NotImplementedError("Only SELECT statements are supported in this example.")

    tokens = stmt.tokens

    select_clause = None
    from_tokens = []
    where_tokens = []
    mode = None

    for token in tokens:
        if token.is_whitespace:
            continue
        if token.ttype is DML and token.value.upper() == "SELECT":
            mode = "SELECT"
            continue
        if token.ttype is Keyword and token.value.upper() == "FROM":
            mode = "FROM"
            continue
        if token.ttype is Keyword and token.value.upper() == "WHERE":
            mode = "WHERE"
            continue

        if mode == "SELECT":
            select_clause = token  # Not used further in this demo.
        elif mode == "FROM":
            from_tokens.append(token)
        elif mode == "WHERE":
            where_tokens.append(token)

    # --- Build FROM clause plan (support JOINs) ---
    def get_scan_node(token):
        if isinstance(token, Identifier):
            return PlanNode("SCAN", expr=token.get_real_name())
        elif isinstance(token, IdentifierList):
            # In case of multiple identifiers separated by commas (should be rare if JOIN is used)
            nodes = []
            for identifier in token.get_identifiers():
                nodes.append(PlanNode("SCAN", expr=identifier.get_real_name()))
            return nodes[0] if len(nodes) == 1 else build_join_chain(nodes, None)
        else:
            return None

    def build_join_chain(nodes, join_conditions):
        """
        Create a left-deep join tree from a list of nodes.
        For simplicity, if join_conditions is provided as a list,
        we attach them in order to the join nodes.
        """
        if not nodes:
            return None
        current = nodes[0]
        conds = join_conditions if join_conditions else [None] * (len(nodes) - 1)
        for i in range(1, len(nodes)):
            current = PlanNode("JOIN", children=[current, nodes[i]], expr=conds[i - 1])
        return current

    # Process the FROM tokens.
    # We'll assume tokens come in a pattern:
    # [table, {JOIN, table, ON, condition}...]
    from_nodes = []
    join_conditions = []
    tokens_iter = iter(from_tokens)
    try:
        first_token = next(tokens_iter)
    except StopIteration:
        raise ValueError("No FROM clause tokens found")

    # Get first table (SCAN node)
    first_scan = get_scan_node(first_token)
    if not first_scan:
        raise ValueError("Invalid table reference in FROM clause.")
    from_nodes.append(first_scan)

    # Process remaining tokens for JOINs.
    while True:
        try:
            token = next(tokens_iter)
        except StopIteration:
            break
        # If token is a JOIN keyword
        if token.ttype is Keyword and token.value.upper() == "JOIN":
            # Next token should be a table identifier.
            table_token = next(tokens_iter, None)
            if not table_token:
                raise ValueError("JOIN keyword without table.")
            scan_node = get_scan_node(table_token)
            from_nodes.append(scan_node)
            # Look ahead for an ON clause.
            join_cond = None
            next_token = next(tokens_iter, None)
            if next_token and next_token.ttype is Keyword and next_token.value.upper() == "ON":
                # The next token is assumed to be the join condition.
                cond_token = next(tokens_iter, None)
                if cond_token:
                    join_cond = cond_token.value.strip()
            join_conditions.append(join_cond)
        # Otherwise ignore (commas, etc.)
    from_plan = build_join_chain(from_nodes, join_conditions)

    # --- Process WHERE clause (including nested subqueries) ---
    where_condition = None
    subquery_nodes = []
    if where_tokens:
        # Join all tokens to form the condition string.
        where_condition = " ".join(token.value for token in where_tokens).strip()

        # Detect nested subqueries via regex; this is simplistic.
        subquery_pattern = re.compile(r'\(SELECT.*?\)', re.IGNORECASE | re.DOTALL)
        matches = subquery_pattern.findall(where_condition)
        for match in matches:
            # Remove surrounding parentheses and recursively parse the subquery.
            subquery_sql = match.strip("()")
            subquery_plan = parse_sql_to_plan(subquery_sql)
            subquery_nodes.append(subquery_plan)
            # Replace the subquery text with a placeholder.
            where_condition = where_condition.replace(match, "<subquery>")
        # For a more integrated plan, you might attach subquery_nodes as children of the FILTER node.

    # Wrap the FROM clause with a FILTER node if needed.
    if where_condition:
        from_plan = PlanNode("FILTER", children=[from_plan], expr=where_condition)

    # Finally, wrap with a SELECT node (projection details could be added using select_clause).
    plan = PlanNode("SELECT", children=[from_plan], expr="projection")
    return plan


def main():
    sql = input("Enter SQL statement: ")

    try:
        plan_tree = parse_sql_to_plan(sql)
    except Exception as e:
        print(f"Error parsing SQL: {e}")
        return

    print("\nOriginal Plan Tree:")
    print_dag(plan_tree)

    print("\nDeduplicating common subexpressions...\n")
    optimized_plan = deduplicate_plan(plan_tree)

    print("Optimized Plan DAG:")
    print_dag(optimized_plan)


if __name__ == "__main__":
    main()
