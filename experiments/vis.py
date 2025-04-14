import json
from graphviz import Digraph

# Replace this with your input
query_file = "in.json"
with open(query_file, 'r') as f:
    query_plan = json.load(f)

graph = Digraph('QueryPlan', format='png')
graph.attr(rankdir='TB')  # Top-to-bottom flow
graph.attr('node', style='filled', fontname='Helvetica', fontsize='10')

node_counter = [0]
visited_exprs = {}

def new_node_id():
    node_counter[0] += 1
    return f"node_{node_counter[0]}"

def render_expr(expr, label=None):
    if expr.get('type') == 'expr_ref':
        expr_id = expr['id']
        if expr_id in visited_exprs:
            return visited_exprs[expr_id]
        else:
            resolved = query_plan['common_expressions'][expr_id]
            node_id = render_expr(resolved, f"{expr_id}: {resolved['type']}")
            visited_exprs[expr_id] = node_id
            return node_id

    node_id = new_node_id()
    node_label = label or expr['type']

    shape = 'box'
    color = 'lightgray'

    if expr['type'] == 'project':
        cols = ', '.join(f"{col['table']}.{col['attr']}" for col in expr['columns'])
        node_label = f"Project\n[{cols}]"
        # color = '#AED6F1'
        color = "#D5F5E3"

        graph.node(node_id, node_label, shape=shape, fillcolor=color)
        input_node = render_expr(expr['input'])
        graph.edge(node_id, input_node)  # root → child

    elif expr['type'] == 'select':
        cond = expr['condition']
        cond_str = f"{cond['left']['table']}.{cond['left']['attr']} {cond['type']} {cond['right']['value']}"
        node_label = f"Select\n({cond_str})"
        shape = 'ellipse'
        # color = '#F9E79F'
        color = '#F5B7B1'

        graph.node(node_id, node_label, shape=shape, fillcolor=color)
        input_node = render_expr(expr['input'])
        graph.edge(node_id, input_node)

    elif expr['type'] == 'join':
        cond = expr['condition']
        cond_str = f"{cond['left']['attr']} {cond['type']} {cond['right']['attr']}"
        node_label = f"Join\n({cond_str})"
        shape = 'diamond'
        # color = '#F5B7B1'
        color = '#AED6F1'

        graph.node(node_id, node_label, shape=shape, fillcolor=color)
        left_node = render_expr(expr['left'])
        right_node = render_expr(expr['right'])
        graph.edge(node_id, left_node)
        graph.edge(node_id, right_node)

    elif expr['type'] == 'base_relation':
        tables = ', '.join(t['name'] for t in expr['tables'])
        node_label = f"Base Relation\n[{tables}]"
        shape = 'oval'
        # color = '#D5F5E3'
        color = '#F9E79F'

        graph.node(node_id, node_label, shape=shape, fillcolor=color)

    elif expr['type'] == 'subquery':
        node_label = f"Subquery\n[{expr['alias']}]"
        shape = 'parallelogram'
        color = '#D7BDE2'

        graph.node(node_id, node_label, shape=shape, fillcolor=color)
        input_node = render_expr(expr['query'])
        graph.edge(node_id, input_node)

    else:
        graph.node(node_id, expr['type'], shape='box', fillcolor=color)

    return node_id

# Render root query
render_expr(query_plan['query'])

# Output
graph.render('query_plan', view=True)
