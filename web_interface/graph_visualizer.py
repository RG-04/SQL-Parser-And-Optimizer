import json
import textwrap
from graphviz import Digraph

def visualize_query_plan(query_plan):
    graph = Digraph('QueryPlan', format='svg')
    graph.attr(rankdir='TB')
    graph.attr('node', style='filled', fontname='Helvetica', fontsize='10')

    node_counter = [0]
    visited_exprs = {}
    
    def new_node_id():
        node_counter[0] += 1
        return f"node_{node_counter[0]}"
    
    def wrap_label(text, width=40):
        # Wrap each line separately in case text already contains newline characters.
        return "\n".join(textwrap.fill(line, width=width) for line in text.split("\n"))
    
    def render_condition(cond):
        """
        Recursively renders a condition dict into its string representation.
        It handles nested conditions (like AND/OR) as well as simple comparisons.
        """
        if isinstance(cond, dict):
            # Check for direct column reference.
            if 'table' in cond and 'attr' in cond:
                return f"{cond['table']}.{cond['attr']}"
            # Literal integer value.
            elif cond.get('type') == 'int':
                return str(cond.get('value'))
            # Otherwise, if the condition has both left and right children, assume it's a composite condition.
            elif 'left' in cond and 'right' in cond:
                left_str = render_condition(cond['left'])
                right_str = render_condition(cond['right'])
                # Wrap nested conditions in parentheses for clarity.
                return f"({left_str} {cond['type']} {right_str})"
        return str(cond)

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
            color = "#D5F5E3"
            graph.node(node_id, wrap_label(node_label), shape=shape, fillcolor=color)
            input_node = render_expr(expr['input'])
            graph.edge(node_id, input_node)

        elif expr['type'] == 'select':
            cond_str = render_condition(expr['condition'])
            node_label = f"Select\n({cond_str})"
            shape = 'ellipse'
            color = '#F5B7B1'
            graph.node(node_id, wrap_label(node_label), shape=shape, fillcolor=color)
            input_node = render_expr(expr['input'])
            graph.edge(node_id, input_node)

        elif expr['type'] == 'join':
            cond_str = render_condition(expr['condition'])
            node_label = f"Join\n({cond_str})"
            shape = 'diamond'
            color = '#AED6F1'
            graph.node(node_id, wrap_label(node_label), shape=shape, fillcolor=color)
            left_node = render_expr(expr['left'])
            right_node = render_expr(expr['right'])
            graph.edge(node_id, left_node)
            graph.edge(node_id, right_node)

        elif expr['type'] == 'base_relation':
            tables = ', '.join(t['name'] for t in expr['tables'])
            node_label = f"Base Relation\n[{tables}]"
            shape = 'oval'
            color = '#F9E79F'
            graph.node(node_id, wrap_label(node_label), shape=shape, fillcolor=color)

        elif expr['type'] == 'subquery':
            node_label = f"Subquery\n[{expr['alias']}]"
            shape = 'parallelogram'
            color = '#D7BDE2'
            graph.node(node_id, wrap_label(node_label), shape=shape, fillcolor=color)
            input_node = render_expr(expr['query'])
            graph.edge(node_id, input_node)

        else:
            graph.node(node_id, wrap_label(expr['type']), shape='box', fillcolor=color)

        return node_id

    # Render the query plan by recursively processing the query expression.
    render_expr(query_plan['query'])
    svg_path = graph.render('query_plan', format='svg', cleanup=True)

    # Read SVG content from file.
    with open(svg_path, 'r') as svg_file:
        svg_content = svg_file.read()
    return svg_content
