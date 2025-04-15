from flask import Flask, render_template, request, jsonify, url_for
import subprocess
import json
import os
import tempfile
import copy
from predicate_pushdown import optimize_query_plan
from join_optimization import QueryOptimizer
from graph_visualizer import visualize_query_plan
from subsequence_elim import QueryTreeOptimizer
from cost_populator import CostCalculator

app = Flask(__name__, static_folder='static')

db_params = {
    'dbname': 'temp',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}
cost_calculator = CostCalculator(db_params)
cost_calculator.connect()


USER_STUFF = {"scale": 1.0}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/parse', methods=['POST'])
def parse_sql():
    sql_query = request.form.get('sql_query', '')

    USER_STUFF = {}
    
    if not sql_query:
        return jsonify({'error': 'Empty SQL query'})
    
    # Create a temporary file for the SQL query
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as temp_file:
        temp_file_name = temp_file.name
        temp_file.write(sql_query)
    
    try:
        # Run the sql_to_ra executable on the temporary file
        result = subprocess.run(
            ['../final_parser/sql_to_ra', temp_file_name],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Try to parse the output as JSON
        try:
            output_json = json.loads(result.stdout)
            return jsonify({
                'success': True,
                'result': output_json
            })
        except json.JSONDecodeError:
            return jsonify({
                'success': False,
                'error': 'Failed to parse JSON output',
                'raw_output': result.stdout
            })
            
    except subprocess.CalledProcessError as e:
        return jsonify({
            'success': False,
            'error': f'Parser execution failed: {e}',
            'stderr': e.stderr
        })
    finally:
        # Clean up the temporary file
        os.unlink(temp_file_name)

@app.route('/optimize/pred_push/', methods=['POST'])
def optimize_predpush():
    print("Predicate pushdown endpoint called: ", request.json)  # Debug output
    
    try:
        # Get the relational algebra JSON from the request
        relational_algebra = request.json.get('relational_algebra', {})
        result = optimize_query_plan(json.dumps(relational_algebra))
        
        optimized_plan = result["optimized_plan_json"]
        original_plan_str = result["original_plan_str"]
        optimized_plan_str = result["optimized_plan_str"]
        
        print("Successfully read example files")  # Debug output
        print("Optimized plan: ", optimized_plan_str)  # Debug output
        print("Original plan: ", original_plan_str)  # Debug output

        USER_STUFF["original_plan_json"] = relational_algebra
        USER_STUFF["pred_plan_json"] = optimized_plan
        optimized_plan_with_cost = copy.deepcopy(optimized_plan)
        optimized_cost, _ = cost_calculator.calculate_cost(optimized_plan_with_cost)
        USER_STUFF["pred_cost"] = optimized_cost
        
        return jsonify({
            'success': True,
            'original_plan_json': relational_algebra,
            'optimized_plan_json': optimized_plan,
            'original_plan_str': original_plan_str,
            'optimized_plan_str': optimized_plan_str
        })
        
            
    except Exception as e:
        print(f"Exception in optimize endpoint: {e}")  # Debug output
        return jsonify({
            'success': False,
            'error': f'Optimization failed: {str(e)}'
        })
    
@app.route('/optimize/join/', methods=['POST'])
def optimize_join():
    print("Join optimization endpoint called: ", request.json)  # Debug output
    
    try:
        # Get the relational algebra JSON from the request
        relational_algebra = request.json.get('relational_algebra', {})

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
        optimizer.connect()
        
        res = optimizer.get_costs_and_plans(USER_STUFF["pred_plan_json"])
        USER_STUFF["join_plan_json"] = res["best_plan"]
        USER_STUFF["join_cost"] = res["best_cost"]
        USER_STUFF["pred_cost"] = res["naive_cost"]
        USER_STUFF["scale"] = res["scale"]

        
        return jsonify({
            'success': True,
            'original_plan_json': res["naive_plan"],
            'optimized_plan_json': res["best_plan"],
            'original_cost': res["naive_cost"],
            'optimized_cost': res["best_cost"],
        })
            
    except Exception as e:
        print(f"Exception in join optimization endpoint: {e}")  # Debug output
        return jsonify({
            'success': False,
            'error': f'Join optimization failed: {str(e)}'
        })

@app.route('/optimize/common_subexpr/', methods=['POST'])
def optimize_common_subexpr():
    print("Common subexpression elimination endpoint called: ", request.json)  
    
    try:
        # Get the relational algebra JSON from the request

        relational_algebra = request.json.get('relational_algebra', {}) # IGNORED for now
        original_cost = None
        if "pred_plan_json" in USER_STUFF:
            # Use the predicate pushdown plan JSON if available
            relational_algebra = USER_STUFF["pred_plan_json"]
            original_cost = USER_STUFF["pred_cost"]
        else:
            relational_algebra = USER_STUFF["original_plan_json"]

        optimize_input_json = None
        if "join_plan_json" in USER_STUFF:
            optimize_input_json = USER_STUFF["join_plan_json"]
        else:
            optimize_input_json = relational_algebra
        
        optimizer = QueryTreeOptimizer()
        optimized_tree = optimizer.optimize_and_cleanup(optimize_input_json)
        print("Optimized plan: ", json.dumps(optimized_tree, indent=2))  # Debug output
        original_plan_svg = visualize_query_plan({"query":relational_algebra})
        optimized_plan_svg = visualize_query_plan(optimized_tree)

        if not original_cost:
            original_cost = cost_calculator.calculate_cost(relational_algebra)

        optimized_tree_with_cost = copy.deepcopy(optimized_tree)
        optimized_cost, _ = cost_calculator.calc_subseq_cost(optimized_tree_with_cost)
        cost_calculator.scale_costs(optimized_tree_with_cost, USER_STUFF["scale"])

        if len(optimized_tree["common_expressions"]) == 0:
            optimized_cost = original_cost
            print("No common subexpressions found. Using original cost.")
            
        optimized_cost *= USER_STUFF["scale"]
        print("Optimized cost: ", optimized_cost)  # Debug output

        USER_STUFF["subseq_plan_json"] = optimized_tree
        USER_STUFF["subseq_cost"] = optimized_cost

        return jsonify({
            'success': True,
            'original_plan_json': relational_algebra,
            'optimized_plan_json': optimized_tree,
            'optimized_plan_svg': optimized_plan_svg,
            'original_plan_svg': original_plan_svg,
            'original_cost': original_cost,
            'optimized_cost': optimized_cost,
        })
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Exception in common subexpression elimination: {e}")
        return jsonify({
            'success': False,
            'error': f'Common subexpression elimination failed: {str(e)}'
        })

if __name__ == '__main__':
    app.run(debug=True)