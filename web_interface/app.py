from flask import Flask, render_template, request, jsonify, url_for
import subprocess
import json
import os
import tempfile
from predicate_pushdown import optimize_query_plan
from join_optimization import QueryOptimizer
app = Flask(__name__, static_folder='static')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/parse', methods=['POST'])
def parse_sql():
    sql_query = request.form.get('sql_query', '')
    
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
def optimize_query():
    print("Predicate pushdown endpoint called: ", request.json)  # Debug output
    
    try:
        # Get the relational algebra JSON from the request
        relational_algebra = request.json.get('relational_algebra', {})
        result = optimize_query_plan(json.dumps(relational_algebra))
        
        optimized_plan = result["optimized_plan_json"]
        
        print("Successfully read example files")  # Debug output
        
        return jsonify({
            'success': True,
            'original_plan_json': relational_algebra,
            'optimized_plan_json': optimized_plan,
            'explanation': 'Predicate pushdown moved the filter condition (b.id > 1) down to be applied directly to the base table.'
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
        
        # Calculate naive cost
        naive_plan = optimizer.calculate_naive_cost(json.dumps(relational_algebra))

        # Run the optimization with forced diverse strategies
        best_plans = optimizer.optimize_join_query(json.dumps(relational_algebra))
        
        # Generate the JSON for the best plan
        best_plan_json = optimizer.generate_best_plan_json(best_plans)
        
        optimizer.disconnect()

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
                
        best_plan_json = json.loads(best_plan_json)
        return jsonify({
            'success': True,
            'original_plan_json': relational_algebra,
            'optimized_plan_json': best_plan_json,
        })
            
    except Exception as e:
        print(f"Exception in join optimization endpoint: {e}")  # Debug output
        return jsonify({
            'success': False,
            'error': f'Join optimization failed: {str(e)}'
        })

if __name__ == '__main__':
    app.run(debug=True)