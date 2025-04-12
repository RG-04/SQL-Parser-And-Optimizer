from flask import Flask, render_template, request, jsonify, url_for
import subprocess
import json
import os
import tempfile
from predicate_pushdown import optimize_query_plan
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

@app.route('/optimize', methods=['POST'])
def optimize_query():
    print("Optimize endpoint called: ", request.json)  # Debug output
    
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

if __name__ == '__main__':
    app.run(debug=True)