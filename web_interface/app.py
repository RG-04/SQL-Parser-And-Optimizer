from flask import Flask, render_template, request, jsonify, url_for
import subprocess
import json
import os
import tempfile

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
    # Placeholder for future optimization steps
    relational_algebra = request.json.get('relational_algebra', {})
    
    # For now, just return the same RA without modifications
    # This will be replaced with actual optimization logic later
    return jsonify({
        'success': True,
        'optimized': relational_algebra
    })

if __name__ == '__main__':
    app.run(debug=True)