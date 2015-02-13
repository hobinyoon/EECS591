# Python Library import
import os

# Uses Flask for RESTful API
from flask import Flask, request, send_from_directory
from werkzeug import secure_filename

# Constants
UPLOAD_FOLDER = 'uploaded/'

# Setup for the app
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/')
def hello():
    return 'hello!'

# Endpoint for PUT method
@app.route('/write', methods=['PUT'])
def write_file():
    file = request.files['file']
    if file:
        filename = secure_filename(file.filename)
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        return 'Write success', 201
    return 'Write Failed', 500

# Endpoint for GET method
@app.route('/read', methods=['GET'])
def read_file():
    filename = request.args.get('filename')
    return send_from_directory(UPLOAD_FOLDER, secure_filename(filename))

@app.route('/transfer', methods=['PUT'])
def transfer():
    return 'Transfer'

@app.route('/replicate', methods=['PUT'])
def replicate():
    return 'Replicate'

@app.route('logs', methods=['GET'])
def logs():
    return 'logs'

if __name__ == '__main__':
    app.run(debug=True)
