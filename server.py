# Python Library import
import sys
import os
import uuid

# Uses Flask for RESTful API
import requests

from flask import Flask, request, send_from_directory
from werkzeug import secure_filename

# Project imports
import util

# Constants
UPLOAD_FOLDER = 'uploaded/'

# Setup for the app
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/')
def hello():
    return 'hello!'

# Endpoint for PUT method
@app.route('/write', methods=['POST'])
def write_file():
    file = request.files['file']
    if file:
        filename = secure_filename(file.filename)
        file_uuid = str(uuid.uuid4())
        if not os.path.isdir(app.config['UPLOAD_FOLDER']):
            os.makedirs(app.config['UPLOAD_FOLDER'])
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], file_uuid))
        return file_uuid, 201
    return 'Write Failed', 500

# Endpoint for GET method
@app.route('/read', methods=['GET'])
def read_file():
    sys.stdout.flush()
    filename = request.args.get('uuid')
    return send_from_directory(UPLOAD_FOLDER, secure_filename(filename))

def move_file(request):
    file_uuid = request.args.get('uuid')
    destination = request.args.get('destination')
    destination_with_endpoint = destination + '/write'
    write_request = util.construct_put_request(destination_with_endpoint, file_uuid)
    return write_request

@app.route('/transfer', methods=['PUT'])
def transfer():
    write_request = move_file(request)
    if write_request.status_code == 201:
        os.remove(os.path.join(app.config['UPLOAD_FOLDER'], file_uuid))
    return write_request

@app.route('/replicate', methods=['PUT'])
def replicate():
    write_request = move_file(request)
    return write_request

@app.route('/logs', methods=['GET'])
def logs():
    return 'logs'

if __name__ == '__main__':
    port = None
    if len(sys.argv) > 1:
        port = sys.argv[1]
    if port == None:
        app.run(host='0.0.0.0', debug=True)
    else:
        app.run(host='0.0.0.0', port=int(port))
