# Python Library import
import socket
import sys
import os
import os.path
import uuid

# Uses Flask for RESTful API
import requests

from flask import Flask, request, send_from_directory
from werkzeug import secure_filename

# Project imports
import metadata_manager
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
@app.route('/write', methods=['PUT'])
def write_file():
    file = request.files['file']
    if file:
        filename = secure_filename(file.filename)
        file_uuid = str(uuid.uuid4())
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], file_uuid))
        metadata = metadata_manager.MetadataManager()
        metadata.update_file_stored(file_uuid, app.config['HOST'])
        return file_uuid, 201
    return 'Write Failed', 500

# Endpoint for GET method
@app.route('/read', methods=['GET'])
def read_file():
    filename = request.args.get('uuid')
    return send_from_directory(UPLOAD_FOLDER, secure_filename(filename))

# Helper method for sending a file to another server
def move_file(request):
    file_uuid = request.args.get('uuid')
    destination = request.args.get('destination')
    destination_with_endpoint = destination + '/write'
    write_request = util.construct_put_request(destination_with_endpoint, file_uuid)
    metadata = metadata_manager.MetadataManager()
    metadata.update_file_stored(file_uuid, destination)
    return write_request

# Transfers the file. This API call should not be open to all users.
@app.route('/transfer', methods=['PUT'])
def transfer():
    write_request = move_file(request)
    if write_request.status_code == 201:
        os.remove(os.path.join(app.config['UPLOAD_FOLDER'], file_uuid))
        metadata.delete_file_stored(request.args.get('uuid'), request.args.get('destination'))
    return write_request

# Replicate the file. This API call should not be open to all users.
@app.route('/replicate', methods=['PUT'])
def replicate():
    write_request = move_file(request)
    return write_request

# Deletes the file. This API call should not be open to all users.
@app.route('/delete', methods=['DELETE'])
def delete():
    file_uuid = request.args.get('uuid')
    file_path = UPLOAD_FOLDER + '/' + file_uuid
    if (os.path.exists(file_path)):
        os.remove(file_path)
        metadata = metadata_manager.MetadataManager()
        metadata.delete_file_stored(file_uuid, app.config['HOST'])
        return 'Success', 200
    return 'File not found', 404

# Returns the log.
@app.route('/logs', methods=['GET'])
def logs():
    return 'logs'

if __name__ == '__main__':
    hostname = '0.0.0.0'
    port = '5000'
    if len(sys.argv) > 1:
        hostname = sys.argv[1]
        port = sys.argv[2]
    app.config['HOST'] = hostname # todo: not sure if this is correct.
    app.run(host=hostname, port=int(port), debug=True)
