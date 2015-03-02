# Python Library import
import argparse
import socket
import sys
import os
import os.path
import urllib
import uuid

# Uses Flask for RESTful API
import requests

from flask import Flask, g, make_response, redirect, request, send_from_directory
from werkzeug import secure_filename

# Project imports
import logger
import metadata_manager
import util

# Constants
UPLOAD_FOLDER = 'uploaded/'
SERVER_LIST_FILE = 'servers.txt'
LOG_DIRECTORY = 'logs'

# Setup for the app
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/')
def hello():
    return 'hello!'

@app.route('/redirect')
def redirect_endpoint():
    return redirect('http://localhost:5000/read?uuid=xxx', code=302)

# Endpoint for write method
@app.route('/write', methods=['POST'])
def write_file():
    ip_address = request.remote_addr if request.args.get('ip') is None else request.args.get('ip')
    file = request.files['file']
    if file is not None:
        metadata = getattr(g, 'metadata', None)
        filename = secure_filename(file.filename)
        file_uuid = str(uuid.uuid4())
        if not os.path.isdir(app.config['UPLOAD_FOLDER']):
            os.makedirs(app.config['UPLOAD_FOLDER'])
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], file_uuid))
        metadata.update_file_stored(file_uuid, app.config['HOST'])
        logger.log(filename, ip_address, app.config['HOST'], 'WRITE', 201, os.path.getsize(file_path))
        return file_uuid, 201
    logger.log(filename, ip_address, app.config['HOST'], 'WRITE', 400, -1)
    return 'Write Failed', 400

# Endpoint for read method
@app.route('/read', methods=['GET'])
def read_file():
    ip_address = request.remote_addr if request.args.get('ip') is None else request.args.get('ip')
    metadata = getattr(g, 'metadata', None)
    filename = request.args.get('uuid')
    file_path = UPLOAD_FOLDER + '/' + secure_filename(filename)
    if (metadata.is_file_exist_locally(filename, app.config['HOST']) is not None):
        logger.log(filename, ip_address, app.config['HOST'], 'READ', 200, os.path.getsize(file_path))
        return send_from_directory(UPLOAD_FOLDER, secure_filename(filename))

    redirect_address = metadata.lookup_file(filename, app.config['HOST'])
    if (redirect_address is not None):
        url = 'http://%s/read?%s' % (redirect_address[0], urllib.urlencode({ 'uuid': filename }))
        logger.log(filename, ip_address, app.config['HOST'], 'READ', 302, -1)
        return redirect(url, code=302)

    other_servers = metadata.get_all_server(app.config['HOST'])
    if (len(other_servers) > 0):
        for server in other_servers:
            url = 'http://%s/file_exists?%s' % (server, urllib.urlencode({ 'uuid': filename }))
            lookup_request = requests.get(url)
            if (lookup_request.status_code == 200):
                redirection_url = 'http://%s/read?%s' % (server, urllib.urlencode({ 'uuid': filename }))
                metadata.update_file_stored(filename, server)
                logger.log(filename, ip_address, app.config['HOST'], 'READ', 302, -1)
                return redirect(redirection_url, code=302)

    logger.log(filename, ip_address, app.config['HOST'], 'READ', 404, -1)
    return 'File Not Found', 404

@app.route('/file_exists', methods=['GET'])
def file_exists():
    filename = request.args.get('uuid')
    file_path = UPLOAD_FOLDER + secure_filename(filename)
    print file_path
    if (os.path.exists(file_path)):
        return app.config['HOST'], 200
    else:
        return 'File not found', 404

# Helper method for sending a file to another server
def move_file(request, method, ip_address):
    metadata = getattr(g, 'metadata', None)
    file_uuid = request.args.get('uuid')
    file_path = UPLOADED_FOLDER + '/' + file_uuid
    if not os.path.exists(file_path):
        return make_response('File not found', 404)
    destination = request.args.get('destination')
    destination_with_endpoint = destination + '/write'
    files = {'file': open(file_path, 'rb')}
    write_request = requests.post(url, files)
    metadata.update_file_stored(file_uuid, destination)
    logger.log(filename, ip_address, app.config['HOST'], method, write_request.status_code, os.path.getsize(file_path))
    return write_request

# Transfers the file. This API call should not be open to all users.
@app.route('/transfer', methods=['PUT'])
def transfer():
    ip_address = request.remote_addr if request.args.get('ip') is None else request.args.get('ip')
    metadata = getattr(g, 'metadata', None)
    write_request = move_file(request, 'TRANSFER', ip_address)
    if write_request.status_code == 201:
        os.remove(os.path.join(app.config['UPLOAD_FOLDER'], file_uuid))
        metadata.delete_file_stored(request.args.get('uuid'), request.args.get('destination'))
    return write_request

# Replicate the file. This API call should not be open to all users.
@app.route('/replicate', methods=['PUT'])
def replicate():
    ip_address = request.remote_addr if request.args.get('ip') is None else request.args.get('ip')
    write_request = move_file(request, 'REPLICATE', ip_address)
    return write_request

# Deletes the file. This API call should not be open to all users.
@app.route('/delete', methods=['DELETE'])
def delete():
    metadata = getattr(g, 'metadata', None)
    file_uuid = request.args.get('uuid')
    file_path = UPLOAD_FOLDER + '/' + file_uuid
    if (metadata.is_file_exist_locally(file_uuid, app.config['HOST'])):
        os.remove(file_path)
        metadata.delete_file_stored(file_uuid, app.config['HOST'])
        return 'Success', 200
    return 'File not found', 404

# Returns the log.
@app.route('/logs', methods=['GET'])
def logs():
    date = request.args.get('date')
    file_name = date + '.log'
    return send_from_directory(LOG_DIRECTORY, secure_filename(file_name))

# Shuts down the server
@app.route('/shutdown', methods=['GET'])
def shutdown():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()
    return 'Server is shutting down...', 200

# Connect to the metadata database
@app.before_request
def before_request():
    g.metadata = metadata_manager.MetadataManager()

# Close the metadata database connection
@app.teardown_request
def teardown_request(exception):
    metadata = getattr(g, 'metadata', None)
    if metadata is not None:
        metadata.close_connection()

# Entry point for the app
if __name__ == '__main__':
    # Default values
    hostname = '0.0.0.0'
    port = '5000'
    server_list = []

    parser = argparse.ArgumentParser()
    parser.add_argument('serverlist', help='the file containing the host of other servers')
    parser.add_argument('--host', help='the host for the server')
    parser.add_argument('--port', help='the port for deployment')

    args = vars(parser.parse_args())
    server_list_file = args['serverlist']

    # Populate when there are arguments
    if args['host'] is not None:
        hostname = args['host']
    if args['port'] is not None:
        port = args['port']

    # Read the file
    with open(SERVER_LIST_FILE, 'rb') as server_file:
        server_list = server_file.readlines()

    # Update the metadata
    metadata = metadata_manager.MetadataManager()
    metadata.clear_metadata()
    metadata.update_servers(server_list)
    metadata.close_connection()

    # Start Flask
    app.config['HOST'] = hostname + ':' + port # todo: not sure if this is correct.
    app.run(host='0.0.0.0', port=int(port), debug=True)
