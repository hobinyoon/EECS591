# Python Library import
import argparse
import json
import socket
import sys
import operator
import os
import os.path
import time
import urllib
import uuid

from ConfigParser import SafeConfigParser

# Uses Flask for RESTful API
import requests

from flask import Flask, g, make_response,  redirect, request, send_from_directory
from werkzeug import secure_filename

# Project imports
import logger
import metadata_manager
import util

# Constants
UPLOAD_FOLDER = 'uploaded/'
SERVER_LIST_FILE = 'servers.txt'
LOG_DIRECTORY = 'logs'
SERVER_CONFIG_FILE = 'server.cnf'

# Setup for the app
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/')
def hello():
    return 'hello!'

@app.route('/redirect')
def redirect_endpoint():
    return redirect('http://localhost:5000/read?uuid=xxx', code=requests.codes.found)

# Endpoint for write method
@app.route('/write', methods=['POST'])
def write_file():
    ip_address = request.args.get('ip') if 'ip' in request.args else request.remote_addr
    file_uuid = request.args.get('uuid') if 'uuid' in request.args else str(uuid.uuid4())
    if 'file' in request.files:
        file = request.files['file']
        metadata = getattr(g, 'metadata', None)
        filename = secure_filename(file.filename)
        if not os.path.isdir(app.config['UPLOAD_FOLDER']):
            os.makedirs(app.config['UPLOAD_FOLDER'])
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], file_uuid)
        file.save(file_path)
        metadata.update_file_stored(file_uuid, app.config['HOST'], get_file_size(file_path))
        host_address = app.config['simulation_ip'] if 'simulation_ip' in app.config else app.config['HOST']
        logger.log(file_uuid, ip_address, 'null', host_address, 'WRITE', requests.codes.created, 0)
        return file_uuid, requests.codes.created
    else:
        host_address = app.config['simulation_ip'] if 'simulation_ip' in app.config else app.config['HOST']
        logger.log('NO_FILE', ip_address, 'null', host_address, 'WRITE', requests.codes.bad_request, -1)
        return 'Write Failed', requests.codes.bad_request

# Endpoint for read method
@app.route('/read', methods=['GET'])
def read_file():
    ip_address = request.args.get('ip') if 'ip' in request.args else request.remote_addr
    metadata = getattr(g, 'metadata', None)
    delay_time = 0 if request.args.get('delay') is None else float(request.args.get('delay'))
    source_uuid = request.args.get('source_uuid') if 'source_uuid' in request.args else 'null'
    filename = request.args.get('uuid')
    host_address = app.config['simulation_ip'] if 'simulation_ip' in app.config else app.config['HOST']

    file_path = os.path.join(UPLOAD_FOLDER, secure_filename(filename))
    if (metadata.is_file_exist_locally(filename, app.config['HOST']) is not None):
        metadata.add_concurrent_request(filename, ip_address)
        if app.config['use_dist_replication']:
            distributed_replication(filename, ip_address, delay_time, metadata)
            # remove the number of concurrent requests to the file
            @after_this_request
            def remove_request(response):
                metadata.remove_concurrent_request(filename, ip_address)
                metadata.close()
        logger.log(filename, ip_address, source_uuid, host_address, 'READ', requests.codes.ok, get_file_size(file_path))
        time.sleep(delay_time)
        return send_from_directory(UPLOAD_FOLDER, secure_filename(filename))

    redirect_address = metadata.lookup_file(filename, app.config['HOST'])
    redirect_url = None
    redirect_args = { 'uuid': filename, 'ip': ip_address, 'delay': delay_time }
    if source_uuid != 'null':
        redirect_args['source_uuid'] = source_uuid
    if (redirect_address is None):
        other_servers = metadata.get_all_server(app.config['HOST'])
        if (len(other_servers) > 0):
            for server in other_servers:
                url = 'http://%s/file_exists?%s' % (server, urllib.urlencode({ 'uuid': filename }))
                lookup_request = requests.get(url)
                if (lookup_request.status_code == requests.codes.ok):
                    redirect_url = 'http://%s/read?%s' % (server, urllib.urlencode(redirect_args))

                    # Update metadata - this might be a little inefficient right now, but want to avoid infinite redirects by
                    # using file_exists
                    update_metadata_from_another_server(server, filename)
    else:
        redirect_url = 'http://%s/read?%s' % (redirect_address, urllib.urlencode(redirect_args))

    if redirect_url is not None:
        logger.log(filename, ip_address, source_uuid, host_address, 'READ', requests.codes.found, -1)
        print 'Redirecting to: ' + redirect_url
        return redirect(redirect_url, code=requests.codes.found)

    logger.log(filename, ip_address, source_uuid, host_address, 'READ', requests.codes.not_found, -1)
    return 'File Not Found', requests.codes.not_found

@app.route('/file_exists', methods=['GET'])
def file_exists():
    filename = request.args.get('uuid')
    file_path = UPLOAD_FOLDER + secure_filename(filename)
    if (os.path.exists(file_path)):
        return app.config['HOST'], requests.codes.ok
    else:
        return 'File not found', requests.codes.not_found

# return a list of files that are stored on this server, seperated by '\n'
@app.route('/local_file_list', methods=['GET'])
def local_file_list():
    server = app.config['HOST']
    file_list = metadata.get_file_list_on_server(server)
    retval = file_list[0]
    for file_uuid in file_list[1:]:
      retval += '\n' + str(file_uuid)
    return retval

# Transfers the file. This API call should not be open to all users.
@app.route('/transfer', methods=['PUT'])
def transfer():
    ip_address = request.args.get('ip') if 'ip' in request.args else request.remote_addr
    file_uuid = request.args.get('uuid')
    destination = request.args.get('destination')
    write_request = clone_file(file_uuid, destination, 'TRANSFER', ip_address)
    if (write_request[1] == requests.codes.created):
        os.remove(os.path.join(app.config['UPLOAD_FOLDER'], file_uuid))
        metadata.delete_file_stored(request.args.get('uuid'), app.config['HOST'])
    return write_request

# Replicate the file. This API call should not be open to all users.
@app.route('/replicate', methods=['PUT'])
def replicate():
    ip_address = request.args.get('ip') if 'ip' in request.args else request.remote_addr
    file_uuid = request.args.get('uuid')
    destination = request.args.get('destination')
    return clone_file(file_uuid, destination, 'REPLICATE', ip_address)

# Deletes the file. This API call should not be open to all users.
@app.route('/delete', methods=['DELETE'])
def delete():
    metadata = getattr(g, 'metadata', None)
    file_uuid = request.args.get('uuid')
    file_path = os.path.join(UPLOAD_FOLDER, file_uuid)
    if (metadata.is_file_exist_locally(file_uuid, app.config['HOST'])):
        os.remove(file_path)
        metadata.delete_file_stored(file_uuid, app.config['HOST'])
        return 'Success', requests.codes.ok
    return 'File not found', requests.codes.not_found

# Returns the log.
@app.route('/logs', methods=['GET'])
def logs():
    if 'date' in request.args:
        date = request.args.get('date')
        file_name = date + '.log'
    else:
        list_of_files = os.listdir(LOG_DIRECTORY)
        list_of_files.sort()
        file_name = list_of_files[0]
    return send_from_directory(LOG_DIRECTORY, secure_filename(file_name))

# Returns whether the server can handle more files.
@app.route('/can_move_file', methods=['GET'])
def can_move_file():
    file_size = float(request.args.get('file_size'))
    storage_limit = app.config['storage_limit']
    current_storage = sum(get_file_size(f) for f in os.listdir(UPLOAD_FOLDER) if os.path.isfile(f))
    space_left = int(storage_limit) - current_storage
    response_message = space_left
    if file_size < space_left:
        return str(response_message), requests.codes.ok
    else:
        return str(response_message), requests.codes.request_entity_too_large

# Returns the capacity of the server
@app.route('/capacity', methods=['GET'])
def capacity():
    return str(app.config['storage_limit']), requests.codes.ok

# Updates and retrieves metadata for a file
@app.route('/metadata', methods=['GET'])
def metadata():
    response = None

    filename = request.args.get('uuid')

    file_info = metadata.is_file_exist_locally(filename, app.config['HOST'])

    if file_info is not None:
        response = { 'uuid': file_info[0], 'server': file_info[1], 'file_size': file_info[2] }
    else:
        server_with_file = metadata.lookup_file(filename, app.config['HOST'])

        if server_with_file is None:
            other_servers = metadata.get_all_server(app.config['HOST'])
            if (len(other_servers) > 0):
                for server in other_servers:
                    url = 'http://%s/file_exists?%s' % (server, urllib.urlencode({ 'uuid': filename }))
                    lookup_request = requests.get(url)
                    if (lookup_request.status_code == requests.codes.ok):
                        server_with_file = server
                        break

        if server_with_file is not None:
            response = update_metadata_from_another_server(server_with_file, filename)

    if response is None:
        return 'File not found', requests.codes.not_found

    return json.dumps(response), requests.codes.ok

# Shuts down the server
@app.route('/shutdown', methods=['GET'])
def shutdown():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()
    return 'Server is shutting down...', requests.codes.ok

###############################################
# Util methods for setting up the request
###############################################

# Connect to the metadata database
@app.before_request
def before_request():
    g.metadata = metadata_manager.MetadataManager()

# Setup the callback method.
@app.after_request
def call_after_request_callbacks(response):
    if hasattr(g, 'after_request_callbacks'):
        for callback in getattr(g, 'after_request_callbacks'):
            callback(response)
    if hasattr(g, 'metadata'):
        g.metadata.close()
        g.metadata = None
    return response

# Helper method for executing function after the request is done.
def after_this_request(f):
    if not hasattr(g, 'after_request_callbacks'):
        g.after_request_callbacks = []
    g.after_request_callbacks.append(f)
    return f

###############################################
#  Helper method section
###############################################

# Update metadata on this server using info from another server
#
# params:
#   server: server to query/ask
#   uuid: uuid to query for
def update_metadata_from_another_server(server, uuid):
    url = 'http://%s/metadata?%s' % (server, urllib.urlencode({ 'uuid': uuid }))
    r = requests.get(url)
    response = json.loads(r.text)
    metadata.update_file_stored(response['uuid'], response['server'], response['file_size'])
    return response

# Get the file size of a file
#
# params:
#   file_path: absolute path to file
def get_file_size(file_path):
    # Here, we would normally run os.stat to retrieve the file size
    # But our emulation is that the file consists of an integer with the file size, so we find that instead.
    file = open(file_path, 'r')
    return int(file.read())

# Helper method for sending a file to another server
#
# params:
#   file_uuid: the file's uuid
#   destination: the destination to clone the file
#   method: the method either REPLICATE or TRANSFER
#   ip_address: the request ip_address
def clone_file(file_uuid, destination, method, ip_address):
    metadata = getattr(g, 'metadata', None)
    file_path = os.path.join(UPLOAD_FOLDER, secure_filename(file_uuid))
    if not os.path.exists(file_path):
        return 'File not found', requests.codes.not_found
    host_address = app.config['simulation_ip'] if 'simulation_ip' in app.config else app.config['HOST']
    destination_with_endpoint = 'http://%s/write?%s' % (destination, urllib.urlencode({ 'uuid': file_uuid, 'ip': host_address }))
    files = {'file': open(file_path, 'rb')}
    write_request = requests.post(destination_with_endpoint, files=files)
    if (write_request.status_code == requests.codes.created):
        metadata.update_file_stored(file_uuid, destination, get_file_size(file_path))
        destination = util.convert_to_simulation_ip(destination)
        logger.log(file_uuid, ip_address, 'null', host_address, method, requests.codes.ok, get_file_size(file_path))
        return 'Success', requests.codes.ok
    else:
        logger.log(file_uuid, ip_address, 'null', host_address, method, requests.codes.internal_server_error, get_file_size(file_path))
        return 'Not okay', requests.codes.internal_server_error

# Helper for distributed replication
#
# params:
#   filename: the filename
#   ip_address: the ip_address of the request
#   metadata: the metadata
def distributed_replication(filename, ip_address, delay_time, metadata):
    metadata.add_concurrent_request(filename, ip_address)
    concurrent_requests = metadata.get_concurrent_request(filename)
    if concurrent_requests is not None:
        # Make sure that the number of concurrent requests is under k.
        # If not, replicate to another server.
        if int(concurrent_requests) >= int(app.config['k']):
            # 1) Find the closest server.
            known_servers = metadata.get_all_server(app.config['HOST'])
            concurrent_connections = metadata.get_concurrent_connections(filename)
            closest_servers = dict()
            for concurrent_connection in concurrent_connections:
                closest_server = util.find_closest_servers_with_ip(concurrent_connection, known_servers)[0]
                if closest_server['server'] not in closest_servers:
                    closest_servers[closest_server['server']] = 1
                else:
                    closest_servers[closest_server['server']] += 1
            # target_server = max(closest_servers)
            target_server = max(closest_servers.iteritems(), key=operator.itemgetter(1))[0]
            target_server = util.convert_to_local_hostname(target_server)
            # 2) Check if there is enough space on the remote server.
            url = 'http://%s/can_move_file?%s' % (target_server, urllib.urlencode({ 'uuid': filename, 'file_size': 0, 'delay': delay_time }))
            response = requests.get(url)
            if response.status_code == requests.codes.ok:
                # 3) Copy the file to that server.
                clone_file(request.args.get('uuid'), target_server, 'DISTRIBUTED_REPLICATE', ip_address)
    else:
        raise Exception('Something fishy is going on... Should have at least one request')

# Entry point for the app
if __name__ == '__main__':
    # Default values
    hostname = 'localhost'
    port = '5000'
    processes = 1
    start_with_debug = False
    server_list = []

    # Argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('serverlist', help='the file containing the host of other servers')
    parser.add_argument('--host', help='the host for the server')
    parser.add_argument('--simulation-ip', help='assigns the simulation-ip to the server')
    parser.add_argument('--port', help='the port for deployment')
    parser.add_argument('--processes', help='specify the number of processes to start the server with')
    parser.add_argument('--with-debug', action='store_true', help='starts the server with debug mode')
    parser.add_argument('--use-dist-replication', action='store_true', help='enables the distributed replication')
    parser.add_argument('--clear-metadata', action='store_true', help='the server should clear the metadata upon starting')

    args = vars(parser.parse_args())
    server_list_file = args['serverlist']
    app.config['use_dist_replication'] = args['use_dist_replication']

    # Populate when there are arguments
    if args['host'] is not None:
        hostname = args['host']
    if args['port'] is not None:
        port = args['port']
    if args['processes'] is not None:
        processes = args['processes']
    if args['with_debug'] is not None:
        start_with_debug = args['with_debug']
    if args['simulation_ip'] is not None:
        app.config['simulation_ip'] = args['simulation_ip']

    # Read the file
    with open(SERVER_LIST_FILE, 'rb') as server_file:
        server_list = server_file.readlines()

    # Update the metadata
    metadata = metadata_manager.MetadataManager()
    current_machine = hostname + ':' + port
    if args['clear_metadata'] is not None and args['clear_metadata']:
        print('Clearing metadata...')
        metadata.clear_metadata() # shouldn't do this!

    # Read configuration file
    parser = SafeConfigParser()
    parser.read(SERVER_CONFIG_FILE)
    app.config['storage_limit'] = parser.get('generic', 'storage_limit')
    if args['use_dist_replication']:
        app.config['k'] = parser.get('distributed_replication_configuration', 'k')
        for server in server_list:
            if server != current_machine:
                # Compute the distance between this server to the other server.
                tokenized_server = server.split(':')
                #distance = util.get_distance(hostname, tokenized_server[0])
                metadata.update_server(server, 0)
    else:
        metadata.update_servers(server_list)

    # Start Flask
    app.config['HOST'] = current_machine # todo: not sure if this is correct.
    print ('Starting server on ' + current_machine + ' with ' + str(processes) + ' processes and debug turned on: ' + str(start_with_debug))
    app.run(host='0.0.0.0', port=int(port), processes=int(processes), debug=start_with_debug)
