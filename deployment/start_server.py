# import Python Native
import os
import sys

from ConfigParser import SafeConfigParser

# import custom libraries
import paramiko

# convenience functions
def print_command(command):
    print '>> ' + command
def execute_ssh_command(ssh_client, command):
    print_command(command)
    stdin, stdout, stderr = ssh_client.exec_command(command)
    # for line in stdout.readlines():
    #    sys.stdout.write(line)
    # for line in stderr.readlines():
    #    sys.stdout.write(line)
    return

CONFIG_FILE = 'deployment.cnf'
SERVER_LIST_FILE = 'servers.txt'
METADATA_FILE = 'metadata.db'
PREFIX = '../'
RUN_FILES = [ 'server.py' ]
PROJECT_NAME = 'eecs591'

parser = SafeConfigParser()
parser.read(CONFIG_FILE)
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

if (os.path.exists(PREFIX + SERVER_LIST_FILE)):
    os.remove(PREFIX + SERVER_LIST_FILE)

# Infer server names and produce a file containing a list of servers being deployed.
for section in parser.sections():
    host = parser.get(section, 'target_location')
    port = parser.get(section, 'deployment_port')
    with open(PREFIX + SERVER_LIST_FILE, 'a') as server_file:
        server_file.write(host + ':' + port + '\n')

for section in parser.sections():
    print 'Deploying ' + section + '...'
    host = parser.get(section, 'target_location')
    deployment_port = parser.get(section, 'deployment_port')

    username = None
    password = None
    private_key_file = None

    if parser.has_option(section, 'username'):
        username = parser.get(section, 'username')
    if parser.has_option(section, 'password'):
        password = parser.get(section, 'password')
    if parser.has_option(section, 'simulation_ip'):
        simulation_ip = parser.get(section, 'simulation_ip')
    if parser.has_option(section, 'key_filename'):
        private_key_filename = parser.get(section, 'key_filename')
        private_key_file = paramiko.RSAKey.from_private_key_file(private_key_filename)
    if parser.has_option(section, 'processes'):
        processes = parser.get(section, 'processes')
    if parser.has_option(section, 'debug'):
        debug = bool(parser.get(section, 'debug'))
    if parser.has_option(section, 'clear_metadata'):
        clear_metadata = bool(parser.get(section, 'clear_metadata'))
    if parser.has_option(section, 'use_distributed_replication'):
        use_distributed_replication = bool(parser.get(section, 'use_distributed_replication'))

    base_directory = parser.get(section, 'directory') # Base directory must exists on the target machine
    application_directory = base_directory  + '/' + PROJECT_NAME
    deployment_directory = application_directory + '/' + section
    ssh.connect(host, username=username, password=password, pkey=private_key_file)

    # Run the server.
    for file in RUN_FILES:
        file_path = application_directory + '/' + section + '/' + file
        prefix = 'nohup python ' + file + ' ' + SERVER_LIST_FILE + ' --host ' + host + ' --port ' + deployment_port + ' --processes ' + str(processes)
        if debug:
            prefix = prefix + ' --with-debug'
        if clear_metadata:
            prefix = prefix + ' --clear-metadata'
        if use_distributed_replication:
            prefix = prefix + ' --use-dist-replication'
        if simulation_ip is not None:
            prefix = prefix + ' --simulation-ip ' + simulation_ip
        suffix = ' &'
        cd_command = 'cd ' + deployment_directory + '; '
        run_command = prefix + suffix
        execute_ssh_command(ssh, cd_command + run_command)

    # Finally, close the connection
    ssh.close()
    print 'Done with ' + section + '.'

print 'Deployment succeeded.'
