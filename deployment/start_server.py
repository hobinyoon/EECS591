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
    host = parser.get(section, 'host')
    port = parser.get(section, 'deployment_port')
    with open(PREFIX + SERVER_LIST_FILE, 'a') as server_file:
        server_file.write(host + ':' + port + '\n')

for section in parser.sections():
    print 'Deploying ' + section + '...'
    host = parser.get(section, 'host')
    deployment_port = parser.get(section, 'deployment_port')

    username = None
    password = None
    private_key_file = None

    if parser.has_option(section, 'username'):
        username = parser.get(section, 'username')
    if parser.has_option(section, 'password'):
        password = parser.get(section, 'password')
    if parser.has_option(section, 'key_filename'):
        private_key_filename = parser.get(section, 'key_filename')
        private_key_file = paramiko.RSAKey.from_private_key_file(private_key_filename)

    base_directory = parser.get(section, 'directory') # Base directory must exists on the target machine
    application_directory = base_directory  + '/' + PROJECT_NAME
    deployment_directory = application_directory + '/' + section
    ssh.connect(host, username=username, password=password, pkey=private_key_file)

    # Run the server.
    for file in RUN_FILES:
        file_path = application_directory + '/' + section + '/' + file
        run_command = 'nohup python ' + file + ' ' + SERVER_LIST_FILE + ' --host ' + host + ' --port ' + deployment_port + ' &'
        execute_ssh_command(ssh, cd_command + run_command)

    # Finally, close the connection
    ssh.close()
    print 'Done with ' + section + '.'

print 'Deployment succeeded.'
