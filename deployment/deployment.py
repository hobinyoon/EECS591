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
    for line in stdout.readlines():
        sys.stdout.write(line)
    for line in stderr.readlines():
        sys.stdout.write(line)
    return

CONFIG_FILE = 'deployment.cnf'
PREFIX = '../'
FILES = [ 'server.py', 'client.py', 'util.py', 'requirements.txt' ]
RUN_FILES = [ 'server.py' ]
PROJECT_NAME = 'eecs591'

parser = SafeConfigParser()
parser.read(CONFIG_FILE)
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

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
    ssh.connect(host, username=username, password=password, pkey=private_key_file)

    # First, create the directories
    execute_ssh_command(ssh, 'mkdir -p ' + application_directory + '/' + section + '/uploaded')

    # Second, copy the necessary files over to the destination
    for filename in FILES:
        scp_command = 'scp ' + PREFIX + filename + ' ' + username + '@' + host + ':' + application_directory + '/' + section
        print_command(scp_command)
        os.system(scp_command)

    # Third, install appropriate dependencies
    cd_command = 'cd ' + application_directory + '/' + section + '; '
    execute_ssh_command(ssh, cd_command + 'virtualenv venv')
    activate_venv = 'source venv/bin/activate; '
    pip_install = 'pip install -r requirements.txt'
    execute_ssh_command(ssh, cd_command + activate_venv + pip_install)

    # Fourth, run the server
    for file in RUN_FILES:
        file_path = application_directory + '/' + section + '/' + file
        run_command = 'nohup python ' + file + ' ' + deployment_port + ' &'
        execute_ssh_command(ssh, cd_command + run_command)

    # Finally, close the connection
    ssh.close()
    print 'Done with ' + section + '.'

print 'Deployment succeeded.'
