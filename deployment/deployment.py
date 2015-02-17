# import Python Native
import os

from ConfigParser import SafeConfigParser

# import custom libraries
import paramiko

CONFIG_FILE = 'deployment.cnf'
PREFIX = '../'
FILES = [ 'server.py', 'client.py', 'util.py' ]
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
        private_key_file = k = paramiko.RSAKey.from_private_key_file(private_key_filename)

    base_directory = parser.get(section, 'directory') # Base directory must exists on the target machine
    application_directory = base_directory  + '/' + PROJECT_NAME
    ssh.connect(host, username=username, password=password, pkey=private_key_file)

    # First, create the directories
    ssh.exec_command('mkdir -p ' + application_directory + '/' + section + '/uploaded')

    # Second, copy the necessary files over to the destination
    for filename in FILES:
        scp_command = 'scp ' + PREFIX + filename + ' ' + username + '@' + host + ':' + application_directory + '/' + section
        os.system(scp_command)

    # Thrid, run the server
    ssh.exec_command('cd ' + application_directory + '/' + section)
    for file in RUN_FILES:
        file_path = application_directory + '/' + section + '/' + file
        cd_command = 'cd ' + application_directory + '/' + section + '; '
        run_command = 'nohup python ' + file + ' ' + deployment_port + ' &'
        print run_command
        ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cd_command + run_command)

    # Finally, close the connection
    ssh.close()
    print 'Done with ' + section + '.'

print 'Deployment succeeded.'
