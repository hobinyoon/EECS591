import argparse
import sys
import requests

# Client argument parsing information.
parser = argparse.ArgumentParser(description='Upload client for EECS591.')
parser.add_argument('--upload', help='Path for a file to upload.')
parser.add_argument('--logs', action='store_true', help='Outputs all logs to stdout.')

args = parser.parse_args()

# Config
server_name = '127.0.0.1'
server_port = '5000'

# Functions
def upload(filename):
  files = { 'file': open(filename, 'rb')  }
  r = requests.post('http://' + server_name + ':' + server_port + '/write', files=files)
  print r.status_code

# Execution
if args.upload is not None:
  upload(args.upload)