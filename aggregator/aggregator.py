# Manages aggregated logs
# It stores the logs in a sqlite database

import argparse
import requests
import datetime
import time
import urllib

# Project imports
import log_manager

# Constants
SECONDS_PER_DAY = 86400
SERVER_LIST_FILE = '../servers.txt'

# Globals
log_mgr = log_manager.LogManager()

def retrieve_server_list():
  with open(SERVER_LIST_FILE, 'rb') as server_file:
    return server_file.read().splitlines() 

def date_to_timestamp(date):
  return int(time.mktime(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple()))

def timestamp_to_date(timestamp):
  return datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')

def request_log_from_server(server, date = None):
  if date is None:
    print "Retrieving earliest log from server <http://" + server + ">"
    url = 'http://%s/logs' % (server,)
  else:
    print "Retrieving logs from server <http://" + server + "> for date: " + date + "..."
    url = 'http://%s/logs?%s' % (server, urllib.urlencode({ 'date': date }))
  print url
  r = requests.get(url)
  if r.status_code == 200:
    return r.text
  else:
    return None

def update_log_from_server(server, start_timestamp):
  # Retrieve first log from server, update, and set next day as start_timestamp  
  if start_timestamp is None:
    first_log = request_log_from_server(server)
    if first_log is None:
      return
    else:
      log_mgr.add_log_entries(first_log)
      start_timestamp = int(first_log.split("\t")[0]) + SECONDS_PER_DAY

  # Find last-aggregated timestamp for this server
  elif start_timestamp == 'update':
    start_timestamp = log_mgr.last_timestamp(server)
  
  now = int(time.time())
  current_timestamp = start_timestamp
  while True:
    date = timestamp_to_date(current_timestamp)
    server_log = request_log_from_server(server, date)
    if server_log is not None:
      log_mgr.add_log_entries(server_log)
    else:
      print 'No entries found for date: %s' % date

    if current_timestamp > now:
      break
    current_timestamp = current_timestamp + SECONDS_PER_DAY

def update_aggregated_logs(start_timestamp = None):
  server_list = retrieve_server_list()
  
  for server in server_list:
    update_log_from_server(server, start_timestamp)

  print 'Updated all logs successfully!'

# Parse arguments for app
parser = argparse.ArgumentParser(description='Aggregator CLI for EECS591.')
parser.add_argument('--update', action='store_true', help='Aggregate logs, beginning from timestamp of last log added to aggregated logs')
parser.add_argument('--time', help='Specify start date in Unix timestamp format.')
parser.add_argument('--date', help='Specify start date in YYYY-MM-DD format.')

args = parser.parse_args()
if args.update is True:
  update_aggregated_logs('update')
elif args.time is not None:
  update_aggregated_logs(args.time)
elif args.date is not None:
  update_aggregated_logs(date_to_timestamp(args.date))
else:
  update_aggregated_logs()
log_mgr.close_connection()