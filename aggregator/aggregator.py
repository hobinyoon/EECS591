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

def request_log_from_server(server, date):
  print "Retrieving logs from server <http://" + server + "> for date: " + date + "..."
  url = 'http://%s/logs?%s' % (server, urllib.urlencode({ 'date': date }))
  print url
  r = requests.get(url)
  if r.status_code == 200:
    return r.text
  else:
    return None

def update_log_from_server(server, start_timestamp, end_timestamp):
  print "Updating logs from server <http://" + server + "> from " + str(start_timestamp) + " to " + str(end_timestamp) + "..."
  current_timestamp = start_timestamp
  while current_timestamp < end_timestamp:
    date = timestamp_to_date(current_timestamp)
    server_log = request_log_from_server(server, date)
    if server_log is not None:
      log_mgr.add_log_entries(server_log)
    current_timestamp = current_timestamp + SECONDS_PER_DAY

def update_aggregated_logs(start_timestamp = None):
  if start_timestamp is None:
    start_timestamp = log_mgr.last_timestamp()
  now = int(time.time())

  server_list = retrieve_server_list()
  for server in server_list:
    update_log_from_server(server, start_timestamp, now)

  print 'Updated all logs up to timestamp ' + str(now) + ' successfully!'

# Parse arguments for app
parser = argparse.ArgumentParser(description='Aggregator CLI for EECS591.')
parser.add_argument('--time', help='Specify start date in Unix timestamp format.')
parser.add_argument('--date', help='Specify start date in YYYY-MM-DD format.')

args = parser.parse_args()
if args.time is not None:
  update_aggregated_logs(args.time)
elif args.date is not None:
  update_aggregated_logs(date_to_timestamp(args.date))
else:
  update_aggregated_logs()
log_mgr.close_connection()