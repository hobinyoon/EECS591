# Manages aggregated logs
# It stores the logs in a sqlite database

import requests
import datetime
import time

# Project imports
import log_manager

# Constants
SECONDS_PER_DAY = 86400
SERVER_LIST_FILE = '../servers.txt'

# Globals
log_mgr = log_manager.LogManager()

def retrieve_server_list():
  with open(SERVER_LIST_FILE, 'rb') as server_file:
    return server_file.readlines()

def timestamp_to_date(timestamp):
  return datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')

def request_log_from_server(server, date):
  print "Retrieving logs from server <http://" + server + "> for date: " + date + "..."
  url = 'http://%s/logs?%s' % (server, urllib.urlencode({ 'date': date }))
  r = requests.get(url)
  if r.status_code == 200
    return r.text
  else
    return None

def update_log_from_server(server, start_timestamp, end_timestamp):
  print "Updating logs from server <http://" + server + ">..."
  current_timestamp = start_timestamp
  while current_timestamp < end_timestamp
    date = timestamp_to_date(current_timestamp)
    server_log = request_log_from_server(server, date)
    if server_log != None
      log_mgr.add_log_entries(server_log)
    current_timestamp = current_timestamp + SECONDS_PER_DAY

def update_aggregated_logs():
  log_mgr.last_timestamp()
  now = int(time.time())

  server_list = retrieve_server_list()
  for server in server_list:
    update_log_from_server(server, start_timestamp, end_timestamp)

  print 'Updated all logs up to timestamp ' + now + ' successfully...'

# Entry point for the app
if __name__ == '__main__':
  update_aggregated_logs()
  log_mgr.close_connection()