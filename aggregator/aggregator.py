# Manages aggregated logs
# It stores the logs in a sqlite database

import argparse
import requests
import datetime
import os
import sys
import time
import urllib

# Project imports
sys.path.insert(0, os.path.normpath('..'))
import log_manager
import util

class Aggregator:
  # Constants
  SECONDS_PER_DAY = 86400

  def __init__(self):
    self.log_mgr = log_manager.LogManager()

  def date_to_timestamp(self, date):
    return int(time.mktime(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple()))

  def timestamp_to_date(self, timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')

  def request_log_from_server(self, server, date = None):
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

  def update_log_from_server(self, server, start_timestamp):
    if start_timestamp == 'update':
      start_timestamp = self.log_mgr.last_timestamp(server)

    # Retrieve first log from server, update, and set next day as start_timestamp
    if start_timestamp is None:
      first_log = self.request_log_from_server(server)
      if first_log is None:
        return
      else:
        self.log_mgr.add_log_entries(first_log)
        start_timestamp = int(first_log.split("\t")[0])
        print "Updated log from server <http://" + server + "> for date: " + self.timestamp_to_date(start_timestamp) + "."
        start_timestamp += self.SECONDS_PER_DAY
        
    now = int(time.time())
    current_timestamp = start_timestamp
    while True:
      date = self.timestamp_to_date(current_timestamp)
      server_log = self.request_log_from_server(server, date)
      if server_log is not None:
        self.log_mgr.add_log_entries(server_log)
      else:
        print 'No entries found for date: %s' % date

      if current_timestamp > now:
        break
      current_timestamp = current_timestamp + self.SECONDS_PER_DAY

  def update_aggregated_logs(self, start_timestamp = None):
    server_list = util.retrieve_server_list()

    for server in server_list:
      self.update_log_from_server(server, start_timestamp)

    print 'Updated all logs successfully!'

  # Retrive successful log read entries in a specified time period
  #
  # params:
  #   start_timestamp: returned logs start from this integer timestamp
  #   end_timestamp: returned logs end by this integer timestamp
  # return val:
  #   list of tuples
  def get_read_log_entries(self, start_timestamp = None, end_timestamp = None):
    # to get latest logs, update first
    self.update_aggregated_logs('update')
    return self.log_mgr.get_reads(start_timestamp, end_timestamp)

  # Retrive successful log on file movement in a specified time period
  #
  # params:
  #   start_timestamp: returned logs start from this integer timestamp
  #   end_timestamp: returned logs end by this integer timestamp
  # return val:
  #   list of tuples
  def get_moving_log_entries(self, start_timestamp = None, end_timestamp = None):
    # to get latest logs, update first
    self.update_aggregated_logs('update')
    return self.log_mgr.get_movings(start_timestamp, end_timestamp)

  # Retrive log on redirect (read with 302) in a specified time period
  #
  # params:
  #   start_timestamp: returned logs start from this integer timestamp
  #   end_timestamp: returned logs end by this integer timestamp
  # return val:
  #   list of tuples
  def get_redirect_log_entries(self, start_timestamp = None, end_timestamp = None):
    # to get latest logs, update first
    self.update_aggregated_logs('update')
    return self.log_mgr.get_redirects(start_timestamp, end_timestamp)

if __name__ == '__main__':
  # Parse arguments for app
  parser = argparse.ArgumentParser(description='Aggregator CLI for EECS591.')
  parser.add_argument('--update', action='store_true', help='Aggregate logs, beginning from timestamp of last log added to aggregated logs')
  parser.add_argument('--time', help='Specify start date in Unix timestamp format.')
  parser.add_argument('--date', help='Specify start date in YYYY-MM-DD format.')

  args = parser.parse_args()
  aggregator = Aggregator()
  if args.update is True:
    aggregator.update_aggregated_logs('update')
  elif args.time is not None:
    aggregator.update_aggregated_logs(args.time)
  elif args.date is not None:
    aggregator.update_aggregated_logs(aggregator.date_to_timestamp(args.date))
  else:
    aggregator.update_aggregated_logs()
