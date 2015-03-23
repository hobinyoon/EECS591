#!/usr/bin/env python
import argparse
import replay_log
import os
import operator
import sys
import util

sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), 'volley'))
from volley import Volley
from greedy_algo import GreedyReplication
from cache.ip_location_cache import ip_location_cache
from aggregator.aggregator import Aggregator

SOURCE_INDEX =  2
DESTINATION_INDEX = 3

def run_simulation(request_log_file, enable_concurrency = True, request_map = None):
  # replaying request log
  start_time, end_time, request_map = replay_log.simulate_requests(request_log_file, enable_concurrency, request_map)
  # collect server logs
  aggregator = Aggregator()
  logs = aggregator.get_log_entries(start_time, end_time)

  # calculate the average latency
  latency_sum = 0
  request_count = 0
  ip_cache = ip_location_cache()
  for log in logs:
    client_loc = ip_cache.get_lat_lon_from_ip(log[SOURCE_INDEX])
    server_loc = ip_cache.get_lat_lon_from_ip(log[DESTINATION_INDEX])
    if client_loc is not None and server_loc is not None:
        distance = util.get_distance(client_loc, server_loc)
        unit = 1000.0
        latency = distance / unit
        request_importance = 1
        latency_sum += latency * request_importance
        request_count += request_importance
  average_latency = latency_sum / request_count
  return average_latency, start_time, end_time, request_map

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--disable-concurrency', action='store_false', help='disable concurrency (no delays on requests)')

  args = vars(parser.parse_args())

  print '************************* Running simulation *************************'
  before_volley, start_time, end_time, request_map = run_simulation('dataset/sample_log_ready', args['disable_concurrency'])
  Volley(start_time, end_time).execute()
  after_volley, start_time, end_time, request_map = run_simulation('dataset/sample_log_ready', False, request_map)
  # greedy = GreedyReplication()
  # greedy.run_replication()
  # after_volley, start_time, end_time, request_map = run_simulation('dataset/sample_log_ready', args['disable_concurrency'], request_map)

  print '************************* Average latency ****************************'
  print 'BEFORE_VOLLEY: ' + str(before_volley)
  print 'AFTER_VOLLEY: ' + str(after_volley)
