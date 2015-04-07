#!/usr/bin/env python
import argparse
import replay_log
import os
import operator
import sys
import util

sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cache'))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), 'volley'))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), 'aggregator'))
from ip_location_cache import ip_location_cache
from volley import Volley
from greedy_algo import GreedyReplication
from cache.ip_location_cache import ip_location_cache
from aggregator import Aggregator

SOURCE_INDEX =  2
DESTINATION_INDEX = 4

def update_ip_lat_long_map(ip_lat_long_map_file):
  cache = ip_location_cache()
  
  fd = open(ip_lat_long_map_file, 'r')

  for line in fd:
    ip, lat, lon, city, region, country = line.split('\t')
    cache.add_entry_to_cache(ip, lat, lon, city, region, country)

def run_simulation(request_log_file, enable_concurrency = True, allow_writes = True):
  # replaying request log
  start_time, end_time = replay_log.simulate_requests(request_log_file, enable_concurrency, allow_writes)
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
  return average_latency, start_time, end_time

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--disable-concurrency', action='store_false', help='disable concurrency (no delays on requests)')

  args = vars(parser.parse_args())


  print '************************* Set up simulation environment *************************'
  update_ip_lat_long_map('dataset/synthetic/01_random_replication/ip_lat_long_map.txt')

  print '************************* Running simulation *************************'
  before_volley, before_start_time, before_end_time = run_simulation('dataset/synthetic/01_random_replication/access_log.txt', args['disable_concurrency'])
  Volley(before_start_time, before_end_time).execute()
  after_volley, after_start_time, after_end_time = run_simulation('dataset/synthetic/01_random_replication/access_log.txt', False, False)
  # greedy = GreedyReplication()
  # greedy.run_replication()
  # after_volley, start_time, end_time, request_map = run_simulation('dataset/sample_log_ready', args['disable_concurrency'], request_map)

  print '************************* Average latency ****************************'
  print 'BEFORE_VOLLEY: ' + str(before_volley) + ', start time: ' + str(before_start_time) + ', end time: ' + str(before_end_time)
  print 'AFTER_VOLLEY: ' + str(after_volley) + ', start time: ' + str(after_start_time) + ', end time: ' + str(after_end_time)
