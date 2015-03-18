#!/usr/bin/env python

import replay_log
import util
from cache.ip_location_cache import ip_location_cache

SOURCE_INDEX =  2
DESTINATION_INDEX = 3

def run_simulation(request_log_file):
  # replaying request log
  start_time, end_time = replay_log.simulate_requests(request_log_file)
  # collect server logs
  logs = util.get_server_logs(start_time, end_time)
  # calculate the average latency
  latency_sum = 0
  request_count = 0
  ip_cache = ip_location_cache()
  for log in logs:
    client_loc = ip_cache.get_lat_lon_from_ip(log[SOURCE_INDEX])
    server_loc = ip_cache.get_lat_lon_from_ip(log[DESTINATION_INDEX])
    distance = util.get_distance(client_loc, server_loc)
    unit = 1000.0
    latency = distance / unit
    request_importance = 1
    latency_sum += latency * request_importance
    request_count += request_importance
  average_latency = latency_sum / request_count
  return average_latency

if __name__ == '__main__':
  print '************************* Running simulation *************************'
  average_latency = run_simulation('sample_log')
  print '************************* Average latency ****************************'
  print average_latency
