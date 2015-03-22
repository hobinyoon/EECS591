#!/usr/bin/env python

# Python Library import
import argparser

# Project imports
import replay_log
import util
from cache.ip_location_cache import ip_location_cache

SOURCE_INDEX =  2
DESTINATION_INDEX = 3

def run_simulation(request_log_file, replication_algo, time_interval):
  if not os.path.exists(CLIENT_UPLOAD_FOLDER):
    os.makedirs(CLIENT_UPLOAD_FOLDER)
  if not os.path.exists(CLIENT_DOWNLOAD_FOLDER):
    os.makedirs(CLIENT_DOWNLOAD_FOLDER)
    
  # start simulation 
  fd = open(request_log_file, 'r')
  request_logs = fd.readlines()
  # populate servers with files from request log
  request_to_file_uuid_map = populate_server_with_logs(request_logs)

  # initiate replication algorithm if specified
  algo = None
  if replication_algo == 'volley':
    algo = volley.Volley()
  elif replication_algo == 'greedy':
    pass
  elif replication_algo == 'distributed':
    pass

  performance_counters = []
  rv = None
  while True:
    start_time = time.time()
    # replay request logs
    rv = replay_log.replay_logs(rv, time_interval, request_logs, request_to_file_uuid_map)
    end_time = time.time()
    # evaluate performance for this round
    avg_distance = evaulate(start_time, end_time)
    performance_counters.append(avg_distance)
    # stop if all request logs are consumed
    if rv is None:
      break
    # run replication algorithm if specified
    if replication_algo == 'volley':
      # FIXME (is this the right way to call volley?)
      placements_by_server = algo.execute()
      print '------------------------ Running Volley Algorithm -------------------------'
      for server, placements in placements_by_server.iteritems():
        print server + ": " + str(len(placements))
    elif replication_algo == 'greedy':
      print '------------------------ Running Greedy Algorithm -------------------------'
      algo.run_replication()
    elif replication_algo == 'distributed':
      # FIXME (how to tell server to run distributed algo?)
      print '------------------------ Running Distributed Algorithm -------------------------'

  fd.close()
  return performance_counters

def evaluate(start_time, end_time):
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
  parser = argparser.ArgumentParser()
  parser.add_argument('log-file', help='the dataset that contains logs to replay')
  parser.add_argument('--algorithm', choices=['volley', 'greedy', 'distributed'], help='the algorithm used for replication')
  parser.add_argument('--interval', default=10, help='the time interval of replication algorithm, only for volley and greedy algorithm')
  
  args = vars(parser.parse_args())
  log_file = args['log-file']
  algorithm = args['algorithm']
  interval = args['interval']
  
  print '************************* Running simulation *************************'
  average_latency = run_simulation(log_file, algorithm, interval)
  print '************************* Average latency ****************************'
  print average_latency
