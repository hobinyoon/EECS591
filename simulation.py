#!/usr/bin/env python
import argparse
import replay_log
import os
import operator
import sys
import util
sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cache'))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), 'volley'))
from ip_location_cache import ip_location_cache
from volley import Volley
from greedy_algo import GreedyReplication
from aggregator import Aggregator

def update_ip_lat_long_map(ip_lat_long_map_file):
  cache = ip_location_cache()
  
  fd = open(ip_lat_long_map_file, 'r')

  for line in fd:
    ip, lat, lon, city, region, country = line.split('\t')
    cache.add_entry_to_cache(ip, lat, lon, city, region, country)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--disable-concurrency', action='store_false', help='disable concurrency (no delays on requests)')

  args = vars(parser.parse_args())


  print '************************* Set up simulation environment *************************'
  update_ip_lat_long_map('dataset/synthetic/01_random_replication/ip_lat_long_map.txt')

  print '************************* Running simulation *************************'
  before_start_time, before_end_time = replay_log.simulate_requests('dataset/synthetic/01_random_replication/access_log.txt', args['disable_concurrency'])
  evaluator = Evaluator(before_start_time, before_end_time)
  average_latency_before_volley, _ = evaluator.evaluate()
  Volley(before_start_time, before_end_time).execute()
  after_start_time, after_end_time = replay_log.simulate_requests('dataset/synthetic/01_random_replication/access_log.txt', False, False)
  evaluator.set_time(before_end_time, after_end_time)
  average_latency_after_volley, inter_datacenter_traffic = evaluator.evaluate()

  # greedy = GreedyReplication()
  # greedy.run_replication()
  # after_volley, start_time, end_time, request_map = replay_log.simulate_requests('dataset/sample_log_ready', args['disable_concurrency'], request_map)

  print '************************* Average latency ****************************'
  print 'BEFORE_VOLLEY: ' + str(average_latency_before_volley) + ', start time: ' + str(before_start_time) + ', end time: ' + str(before_end_time)
  print 'AFTER_VOLLEY: ' + str(average_latency_after_volley) + ', start time: ' + str(after_start_time) + ', end time: ' + str(after_end_time)
  print '*************** Inter Datacenter Communication Cost ******************'
  print 'Volley: ' + str(inter_datacenter_traffic)
