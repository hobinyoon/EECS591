#!/usr/bin/env python
import time
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
from revenge_of_volley import RevengeOfVolley
from greedy_algo import GreedyReplication
from central_greedy import SimpleCentralizedGreedy
from aggregator import Aggregator
from evaluator import Evaluator

def update_ip_lat_long_map(ip_lat_long_map_file):
  cache = ip_location_cache()

  fd = open(ip_lat_long_map_file, 'r')

  for line in fd:
    ip, lat, lon, city, region, country = line.split('\t')
    cache.add_entry_to_cache(ip, lat, lon, city, region, country)

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--disable-concurrency', action='store_false', help='disable concurrency (no delays on requests)')
  parser.add_argument('--algorithm', choices=['volley', 'greedy', 'distributed', 'rov'], help='the algorithm used for replication', required=True)
  parser.add_argument('--dataset', choices=['1', '2', '3', 'twitter'], help='choices for choosing the dataset', required=True)

  args = vars(parser.parse_args())
  algorithm = args['algorithm']
  dataset_name = None
  if args['dataset'] == '1' or args['dataset'] == '2' or args['dataset'] == '3':
    if args['dataset'] == '1':
      dataset_name = '01_random_replication'
    elif args['dataset'] == '2':
      dataset_name = '02_replication_effects'
    elif args['dataset'] == '3':
      dataset_name = '03_real_time_algorithm'
    ip_lat_long_map_filename = 'dataset/synthetic/' + dataset_name + '/ip_lat_long_map.txt'
    access_log_filename = 'dataset/synthetic/' + dataset_name + '/access_log.txt'
  elif args['dataset'] == 'twitter':
    ip_lat_long_map_filename = 'dataset/twitter/ip_lat_long_map.txt'
    access_log_filename = 'dataset/twitter/access_log.txt'

  if algorithm == 'volley' or algorithm == 'greedy' or algorithm == 'rov':
    print '************************* Set up simulation environment *************************'
    update_ip_lat_long_map(ip_lat_long_map_filename)

    print '************************* Running simulation on ' + algorithm + ' *************************'
    before_start_time, before_end_time = replay_log.simulate_requests(access_log_filename, args['disable_concurrency'])
    evaluator = Evaluator(before_start_time, before_end_time)
    average_latency_before, _ = evaluator.evaluate()

    if algorithm == 'volley':
        Volley(before_start_time, before_end_time).execute()
    elif algorithm == 'rov':
        RevengeOfVolley(before_start_time, before_end_time).execute()
    elif algorithm == 'greedy':
        greedy = SimpleCentralizedGreedy()
        greedy.last_timestamp = before_start_time
        greedy.execute()

    after_start_time, after_end_time = replay_log.simulate_requests(access_log_filename, False, False)
    evaluator.set_time(before_end_time, after_end_time)
    average_latency_after, inter_datacenter_traffic = evaluator.evaluate()

    print '************************* Average latency ****************************'
    print 'BEFORE: ' + str(average_latency_before) + ', start time: ' + str(before_start_time) + ', end time: ' + str(before_end_time)
    print 'AFTER: ' + str(average_latency_after) + ', start time: ' + str(after_start_time) + ', end time: ' + str(after_end_time)
    print '*************** Inter Datacenter Communication Cost ******************'
    print algorithm + ': ' + str(inter_datacenter_traffic)
  elif algorithm == 'distributed':
    update_ip_lat_long_map(ip_lat_long_map_filename)
    before_start_time, before_end_time = replay_log.simulate_requests(access_log_filename, args['disable_concurrency'])
    evaluator = Evaluator(before_start_time, before_end_time)
    average_latency_before_volley, inter_datacenter_traffic = evaluator.evaluate()
    print '************************* Average latency ****************************'
    print 'DISTRIBUTED: ' + str(average_latency_before_volley) + ', start time: ' + str(before_start_time) + ', end time: ' + str(before_end_time)
    print '*************** Inter Datacenter Communication Cost ******************'
    print 'DISTRIBUTED: ' + str(inter_datacenter_traffic)
