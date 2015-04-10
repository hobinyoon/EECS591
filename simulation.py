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
from greedy_algo import GreedyReplication
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
  parser.add_argument('--algorithm', choices=['volley', 'greedy', 'distributed'], help='the algorithm used for replication')

  args = vars(parser.parse_args())
  algorithm = args['algorithm']

  if algorithm == 'volley':
    print '************************* Set up simulation environment *************************'
    #update_ip_lat_long_map('dataset/synthetic/01_random_replication/ip_lat_long_map.txt')
    update_ip_lat_long_map('dataset/synthetic/02_replication_effects/ip_lat_long_map.txt')
    #update_ip_lat_long_map('dataset/synthetic/03_real_time_algorithm/ip_lat_long_map.txt')

    print '************************* Running simulation *************************'
    #before_start_time, before_end_time = replay_log.simulate_requests('dataset/synthetic/01_random_replication/access_log.txt', args['disable_concurrency'])
    before_start_time, before_end_time = replay_log.simulate_requests('dataset/synthetic/02_replication_effects/access_log.txt', args['disable_concurrency'])
    #before_start_time, before_end_time = replay_log.simulate_requests('dataset/synthetic/03_real_time_algorithm/access_log.txt', args['disable_concurrency'])
    evaluator = Evaluator(before_start_time, before_end_time)
    average_latency_before_volley, _ = evaluator.evaluate()
    Volley(before_start_time, before_end_time).execute()

    #after_start_time, after_end_time = replay_log.simulate_requests('dataset/synthetic/01_random_replication/access_log.txt', False, False)
    after_start_time, after_end_time = replay_log.simulate_requests('dataset/synthetic/02_replication_effects/access_log.txt', False, False)
    #after_start_time, after_end_time = replay_log.simulate_requests('dataset/synthetic/03_real_time_algorithm/access_log.txt', False, False)
    evaluator.set_time(before_end_time, after_end_time)
    average_latency_after_volley, inter_datacenter_traffic = evaluator.evaluate()

    print '************************* Average latency ****************************'
    print 'BEFORE_VOLLEY: ' + str(average_latency_before_volley) + ', start time: ' + str(before_start_time) + ', end time: ' + str(before_end_time)
    print 'AFTER_VOLLEY: ' + str(average_latency_after_volley) + ', start time: ' + str(after_start_time) + ', end time: ' + str(after_end_time)
    print '*************** Inter Datacenter Communication Cost ******************'
    print 'Volley: ' + str(inter_datacenter_traffic)
  elif algorithm == 'greedy':
    print '************************* Set up simulation environment *************************'
    update_ip_lat_long_map('dataset/synthetic/01_random_replication/ip_lat_long_map.txt')
    # update_ip_lat_long_map('dataset/synthetic/02_replication_effects/ip_lat_long_map.txt')
    # update_ip_lat_long_map('dataset/synthetic/03_real_time_algorithm/ip_lat_long_map.txt')

    print '************************* Running simulation *************************'
    before_start_time, before_end_time = replay_log.simulate_requests('dataset/synthetic/01_random_replication/access_log.txt', args['disable_concurrency'])
    # before_start_time, before_end_time = replay_log.simulate_requests('dataset/synthetic/02_replication_effects/access_log.txt', args['disable_concurrency'])
    # before_start_time, before_end_time = replay_log.simulate_requests('dataset/synthetic/03_real_time_algorithm/access_log.txt', args['disable_concurrency'])
    evaluator = Evaluator(before_start_time, before_end_time)
    average_latency_before_greedy, _ = evaluator.evaluate()
    greedy = GreedyReplication()
    greedy.last_timestamp = before_end_time
    greedy.run_replication()

    after_start_time, after_end_time = replay_log.simulate_requests('dataset/synthetic/01_random_replication/access_log.txt', False, False)
    # after_start_time, after_end_time = replay_log.simulate_requests('dataset/synthetic/02_replication_effects/access_log.txt', False, False)
    # after_start_time, after_end_time = replay_log.simulate_requests('dataset/synthetic/03_real_time_algorithm/access_log.txt', True, False)
    evaluator.set_time(before_end_time, after_end_time)
    average_latency_after_greedy, inter_datacenter_traffic = evaluator.evaluate()

    print '************************* Average latency ****************************'
    print 'BEFORE_GREEDY: ' + str(average_latency_before_greedy) + ', start time: ' + str(before_start_time) + ', end time: ' + str(before_end_time)
    print 'AFTER_GREEDY: ' + str(average_latency_after_greedy) + ', start time: ' + str(after_start_time) + ', end time: ' + str(after_end_time)
    print '*************** Inter Datacenter Communication Cost ******************'
    print 'Greedy: ' + str(inter_datacenter_traffic)
  elif algorithm == 'distributed':
    update_ip_lat_long_map('dataset/synthetic/03_real_time_algorithm/ip_lat_long_map.txt')
    before_start_time, before_end_time = replay_log.simulate_requests('dataset/synthetic/03_real_time_algorithm/access_log.txt', args['disable_concurrency'])
    evaluator = Evaluator(before_start_time, before_end_time)
    average_latency_before_volley, inter_datacenter_traffic = evaluator.evaluate()
    print '************************* Average latency ****************************'
    print 'DISTRIBUTED: ' + str(average_latency_before_volley) + ', start time: ' + str(before_start_time) + ', end time: ' + str(before_end_time)
    print '*************** Inter Datacenter Communication Cost ******************'
    print 'DISTRIBUTED: ' + str(inter_datacenter_traffic)
