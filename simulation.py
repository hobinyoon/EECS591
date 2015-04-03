#!/usr/bin/env python
import argparse
import replay_log
import os
import operator
import sys
import util

sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), 'volley'))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), 'aggregator'))
from volley import Volley
from greedy_algo import GreedyReplication
from cache.ip_location_cache import ip_location_cache
from aggregator import Aggregator
from evaluator import Evaluator

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--disable-concurrency', action='store_false', help='disable concurrency (no delays on requests)')

  args = vars(parser.parse_args())

  print '************************* Running simulation *************************'
  start_time, end_time, request_map = run_simulation('dataset/sample_log_ready', args['disable_concurrency'], None)
  evaluator = Evaluator(start_time, end_time)
  average_latency_before_volley, inter_datacenter_traffic = evaluator.evaluate()
  time_before_volley = end_time
  Volley(start_time, end_time).execute()
  start_time, end_time, request_map = run_simulation('dataset/sample_log_ready', False, request_map)
  evaluator.set_time(time_before_volley, end_time)
  average_latency_after_volley, inter_datacenter_traffic = evaluator.evaluate()

  # greedy = GreedyReplication()
  # greedy.run_replication()
  # after_volley, start_time, end_time, request_map = run_simulation('dataset/sample_log_ready', args['disable_concurrency'], request_map)

  print '************************* Average latency ****************************'
  print 'BEFORE_VOLLEY: ' + str(average_latency_before_volley)
  print 'AFTER_VOLLEY: ' + str(average_latency_after_volley)
  print '*************** Inter Datacenter Communication Cost ******************'
  print 'Volley: ' + str(inter_datacenter_traffic)
