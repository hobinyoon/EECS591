# Test case for evaluator

# Python imports
import sys
import os

sys.path.insert(0, os.path.normpath('..'))

# Project imports
from evaluator import Evaluator


if __name__ == '__main__':
  # log format
  # (timestamp, uuid, source, source_uuid, dest, req_type, status, response_size)
  read_logs = []
  # add faked logs for testing
  read_logs.append(('', '', '54.175.68.60', '', '54.65.80.55', 'READ', '', ''))
  read_logs.append(('', '', '54.175.68.60', '', '54.65.80.55', 'READ', '', ''))
  read_logs.append(('', '', '54.65.80.55', '', '54.175.68.60', 'READ', '', ''))
  read_logs.append(('', '', '54.65.80.55', '', '54.175.68.60', 'READ', '', ''))
  moving_logs = []
  moving_logs.append(('', '', '54.65.80.55', '', '54.175.68.60', 'REPLICATE', '', ''))
  moving_logs.append(('', '', '54.65.80.55', '', '54.175.68.60', 'REPLICATE', '', ''))
  moving_logs.append(('', '', '54.65.80.55', '', '54.175.68.60', 'REPLICATE', '', ''))

  print '********************** Test Evaluator ************************'
  evaluator = Evaluator(111111, 222222)
  avg_latency, traffic = evaluator.evaluate(read_logs, moving_logs)
  assert (avg_latency > 10 and avg_latency < 11), 'Wrong latency'
  assert traffic == 3, 'Wrong inter datacenter traffic'
  print 'average latency: ' + str(avg_latency)
  print 'inter datacenter traffic: ' + str(traffic)

