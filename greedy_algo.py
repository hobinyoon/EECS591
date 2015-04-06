# This file implement simplified greedy replication algorithm.

# Python import
import os
import sys
import time

# Project imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), 'aggregator'))
import util
from aggregator import Aggregator

class GreedyReplication:

  def __init__(self):
    self.aggregator = Aggregator() # to retrive server logs
    self.client_set = set([]) # [client_ip, ]
    self.server_set = set(self.aggregator.retrieve_server_set()) # [server_ip, ]
    self.content_set = set([]) # [uuid, ]
    self.access_map = {} # {uuid: {client_ip: num_request}}
    self.replica_map = {} # {uuid: {server_ip: num_replica}}
    self.last_timestamp = 0 # the timestamp of last update
    self.sample_interval = 1000 # the time interval between two rounds in second

  # update client_set, server_set, content_set, access_info
  # and replication status
  # call this function before running greedy algorithm
  def update(self):
    current_timestamp = int(time.time())
    logs = self.aggregator.get_read_log_entries(last_timestamp, current_timestamp)
    # used recently generated logs to update inner data structure
    for log in logs:
      timestamp, uuid, source, source_uuid, dest, req_type, status, response_size = log.split()
      self.content_set.add(uuid)
      if uuid not in self.access_map:
        self.access_map[uuid] = {}
      if uuid not in self.replica_map:
        self.replica_map[uuid] = {}
      self.client_set.add(source)
      if req_type == 'WRITE':
        if dest not in self.replica_map[uuid]:
          self.replica_map[uuid][dest] = 1
        else:
          # usually we don't write file more than once on a server
          self.replica_map[uuid][dest] += 1
      elif req_type == 'READ':
        if source not in self.access_map[uuid]:
          self.access_map[uuid][source] = 1
        else:
          self.access_map[uuid][source] += 1
      elif req_type == 'TRANSFER':
        if dest not in self.replica_map[uuid]:
          self.replica_map[uuid][dest] = 1
        else:
          # usually we don't write file more than once on a server
          self.replica_map[uuid][dest] += 1
        self.replica_map[uuid][source] = 0
      elif req_type == 'TRANSFER' or req_type == 'DELETE':
        self.replica_map[uuid][source] = 0
    self.last_timestamp = current_timestamp

  def run_replication(self):
    self.update()
    if not self.enough_replica_on_increase():
      self.add_replica()
    # currently we don't remove any replica
    # else:
      # remove_replica()
      
  # test whether current replicas can handle more requests
  #
  # delta: specify the amount of request increased every time 
  def enough_replica_on_increase(self, delta):
    for c in self.content_set:
      for a in self.client_set:
        # add a small amount of requests for content c from client a
        self.access_map[c][a] += delta
        # test whether current replicas can handle that much request
        is_enough = self.enough_replica()
        # back tracking,
        self.access_map[c][a] -= delta
        return is_enough

  def add_replica(self, request_delta, replica_delta):
    I = []
    for c in self.content_set:
      for a in self.client_set:
        # add a small amount of requests for content c from client a
        self.access_map[c][a] += delta
        # test whether current replicas can handle that much request
        if not self.enough_replica():
          I.append((a,c))
        # back tracking,
        self.access_map[c][a] -= delta
    max_satisfied_num = 0
    best_c = None
    best_s = None
    # find the server s to replicate content c so that  
    # maximum number of starved clients can be satisfied
    for c in self.content_set:
      for s in self.server_set:
        satisfied_num = 0
        for a, I_c in I:
          if I_c == c:
            self.access_map[c][a] += request_delta
            self.replica_map[c][s] += replica_delta
            if self.enough_replica():
              satisfied_num += 1
            self.access_map[c][a] -= request_delta
            self.replica_map[c][a] -= replica_delta
        if (satisfied_num > max_satisfied_num):
          max_satisfied_num = satisfied_num
          best_c = c
          best_s = s
    if max_satisfied_num > 0:
      source = replica_map[best_c].itervalues().next()
      util.replicate(best_c, source, best_s)

  def enough_replica(self):
    # this is an approximate implementation, may need to
    # construct a bipartite graph and run min matching algo
    for c in self.content_set:
      request_sum = 0
      for a in self.client_set:
        request_sum += self.access_map[c][a]
      replica_sum = 0 # total amount of requests can be handled by replicas
      for s in self.server_set:
        replica_sum += self.replica_map[c][s]
      if request_sum > replica_sum:
        return False
    return True
