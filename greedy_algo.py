# This file implement simplified greedy replication algorithm.
# Python import
import random
import time

# Project imports
import util
from aggregator.aggregator import Aggregator

def convert_server_to_test_server(server):
  # hardcode AWS servers for simulation
  if server == '54.175.68.60':
    server = 'localhost:5000'
  elif server == '54.65.80.55':
    server = 'localhost:5001'
  elif server == '54.93.104.58':
    server = 'localhost:5002'
  elif server == '54.207.24.208':
    server = 'localhost:5003'
  elif server == '54.69.237.99':
    server = 'localhost:5004'
  return server

class GreedyReplication:

  def __init__(self):
    self.aggregator = Aggregator() # to retrive server logs
    self.client_set = set([]) # [client_ip, ]
    self.server_set = set(util.retrieve_server_list()) # [server_ip, ]
    self.content_set = set([]) # [uuid, ]
    self.access_map = {} # {uuid: {client_ip: num_request}}
    self.replica_map = {} # {uuid: {server_ip: num_replica}}
    self.last_timestamp = 0 # the timestamp of last update
    self.requests_per_replica = 500
    self.uuid_to_server = None
    # self.sample_interval = 1000 # the time interval between two rounds in second

  # update client_set, server_set, content_set, access_info
  # and replication status
  # call this function before running greedy algorithm
  def update(self):
    current_timestamp = int(time.time())
    logs = self.aggregator.get_log_entries(self.last_timestamp, current_timestamp)
    # used recently generated logs to update inner data structure
    for log in logs:
      print '-----------------log---------------'
      print log 
      timestamp, uuid, source, dest, req_type, status, response_size = log
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
      elif req_type == 'READ' and status != '302':
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
      elif req_type == 'DELETE':
        self.replica_map[uuid][source] = 0
      elif req_type == 'REPLICATE':
        if dest not in self.replica_map[uuid]:
          self.replica_map[uuid][dest] = 1
        else:
          # usually we don't write file more than once on a server
          self.replica_map[uuid][dest] += 1
    self.last_timestamp = current_timestamp

  def run_replication(self):
    self.update()
    request_delta = self.requests_per_replica / 10
    replica_delta = 1
    i = 0
    if not self.enough_replica_on_increase(request_delta):
      self.add_replica(request_delta, replica_delta)
    # currently we don't remove any replica
    # else:
      # remove_replica()
      
  # test whether current replicas can handle more requests
  #
  # delta: specify the amount of request increased every time 
  def enough_replica_on_increase(self, delta):
    for c in self.content_set:
      if c in self.access_map:
        for a in self.access_map[c].keys():
          print "content: " + c + " access: " + a
          # add a small amount of requests for content c from client a
          self.access_map[c][a] += delta
          # test whether current replicas can handle that much request
          is_enough = self.enough_replica()
          # back tracking,
          self.access_map[c][a] -= delta
          return is_enough
    return True

  def add_replica(self, request_delta, replica_delta):
    I = []
    for c in self.content_set:
      if c in self.access_map:
        for a in self.access_map[c].keys():
          # add a small amount of requests for content c from client a
          self.access_map[c][a] += request_delta
          # test whether current replicas can handle that much request
          if not self.enough_replica():
            I.append((a,c))
          # back tracking,
          self.access_map[c][a] -= request_delta
    max_satisfied_num = 0
    best_c = None
    best_s = None
    # find the server s to replicate content c so that  
    # maximum number of starved clients can be satisfied
    for a, c in I:
      for s in self.server_set:
        satisfied_num = 0
        self.access_map[c][a] += request_delta
        if s not in self.replica_map[c]:
          self.replica_map[c][s] = 0
        self.replica_map[c][s] += replica_delta
        if self.enough_replica():
          satisfied_num += 1
        self.access_map[c][a] -= request_delta
        self.replica_map[c][s] -= replica_delta
        if self.replica_map[c][s] == 0:
          self.replica_map[c].pop(s)
        if (satisfied_num > max_satisfied_num):
          max_satisfied_num = satisfied_num
          best_c = c
          best_s = s
    if max_satisfied_num > 0:
      source = replica_map[best_c].itervalues().next()
      self.replicate(best_c, source, best_s)
    else:
      # replicate everything
      print 'replicate to all servers'
      for content in self.content_set:
        if not self.enough_replica_for_content(content):
          if content not in self.uuid_to_server:
            continue
          source = self.uuid_to_server[content]
          #select first none zero replica
          for server in self.server_set:
            print "replicate " + "content: " + content + " from: " + source + " to " + server
            util.replicate(content, source, server)

  def enough_replica(self):
    # this is an approximate implementation, may need to
    # construct a bipartite graph and run min matching algo
    for c in self.content_set:
      server_to_request_sum_map = {}
      for a in self.access_map[c].keys():
        nearest_server = convert_server_to_test_server(util.find_closest_servers_with_ip(a, self.server_set)[0]['server'])
        # print "nearest server: " + nearest_server
        if nearest_server not in server_to_request_sum_map:
          server_to_request_sum_map[nearest_server] = 0 
        server_to_request_sum_map[nearest_server] += self.access_map[c][a]
      for server, request_sum in server_to_request_sum_map.iteritems():
        if (c not in self.replica_map) or (server not in self.replica_map[c]):
          return False
        if self.replica_map[c][server] * self.requests_per_replica < request_sum:
          return False
    return True
  
  def enough_replica_for_content(self, c):
    # this is an approximate implementation, may need to
    # construct a bipartite graph and run min matching algo
    server_to_request_sum_map = {}
    for a in self.access_map[c].keys():
      nearest_server = convert_server_to_test_server(util.find_closest_servers_with_ip(a, self.server_set)[0]['server'])
      # print "nearest server: " + nearest_server
      if nearest_server not in server_to_request_sum_map:
        server_to_request_sum_map[nearest_server] = 0 
      server_to_request_sum_map[nearest_server] += self.access_map[c][a]
    for server, request_sum in server_to_request_sum_map.iteritems():
      if (c not in self.replica_map) or (server not in self.replica_map[c]):
        return False
      if self.replica_map[c][server] * self.requests_per_replica < request_sum:
        return False
    return True

  def replicate(self, content, source, dest):
    if source == dest:
      if dest not in self.replica_map[content]:
        self.replica_map[content] = 0
      self.replica_map[content][dest] += 1
    else:
      util.replicate(content, source, dest)

