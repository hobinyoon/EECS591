# This file implement simplified greedy replication algorithm.

# Python import
import random
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
    self.server_set = set(util.retrieve_server_list()) # [server_ip, ]
    self.content_set = set([])
    self.replica_map = {} # { file_uuid: servers that store the file }
    self.replication_task = []
    self.last_timestamp = 0 # the time stamp of last update
  
  def update(self):
    # update inner data
    self.replica_map = {}
    self.replication_task = []
    # update content_set, replica_map
    for server in self.server_set:
      file_list = util.get_file_list_on_server(server)
      for file_uuid in file_list:
        self.content_set.add(file_uuid)
        if file_uuid not in self.replica_map:
          self.replica_map[file_uuid] = []
        if server not in self.replica_map[file_uuid]:
          self.replica_map[file_uuid].append(server)

    current_timestamp = int(time.time())
    # TODO: implement get_redirect_log_entries
    logs = self.aggregator.get_redirect_log_entries(self.last_timestamp, current_timestamp)
    # used recently generated redirect logs to instruct replication
    for log in logs:
      timestamp, uuid, source, source_uuid, dest, req_type, status, response_size = log
      dest = util.convert_to_local_hostname(dest)
      if (uuid, dest) not in self.replication_task:
        self.replication_task.append((uuid, dest))
    self.last_timestamp = current_timestamp

  def execute(self):
    self.update()
    for file_uuid, target in self.replication_task:
      candidate_servers = replica_map[file_uuid]
      source = util.find_closest_servers_with_ip(target, candidate_servers)[0]
      self.replicate(file_uuid, source, target)

  def replicate(self, content, source, dest):
    print 'Greedy: replicate file %s from %s to %s', (content, source, dest)
    if source == dest:
      if dest not in self.replica_map[content]:
        self.replica_map[content] = 0
      self.replica_map[content][dest] += 1
    else:
      util.replicate(content, source, dest)

