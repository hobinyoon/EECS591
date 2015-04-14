# Python Library import
import time
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), 'aggregator'))

# Project imports
import util
from aggregator import Aggregator
from cache.ip_location_cache import ip_location_cache

class Evaluator:

  def __init__(self, start_time, end_time):
    self.set_time(start_time, end_time)
    self.aggregator = Aggregator()

  def set_time(self, start_time, end_time):
    self.start_time = start_time
    self.end_time = end_time

  # retrieve uuid to servers it's located on
  def uuid_to_locations(self):
    uuid_to_locations = {}

    for server, file_list in util.local_files_per_server().iteritems():
      for uuid in file_list:
        if uuid not in uuid_to_locations:
          uuid_to_locations[uuid] = []

        if server not in uuid_to_locations[uuid]:
          server = util.convert_to_simulation_ip(server)
          uuid_to_locations[uuid].append(server)

    return uuid_to_locations


  # read_logs and moving_logs are always emited unless for testing
  def evaluate(self, read_logs=None, moving_logs=None):
    # collect server logs
    self.aggregator.update_aggregated_logs()

    uuid_to_locations = self.uuid_to_locations()

    if read_logs is None:
      read_logs = self.aggregator.get_read_log_entries(self.start_time, self.end_time)
    if moving_logs is None:
      moving_logs = self.aggregator.get_moving_log_entries(self.start_time, self.end_time)
    # calculate the average latency
    latency_sum = 0
    request_count = 0
    ip_cache = ip_location_cache()
    for log in read_logs:
      timestamp, uuid, source, source_uuid, dest, req_type, status, response_size = log

      if source_uuid is None:
        client_loc = ip_cache.get_lat_lon_from_ip(source)
        min_distance = None
        for dest in uuid_to_locations[uuid]:
          dest_loc = ip_cache.get_lat_lon_from_ip(dest)
          distance = util.get_distance(client_loc, dest_loc)
          if min_distance is None or distance < min_distance:
            min_distance = distance
        distance = min_distance
      else:
        possible_sources = uuid_to_locations[source_uuid]

        min_distance = None
        for dest in uuid_to_locations[uuid]:
          server_loc = ip_cache.get_lat_lon_from_ip(dest)
          for source in possible_sources:
            client_loc = ip_cache.get_lat_lon_from_ip(source)
            distance = util.get_distance(client_loc, server_loc)
            if min_distance is None or distance < min_distance:
              min_distance = distance
          distance = min_distance

      unit = 1000.0
      latency = distance / unit
      request_importance = 1
      latency_sum += latency * request_importance
      request_count += request_importance
    average_latency = latency_sum / request_count

    inter_datacenter_traffic = 0
    for log in moving_logs:
      timestamp, uuid, source, source_uuid, dest, req_type, status, response_size = log
      # treat all files as uniform size
      inter_datacenter_traffic += 1
    # display latency, cost, etc
    return average_latency, inter_datacenter_traffic
