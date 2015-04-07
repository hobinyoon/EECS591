# Python Library import
import time

# Project imports
import util

sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), 'aggregator'))

class Evaluator:

  def init(self, start_time, end_time):
    self.set_time(start_time, end_time)
    self.aggregator = Aggregator()

  def set_time(start_time, end_time):
    self.start_time = start_time
    self.end_time = end_time

  def evaluate(self):
    # collect server logs
    read_logs = self.aggregator.get_read_log_entries(self.start_time, self.end_time)
    moving_logs = self.aggregator.get_file_moving_log_entries(self.start_time, self.end_time)
    # calculate the average latency
    latency_sum = 0
    request_count = 0
    ip_cache = ip_location_cache()
    for log in read_logs:
      timestamp, uuid, source, source_uuid, dest, req_type, status, response_size = log
      client_loc = ip_cache.get_lat_lon_from_ip(source)
      server_loc = ip_cache.get_lat_lon_from_ip(dest)
      distance = util.get_distance(client_loc, server_loc)
      unit = 1000.0
      latency = distance / unit
      request_importance = 1
      latency_sum += latency * request_importance
      request_count += request_importance
    average_latency = latency_sum / request_count

    inter_datacenter_traffic = 0
    for log in file_moving_logs:
      timestamp, uuid, source, source_uuid, dest, req_type, status, response_size = log
      # treat all files as uniform size
      inter_datacenter_traffic += 1
    # display latency, cost, etc
    return average_latency, inter_datacenter_traffic
