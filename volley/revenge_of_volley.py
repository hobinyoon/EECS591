# Implementation of Volley
import geopy
import json
import math
import operator
import os
import requests
import sqlite3
import sys
import time
import urllib

from geopy.distance import great_circle

# Project Imports
up_one_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')
sys.path.insert(0, up_one_dir)
sys.path.insert(0, os.path.join(up_one_dir, 'cache'))
sys.path.insert(0, os.path.join(up_one_dir, 'aggregator'))
import ip_location_cache
from log_manager import LogManager
import util

# Configurable Constants
ITERATIONS = 5
KAPPA = 5
GV_RATIO_THRESHOLD = 1.1

class RevengeOfVolley:

  def __init__(self, start_time = 0, end_time = int(time.time())):
    self.log_manager = LogManager(start_time, end_time)
    self.ip_cache = ip_location_cache.ip_location_cache()

    self.servers = []
    for server in util.retrieve_server_list():
      self.servers.append(util.convert_to_simulation_ip(server))

    self.uuid_to_locations = {}       # dictionary mapping uuid -> optimal locations
    self.uuid_to_servers = {}         # dictionary mapping uuid -> server locations
    self.uuid_to_clients = {}         # dictionary mapping uuid -> set([{ location: (lat, long), request_count: 5 }])
    self.uuid_to_server_ranking = {}  # dictionary mapping uuid -> list of servers (most requests made from locations closest to that server first)
    self.uuid_metadata = {}           # dictionary mapping uuid -> metadata (includes state-ful data)
    self.placements_by_server = {}    # dictionary mapping server -> uuids

  # Execute Volley algorithm
  def execute(self):
    self.place_initial()
    self.reduce_latency()
    self.collapse_to_datacenters()
    self.migrate_to_locations()
    print 'Revenge of Volley execution complete!'

  # PHASE 1: Compute Initial Placement
  def place_initial(self):
    uuids = self.log_manager.get_unique_uuids()

    for uuid in uuids:
      self.uuid_to_locations[uuid] = self.find_centroids(uuid)

  # PHASE 2: Iteratively Move Data to Reduce Latency
  def reduce_latency(self):
    for i in range(ITERATIONS):
      for uuid, locations in self.uuid_to_locations.iteritems():
        uuid_to_interdependencies = self.log_manager.get_interdependency_grouped_by_uuid(uuid)

        for tuple in uuid_to_interdependencies:
          other_item_uuid = tuple[0]
          if other_item_uuid not in self.uuid_to_locations:
            continue
          other_item_locations = self.uuid_to_locations[other_item_uuid]
          request_count = tuple[1]

          # find closest location pair
          min_distance = None
          location_pair = None
          selected_location_index = None

          for location_index, location in enumerate(locations):
            for other_item_location in other_item_locations:
              distance = util.get_distance(location, other_item_location)
              if min_distance is None or distance < min_distance:
                min_distance = distance
                location_pair = (location, other_item_location)
                selected_location_index = location_index

          weight = 1 / (1 + (KAPPA * distance * request_count))
          updated_location = self.interp(weight, location_pair[0], location_pair[1])
          self.uuid_to_locations[uuid][selected_location_index] = updated_location

        for client_ip, client_info in self.uuid_to_clients[uuid].iteritems():
          client_location = client_info['location']
          request_count = client_info['request_count']
          distance = util.get_distance(location, client_location)
          weight = 1 / (1 + (KAPPA * distance * request_count))

          # find closest location pair
          min_distance = None
          location_pair = None
          selected_location_index = None

          for location_index, location in enumerate(locations):
            distance = util.get_distance(location, other_item_location)
            if min_distance is None or distance < min_distance:
              min_distance = distance
              location_pair = (location, client_location)
              selected_location_index = location_index

          weight = 1 / (1 + (KAPPA * distance * request_count))
          updated_location = self.interp(weight, location_pair[0], location_pair[1])
          self.uuid_to_locations[uuid][selected_location_index] = updated_location

  # PHASE 3: Iteratively Collapse Data to Datacenters
  def collapse_to_datacenters(self):
    for server in self.servers:
      self.placements_by_server[server] = []

    for uuid, locations in self.uuid_to_locations.iteritems():
      metadata = {'current_server': None, 'optimal_locations': locations, 'uuid': uuid, 'dist': None, 'file_size': None, 'request_count': None}
      best_server = None

      # Query any server for metadata - server will update and get information
      any_server = util.convert_to_local_hostname(self.servers[0])
      url = 'http://%s/metadata?%s' % (any_server, urllib.urlencode({ 'uuid': uuid }))
      print url
      r = requests.get(url)
      print r.text
      response = json.loads(r.text)
      metadata['current_server'] = response['server']
      metadata['file_size'] = response['file_size']
      metadata['request_count'] = self.log_manager.successful_read_count(uuid)

      remaining_server_list = list(self.servers)
      result_server_list = []

      for location in locations:
        best_servers = self.find_closest_servers(location, remaining_server_list)
        result_server_list.append(best_servers.pop(0)['server'])

      self.uuid_metadata[uuid] = metadata
      for result_server in result_server_list:
        self.placements_by_server[result_server].append(uuid)
        if uuid not in self.uuid_to_servers:
          self.uuid_to_servers[uuid] = set()
        self.uuid_to_servers[uuid].add(result_server)

    self.redistribute_server_data_by_capacity()

  # PHASE 4: Call migration methods on each server
  def migrate_to_locations(self):
    for optimal_server, uuids in self.placements_by_server.iteritems():
      for uuid in uuids:
        current_server = self.uuid_metadata[uuid]['current_server']

        # convert to local hostname in case of simulation
        current_server = util.convert_to_local_hostname(current_server)
        optimal_server = util.convert_to_local_hostname(optimal_server)

        transfer = False

        if optimal_server != current_server:
          if util.convert_to_simulation_ip(current_server) in self.uuid_to_servers[uuid]:
            url = 'http://%s/replicate?%s' % (current_server, urllib.urlencode({ 'uuid': uuid, 'destination': optimal_server }))
          else:
            url = 'http://%s/transfer?%s' % (current_server, urllib.urlencode({ 'uuid': uuid, 'destination': optimal_server }))
            transfer = True
          print url
          r = requests.put(url, timeout=30)
          if r.status_code == requests.codes.ok:
            print 'SUCCESS: Migrating ' + uuid + ' from <' + current_server + '> to <' + optimal_server + '>'
            if transfer:
              self.uuid_metadata[uuid]['current_server'] = optimal_server
          else:
            raise Exception('FAILED: Migrating ' + uuid + ' from <' + current_server + '> to <' + optimal_server + '>')

  # Gets sort key for sort function to sort by ascending request_count
  def get_sort_key_by_request_count(self, uuid):
    return self.uuid_metadata[uuid]['request_count']

  # Gets sort key for sort function to sort by ascending distance
  def get_distance_key(self, server_dict):
    return server_dict['distance']

  # Finds the closest server for a lat/long tuple pair
  #
  # params:
  #   location: lat/long tuple
  #   servers_to_search: a list of servers to search. Uses self.servers by default.
  # returns: list of closest to furthest, where each item is a dict with `server` and `distance`
  def find_closest_servers(self, location, servers_to_search = None):
    if servers_to_search is None:
      servers_to_search = self.servers

    best_servers = []

    for server in servers_to_search:
      server_dict = { 'server': server, 'distance': None }
      item_location = geopy.Point(location[0], location[1])
      server_lat_lon = self.ip_cache.get_lat_lon_from_ip(server)
      if server_lat_lon is None:
        raise ValueError('Server <' + server + '> latitude/longitude could not be found!')
      server_location = geopy.Point(server_lat_lon[0], server_lat_lon[1])
      server_dict['distance'] = great_circle(item_location, server_location).km

      best_servers.append(server_dict)

    best_servers.sort(key=self.get_distance_key)

    return best_servers

  # Check capacity of server
  #
  # params:
  #   server: hostname of server to check
  def total_server_capacity(self, server):
    server = util.convert_to_local_hostname(server)

    url = 'http://%s/capacity?%s' % (server, urllib.urlencode({ 'file_size': 0 }))
    r = requests.get(url, timeout=30)
    return float(r.text)

  # Check capacity and redistribute data to each server
  def redistribute_server_data_by_capacity(self):
    space_remaining = {}
    servers_with_capacity = set()
    servers_over_capacity = set()

    for server in self.servers:
      space_remaining[server] = self.total_server_capacity(server)

      placements = self.placements_by_server[server]

      for uuid in placements:
        metadata = self.uuid_metadata[uuid]
        space_remaining[server] -= metadata['file_size']

      if space_remaining[server] > 0:
        servers_with_capacity.add(server)
      else:
        servers_over_capacity.add(server)

    for server in servers_over_capacity:
      placements = self.placements_by_server[server]
      placements.sort(key=self.get_sort_key_by_request_count)

      # move top uuids to nearest locations (or second-nearest, etc if other servers are full)
      # until server is no longer over capacity
      while space_remaining[server] < 0:
        uuid = placements.pop()
        self.uuid_to_servers[uuid].remove(server)
        metadata = self.uuid_metadata[uuid]
        best_servers = self.find_closest_servers(metadata['optimal_location'][0], servers_with_capacity)

        for i, best_server_info in enumerate(best_servers):
          best_server = best_server_info['server']
          if space_remaining[best_server] >= metadata['file_size']:
            space_remaining[best_server] -= metadata['file_size']
            self.placements_by_server[best_server].append(uuid)
            self.uuid_to_servers[uuid].add(best_server)
            break
          if i == (len(best_servers) - 1):   # haven't found a server on the last iteration
            raise ValueError("There is too much data for the servers' storage capacity to handle.")

        space_remaining[server] += metadata['file_size']

        print 'PLACEMENTS: ' + str(placements)

  # Convert from latitude to radians from the North Pole
  def convert_lat_to_radians(self, lat):
    # Subtract 90 to make range [-180, 0], then negate to make it [0, 180]
    # degrees_from_north_pole = (lat - 90) * -1;

    # return math.radians(degrees_from_north_pole)
    return math.radians(lat)

  # Convert longitude to radians
  def convert_lng_to_radians(self, lng):
    # Add 180 to make range [0, 360]
    # lng = lng + 180
    return math.radians(lng)

  # Convert from radians from the North Pole to degrees
  def convert_lat_to_degrees(self, lat):
    # degrees_from_north_pole = math.degrees(lat)
    # degrees_from_equator = (degrees_from_north_pole * -1) + 90

    # return degrees_from_equator
    return math.degrees(lat)

  # Convert longitude to degrees
  def convert_lng_to_degrees(self, lng):
    # lng = math.degrees(lng)
    # return lng - 180
    return math.degrees(lng)

  # Normalize radian values from [-Inf, Inf] to specified range - default = [0, 2pi)
  def normalize_radians(self, lng, min_val = 0, max_val = (2 * math.pi)):
    if math.fabs(max_val - min_val - (2 * math.pi)) >= 0.001:
      raise ValueError('Range for function must be around 2*pi.')
    while lng < min_val:
      lng = lng + (2 * math.pi)
    while lng >= max_val:
      lng = lng - (2 * math.pi)
    return lng

  # Find the average longitude of two points where longitude is given from [0, Inf]
  def find_avg_lng(self, lng_a, lng_b):
    # Need to normalize to [0, 2pi) so that arithmetic mean comes out to a reasonable value
    lng_a = self.normalize_radians(lng_a)
    lng_b = self.normalize_radians(lng_b)

    # Here, normalize to [-pi, pi) for longitude
    return self.normalize_radians((lng_a + lng_b) / 2, -1 * math.pi, math.pi)

  # Spherical interpolation, defined in Volley paper
  #
  # params:
  #   weight: weight for interpolation
  #   loc_a: lat/lng for location A
  #   loc_b: lat/lng for location B
  def interp(self, weight, loc_a, loc_b):
    lat_a = self.convert_lat_to_radians(loc_a[0])
    lng_a = self.convert_lng_to_radians(loc_a[1])
    lat_b = self.convert_lat_to_radians(loc_b[0])
    lng_b = self.convert_lng_to_radians(loc_b[1])

    d_first = math.cos(lat_a) * math.cos(lat_b)
    d_second = math.sin(lat_a) * math.sin(lat_b) * math.cos(lng_b - lng_a)
    d_intermediate = d_first + d_second
    if d_intermediate >= 1:
      d_intermediate = 1
    d = math.acos(d_intermediate)

    gamma_numerator = math.sin(lat_b) * math.sin(lat_a) * math.sin(lng_b - lng_a)
    gamma_denominator = math.cos(lat_a) - (math.cos(d) * math.cos(lat_b))
    gamma = math.atan2(gamma_numerator, gamma_denominator)

    beta_numerator = math.sin(lat_b) * math.sin(weight * d) * math.sin(gamma)
    beta_denominator = math.cos(weight * d) - (math.cos(lat_a) * math.cos(lat_b))
    beta = math.atan2(beta_numerator, beta_denominator)

    lat_c_first = math.cos(weight * d) * math.cos(lat_b)
    lat_c_second = math.sin(weight * d) * math.sin(lat_b) * math.cos(gamma)
    lat_c = math.acos(lat_c_first + lat_c_second)
    lat_c = self.convert_lat_to_degrees(lat_c)

    # Find an average of coming from either direction for antipodal nodes
    lng_c_1 = lng_b - beta
    lng_c_2 = lng_a + beta
    lng_c = self.find_avg_lng(lng_c_1, lng_c_2)
    lng_c = self.convert_lng_to_degrees(lng_c)

    # print 'weight:' + str(weight)
    # print 'loc_a:' + str(loc_a)
    # print 'loc_b:' + str(loc_b)
    # print (lat_c, lng_c)

    return (lat_c, lng_c)

  # Found at: http://www.geomidpoint.com/calculation.html
  def weighted_spherical_mean(self, weights, locations):
    cartesian_locations = []

    total_x = 0
    total_y = 0
    total_z = 0

    for i, location in enumerate(locations):
      lat_radians = self.convert_lat_to_radians(location[0])
      lng_radians = self.convert_lng_to_radians(location[1])
      x = math.cos(lat_radians) * math.cos(lng_radians)
      y = math.cos(lat_radians) * math.sin(lng_radians)
      z = math.sin(lat_radians)

      total_x += x
      total_y += y
      total_z += z

    total_weight = float(sum(weights))

    average_x = total_x / total_weight
    average_y = total_y / total_weight
    average_z = total_z / total_weight

    hyp = math.sqrt((average_x * average_x) + (average_y * average_y))
    result_lat_radians = math.atan2(average_z, hyp)
    result_lng_radians = math.atan2(average_y, average_x)

    result_lat = self.convert_lat_to_degrees(result_lat_radians)
    result_lng = self.convert_lng_to_degrees(result_lng_radians)

    return (result_lat, result_lng)

  # Find the weighted spherical mean locations for a data item
  #
  # params:
  #   uuid: uuid of the data item
  def find_centroids(self, uuid):
    request_logs = self.log_manager.get_reads_grouped_by_source(uuid)
    if len(request_logs) == 0:
      return None

    self.uuid_to_server_ranking[uuid] = []
    self.uuid_to_clients[uuid] = {}
    requests_per_server = {}

    for req in request_logs:
      client_loc = self.ip_cache.get_lat_lon_from_ip(req[0])
      if client_loc is None:
        raise NameError('Could not find client ' + req[0] + ' in client DB.')
      request_count = int(req[1])
      if req[0] not in self.servers:
        self.uuid_to_clients[uuid][req[0]] = { 'location': client_loc, 'request_count': request_count }
      closest_server_to_client_list = self.find_closest_servers(client_loc, self.servers)
      closest_server_to_client = closest_server_to_client_list[0]['server']
      if closest_server_to_client not in requests_per_server:
        requests_per_server[closest_server_to_client] = 0
      requests_per_server[closest_server_to_client] += request_count

    sorted_server_tuples_list = sorted(requests_per_server.items(), key=operator.itemgetter(1))
    self.uuid_to_server_ranking[uuid] = [ server_tuple[0] for server_tuple in sorted_server_tuples_list ]

    number_of_centroids = 1

    while True:
      centroids = []

      server_locations = []
      for i in range(number_of_centroids):
        if i >= len(self.uuid_to_server_ranking[uuid]):
          break
        server_locations.append(self.uuid_to_server_ranking[uuid][i])

      servers_to_weights_and_locations = {}

      total_cumulative_distance_to_ideal_server = 0

      # partition to servers closest to each server
      client_info = self.uuid_to_clients[uuid]
      for client, client_dict in client_info.iteritems():
        closest_servers = self.find_closest_servers(client_dict['location'], server_locations)
        closest_server = closest_servers[0]['server']
        closest_server_location = self.ip_cache.get_lat_lon_from_ip(closest_server)

        ideal_servers = self.find_closest_servers(client_dict['location'], self.servers)
        ideal_server = ideal_servers[0]['server']
        ideal_server_location = self.ip_cache.get_lat_lon_from_ip(ideal_server)

        if closest_server not in servers_to_weights_and_locations:
          servers_to_weights_and_locations[closest_server] = { 'weights': [], 'locations': [], 'total_weight': 0 }
        servers_to_weights_and_locations[closest_server]['weights'].append(client_dict['request_count'])
        servers_to_weights_and_locations[closest_server]['locations'].append(client_dict['location'])
        total_cumulative_distance_to_ideal_server += client_dict['request_count'] * util.get_distance(client_dict['location'], ideal_server_location)
        servers_to_weights_and_locations[closest_server]['total_weight'] += client_dict['request_count']

      total_cumulative_distance_to_centroids = 0

      for server, server_dict in servers_to_weights_and_locations.iteritems():
        centroid = self.weighted_spherical_mean(list(server_dict['weights']), list(server_dict['locations']))
        centroids.append(centroid)

        for i in range(len(server_dict['weights'])):
          total_cumulative_distance_to_centroids += server_dict['weights'][i] * util.get_distance(server_dict['locations'][i], centroid)

      # Check if (weighted avg dist to closest server + 1)/(weighted avg dist to closest centroid + 1) < 0.5, or k >= number_of_servers
      # add 1 to numerator and denominator to avoid divide by 0
      greedy_volley_ratio = float(1 + total_cumulative_distance_to_ideal_server) / float(1 + total_cumulative_distance_to_centroids)
      print uuid, greedy_volley_ratio
      if greedy_volley_ratio >= GV_RATIO_THRESHOLD or number_of_centroids >= len(self.servers):
        break

      number_of_centroids += 1

    if len(centroids) == 0:
      # if no centroids returned, there were no pure client requests, only
      # data interdependency requests. If so, return the location of the server of where
      # the initial interdependent requests come from
      initial_location = self.ip_cache.get_lat_lon_from_ip(self.uuid_to_server_ranking[uuid][0])
      centroids.append(initial_location)

    return centroids

if __name__ == '__main__':
  if (len(sys.argv) < 3):
    print 'Usage: python revenge_of_volley.py 1426809600 1427395218'
    print 'Integers are Unix timestamps for start and end times to retrieve log data'
    exit(1)
  start_time = sys.argv[1]
  end_time = sys.argv[2]
  rov = RevengeOfVolley(start_time, end_time)
  rov.execute()
