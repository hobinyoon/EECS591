# Implementation of Volley

import geopy
import math
import os
import requests
import sqlite3
import sys
import urllib

from geopy.distance import great_circle

# Project Imports
up_one_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')
sys.path.insert(0, up_one_dir)
sys.path.insert(0, os.path.join(up_one_dir, 'cache'))
import ip_location_cache
import util

# Config
LOG_DATABASE = os.path.join(up_one_dir, 'aggregator/aggregated_logs.db')

class Volley:

  def __init__(self):
    self.log_conn = sqlite3.connect(LOG_DATABASE)
    self.log_cursor = self.log_conn.cursor()
    self.ip_cache = ip_location_cache.ip_location_cache()
    self.servers = util.retrieve_server_list()
    self.uuid_metadata = {}     # dictionary mapping uuid -> metadata (includes state-ful data)

  # Closes the connection to the database
  def close_connection(self):
    self.log_conn.close()

  # Execute Volley algorithm
  def execute(self):
    locations_by_uuid = self.place_initial()
    locations_by_uuid = self.reduce_latency(locations_by_uuid)
    return self.collapse_to_datacenters(locations_by_uuid)

  # PHASE 1: Compute Initial Placement
  def place_initial(self):
    self.log_cursor.execute('SELECT DISTINCT uuid FROM Log WHERE request_type = "READ"')
    uuid_tuples = self.log_cursor.fetchall()

    locations_by_uuid = {}

    for uuid_tuple in uuid_tuples:
      uuid = uuid_tuple[0]
      locations_by_uuid[uuid] = self.weighted_spherical_mean(uuid)

    return locations_by_uuid

  # PHASE 2: Iteratively Move Data to Reduce Latency
  def reduce_latency(self, locations_by_uuid):
    # TODO: need to have a dataset where data items reference each other
    return locations_by_uuid

  # PHASE 3: Iteratively Collapse Data to Datacenters
  def collapse_to_datacenters(self, locations_by_uuid):
    placements_by_server = {}

    for server in self.servers:
      placements_by_server[server] = []

    for uuid, location in locations_by_uuid.iteritems():
      metadata = {'optimal_location': location, 'uuid': uuid, 'dist': None, 'file_size': None, 'request_count': None}
      best_server = None

      self.log_cursor.execute('SELECT response_size FROM Log WHERE request_type = "READ" AND uuid = ? AND status = 200', (uuid,))
      file_size_result = self.log_cursor.fetchone()
      if file_size_result is None:
        raise ValueError('File size could not be found for uuid: ' + uuid)
      metadata['file_size'] = file_size_result[0]

      self.log_cursor.execute('SELECT count(*) FROM Log WHERE request_type = "READ" AND uuid = ?', (uuid,))
      request_count_result = self.log_cursor.fetchone()
      if request_count_result is None:
        raise ValueError('Number of requests could not be found for uuid: ' + uuid)
      metadata['request_count'] = request_count_result[0]
      
      best_servers = self.find_closest_servers(location)
      best_server = best_servers[0]
      metadata['dist'] = best_server['distance']

      self.uuid_metadata[uuid] = metadata
      placements_by_server[best_server['server']].append(uuid)

    placements_by_server = self.redistribute_server_data_by_capacity(placements_by_server)

    return placements_by_server

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
    url = 'http://%s/can_move_file?%s' % (server, urllib.urlencode({ 'file_size': 0 }))
    r = requests.get(url)
    return float(r.text)

  # Check capacity and redistribute data to each server
  #
  # params:
  #   placements_by_server: dictionary mapping server hostname -> set of uuids
  def redistribute_server_data_by_capacity(self, placements_by_server):
    space_remaining = {}
    servers_with_capacity = set()
    servers_over_capacity = set()
    
    for server in self.servers:
      space_remaining[server] = self.total_server_capacity(server)

      placements = placements_by_server[server]

      for uuid in placements:
        metadata = self.uuid_metadata[uuid]
        space_remaining[server] -= metadata['file_size']

      if space_remaining[server] > 0:
        servers_with_capacity.add(server)
      else:
        servers_over_capacity.add(server)

    for server in servers_over_capacity:
      placements = placements_by_server[server]
      placements.sort(key=self.get_sort_key_by_request_count)
      
      # move top uuids to nearest locations (or second-nearest, etc if other servers are full)
      # until server is no longer over capacity
      while space_remaining[server] < 0:
        uuid = placements.pop()
        metadata = self.uuid_metadata[uuid]
        best_servers = self.find_closest_servers(metadata['optimal_location'], servers_with_capacity)

        for i, best_server_info in enumerate(best_servers):
          best_server = best_server_info['server']
          if space_remaining[best_server] >= metadata['file_size']:
            space_remaining[best_server] -= metadata['file_size']
            placements_by_server[best_server].append(uuid)
            break
          if i == (len(best_servers) - 1):   # haven't found a server on the last iteration
            raise ValueError("There is too much data for the servers' storage capacity to handle.")

        space_remaining[server] += metadata['file_size']

        print 'PLACEMENTS: ' + str(placements)

    return placements_by_server

  # Convert from latitude to radians from the North Pole
  def convert_lat_to_radians(self, lat):
    # Subtract 90 to make range [-180, 0], then negate to make it [0, 180]
    degrees_from_north_pole = (lat - 90) * -1;

    return math.radians(degrees_from_north_pole)

  # Convert longitude to radians
  def convert_lng_to_radians(self, lng):
    # Add 180 to make range [0, 360]
    # lng = lng + 180
    return math.radians(lng)

  # Convert from radians from the North Pole to degrees
  def convert_lat_to_degrees(self, lat):
    degrees_from_north_pole = math.degrees(lat)
    degrees_from_equator = (degrees_from_north_pole * -1) + 90
    
    return degrees_from_equator

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

  # Helper for weighted_spherical_mean, defined in Volley paper
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
    d = math.acos(d_first + d_second)

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

    return (lat_c, lng_c)

  # Recursive helper for weighted_spherical_mean
  #
  # params:
  #   weights: a list of weights
  #   locations: a list of latitude/longitude tuples for clients, has same cardinality as weights
  def weighted_spherical_mean_helper(self, total_weight, weights, locations):
    if len(weights) != len(locations):
      raise ValueError('Weights and locations must have the same length.')
    
    length = len(weights)

    print '------ Weighted spherical mean -------- '
    print 'Length: ' + str(length)
    print 'WEIGHTS: ' + str(weights)
    print 'LOCATIONS: ' + str(locations)

    current_weight = float(weights.pop())
    weight = current_weight / total_weight
    location = locations.pop()

    print 'current_weight: ' + str(weight)

    if length == 1:
      return location

    return self.interp(weight, location, self.weighted_spherical_mean_helper(total_weight, weights, locations))

  # Find the weighted spherical mean location for a data item
  #
  # params:
  #   uuid: uuid of the data item
  def weighted_spherical_mean(self, uuid):
    
    # Find results for each source entity
    self.log_cursor.execute('SELECT source_entity, COUNT(source_entity) AS weight FROM Log '
      'WHERE uuid = ? AND request_type = "READ" GROUP BY source_entity', (uuid,))
    request_logs = self.log_cursor.fetchall()
    if len(request_logs) == 0:
      return None

    weights = []
    locations = []

    for req in request_logs:
      client_loc = self.ip_cache.get_lat_lon_from_ip(req[0])
      if client_loc is None:
        raise NameError('Could not find client ' + req[0] + ' in client DB.')
      locations.append(client_loc)
      weights.append(req[1])

    total_weight = float(sum(weights))
    return self.weighted_spherical_mean_helper(total_weight, weights, locations)