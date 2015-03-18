# Implementation of Volley

import math
import os
import requests
import sqlite3

import sys
sys.path.insert(0, os.path.normpath('../cache'))

import ip_location_cache

# Config
LOG_DATABASE = os.path.normpath('../aggregator/aggregated_logs.db')

class Volley:

  def __init__(self):
    self.log_conn = sqlite3.connect(LOG_DATABASE)
    self.log_cursor = self.log_conn.cursor()
    self.ip_cache = ip_location_cache.ip_location_cache()

  # Closes the connection to the database
  def close_connection(self):
    self.log_conn.close()

  # Phase 1: Compute Initial Placement
  # def place_initial(self):

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
    requests = self.log_cursor.fetchall()
    if len(requests) == 0:
      return None

    weights = []
    locations = []

    for req in requests:
      client_loc = self.ip_cache.get_lat_lon_from_ip(req[0])
      if client_loc is None:
        raise NameError('Could not find client ' + req[0] + ' in client DB.')
      locations.append(client_loc)
      weights.append(req[1])

    total_weight = float(sum(weights))
    return self.weighted_spherical_mean_helper(total_weight, weights, locations)