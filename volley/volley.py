# Implementation of Volley

import math
import os
import requests
import sqlite3

# Config
LOG_DATABASE = os.path.normpath('../aggregator/aggregated_logs.db')
VOLLEY_DATABASE = os.path.normpath('volley.db')
VOLLEY_INITIALIZATION = os.path.normpath('volley.sql')

class Volley:

  def __init__(self):
    self.conn = sqlite3.connect(VOLLEY_DATABASE)
    with open(VOLLEY_INITIALIZATION, 'rb') as initialization_file:
      self.conn.executescript(initialization_file.read())
    self.cursor = self.conn.cursor()
    self.log_conn = sqlite3.connect(LOG_DATABASE)
    self.log_cursor = self.log_conn.cursor()

  # Phase 1: Compute Initial Placement
  # def place_initial(self):
    # self.map_ip_to_locations()


  # Closes the connection to the database
  def close_connection(self):
    self.log_conn.close()
    self.conn.close()

  # Maps all distinct source_entity IP addresses to latitude and longitude in database
  def map_ip_to_locations(self):
    # Find unique IPs
    self.log_cursor.execute('SELECT DISTINCT source_entity FROM Log')
    client_ip = self.log_cursor.fetchone()

    while client_ip is not None:
      # Check already-mapped IPs
      self.cursor.execute('SELECT ip, lat, lng FROM Client WHERE ip = ?', client_ip)
      
      if self.cursor.fetchone() is None:
        print 'Retrieving data for ' + client_ip[0] + '...'
        r = requests.get('http://ipinfo.io/' + client_ip[0] + '/json')
        try:
          ip_info = r.json()
          loc = ip_info['loc'].split(',')
          print 'Data for ' + client_ip[0] + ' found. lat: ' + loc[0] + ', lng: ' + loc[1]
          self.cursor.execute('INSERT INTO Client VALUES (?, ?, ?, ?, ?, ?)',
            (ip_info['ip'], loc[0], loc[1],
             ip_info['city'], ip_info['region'], ip_info['country']))
          self.conn.commit()
          print 'Inserted into database.'
        except ValueError:
          print 'Error retrieving data for ' + client_ip
          pass
      client_ip = self.log_cursor.fetchone()

  # Convert from latitude to radians from the North Pole
  def convert_lat_to_radians(self, lat):
    # Subtract 90 to make range [-180, 0], then negate to make it [0, 180]
    degrees_from_north_pole = (lat - 90) * -1;

    return math.radians(degrees_from_north_pole)

  # Convert longitude to radians
  def convert_lng_to_radians(self, lng):
    return math.radians(lng)

  # Convert from radians from the North Pole to degrees
  def convert_lat_to_degrees(self, lat):
    degrees_from_north_pole = math.degrees(lat)
    degrees_from_equator = (degrees_from_north_pole * -1) + 90
    
    return degrees_from_equator

  # Convert longitude to degrees
  def convert_lng_to_degrees(self, lng):
    return math.degrees(lng)

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
    gamma = math.atan(gamma_numerator / gamma_denominator)

    beta_numerator = math.sin(lat_b) * math.sin(weight * d) * math.sin(gamma)
    beta_denominator = math.cos(weight * d) - (math.cos(lat_a) * math.cos(lat_b))
    beta = math.atan(beta_numerator / beta_denominator)

    lat_c_first = math.cos(weight * d) * math.cos(lat_b)
    lat_c_second = math.sin(weight * d) * math.sin(lat_b) * math.cos(gamma)

    # Trying two different options to get calculations to work for trivial 2 location case...

    # A: as written in Volley paper
    lat_c = math.acos(lat_c_first + lat_c_second)
    
    # B: random hack
    # if lat_b > lat_a:
    #   lat_c = math.acos(lat_c_first + lat_c_second)
    # else:
    #   lat_c = math.acos(lat_c_first - lat_c_second)

    lat_c = self.convert_lat_to_degrees(lat_c)

    # Trying two different options to get calculations to work for trivial 2 location case...
    # A: as written in Volley paper
    lng_c = lng_b - beta

    # B: random hack
    # beta = math.fabs(beta)
    # if lng_b > lng_a:
    #   lng_c = lng_b - beta
    # else:
    #   lng_c = lng_b + beta

    lng_c = self.convert_lng_to_degrees(lng_c)

    print "INTERP: " + str((lat_c, lng_c))
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

    print 'Length: ' + str(length)
    print 'WEIGHTS: ' + str(weights)
    print 'LOCATIONS: ' + str(locations)

    current_weight = float(weights.pop())
    weight = current_weight / total_weight
    location = locations.pop()

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
      'WHERE uuid = ? GROUP BY source_entity', (uuid,))
    requests = self.log_cursor.fetchall()
    if len(requests) == 0:
      return None

    weights = []
    locations = []

    for req in requests:
      self.cursor.execute('SELECT lat, lng from Client WHERE ip = ?', (req[0],))
      client_loc = self.cursor.fetchone()
      if client_loc is None:
        raise NameError('Could not find client ' + req[0] + ' in client DB.')
      locations.append(client_loc)
      weights.append(req[1])
    
    locations.reverse()

    print locations

    total_weight = float(sum(weights))
    return self.weighted_spherical_mean_helper(total_weight, weights, locations)