# Utility class
import geopy
import os
import requests
import urllib
import sys
sys.path.insert(0, 'cache')

# Project imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cache'))
from ip_location_cache import ip_location_cache
from geopy.distance import great_circle

# Config
SERVER_LIST_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'servers.txt')

# get distance between two (lat,log) pairs
def get_distance(location1, location2):
    pt1 = geopy.Point(location1[0], location1[1])
    pt2 = geopy.Point(location2[0], location2[1])
    dist = great_circle(pt1, pt2).km
    return dist

def replicate(file_uuid, source_ip, dest_ip):
  print 'Replicate file ' + file_uuid + ' from ' + source_ip + ' to ' + dest_ip
  url = 'http://%s/replicate?%s' % (source_ip, urllib.urlencode({'ip': dest_ip}))
  r = requests.put(url)
  if r.status_code == requests.codes.ok:
    print "\t succeed!"
  else:
    print "\t fail!"

# Finds the closest server for a lat/long tuple pair
#
# params:
#   location: lat/long tuple
#   servers_to_search: a list of servers to search. Uses self.servers by default.
# returns: list of closest to furthest, where each item is a dict with `server` and `distance`
def find_closest_servers(self, location, servers_to_search = None):
    if servers_to_search is None:
        ip_cache = ip_location_cache()
        servers_to_search = self.servers

        best_servers = []

        for server in servers_to_search:
            server_dict = { 'server': server, 'distance': None }
            item_location = geopy.Point(location[0], location[1])
            server_lat_lon = ip_cache.get_lat_lon_from_ip(server)
            if server_lat_lon is None:
                raise ValueError('Server <' + server + '> latitude/longitude could not be found!')
            server_location = geopy.Point(server_lat_lon[0], server_lat_lon[1])
            server_dict['distance'] = great_circle(item_location, server_location).km

            best_servers.append(server_dict)

        best_servers.sort(key=self.get_distance_key)

    return best_servers

# Finds the closest server for a lat/long tuple pair
#
# params:
#   ip_addr: the ip_addr
#   servers_to_search: a list of servers to search. Uses self.servers by default.
# returns: list of closest to furthest, where each item is a dict with `server` and `distance`
def find_closest_servers_with_ip(self, ip_addr, servers_to_search = None):
    if servers_to_search is None:
        ip_cache = ip_location_cache()
        servers_to_search = self.servers

        best_servers = []

        for server in servers_to_search:
            server_dict = { 'server': server, 'distance': None }
            item_location = ip_cache.get_lat_lon_from_ip(ip_addr)
            server_lat_lon = ip_cache.get_lat_lon_from_ip(server)
            if server_lat_lon is None:
                raise ValueError('Server <' + server + '> latitude/longitude could not be found!')
            server_location = geopy.Point(server_lat_lon[0], server_lat_lon[1])
            server_dict['distance'] = great_circle(item_location, server_location).km

            best_servers.append(server_dict)

        best_servers.sort(key=self.get_distance_key)

    return best_servers

# Gets sort key for sort function to sort by ascending distance
def get_distance_key(self, server_dict):
    return server_dict['distance']

# get distance between two ip addresses
def get_distance_from_ip(ip_addr1, ip_addr2):
    ip_cache = ip_location_cache()
    location1 = ip_cache.get_lat_lon_from_ip(ip_addr1)
    location2 = ip_cache.get_lat_lon_from_ip(ip_addr2)
    return get_distance(location1, location2)

def retrieve_server_list():
    with open(SERVER_LIST_FILE, 'rb') as server_file:
        return server_file.read().splitlines()

# Construct a put request which involves a url and a uuid of the file
def construct_put_request(url, uuid):
    files = {'file': open(uuid, 'rb')}
    request = requests.put(url, files)
    return request

# Construct a post request which involves a url and a uuid of the file
def construct_post_request(url, uuid):
    files = {'file': open(uuid, 'rb')}
    request = requests.post(url, files)
    return request
