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
