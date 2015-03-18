# Utility class
import geopy
import pyipinfodb
import requests
import sys
sys.path.insert(0, 'cache')

from geopy.distance import great_circle

import ip_location_cache

# This function may be replaced by cached version calculate distance
# The distance returned is in kilometers.
def get_distance(ip_addr1, ip_addr2):
    ip_cache = ip_location_cache.ip_location_cache()
    location1 = ip_cache.get_lat_lon_from_ip(ip_addr1)
    location2 = ip_cache.get_lat_lon_from_ip(ip_addr2)
    print location1
    print location2
    pt1 = geopy.Point(location1[0], location1[1])
    pt2 = geopy.Point(location2[0], location2[1])
    dist = great_circle(pt1, pt2).km
    return dist

# get server logs during the experiment
def get_server_logs(start_time, end_time):
    # retrive server logs from database(TODO)
    # this is just for testing
    sample_log = []
    for i in range(10):
        sample_log.append([ '', '', '119.63.196.102', '8.8.8.8', '', '', '', ''])
    return sample_log

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
