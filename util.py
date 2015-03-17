# Utility class
import geopy
import pyipinfodb
import requests

from geopy.distance import great_circle

API_KEY = 'f096f204a09d53c278c457d8de90ba1aca8d9ede50a6399849045954a4e23535'

# This function may be replaced by cached version calculate distance
# The distance returned is in kilometers.
def get_distance(ip_addr1, ip_addr2):
    ip_translator = pyipinfodb.IPInfo(API_KEY)
    location1 = ip_translator.get_city(ip_addr1)
    location2 = ip_translator.get_city(ip_addr2)
    pt1 = geopy.Point(location1['latitude'], location1['longitude'])
    pt2 = geopy.Point(location2['latitude'], location2['longitude'])
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
