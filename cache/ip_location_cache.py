# A cache for ip to location mapping
import os
import requests
import sqlite3

# Config
CACHE_INITIALIZATION = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cache.sql')

class ip_location_cache:

    def __init__(self):
        self.conn = sqlite3.connect('ip_location_cache.db')
        with open(CACHE_INITIALIZATION, 'rb') as initialization_file:
            self.conn.executescript(initialization_file.read())
        self.cursor = self.conn.cursor()

    # Add an entry to the cache based on the ip address.
    def find_and_add_entry(self, ip):
        print 'Retrieving data for ' + ip + '...'
        r = requests.get('http://ipinfo.io/' + ip + '/json')
        try:
            ip_info = r.json()
            if 'loc' not in ip_info:
                raise ValueError
            loc = ip_info['loc'].split(',')
            print 'Data for ' + ip + ' found. lat: ' + loc[0] + ', lng: ' + loc[1]

            loc[0] = float(loc[0])
            loc[1] = float(loc[1])

            self.add_entry_to_cache(ip_info['ip'], loc[0], loc[1], ip_info['city'], ip_info['region'], ip_info['country'])
            print 'Inserted into database.'
            return (loc[0], loc[1])
        except ValueError:
            print 'Error retrieving data for ' + ip
            pass

    # Add an entry to the cache.
    def add_entry_to_cache(self, ip, lat, lon, city, region, country):
        self.cursor.execute('INSERT INTO IpLocationMap VALUES (?, ?, ?, ?, ?, ?)', (ip, lat, lon, city, region, country))
        self.conn.commit()

    # Returns the (lat, lon) information of the ip address.
    def get_lat_lon_from_ip(self, ip):
        self.cursor.execute('SELECT lat, long FROM IpLocationMap WHERE ip=?', (ip,))
        result = self.cursor.fetchone()
        if result is None:
            return self.find_and_add_entry(ip)
        else:
            return result
