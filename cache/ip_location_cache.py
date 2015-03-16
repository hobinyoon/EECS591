# A cache for ip to location mapping
import sqlite3

class ip_location_cache:

    def __init__(self):
        self.conn = sqlite3.connect('ip_location_cache.db')
        self.cursor = self.conn.cursor()

    # Add an entry to the cache based on the ip address.
    def add_entry(self, ip):
        print 'Retrieving data for ' + ip + '...'
        r = requests.get('http://ipinfo.io/' + ip + '/json')
        try:
            ip_info = r.json()
            loc = ip_info['loc'].split(',')
            print 'Data for ' + client_ip[0] + ' found. lat: ' + loc[0] + ', lng: ' + loc[1]
            add_entry(ip_info['ip'], loc[0], loc[1], ip_info['city'], ip_info['region'], ip_info['country'])
            print 'Inserted into database.'
        except ValueError:
            print 'Error retrieving data for ' + client_ip
            pass

    # Add an entry to the cache.
    def add_entry(self, ip, lat, lon, city, region, country):
        self.cursor.execute('INSERT INTO IpLocationMap VALUES (?, ?, ?, ?, ?, ?)', (ip, lat, lon, city, region, country))
        self.conn.commit()

    # Returns the (lat, lon) information of the ip address.
    def get_lat_lon_from_ip(self, ip):
        self.cursor.execute('SELECT lat, long FROM IpLocationMap WHERE ip=?', (ip,))
        result = self.cursor.fetchone()
        if result is None:
            add_entry(ip)
            self.cursor.execute('SELECT lat, long FROM IpLocationMap WHERE ip=?', (ip,))
            return self.cursor.fetchone()
        else:
            return result
