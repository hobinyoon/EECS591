import unittest
import mock
import os
import sys

# Files to test
sys.path.insert(0, os.path.normpath('..'))
import volley

class TestVolley(unittest.TestCase):
  def setUp(self):
    # Test information
    self.servers = {
      'virginia': '54.175.68.60',
      'oregon': '54.69.237.99',
      'tokyo': '54.65.80.55',
      'germany': '54.93.104.58',
      'brazil': '54.207.24.208',
    }
    self.locations = {
      'ann_arbor': (42.2733204, -83.7376894),
      'chicago': (41.8337329, -87.7321555),
      'san_francisco': (37.7577, -122.4376)
    }

    # Volley setup
    self.volley = volley.Volley()
    self.volley.servers = [ server for name, server in self.servers.iteritems() ]

  def test_find_closest_servers(self):
    servers_to_search = [self.servers['virginia'], self.servers['oregon'],
      self.servers['tokyo'], self.servers['germany'], self.servers['brazil']]
    
    best_servers = self.volley.find_closest_servers(self.locations['ann_arbor'], servers_to_search)

    self.assertEqual(best_servers[0]['server'], self.servers['virginia'])
    self.assertEqual(best_servers[1]['server'], self.servers['oregon'])
    self.assertEqual(best_servers[2]['server'], self.servers['germany'])
    self.assertEqual(best_servers[3]['server'], self.servers['brazil'])
    self.assertEqual(best_servers[4]['server'], self.servers['tokyo'])

  def test_redistribute_server_data_by_capacity(self):
    def side_effect(*args, **kwargs):
      if args[0] == self.servers['virginia']:
        return 40
      elif args[0] == self.servers['oregon']:
        return 10
      elif args[0] == self.servers['tokyo']:
        return 30
      else:
        return 0
    self.volley.total_server_capacity = mock.Mock(side_effect=side_effect)
    
    self.volley.uuid_metadata = {
      '1': {'file_size': 20, 'request_count': 10, 'optimal_location': self.locations['san_francisco'] },
      '2': {'file_size': 20, 'request_count': 4, 'optimal_location': self.locations['chicago'] },
      '3': {'file_size': 20, 'request_count': 1, 'optimal_location': self.locations['ann_arbor'] }
    }

    placements_by_server = {
      self.servers['virginia']: [],
      self.servers['oregon']: ['1', '2', '3'],
      self.servers['tokyo']: [],
      self.servers['brazil']: [],
      self.servers['germany']: []
    }

    results = self.volley.redistribute_server_data_by_capacity(placements_by_server)

    self.assertEqual(len(results[self.servers['virginia']]), 2)
    self.assertEqual(len(results[self.servers['oregon']]), 0)
    self.assertEqual(len(results[self.servers['tokyo']]), 1)
    self.assertIn('1', results[self.servers['virginia']])
    self.assertIn('2', results[self.servers['virginia']])
    self.assertIn('3', results[self.servers['tokyo']])

  def tearDown(self):
    self.volley.close_connection()

if __name__ == '__main__':
  unittest.main()