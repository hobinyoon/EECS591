import volley

volley = volley.Volley()

# volley.map_ip_to_locations()
placements_by_server = volley.execute()
for server, placements in placements_by_server.iteritems():
  print server + ": " + str(len(placements))