import argparse
import random

LOG_FORMAT = '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n'  # Schema: [timestamp]\t[target_uuid]\t[src]\t[source_dependency]\t[dest]\t[type]\t[status]\t[size]

def generate_data(clients, servers, client_to_server_map, server_to_client_map):
    timestamp = 1
    tweet_uid = 1
    file_to_server_map = dict()
    with open('access_log.txt', 'wb') as result_file:
        for client in clients:
            # write the files.
            target_server = client_to_server_map[client]
            result_file.write(LOG_FORMAT % (timestamp, tweet_uid, client, 'null', target_server, 'WRITE', '201', random.randint(2, 350)))
            file_to_server_map[tweet_uid] = target_server
            timestamp += 1
            tweet_uid += 1

        for file, server in file_to_server_map.iteritems():
            for other_server in servers:
                if server != other_server:
                    target_clients = server_to_client_map[other_server]
                    for client in target_clients:
                        result_file.write(LOG_FORMAT % (timestamp, file, client, 'null', other_server, 'READ', '200', 'null'))
                        timestamp += 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('client_list', help='list of clients')
    parser.add_argument('server_list', help='list of servers')
    parser.add_argument('client_server_map', help='map of clients to server')
    args = vars(parser.parse_args())
    client_list = args['client_list']
    server_list = args['server_list']
    client_server_map = args['client_server_map']
    clients = []
    servers = []
    client_to_server_map = dict()
    server_to_client_map = dict()
    with open(client_list, 'rb') as client_file, open(server_list, 'rb') as server_file, open(client_server_map, 'rb') as client_server_map_file:
        for client in client_file:
            clients.append(client.strip())

        for server in server_file:
            servers.append(server.strip())

        for client_server in client_server_map_file:
            line = client_server.strip().split()
            print 'line[0]: ' + line[0]
            print 'line[1]: ' + line[1]
            key = line[1].strip()
            if key not in server_to_client_map:
                server_to_client_map[line[1]] = []
            server_to_client_map[line[1]].append(line[0])
            client_to_server_map[line[0]] = line[1]


    print clients
    print servers
    print server_to_client_map

    generate_data(clients, servers, client_to_server_map, server_to_client_map)
