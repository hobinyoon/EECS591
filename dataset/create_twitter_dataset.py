# This file takes the twitter dataset and transform it into the
# format described in https://github.com/paivaspol/EECS591/issues/25
# The result dataset will support data interdependency.
import argparse
import math
import random
import uuid

# Constants for twitter modes.
TWITTER_RT = 'RT'
TWITTER_MT = 'MT'
TWITTER_RE = 'RE'
UUID_LIST_FILE = 'uuid_list.txt'
USER_IP_MAP_FILE = 'user_ip_map.txt'
TWEET_SIZE = 140
TIMELINE_LIMIT = 20
LOG_FORMAT = '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n'  # Schema: [timestamp]\t[target_uuid]\t[src]\t[source_dependency]\t[dest]\t[type]\t[status]\t[size]

# Generates the dataset.
#
# params:
#   filename: the name of the file containing the twitter data
#   user_ip_map: a map containing the user
def generate_dataset(filename, user_ip_map, uuid_server_map, rt_re_set):
    print 'Generating access logs...'
    reply_stream_map = dict() # key: the tweet uid that is a reply, value: the target stream
    user_timeline_map = dict() # key: the uuid of the stream, value: list containing the tweets' uuid for that stream
    tweet_server = dict() # key: the tweet uuid, value: the corresponding server
    tweet_uuid = dict()
    with open(filename, 'rb') as dataset, open(filename + '.ready', 'wb') as result_file, open(UUID_LIST_FILE, 'wb') as uuid_file:
        for line in dataset:
            line_trimmed = line.strip()
            line_splitted = line.split(' ')
            timestamp = line_splitted[2]
            mode = line_splitted[3].strip()
            first_uid = line_splitted[0]
            first_user_ip = user_ip_map[first_uid]
            second_uid = line_splitted[1]
            second_user_ip = user_ip_map[second_uid]
            first_target_server = uuid_server_map[first_uid]
            second_target_server = uuid_server_map[second_uid]
            tweet_uid = str(uuid.uuid4())
            if mode == TWITTER_MT:
                triplet = (first_uid, second_uid, timestamp)
                if triplet not in rt_re_set:  # make sure that we don't process the tweet twice
                    # Three steps:
                    #   1) A reads A's timeline
                    #   2) A writes to his own timeline
                    #   3) A reads his own timeline
                    access_timeline(timestamp, first_uid, first_user_ip, first_target_server, result_file, user_timeline_map, tweet_server, reply_stream_map)
                    result_file.write(LOG_FORMAT % (timestamp, tweet_uid, first_user_ip, 'null', first_target_server, 'WRITE', '201', '0'))
                    tweet_server[tweet_uid] = first_target_server  # must be here, because this is were the tweet is already stored on the server.
                    if first_uid not in user_timeline_map:  # add the tweet to the timeline
                        user_timeline_map[first_uid] = []
                    elif len(user_timeline_map[first_uid]) > TIMELINE_LIMIT:
                        user_timeline_map[first_uid].pop(0)  # remove the first element
                    user_timeline_map[first_uid].append(tweet_uid)
                    access_timeline(timestamp, first_uid, first_user_ip, first_target_server, result_file, user_timeline_map, tweet_server, reply_stream_map)

            elif mode == TWITTER_RT or mode == TWITTER_RE:
                # Five steps: assuming A is the sender of the tweet and B is the recipient of the tweet.
                #   1) A reads B's timeline
                #   2) A reads his own timeline
                #   3) A writes the tweet to A's timeline
                #   4) A reads B's timeline (as an effect of retweeting and replying)
                #   5) A reads A's timeline
                access_timeline(timestamp, second_uid, first_user_ip, second_target_server, result_file, user_timeline_map, tweet_server, reply_stream_map)
                access_timeline(timestamp, first_uid, first_user_ip, first_target_server, result_file, user_timeline_map, tweet_server, reply_stream_map)
                result_file.write(LOG_FORMAT % (timestamp, tweet_uid, first_user_ip, 'null', first_target_server, 'WRITE', '201', '0'))
                tweet_server[tweet_uid] = first_target_server  # must be here, because this is were the tweet is already stored on the server.
                if first_uid not in user_timeline_map:  # add the tweet to the timeline
                    user_timeline_map[first_uid] = []
                elif len(user_timeline_map[first_uid]) > TIMELINE_LIMIT:
                    user_timeline_map[first_uid].pop(0)  # remove the first element
                user_timeline_map[first_uid].append(tweet_uid)  # add the tweet to the timeline
                if mode == TWITTER_RE:
                    reply_stream_map[tweet_uid] = second_uid # the tweet is directed to the second_uid

                result_file.write(LOG_FORMAT % (timestamp, second_uid, first_target_server, tweet_uid, second_target_server, 'READ', '200', str(TWEET_SIZE)))
                access_timeline(timestamp, first_uid, first_user_ip, first_target_server, result_file, user_timeline_map, tweet_server, reply_stream_map)
            uuid_file.write('%s\n' % tweet_uid)

# Helper method for populating log entries for reading from a timeline
#
# params:
#   timestamp: the timestamp
#   timeline_uuid: the uuid of the source's target timeline
#   source: the source's ip address
#   target_server: the target server's ip address
#   result_file: the file objet for writing the logs
#   user_timeline_map: a mapping from user uuid --> timeline content
#   tweet_server: a mapping from tweet uuid --> server
#   reply_stream_map: a mapping from reply tweet uuid --> destination stream
def access_timeline(timestamp, timeline_uuid, source, target_server, result_file, user_timeline_map, tweet_server, reply_stream_map):
    # Read source's timeline.
    timeline_list = [] if timeline_uuid not in user_timeline_map else user_timeline_map[timeline_uuid] # get the list containing the uuid of the tweets for that timeline
    read_size = TWEET_SIZE * len(timeline_list)
    result_file.write(LOG_FORMAT % (timestamp, timeline_uuid, source, 'null', target_server, 'READ', '200', str(read_size)))
    # Read each tweet from the server.
    for tweet in timeline_list:
        read_target_server = tweet_server[tweet]
        result_file.write(LOG_FORMAT % (timestamp, tweet, target_server, timeline_uuid, read_target_server, 'READ', '200', str(TWEET_SIZE)))
        if tweet in reply_stream_map:  # read the timeline where the origin of this reply tweet resides
            target_timeline = reply_stream_map[tweet]
            result_file.write(LOG_FORMAT % (timestamp, target_timeline, target_server, timeline_uuid, read_target_server, 'READ', '200', str(TWEET_SIZE)))

# Generates the user to ip address map
# Also output to a file called 'user_ip_map.txt'
# Each line contains [user]\t[ip_address]\t[lat]\t[long] where each column is tab delimitted.
#
# params:
#   filename: filename containing the twitter dataset
def generate_user_ip_map(filename, servers):
    user_ip_map = dict()
    rt_re_set = set()
    with open(filename, 'rb') as twitter_file:
        for line in twitter_file:
            line_trimmed = line.strip()
            line_splitted = line.split(' ')
            first_uid = line_splitted[0]
            second_uid = line_splitted[1]
            if first_uid not in user_ip_map.keys():
                first_ip_address = generate_ip_address()
                user_ip_map[first_uid] = first_ip_address
            if second_uid not in user_ip_map.keys():
                second_ip_address = generate_ip_address()
                user_ip_map[second_uid] = second_ip_address
            timestamp = line_splitted[2]
            mode = line_splitted[3].strip()
            if mode == TWITTER_RE or mode == TWITTER_RT:
                rt_re_set.add((first_uid, second_uid, timestamp))

    uuid_server_map = dict()
    user_count = 0
    with open(USER_IP_MAP_FILE, 'wb') as user_ip_map_file:
        for key, value in user_ip_map.iteritems():
            ip_address = value
            lat = generate_random_lat_long()
            lon = generate_random_lat_long()
            user_ip_map_file.write('%s\t%s\t%s\t%s\n' % (key, ip_address, lat, lon))
            uuid_server_map[key] = servers[user_count % len(servers)]
    return (user_ip_map, uuid_server_map, rt_re_set)

# Generates an IP address ranging from 0.0.0.0 --> 255.255.255.255
def generate_ip_address():
    w = str(random.randrange(1, 256))
    x = str(random.randrange(0, 256))
    y = str(random.randrange(0, 256))
    z = str(random.randrange(0, 256))
    result = '%s.%s.%s.%s' % (w, x, y, z)
    return result

# Generates a latitude, longitude pair randomly.
# The result can even be in the north pole!
def generate_random_lat_long():
    lat = random.randrange(-180, 181)
    lon = random.randrange(-180, 181)
    return (lat, lon)

# Generates a lat, long tuple from based on the information of x, y and radius
#
# params:
#   lat: the latitude
#   long: the longitude
#   radius: the radius around the lat, long coordinates (in meters)
# return: a tuple (lat, long)
def generate_random_lat_long_near_location(x_0, y_0, radius):
    radius_in_degrees = radius / 111000
    u = random.uniform(0, 1)
    v = random.uniform(0, 1)
    w = radius_in_degrees * math.sqrt(u)
    t = 2 * math.pi * v
    x = w * math.cos(t)
    y = w * math.sin(t)
    new_x = x / math.cos(y_0)
    result_x = new_x + x_0
    result_y = y + y_0
    return (result_x, result_y)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help='the file containing the original dataset')
    parser.add_argument('servers', help='The file containing the list of servers that are available')
    args = vars(parser.parse_args())
    filename = args['filename']
    servers_filename = args['servers']
    servers = []
    with open(servers_filename, 'rb') as servers_file:
        for server in servers_file:
            servers.append(server.strip())

    user_ip_map, uuid_server_map, rt_re_set = generate_user_ip_map(filename, servers)
    generate_dataset(filename, user_ip_map, uuid_server_map, rt_re_set)
    print 'Files generated:'
    print '* %s.ready: the access log' % filename
    print '* %s: contains all the tweet UUID generated' % UUID_LIST_FILE
    print '* %s: contains the user id map to the ip' % USER_IP_MAP_FILE
