import util
import metadata_manager

class GreedyReplication:
  
  def __init__(self):
    pass

  # update access_info and replication status
  def refresh():
    metadata = MetadataManager()
    client_list = metadata.get_client_list() # [client_ip, ]
    server_list = metadata.get_server_list() # [server_ip, ]
    content_list = metadata.get_file_list() # [uuid, ]
    
    access_map = metadata.get_access_map() # {uuid: {client_ip: num_request}}
    replica_map = metadata.get_replica_map() # {uuid: {server_ip: num_replica}}

  def run_replication():
    refresh()
    if enough_replica_on_increase() == True:
      # currently we don't remove any replica
      return
      # remove_replica()
    else:
      add_replica()
      
  # test whether current replicas can handle more requests
  #
  # delta: specify the amount of request increased every time 
  def enough_replica_on_increase(delta):
    for c in content_list:
      for a in client_list
        # add a small amount of requests for content c from client a
        access_map[c][a] += delta
        # test whether current replicas can handle that much request
        is_enough = enough_replica()
        # back tracking,
        access_map[c][a] -= delta
        return is_enough

  def add_replica(request_delta, replica_delta):
    I = []
    for c in content_list:
      for a in client_list
        # add a small amount of requests for content c from client a
        access_map[c][a] += delta
        # test whether current replicas can handle that much request
        if not enough_replica():
          I.append((a,c))
        # back tracking,
        access_map[c][a] -= delta
    max_satisfied_num = 0
    best_c = None
    best_s = None
    # find the server s to replicate content c so that  
    # maximum number of starved clients can be satisfied
    for c in content_list:
      for s in server_list
        satisfied_num = 0
        for a, I_c in I:
          if I_c == c:
            access_map[c][a] += request_delta
            replica_map[c][s] += replica_delta
            if enough_replica():
              satisfied_num += 1
            access_map[c][a] -= request_delta
            replica_map[c][a] -= replica_delta
        if (satisfied_num > max_satisfied_num):
          max_satisfied_num = satisfied_num
          best_c = c
          best_s = s
    if max_satisfied_num > 0:
      util.replicate(best_c, best_s)

  def enough_replica():
    # this is an approximate implementation, may need to
    # construct a bipartite graph and run min matching algo
    for c in content_list:
      request_sum = 0
      for a in client_list:
        request_sum += access_map[c][a]
      replica_sum = 0 # total amount of requests can be handled by replicas
      for s in server_list:
        replica_sum += replica_map[c][s]
      if request_sum > replica_sum:
        return False
    return true








