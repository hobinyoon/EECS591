# a utility class for logging
import os
import time

# Log format:
#   [timestamp]\t[uuid]\t[source_entity]\t[destination_entity]\t[request_type]\t[status]\t[request_size]\t[response_size]
def log(uuid, source_entity, destination_entity, request_type, status, request_size, response_size):
    log_file = time.strftime('%d-%m-%Y') + '.log'
    timestamp = time.time()
    log_entry = timestamp + '\t' + \
                uuid + '\t' + \
                source_entity + '\t' + \
                destination_entity + '\t' + \
                request_type + '\t' + \
                status + '\t' + \
                request_size + '\t' + \
                response_size
    with open(log_file, 'a+') as log:
        log.write(log_entry)
