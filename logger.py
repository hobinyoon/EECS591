# a utility class for logging
import datetime
import os
import time

LOG_DIRECTORY = 'logs'

# Log format:
#   [timestamp]\t[uuid]\t[source_entity]\t[source_uuid]\t[destination_entity]\t[request_type]\t[status]\t[response_size]
def log(uuid, source_entity, source_uuid, destination_entity, request_type, status, response_size):
    log_file = datetime.datetime.utcnow().strftime('%Y-%m-%d') + '.log'
    timestamp = time.time()
    log_entry = str(int(timestamp)) + '\t' + \
                str(uuid) + '\t' + \
                str(source_entity) + '\t' + \
                str(source_uuid) + '\t' + \
                str(destination_entity) + '\t' + \
                str(request_type) + '\t' + \
                str(status) + '\t' + \
                str(response_size) + '\n'
    log_file_path = os.path.join(LOG_DIRECTORY, log_file)
    with open(log_file_path, 'a+') as log:
        log.write(log_entry)
