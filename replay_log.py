#!/usr/bin/env python

import os
from datetime import datetime
import time
from multiprocessing import Process
import requests
import urllib

CLIENT_UPLOAD_FOLDER = 'client_upload/'
CLIENT_DOWNLOAD_FOLDER = 'client_download/'
SERVER_HOST = 'localhost:5000'

DELAY_FACTOR = 5000   # Factor to divide filesize by to get delay time
MAX_DELAY = 0.5       # Maximum delay time for concurrent requests

def write_file(filename):
  write_url = 'http://%s/write' % (SERVER_HOST)
  if not os.path.exists(filename):
    return None
  files = {'file': open(filename, 'rb')}
  # may need get latency number
  r = requests.post(write_url, files = files)
  if (r.status_code != requests.codes.created):
    # request failed
    return None
  file_uuid = r.text
  return file_uuid

def read_file(file_uuid, source_ip = None, delay = None):
  print 'READ: source_ip: ' + source_ip + ', delay: ' + str(delay) + ', uuid: ' + file_uuid
  query_parameters = {'uuid': file_uuid}
  if source_ip is not None:
    query_parameters['ip'] = source_ip
  if delay is not None:
    query_parameters['delay'] = delay
  
  read_url = 'http://%s/read?%s' % (SERVER_HOST, urllib.urlencode(query_parameters))

  # may need get latency number
  r = requests.get(read_url, stream=True)
  if (r.status_code == requests.codes.ok):
    with open(CLIENT_DOWNLOAD_FOLDER + file_uuid, 'wb') as fd:
      chunk_size = 1024
      for chunk in r.iter_content(chunk_size):
        fd.write(chunk)
      fd.close()
      print 'DONE: source_ip: ' + source_ip + ', delay: ' + str(delay) + ', uuid: ' + file_uuid
      return True
  # request failed
  print 'FAIL: source_ip: ' + source_ip + ', delay: ' + str(delay) + ', uuid: ' + file_uuid
  print '\tSTATUS: ' + str(r.status_code) + ', TEXT: ' + r.text
  return False

def populate_server_with_log(log_file):
  write_url = 'http://%s/write' % (SERVER_HOST)
  request_to_file_uuid = {}
  fd = open(log_file, 'r')
  i = 0
  for line in fd:
    request_source, request_content, request_size, concurrent = line.split('\t')
    if not request_to_file_uuid.has_key(request_content):
      # take request as a file
      files = {'file': ('request' + str(i), request_content)}
      i += 1
      r = requests.post(write_url, files = files)
      print write_url
      print files
      if (r.status_code != requests.codes.created):
        print r.status_code
        print r.text
        raise ValueError('post request failed')
      file_uuid = r.text
      request_to_file_uuid[request_content] = file_uuid
  fd.close()
  return request_to_file_uuid

# helper function to pause until concurrent execution is finished
def check_concurrent_execution_and_wait(concurrent_processes, concurrent_uuid, uuid):
  if len(concurrent_processes) > 0 and concurrent_uuid != uuid:
    print 'Waiting on concurrency of cardinality ' + str(len(concurrent_processes)) + ' on uuid ' + str(concurrent_uuid)
    for process in concurrent_processes:
      process.join()
    concurrent_processes = []
    concurrent_uuid = None
  return concurrent_processes, concurrent_uuid

def replay_log(log_file, request_to_file_uuid):
  fd = open(log_file, 'r')

  # pool of processes concurrently running
  concurrent_processes = []
  # uuid of file that's concurrently running
  concurrent_uuid = None

  for line in fd:
    request_source, request_content, request_size, concurrent = line.split('\t')
    concurrent = concurrent.strip() # strip newlines
    print 'request_source: ' + str(request_source)
    print 'request_content: ' + str(request_content)
    print 'request_size: ' + str(request_size)
    print 'concurrent: ' + str(concurrent)
    uuid = request_to_file_uuid[request_content]

    # If concurrent, run concurrently with delay
    if concurrent == 'C':
      concurrent_processes, concurrent_uuid = check_concurrent_execution_and_wait(concurrent_processes, concurrent_uuid, uuid)

      delay = float(request_size) / DELAY_FACTOR
      
      if delay > MAX_DELAY:
        delay = MAX_DELAY
      
      # run concurrently        
      process = Process(target=read_file, args=(uuid, request_source, delay))
      process.start()
      concurrent_processes.append(process)
      concurrent_uuid = uuid
    else:
      concurrent_processes, concurrent_uuid = check_concurrent_execution_and_wait(concurrent_processes, concurrent_uuid, uuid)

      succeed = read_file(uuid, request_source)
      if not succeed:
        raise ValueError('request failed with file uuid: ', uuid)

def simulate_requests(request_log_file):
  if not os.path.exists(CLIENT_UPLOAD_FOLDER):
    os.makedirs(CLIENT_UPLOAD_FOLDER)
  if not os.path.exists(CLIENT_DOWNLOAD_FOLDER):
    os.makedirs(CLIENT_DOWNLOAD_FOLDER)
  request_map = populate_server_with_log(request_log_file)
  start_time = int(time.time())
  end_time = int(time.time())
  replay_log(request_log_file, request_map)
  return (start_time, end_time)

if __name__ == '__main__':
  simulate_requests('sample_log_ready')

