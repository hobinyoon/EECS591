#!/usr/bin/env python

import os
from datetime import datetime
import time
from multiprocessing import Process
import requests
import urllib

import util

CLIENT_UPLOAD_FOLDER = 'client_upload/'
CLIENT_DOWNLOAD_FOLDER = 'client_download/'
SERVER_LIST = util.retrieve_server_list()

NULL = 'null'
READ_REQUEST = 'READ'
WRITE_REQUEST = 'WRITE'

DELAY_FACTOR = 5000   # Factor to divide filesize by to get delay time
MAX_DELAY = 0.5       # Maximum delay time for concurrent requests

def write_file(uuid, source, dest, response_size):
  print 'WRITE: source: ' + source + ', uuid: ' + uuid + ', response_size: ' + response_size
  query_parameters = { 'uuid': uuid, 'ip': source }
  dest = util.convert_to_local_hostname(dest)
  write_url = 'http://%s/write?%s' % (dest, urllib.urlencode(query_parameters))
  print write_url

  # make the content of the file the file's theoretical size
  files = {'file': response_size}

  r = requests.post(write_url, files = files)
  if (r.status_code != requests.codes.created):
    print 'FAIL: source: ' + source + ', uuid: ' + uuid + ', response_size: ' + response_size
    return None
  
  print 'DONE: source: ' + source + ', uuid: ' + uuid + ', response_size: ' + response_size
  return uuid

def read_file(uuid, source_uuid, source, dest, delay):
  print 'READ: source: ' + source + ', uuid: ' + uuid + ', source_uuid: ' + source_uuid + ', delay: ' + str(delay)
  query_parameters = { 'uuid': uuid, 'ip': source, 'source_uuid': source_uuid }
  if delay is not None:
    query_parameters['delay'] = delay

  dest = util.convert_to_local_hostname(dest)
  read_url = 'http://%s/read?%s' % (dest, urllib.urlencode(query_parameters))
  print read_url

  # may need get latency number
  r = requests.get(read_url, stream=True)
  if (r.status_code == requests.codes.ok):
    with open(CLIENT_DOWNLOAD_FOLDER + uuid, 'wb') as fd:
      chunk_size = 1024
      for chunk in r.iter_content(chunk_size):
        fd.write(chunk)
      fd.close()
      print 'DONE: source: ' + source + ', uuid: ' + uuid + ', source_uuid: ' + source_uuid
      return True

  # request failed
  print 'FAIL: source: ' + source + ', uuid: ' + uuid + ', source_uuid: ' + source_uuid
  print '\tSTATUS: ' + str(r.status_code) + ', TEXT: ' + r.text
  return False

def execute_log_line(uuid, source, source_uuid, dest, request_type, response_size, delay = None):
  if request_type == READ_REQUEST:
    return read_file(uuid, source_uuid, source, dest, delay)
  elif request_type == WRITE_REQUEST:
    return write_file(uuid, source, dest, response_size)

# helper function to pause until concurrent execution is finished
def check_concurrent_execution_and_wait(concurrent_processes, uuid):
  if len(concurrent_processes) > 0:
    for process in concurrent_processes:
      process.join()
    concurrent_processes = []
  return concurrent_processes

def replay_log(log_file, enable_concurrency = True, allow_writes = True):
  fd = open(log_file, 'r')

  # pool of processes concurrently running
  concurrent_processes = []
  # uuid of file that's concurrently running
  concurrent_uuid = None

  last_timestamp = 0

  for line in fd:
    timestamp, uuid, source, source_uuid, dest, request_type, response_code, response_size = line.split('\t')

    if request_type == WRITE_REQUEST and not allow_writes:
      continue
      
    # If concurrent, run concurrently with delay
    if enable_concurrency and timestamp == last_timestamp:
      concurrent_processes = check_concurrent_execution_and_wait(concurrent_processes, uuid)

      delay = float(request_size) / DELAY_FACTOR

      if delay > MAX_DELAY:
        delay = MAX_DELAY

      # run concurrently
      process = Process(target=execute_log_line, args=(uuid, source, source_uuid, dest, request_type, response_size, delay))
      process.start()
      concurrent_processes.append(process)
    else:
      if enable_concurrency:
        concurrent_processes = check_concurrent_execution_and_wait(concurrent_processes, uuid)
      
      succeed = execute_log_line(uuid, source, source_uuid, dest, request_type, response_size)
      if not succeed:
        raise ValueError('request failed with file uuid: ', uuid)

    last_timestamp = timestamp

  check_concurrent_execution_and_wait(concurrent_processes, None)

def simulate_requests(request_log_file, enable_concurrency = True, allow_writes = True):
  if not os.path.exists(CLIENT_UPLOAD_FOLDER):
    os.makedirs(CLIENT_UPLOAD_FOLDER)
  if not os.path.exists(CLIENT_DOWNLOAD_FOLDER):
    os.makedirs(CLIENT_DOWNLOAD_FOLDER)
  start_time = int(time.time())
  replay_log(request_log_file, enable_concurrency, allow_writes)
  end_time = int(time.time())
  return (start_time, end_time)