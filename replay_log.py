#!/usr/bin/env python

import os
from datetime import datetime
import requests
import urllib

CLIENT_UPLOAD_FOLDER = 'client_upload/'
CLIENT_DOWNLOAD_FOLDER = 'client_download/'
SERVER_HOST = 'localhost:5000'
SECONDS_PER_DAY = 86400

def date_to_timestamp(date):
  return int(time.mktime(datetime.datetime.strptime(date, '%d/%b/%Y:%H:%M:%S').timetuple()))

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

def read_file(file_uuid, source_ip = None):
  if source_ip is None:
    read_url = 'http://%s/read?%s' % (SERVER_HOST, urllib.urlencode({'uuid': file_uuid}))
  else:
    read_url = 'http://%s/read?%s' % (SERVER_HOST, urllib.urlencode({'uuid': file_uuid, 'ip': source_ip}))
  # may need get latency number
  r = requests.get(read_url, stream=True)
  if (r.status_code == requests.codes.ok):
    with open(CLIENT_DOWNLOAD_FOLDER + file_uuid, 'wb') as fd:
      chunk_size = 1024
      for chunk in r.iter_content(chunk_size):
        fd.write(chunk)
      fd.close()
      return True
  # request failed
  return False

def populate_server_with_logs(logs):
  write_url = 'http://%s/write' % (SERVER_HOST)
  request_to_file_uuid = {}
  i = 0
  for log in logs:
    request_source, request_content, reply = log.split('"')
    if not request_to_file_uuid.has_key(request_content):
      # take request as a file
      files = {'file': ('request' + str(i), request_content)}
      i += 1
      r = requests.post(write_url, files = files)
      if (r.status_code != requests.codes.created):
        raise ValueError('post request failed')
      file_uuid = r.text
      request_to_file_uuid[request_content] = file_uuid
  return request_to_file_uuid

def replay_logs(last_index, time_interval, request_logs, request_to_file_uuid_map):
  last_date = request_logs[last_index].split('"')[0].split()[0].split('[')[1].split()[0]
  last_timestamp = date_to_timestamp(last_date)
  time_limit = last_timestamp + SECONDS_PER_DAY
  curr_index = last_index
  for log in logs[last_index+1:]
    request_source_info, request_content, reply = line.split('"')
    request_source_ip = request_source_info.split()[0]
    datetime = request_source_info.split('[')[1].split('-')[0]
    timestamp = date_to_timestamp(datetime)
    if timestamp > time_limit:
      break
    file_uuid = request_to_file_uuid[request_content]
    succeed = read_file(file_uuid, request_source_ip)
    if not succeed:
      raise ValueError('request failed with file uuid: ', file_uuid)
    curr_index += 1
  if curr_index >= len(request_logs):
    return None
  else:
    return curr_index
