#!/usr/bin/env python

import os
import requests


CLIENT_UPLOAD_FOLDER = "client_upload/"
CLIENT_DOWNLOAD_FOLDER = "client_download/"
SERVER_URL = "http://localhost:5000"

def write_file(filename):
  write_url = SERVER_URL + "/write"
  if not os.path.exists(filename):
    return None
  files = {'file': open(filename, 'rb')}
  # may need get latency number
  r = requests.put(write_url, files = files)
  if (r.status_code != requests.codes.created):
    # request failed
    return None
  file_uuid = r.text
  return file_uuid

def read_file(file_uuid):
  read_url = SERVER_URL + "/read?uuid=" + file_uuid
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

def populate_server_with_log(log_file):
  write_url = SERVER_URL + "/write"
  request_to_file_uuid = {}
  fd = open(log_file, "r")
  i = 0
  for line in fd:
    request_source, request_content, reply = line.split('"')
    if not request_to_file_uuid.has_key(request_content):
      # take request as a file
      files = {'file': ('request' + str(i), request_content)}
      i += 1
      r = requests.put(write_url, files = files)
      if (r.status_code != requests.codes.created):
        raise ValueError('put request failed')
      file_uuid = r.text
      request_to_file_uuid[request_content] = file_uuid
  fd.close()
  return request_to_file_uuid

def reply_log(log_file, request_to_file_uuid):
  fd = open(log_file, "r")
  for line in fd:
    request_source, request_content, reply = line.split('"')
    file_uuid = request_to_file_uuid[request_content]
    succeed = read_file(file_uuid)
    if not succeed:
      raise ValueError('request failed with file uuid: ', file_uuid)

if __name__ == "__main__":
  if not os.path.exists(CLIENT_UPLOAD_FOLDER):
    os.makedirs(CLIENT_UPLOAD_FOLDER)
  if not os.path.exists(CLIENT_DOWNLOAD_FOLDER):
    os.makedirs(CLIENT_DOWNLOAD_FOLDER)

  log_file = "sample_log"
  request_map = populate_server_with_log(log_file)
  reply_log(log_file, request_map)

