import argparse
import os
import random
import socket

def prepare_dataset(file_name, k):
    os.remove(file_name + '_ready')
    with open(file_name) as f:
        content = f.readlines()

    for line in content:
        request_source, request_content, reply = line.split('"')

        # (1) modify the request_source's hostname to ip address
        result_line = ''
        request_source_splitted = request_source.split(' ')
        splitted_hostname = request_source_splitted[0].split('.')
        addr = None
        hostname = ''
        for i in range(len(splitted_hostname) - 1, 0, -1):
            hostname = splitted_hostname[i] + '.' + hostname
            try:
                addr = socket.gethostbyname(hostname)
            except Exception:
                addr = None

            if addr is not None:
                result_line = result_line + str(addr)
                break

        # (2) add delay and concurrent requests to the dataset
        if addr is not None:
            result_line = result_line + '\t' + request_content
            reply_splitted = reply.split(' ')
            result_line = result_line + '\t' + reply_splitted[2].strip()
            concurrent_chance = random.random()
            if (concurrent_chance < 0.4):
                result_line = result_line + '\tC\n'
                for i in range(1, int(k) + 1):
                    with open(file_name + '_ready', 'a') as output:
                        output.write(result_line)
            else:
                result_line = result_line + '\tI\n'
                with open(file_name + '_ready', 'a') as output:
                    output.write(result_line)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('file_name', help='the dataset file in HTML request format')
    parser.add_argument('k', help='the number of concurrent requests')
    args = vars(parser.parse_args())

    prepare_dataset(args['file_name'], args['k'])
