import socket
import sys

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

server_address = ('localhost', 4000)
sock.connect(server_address)

try:
    msg = 'hello!'
    sock.sendall(msg)

    amount_received = 0
    amount_expected = len(msg)

    while (amount_received < amount_expected):
        data = sock.recv(1024)
        amount_received += len(data)

finally:
    sock.close()
