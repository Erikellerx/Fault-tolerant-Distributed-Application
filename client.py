import socket
import argparse
import pickle
import time
from CustomLog import CustomFormatter
import logging
from collections import defaultdict

from message import *

class Client:
    def __init__(self, name, server_addresses, server_ports):
        # Initialize client settings
        self.name = name
        self.GFD_hst = 'localhost'
        self.GFD_prt = 5005
        self.server_addresses = server_addresses
        self.server_ports = server_ports
        self.received_replies = set()

        ch = logging.StreamHandler()
        ch.setFormatter(CustomFormatter())
        self.logger = logging.getLogger()
        self.logger.addHandler(ch)
        self.logger.setLevel(logging.DEBUG)

    # Request membership from GFD
    def request_membership(self):
        try:
            # Create socket and connect to GFD
            GFD_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            GFD_socket.connect((self.GFD_hst, self.GFD_prt))
            # Log the request
            self.logger.info("send request to GFD")
            self.send(GFD_socket, Message(self.name, "request"))
        except socket.error as socketerror:
            # Handle socket errors
            self.logger.error(f"Port {self.GFD_prt} is not available, {socketerror}")
        return self.receive(GFD_socket).data


    # Send message to server
    def send_message(self, message, request_num):
        # Get the membership list from GFD
        for entry in self.request_membership():
            print(entry)
            address, port = entry.split(':')
            try:
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect((address, int(port)))
                # Log the sent message
                self.logger.send(f"{self.name} send messgae {message} to primary S{port[-1]}")
                self.send(client_socket, Message(self.name, message, request_num = request_num))
                reply = self.receive(client_socket)
                if request_num in self.received_replies:
                    self.logger.info(f"request_num {request_num}: Discarded duplicate reply from {reply.name}")
                else:
                    # Add the request number to the set of received replies
                    self.received_replies.add(request_num)
                    self.logger.receive(f"{reply.name} from Port {port} received: {reply.data}")
                client_socket.close()
            except socket.error as socketerror:
                self.logger.error(f"Port {port} is not available, {socketerror}")

    def send(self, client_socket, data):
        msg = pickle.dumps(data)
        package = len(msg).to_bytes(4, 'big') + msg
        client_socket.sendall(package)

    def receive(self, client_socket):
        data_length = int.from_bytes(client_socket.recv(4), 'big')
        if not data_length:
            return None
        return pickle.loads(self._receive_n_len(client_socket, data_length))

    def _receive_n_len(self, client_socket, n):
        # Receive n bytes of data, handling socket timeout and incomplete packet reception
        data = bytearray()
        while len(data) < n:
            try:
                packet = client_socket.recv(n - len(data))
            except socket.timeout:
                continue
            if not packet:
                break
            data.extend(packet)
        return data

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Client")
    parser.add_argument('--hosts', '-o', default=["localhost", "localhost", "localhost"], nargs='+', type=str, help="List of server addresses")
    parser.add_argument('--name', '-n', default="C0", type=str, help="name of this client")
    parser.add_argument('--server_ports', '-sp', default=[6000, 6001, 6002], nargs='+', type=int, help="List of server ports")
    args = parser.parse_args()

    client = Client(args.name, args.hosts, args.server_ports)
    request_num = 0

    while True:
        # Regularly send messages to servers
        time.sleep(0.5)
        data = "hi"
        if data == "quit":
            break
        request_num += 1
        try:
            client.send_message(data, request_num)
        except Exception as e:
            print(e)
            continue
