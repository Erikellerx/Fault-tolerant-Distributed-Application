import socket
import argparse
import pickle
from message import *
from enum import Enum
import struct
from datetime import datetime
import time
import sys
from CustomLog import CustomFormatter
import logging


# Replication Manager
class RM:
    def __init__(self, host='', port=5009):
        # Initialize RM settings
        self.name = "RM"
        self.membership = []
        self.RM_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.RM_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.RM_socket.bind((host, port))
        self.RM_socket.listen(5)

        ch = logging.StreamHandler()
        ch.setFormatter(CustomFormatter())
        self.logger = logging.getLogger()
        self.logger.addHandler(ch)
        self.logger.setLevel(logging.DEBUG)

        # Log the IP address of this RM
        self.logger.info(socket.gethostbyname(socket.gethostname()))

    def run(self):
        while True:
            try:
                gfd_socket, addr = self.RM_socket.accept()
                message = self.receive(gfd_socket)
                if message:
                    self.process_message(message)
                gfd_socket.close()
            except KeyboardInterrupt:
                gfd_socket.close()
                exit()
            except socket.timeout:
                continue

    def process_message(self, message):
        # Update membership based on the message from GFD
        if message.data == "update_membership":
            self.membership = message.server_name
            self.print_membership()

    def print_membership(self):
        member_count = len(self.membership)
        # Log the number of members in the RM
        if member_count == 0:
            self.logger.info("RM: 0 members")
        elif member_count == 1:
            self.logger.info(f"RM: 1 member: {self.membership[0]}")
        else:
            members_str = ', '.join(self.membership)
            self.logger.info(f"RM: {member_count} members: {members_str}")

    def send(self, client_socket, data):
        msg = pickle.dumps(Message(self.name, data))
        package = len(msg).to_bytes(4, 'big') + msg
        client_socket.sendall(package)

    def receive(self, client_socket):
        data_length = int.from_bytes(client_socket.recv(4), 'big')
        if not data_length:
            return None
        return pickle.loads(self._receive_n_len(client_socket, data_length))

    def _receive_n_len(self, client_socket, n):
        # Receive n bytes of data, handling incomplete packet reception
        data = bytearray()
        while len(data) < n:
            packet = client_socket.recv(n - len(data))
            if not packet:
                break
            data.extend(packet)
        return data


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="RM")
    parser.add_argument('--port', '-p', default=5009,
                        type=int, help="Port number for RM")
    args = parser.parse_args()

    rm = RM(port=args.port)
    rm.run()
