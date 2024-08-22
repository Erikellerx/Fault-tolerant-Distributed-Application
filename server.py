import socket
import argparse
import pickle
from message import *
from enum import Enum
import struct
from datetime import datetime
import threading
import time
import sys
from CustomLog import CustomFormatter
import logging


class Serverstatus(Enum):
    """Worker status."""

    DEAD = 0 
    READY = 1
    BUSY = 2


class Server:
    def __init__(self, host, port):
        # Initialize server state and properties
        self.my_state = 0
        self.checkpoint = 0
        self.hst = host
        self.prt = port
        # Setting up server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((host, port))
        self.server_socket.listen(5)
        self.backup_server = []

        socket.setdefaulttimeout(6001)

        self.ready = False
        self.high_watermark_request_num = []
        self.buffer = 0


        ch = logging.StreamHandler()
        ch.setFormatter(CustomFormatter())
        self.logger = logging.getLogger()
        self.logger.addHandler(ch)
        self.logger.setLevel(logging.DEBUG)

        self.logger.info(f"[State: {self.my_state}] Server started and listening on port: {port}")
        self.logger.info(socket.gethostbyname(socket.gethostname()))

    def run(self):
        while True:
            # Accept client connection
            client_socket, addr = self.server_socket.accept()
            message = self.receive(client_socket)
            # Check if the message is valid
            if not message:
                self.logger.warning(f"Received bad message")
                continue

            # Process different types of messages
            if message.data == "init":
                self.ready = True
                self.logger.state(f"[S{str(self.prt)[-1]}] is initialized")
                continue

            if message.data == "heartbeat":
                self.logger.heartbeat(f"[State: {self.my_state}] Received heartbeat from LFD{str(self.prt)[-1]}")
                self.send(client_socket, "heartbeat_received")
            elif message.data == "checkpoint_time":
                for backup_info in message.server_name:
                    try:
                        checkpoint_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        host, port = backup_info.split(":")
                        checkpoint_socket.connect((host, int(port)))
                        self.send(checkpoint_socket, "checkpoint_msg", ready = message.ready)
                        self.logger.state(f"[Primary S{str(self.prt)[-1]}] send checkpoint_count {self.checkpoint} and state {self.my_state} to backup server {backup_info[-1]}") 
                        checkpoint_socket.close()
                    except Exception as e:
                        self.logger.error(f"Error connecting to backup server {backup_info[1]}: {e}")
                self.checkpoint += 1
            # Checkpoint message received by a backup server
            elif message.data == "checkpoint_msg":
                self.logger.state(f"[Backup S{str(self.prt)[-1]}] received checkpoint_count: {message.checkpoint}")
                self.logger.state(f"[Backup S{str(self.prt)[-1]}] received checkpoint state: {message.state}")
                self.high_watermark_request_num += [message.state]
                self.my_state = max(self.high_watermark_request_num) + self.buffer
                self.checkpoint = message.checkpoint 
                self.ready = message.ready
                self.logger.state(f"[S{str(self.prt)[-1]}] is ready/up-to-date")
            # Processing client request
            elif self.ready:
                self.logger.receive(f"[S{str(self.prt)[-1]}-State: {self.my_state}] Client:{message.name}, message received: {message.data}")
                self.send(client_socket, f"{self.hst}:{self.prt}, message_received")
                self.my_state += 1
            else:
                self.logger.receive(f"[S{str(self.prt)[-1]}-State: {self.my_state}] Client:{message.name}, message received: {message.data} (Server is still recovering)")
                self.buffer += 1


            client_socket.close()



    # Send a message to a client socket
    def send(self, client_socket, data, ready = True):
        msg = pickle.dumps(Message(f"{self.hst}:{self.prt}", data, checkpoint=self.checkpoint, state = self.my_state, ready = ready))
        package = len(msg).to_bytes(4, 'big') + msg
        client_socket.sendall(package)


    # Receive a message from a client socket
    def receive(self, client_socket):
        data_length  = int.from_bytes(client_socket.recv(4), 'big')
        if not data_length:
            return None
        return pickle.loads(self._receive_n_len(client_socket, data_length))


    # Receive a specified length of data from a client socket
    def _receive_n_len(self, client_socket, n):
        data = bytearray()
        while len(data) < n:
            try:
                packet = client_socket.recv(n - len(data))
            except socket.timeout:
                self.logger.error("receive timeout")
                continue
            if not packet:
                break
            data.extend(packet)
        return data




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = "Server")
    parser.add_argument('--name', '-n', default = 'S1', type = str, help = "server name")
    parser.add_argument('--host', '-o', default = '', type = str, help = "server address")
    parser.add_argument('--port', '-p', default = 6001, type = int, help = "server port")
    args = parser.parse_args()

    server = Server(args.host, args.port)
    server.run()