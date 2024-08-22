import socket
import time
import pickle
from message import *
import struct
import argparse
from datetime import datetime
from CustomLog import CustomFormatter
import logging

# Local Fault Detector
class LFD:
    def __init__(self, name = "LFD", server_host = '', gfd_host = '', server_port=6001, heartbeat_freq=3):
        # Initialize LFD settings
        self.server_host = socket.gethostbyname(socket.gethostname())
        self.server_port = server_port
        self.heartbeat_freq = heartbeat_freq
        self.name = name

        self.GFD_host = gfd_host
        self.GFD_port = 5005

        ch = logging.StreamHandler()
        ch.setFormatter(CustomFormatter())
        self.logger = logging.getLogger()
        self.logger.addHandler(ch)
        self.logger.setLevel(logging.DEBUG)

        # Name for logging purposes, identifying the server monitored by this LFD.
        self.server_name = f"S{str(server_port)[-1]}"

    def run(self):
        server_status = "Off"
        while True:
            #ping server
            try:
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect((self.server_host, self.server_port))
                self.send(client_socket, "heartbeat")
                message = self.receive(client_socket)  
                if not message:
                    self.logger.warning(f"Received bad message from")          
                elif message.data == "heartbeat_received":
                    self.logger.heartbeat(f"{self.server_name} is online")
                    server_status = "On"
                client_socket.close()
            except socket.error:
                self.logger.error(f"Failed to send heartbeat port: {self.server_port}. Server might be down!")
                server_status = "Off"



            #ping GFD
            try:
                GFD_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                GFD_socket.connect((self.GFD_host, self.GFD_port))
                self.send(GFD_socket, server_status)
                message = self.receive(GFD_socket)
                if not message:
                    self.logger.warning(f"Received bad message")             
                elif message.name == "GFD" and message.data == "processed":
                    self.logger.heartbeat(f"heartbeat_received from {message.name}")
                GFD_socket.close()
            except socket.error:
                self.logger.error(f"Failed to receive heartbeat from: {self.GFD_port}. GFD might be down!")

            time.sleep(self.heartbeat_freq)


    # Send message to a client socket
    def send(self, client_socket, data):
        msg = pickle.dumps(Message(f"{client_socket.getsockname()[0]}:{self.server_port}", data))
        package = len(msg).to_bytes(4, 'big') + msg
        client_socket.sendall(package)


    # Receive message from a client socket
    def receive(self, client_socket):
        data_length  = int.from_bytes(client_socket.recv(4), 'big')
        if not data_length:
            return None
        return pickle.loads(self._receive_n_len(client_socket, data_length))


    # Receive a fixed length of data from a client socket
    def _receive_n_len(self, client_socket, n):
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
    parser = argparse.ArgumentParser(description = "LFD")
    parser.add_argument('--name', '-n', default = 'LFD', type = str, help = "name of this lfd")
    parser.add_argument('--server_host', '-so', default = '', type = str, help = "server address")
    parser.add_argument('--GFD_host', '-go', default = '', type = str, help = "server address")
    parser.add_argument('--port', '-p', default = 6001, type = int, help = "server port")
    parser.add_argument('--heartbeat_freq', '-hb', default = 3, type = int, help = "heartbeat_freq")
    args = parser.parse_args()

    lfd = LFD(args.name ,args.server_host, args.GFD_host, server_port=args.port, heartbeat_freq=args.heartbeat_freq)
    lfd.run()