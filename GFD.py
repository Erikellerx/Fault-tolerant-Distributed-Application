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
import threading

class GFD:
    def __init__(self, host='', port=5005):

        # initialization of GFD
        self.name = "GFD"
        self.primary_server = None #To keep track of the primary server
        self.membership = [] #List to keep track of members

        self.backup_freq = 5 

        self.rm_port = 5009

        # Set up socket for GFD
        self.GFD_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.GFD_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.GFD_socket.bind((host, port))
        self.GFD_socket.listen(5) # Listens for up to 5 connections

        # Set up logging
        ch = logging.StreamHandler()
        ch.setFormatter(CustomFormatter())
        self.logger = logging.getLogger()
        self.logger.addHandler(ch)
        self.logger.setLevel(logging.DEBUG)

        # Logging the IP address of the GFD
        self.logger.info(socket.gethostbyname(socket.gethostname()))
    
    # Run the GFD in active mode
    def run_active(self):
        while True:
            self.logger.info(f"Current availale Server number {len(self.membership)}: { [ f'S{p[-1]}' for p in self.membership]}")
            try:
                # Accepting connections from LFD
                lfd_socket, addr = self.GFD_socket.accept()
                # Receiving message from LFD
                message = self.receive(lfd_socket)
            except KeyboardInterrupt:
                lfd_socket.close()
                exit()
            except socket.timeout:
                continue

            # Handle received message from LFD
            if not message:
                print(f"Received bad message")

            if message.data == "On":
                self.logger.heartbeat(f"LFD{message.name[-1]} is online")
                if message.name not in self.membership:
                    # Log the addition of a new replica
                    self.logger.receive(f"LFD add replica S{message.name[-1]}")
                    
                    # If there are already other servers in the membership list
                    if self.membership:
                        for i, server in enumerate(self.membership):
                            backup_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            host, port = server.split(':')
                            backup_socket.connect((host, int(port)))
                            self.send(backup_socket,'checkpoint_time', backupinfo = [message.name], ready = i==len(self.membership)-1)
                            backup_socket.close()
                        # log the checkpointing of the new server
                        self.logger.state(f"S{message.name[-1]} starts to checkpointing")
                        
                    else:
                        first_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        host, port = message.name.split(':')
                        first_server_socket.connect((host, int(port)))
                        self.send(first_server_socket,'init')
                        self.logger.state(f"S{message.name[-1]} starts to initializing")
                        first_server_socket.close()


                    # Add the server to the membership list
                    self.membership.append(message.name)

                # Acknowledge the processing of the "on" message
                self.send(lfd_socket, "processed")
            elif message.data == "Off":
                self.logger.heartbeat(f"LFD{message.name[-1]} is online")
                if message.name in self.membership:
                    self.logger.warning(f"S{message.name[-1]} down")
                    self.membership.remove(message.name)
                self.send(lfd_socket, "processed")      
            elif message.data == "request":
                self.send(lfd_socket, list(self.membership))
            else:
                print(message.data, "possible error")

            
            rm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            rm_socket.connect(("localhost", self.rm_port))
            self.send(rm_socket, "update_membership", backupinfo = self.membership)
            rm_socket.close()


            lfd_socket.close()
        
    # Passive mode of GFD
    def run_passive(self):
        # Create threads for receiving and sending
        threads = [threading.Thread(target=self.receive_thread_func), threading.Thread(target=self.send_thread_func)]
        for thread in threads:
            thread.daemon = True
            thread.start()

        try:
            while True:
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("Closing the GFD...")
            sys.exit()
            self.alive = False

        for thread in threads:
            thread.join()

    # Handle receiving messages in a separate thread
    def receive_thread_func(self):
        while True:
            self.logger.info(f"Current available Server number {len(self.membership)}: {[f'S{p[-1]}' for p in self.membership]}")
            if self.primary_server:
                self.logger.info(f"Primary server is {self.primary_server}")
            try:
                lfd_socket, addr = self.GFD_socket.accept()
                message = self.receive(lfd_socket)
            except KeyboardInterrupt:
                lfd_socket.close()
                exit()
            except socket.timeout:
                continue

            if not message:
                print(f"Received bad message")

            if message.data == "request":
                if self.primary_server:
                    self.send(lfd_socket, [self.primary_server])
                else:
                    self.send(lfd_socket, list(self.membership))
                continue
                
            receive_host, receive_port = message.name.split(':')
            receive_name = f"S{receive_port[-1]}"
            #print(f"message data is {message.data}")
            if message.data == "On":
                self.logger.heartbeat(f"LFD{message.name[-1]} is online")
                if message.name not in [replica for replica in self.membership]:
                    self.logger.receive(f"LFD add replica S{message.name[-1]} with address {receive_host}:{receive_port}")
                    self.add_replica(receive_name, receive_host, receive_port)
                    #initialize the new server
                    first_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    host, port = message.name.split(':')
                    first_server_socket.connect((host, int(port)))
                    self.send(first_server_socket,'init')
                    self.logger.state(f"S{message.name[-1]} starts to initializing")
                    first_server_socket.close()
                self.send(lfd_socket, "processed")

            elif message.data == "Off":
                self.logger.heartbeat(f"LFD{message.name[-1]} is online")
                if message.name in [replica for replica in self.membership]:
                    self.logger.warning(f"LFD remove replica S{message.name[-1]}")
                    self.remove_replica(message.name)
                self.send(lfd_socket, "processed")
            else:
                print(message.data, "possible error")


            rm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            rm_socket.connect(("localhost", self.rm_port))
            self.send(rm_socket, "update_membership", backupinfo = self.membership)
            rm_socket.close()
            
            lfd_socket.close()
                         
    # Handle sending messages in a separate thread
    def send_thread_func(self):
        while True:
            # Wait for a specified interval
            time.sleep(self.backup_freq)
            backup_info = [replica for replica in self.membership if replica != self.primary_server]
            if backup_info:
                try:
                    backup_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    host, port = self.primary_server.split(':')
                    backup_socket.connect((host, int(port)))
                    self.send(backup_socket,"checkpoint_time", backupinfo = backup_info)
                    backup_socket.close()
                except Exception as e:
                    self.logger.error(f"Error sending to primary server {self.primary_server}: {e}")

    # Add a new replica to the membership list
    def add_replica(self, replica_name, server_address, port):
        if server_address + ":" + port not in self.membership:
            self.membership.append(server_address + ":" + port)
            if not self.primary_server:
                self.primary_server = server_address + ":" + port
                self.logger.info(f"Primary server is {self.primary_server[0]}")

    # Remove a replica from the membership list
    def remove_replica(self, replica_name):
        for replica in self.membership:
            if replica == replica_name:
                self.membership.remove(replica)
                if self.primary_server == replica:
                    if self.membership:
                        self.primary_server = self.membership[0]
                    else:
                        self.primary_server = None

    # Send a message to a client socket
    def send(self, client_socket, data, backupinfo = None, ready=True): 
        msg = pickle.dumps(Message(self.name, data, server_name = backupinfo, ready=ready))
        package = len(msg).to_bytes(4, 'big') + msg
        client_socket.sendall(package)


    # Receive a message from a client socket
    def receive(self, client_socket):
        data_length  = int.from_bytes(client_socket.recv(4), 'big')
        if not data_length:
            return None
        return pickle.loads(self._receive_n_len(client_socket, data_length))


    # Receive a message of a specified length from a client socket
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

    parser = argparse.ArgumentParser(description = "GFD")
    parser.add_argument('--mode', '-m', default = 'active', type = str, help = "fault mode")
    
    gfd = GFD()
    
    if parser.parse_args().mode == "active":
        gfd.run_active()
    elif parser.parse_args().mode == "passive":
        gfd.run_passive()
    else:
        print("mode error")

    