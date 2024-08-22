# 18749 Build Reliable Distribute System - MILESTONE 1

## Goal
- Get the client-server application running. 
- Demonstrate that the LFD works. 
- Support configurable heartbeat frequencies at the LFD. 

## Components
- [ ] server replica (S1)
- [ ] independent clients (not replicated) 
- [ ] LFD (LFD1)

## Rubric
- [ ] (1) Launch the server replica, S1.
- [ ] (2) Launch the LFD1, on the same machine, with some default heartbeat_freq. It should start heartbeating the server replica periodically, and in a continuous loop. The periodic messages from LFD1→S1 must be visible and printed on the console window for the LFD1. The sending and receipt of these messages must be printed on the console window for server S1. 
- [ ] (3) Launch the 3 independent clients, C1, C2 and C3, one at a time.
- [ ] (4) The 3 clients should start sending messages to the server replica, S1. You can manually send messages from each client. 
- [ ] (5) The sending of the request messages from C1→S1 , C2→S1, C3→S1 must be printed on the console windows for each of the clients, C1, C2, and C3, respectively. The receipt of these request messages must also be visible on the console window for server S1.
- [ ] (6) The sending of the reply messages S1→C1, S1→C2, S1→C3 must be printed on the console windows for each of the clients, C1, C2, and C3, respectively. The sending of these reply messages must also be visible on the console window for server S1. 
- [ ] (7) Show that the server S1 is “stateful,” and that its state is being modified by the receipt of the clients’ messages. On the console window, print the value of my_state before returning the reply to the client.
- [ ] (8) Try killing (Control-C) the replica, S1, and you should see a heartbeat fail (i.e., timeout expiration) at the LFD1. The LFD1 should be able to detect that the replica, S1, died. Print this failed-heartbeat message on the console window of LFD1.
- [ ] (9) Restart the whole system (repeat steps above) with a different heartbeat_freq for the LFD1. 

