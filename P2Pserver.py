import socket
import threading
import json
import binascii  # for converting hex to bytes
import hashlib

# The class describes the P2P server
class P2PServer:
  def __init__(self, host, port) -> None:
    # here, we specify using IPv4 format and TCP protocol
    self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.server.bind((host, port))
    self.server.listen()

    # store all the peers in the dict
    # format: {peer_id: {'address': address, 'files': {filename: [chunk_ids]}}}
    self.peers = {}
    self.peer_counter = 0

 # register a peer to the list
  def register_peer(self, address, listening_port) -> int:
      print(f"Registering peer address: {address}, listening port: {listening_port}")
      combined_address = (address[0], listening_port)  # Combine IP and listening port
      
      existing_peer_id = None
      for peer_id, data in self.peers.items():
        if data['addr'] == combined_address:
          existing_peer_id = peer_id

      if existing_peer_id:
          print(f"[Warning]: Peer already exists, ID {existing_peer_id[0]}")
          return existing_peer_id[0]
      else:
          self.peers[self.peer_counter] = {'addr': combined_address, 'files': {}}  # filename - own chunks
          current_peer_id = self.peer_counter
          self.peer_counter += 1
          print(f"Successfully registered peer with ID {current_peer_id}")
          return current_peer_id

  #  receive commands from the peer, and proceed to process the command
  def handle_peer(self, peer_socket, addr) -> None:
    # step 1: register the peer if it's not in the peer list
    # Check if this address is already registered
    try:
      print(addr, self.peers)
      existing_peer_id = [peer_id for peer_id, data in self.peers.items() if data['addr'] == addr]
      peer_id = -1
      if existing_peer_id:
        peer_id = existing_peer_id[0]
      else:
        # If it's a new peer, expect the initial 'register_peer' message
        initial_msg = peer_socket.recv(1024).decode()
        initial_command = json.loads(initial_msg)
        if initial_command['type'] == 'register_peer':
          listening_port = initial_command['listening_port']
          peer_id = self.register_peer(addr, listening_port)
        else:
          print("Expected 'register_peer' message, but received a different type. Disconnecting.")
          return

      if peer_id < 0:
        return

      # step 2:  
      while True:
        # receive command in JSON format
        print("Waiting to receive message...")
        msg = peer_socket.recv(1024).decode()
        print(f"Received raw message: {msg}")
        if not msg:
          print("Message is empty, breaking out of loop.")
          break
        
        # retrieve the commands
        command = json.loads(msg)
        cmd_type = command['type']

        if cmd_type == 'register_file_chunk':
          # Extract filename, chunk_id, and chunk_data from the message
          filename = command['filename']
          chunk_id = command['chunk_id']
          chunk_data_hex = command['chunk_data']
          
          
          # Convert the hex data back to bytes
          chunk_data_bytes = binascii.unhexlify(chunk_data_hex)
          
          # Update the server's record for this peer and filename
            # Update the server's record for this peer and filename
          if peer_id in self.peers:
            if 'files' not in self.peers[peer_id]:
              self.peers[peer_id]['files'] = {}
            if filename not in self.peers[peer_id]['files']:
              self.peers[peer_id]['files'][filename] = {
                'chunks': {},
                'total_chunks': 0,  # Initialize total_chunks for this file
              }

            self.peers[peer_id]['files'][filename]['chunks'][chunk_id] = (chunk_data_bytes)

            self.peers[peer_id]['files'][filename]['total_chunks'] = max(
              self.peers[peer_id]['files'][filename]['total_chunks'], chunk_id
            )  # Update total_chunks


        elif cmd_type == "register_file_hash":
          filename = command['filename']
          hash = command['hash']
          self.peers[peer_id]['files'][filename]['hash'] = hash

        elif cmd_type == "request_file_hash":
          filename = command['filename']
          for peer in self.peers:
            if filename in self.peers[peer]['files']:
              hash_info = {
                'hash': self.peers[peer]['files'][filename]['hash']
              }
              peer_socket.send(json.dumps(hash_info).encode())
              break


        elif cmd_type == "request_file_list":
          # Send only the filenames and the total number of chunks for each file, not the actual chunk data
          file_list = {}

          for peer in self.peers:
            for filename in self.peers[peer]['files']:
              file_list[filename]={'total_chunks': self.peers[peer]['files'][filename]['total_chunks']}
              
          peer_socket.send(json.dumps(file_list).encode())


        elif cmd_type == "request_file_info":
          cmd_filename = command['filename']
          # Send both the peer IDs and the specific chunk IDs they have for the requested file,
          # as well as the total number of chunks for that file
          file_info = {
              peer: {
                'chunks': list(data['files'].get(cmd_filename, {}).get('chunks', {}).keys()),
                'total_chunks': data['files'].get(cmd_filename, {}).get('total_chunks', 0)
              }
              for peer, data in self.peers.items() if cmd_filename in data['files']
          }
          peer_socket.send(json.dumps(file_info).encode())

        # send requested peer address based on the id
        elif cmd_type == "request_peer_address":
          requested_peer_id = int(command["peer_id"]) # convert from str to int
          if requested_peer_id in self.peers:
            peer_address = self.peers[requested_peer_id]["addr"]
            response = {"type": "peer_address_response", "address": peer_address}
            peer_socket.send(json.dumps(response).encode())
          else:
            print(f"Peer ID {requested_peer_id} not found.") 

        # when client have new chunk, it notifies server, so the server will update its' list of chunks for that peers as well 
        elif cmd_type == 'new_chunk':
          # Extract filename and chunk_id from the message
          filename = command['filename']
          chunk_id = command['chunk_id']

          # Update the server's record for this peer and filename
          if peer_id in self.peers:
              if 'files' not in self.peers[peer_id]:
                self.peers[peer_id]['files'] = {}
              if filename not in self.peers[peer_id]['files']:
                self.peers[peer_id]['files'][filename] = {
                  'chunks': {},
                  'total_chunks': 0
                }

              self.peers[peer_id]['files'][filename]['chunks'][chunk_id] = None
              self.peers[peer_id]['files'][filename]['total_chunks'] += 1

              print(f"Updated server record for peer {peer_id}, filename {filename}, chunk {chunk_id}")
          else:
              print(f"Peer ID {peer_id} not found. Cannot update chunk information.")


    except Exception as e:
      print(f"An error occurred while handling peer {peer_id}: {e}")
    finally:
      # Remove the disconnected peer's information
      if peer_id in self.peers:
        del self.peers[peer_id]
        print(f"Successfully removed information about peer {peer_id}")
    
  #   del self.peers
  def run(self) -> None:
    print("Server started...")
    while True:
      peer_socket, address = self.server.accept()
      peer_thread = threading.Thread(target=self.handle_peer, args=(peer_socket, address))
      peer_thread.start()

if __name__ == "__main__":
  server = P2PServer("127.0.0.1", 8000)
  server.run()