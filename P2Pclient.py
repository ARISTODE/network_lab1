import socket
import threading
import json
import os
import binascii

class P2PClient:
  def __init__(self, server_host, server_port) -> None:
   # Socket for listening to other peers
    self.listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.listen_sock.bind(("127.0.0.1", 0))  # Bind to any available port
    self.listening_port = self.listen_sock.getsockname()[1]  # Get the OS-assigned port

    print(f"Listening for incoming requests on port {self.listening_port}")

    # Start a thread to handle incoming requests
    threading.Thread(target=self.handle_incoming_requests, args=(self.listen_sock,)).start()

    # Socket for communicating with the server
    self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.server_address = (server_host, server_port)
    self.client.connect(self.server_address)  # Connect to the server

    # Register this client with its listening port as soon as it connects to the server
    register_message = {
      'type': 'register_peer',
      'listening_port': self.listening_port
    }
    self.client.send(json.dumps(register_message).encode())

    self.files = {}
    self.CHUNK_SIZE = 1024  # Size of each chunk in bytes

  # talk to server and register a file, along with the chunks
  def register_file(self, filepath):
    filename = os.path.basename(filepath)
    chunk_dict = {}  # Initialize an empty dictionary to store chunk IDs and their data
    try:
      with open(filepath, 'rb') as f:
          chunk_id = 1
          while True:
            chunk_data = f.read(self.CHUNK_SIZE)
            if not chunk_data:
                break

            # Convert binary data to a hex string for JSON serialization
            chunk_hex = chunk_data.hex()

            # Prepare the message for this chunk
            message = json.dumps({
                'type': 'register_file_chunk',
                'filename': filename,
                'chunk_id': chunk_id,
                'chunk_data': chunk_hex
            })

            # Send the message to the server
            self.client.send(message.encode())

            # Store the chunk locally (if needed)
            chunk_dict[chunk_id] = chunk_hex  # Store the hex data along with its ID in the dictionary
            chunk_id += 1

      self.files[filename] = chunk_dict
      print(f"Successfully registered file {filename} with {len(chunk_dict)} chunks.")
   
    except FileNotFoundError:
      print(f"File {filepath} not found.")
    except IOError:
      print(f"An error occurred while reading the file {filepath}.")

  # # Request a list of available files from the server
  def request_file_list(self):
    message = json.dumps({'type': 'request_file_list'})
    self.client.send(message.encode())
    response = self.client.recv(1024).decode()
    return json.loads(response)
  
  def request_file_info(self, filename):
    # Request information about which peers have chunks of a specific file
    message = json.dumps({'type': 'request_file_info', 'filename': filename})
    self.client.send(message.encode())
    response = self.client.recv(1024).decode()
    return json.loads(response)
  
  def download_file(self, filename):
    peer_info = self.request_file_info(filename)
    print(f"Peer info for {filename}: {peer_info}")

    # Determine which chunks are missing
    missing_chunks = set()
    total_chunks = 0
    for peer_data in peer_info.values():
      missing_chunks.update(peer_data['chunks'])
      total_chunks = max(total_chunks, peer_data['total_chunks'])  # Update the total number of chunks
    
    # Remove chunks that we already have
    if filename in self.files:
      existing_chunks = set(self.files[filename].keys())
      missing_chunks.difference_update(existing_chunks)

    # Download missing chunks from available peers
    if missing_chunks:
      for peer_id, peer_data in peer_info.items():
          available_chunks = missing_chunks.intersection(set(peer_data['chunks']))
          if available_chunks:
            peer_address = self.request_peer_address(peer_id)
            if not peer_address:
              print(f"Cannot find peer address {peer_id}")
            else:
              print(f"Start downloading from peer {peer_address}", available_chunks)
              download_thread = threading.Thread(target=self.download_from_peer, args=(peer_address, filename, available_chunks))
              download_thread.start()
              download_thread.join()

    # Validate if we downloaded all chunks
    if filename in self.files:
      all_chunks = self.files[filename]
      if len(all_chunks.keys()) == total_chunks:  # We have all the chunks
        print(f"Successfully downloaded all {total_chunks} chunks. Saving to file download_{filename}")
        self.reassemble_and_output_file(filename, all_chunks)
      else:
        print(f"Incomplete download. Got {len(all_chunks.keys())} out of {total_chunks} chunks.")
  
  # write all chunks to a local file
  def reassemble_and_output_file(self, filename, chunks):
    # Ensure the output directory exists
    if not os.path.exists("clientFiles"):
      os.makedirs("clientFiles")
    # Sort the chunks based on chunk_id to ensure correct order
    sorted_chunks = [data for chunk_id, data in sorted(chunks.items(), key=lambda x: x[0])]

    # Reassemble the file
    file_data = b''.join(chunk for chunk in sorted_chunks)  # Assuming chunks are stored as bytes

    # Write to output file
    output_filepath = os.path.join("clientFiles", f"downloaded_{filename}")
    with open(output_filepath, 'wb') as f:
      f.write(file_data)
    
    print(f"File reassembled and saved as {output_filepath}")

  # request server to send the peer address based on the id
  def request_peer_address(self, peer_id):
      # Create a socket object to connect to the server
      # Prepare the request message
      request = {
          "type": "request_peer_address",
          "peer_id": peer_id
      }

      # Send the request to the server
      self.client.send(json.dumps(request).encode())

      # Receive the response from the server
      response_data = self.client.recv(1024)
      response = json.loads(response_data.decode())

      # Extract the peer address from the response
      if "address" in response:
          return response["address"]
      else:
          print(f"Could not retrieve address for peer {peer_id}")
          return None  

  # download chunks from peers 
  def download_from_peer(self, peer_address, filename, chunks):
    # Connect to a peer and download specified chunks
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as peer_socket:
      try:
        peer_socket.connect(tuple(peer_address))
      except ConnectionRefusedError:
        print(f"Could not connect to peer at {peer_address}")
        return

      for chunk_id in chunks:
        message = json.dumps({'type': 'request_chunk', 'filename': filename, 'chunk_id': chunk_id})
        peer_socket.send(message.encode())

        # Receive the chunk from the peer
        chunk_data = peer_socket.recv(1024).decode()
        command = json.loads(chunk_data)
        if command['type'] == 'send_chunk':
            received_chunk_hex = command['chunk_data']

            # Convert hex to bytes
            received_chunk_bytes = bytes.fromhex(received_chunk_hex)

            # Store and process the received chunk
            if filename not in self.files:
                self.files[filename] = {}
            print(f"store chunk {chunk_id} for file {filename}")
            self.files[filename][chunk_id] = received_chunk_bytes

            # Notify the server that we have a new chunk
            self.notify_server_new_chunk(filename, chunk_id)
  
  #  notify server we have a new chunk
  def notify_server_new_chunk(self, filename, chunk_id):
    # Create a socket object to connect to the server
    # Prepare the notification message
    notification = {
        "type": "new_chunk",
        "filename": filename,
        "chunk_id": chunk_id
    }

    # Send the notification to the server
    self.client.send(json.dumps(notification).encode())


  # features for handling incoming requests
  def handle_incoming_requests(self, sock):
    sock.listen()  # Start listening for incoming connections
    while True:
      peer_socket, _ = sock.accept()
      threading.Thread(target=self.serve_chunk, args=(peer_socket,)).start()

  def serve_chunk(self, peer_socket):
      # Serve a requested chunk to a connected peer
      try:
        message = peer_socket.recv(1024).decode()
        command = json.loads(message)

        if command['type'] == 'request_chunk':
          filename = command['filename']
          chunk_id = int(command['chunk_id'])

            # Assume self.files contains a mapping from filenames to a list of chunks (or chunk data)
          if filename in self.files and chunk_id in self.files[filename]:
            chunk_data = self.files[filename][chunk_id]  # Replace with actual chunk data

            response = json.dumps({
                'type': 'send_chunk',
                'filename': filename,
                'chunk_id': chunk_id,
                'chunk_data': chunk_data
            })
            print(f"sending chunk {chunk_id}")
            peer_socket.send(response.encode())
          else:
            print(f"File {filename} or chunk {chunk_id} not found.")
      except Exception as e:
        print(f"An error occurred while serving chunk: {e}")
      finally:
        peer_socket.close()
  

  def run(self):
    while True:
      print("\nCommands:")
      print("1: Register a file")
      print("2: Request list of available files")
      print("3: Download a file")
      print("4: Adjust chunk size (default 1024)")
      print("5: Exit")
      
      choice = input("Enter your choice: ")
      
      if choice == '1':
        filepath = input("Enter the filepath to register: ")
        self.register_file(filepath)
          
      elif choice == '2':
        file_list = self.request_file_list()
        print("Available files:", file_list)
          
      elif choice == '3':
        filename = input("Enter the filename to download: ")
        peer_info = self.request_file_info(filename)
        print(f"Peer info for {filename}: {peer_info}")
        self.download_file(filename)

      elif choice == '4':
        new_chunk_size = input("Enter new chunk size: ")
        if not new_chunk_size.isnumeric():
          print("Please enter a valid chunk size")
          continue
        if int(new_chunk_size) <= 0:
          print("cannot set size less than 0")
          continue
        self.CHUNK_SIZE = int(new_chunk_size)

      elif choice == '5':
          print("Exiting...")
          break
          
      else:
          print("Invalid choice. Please try again.")

if __name__ == "__main__":
  client = P2PClient("127.0.0.1", 8000)
  client.run()