import hashlib
import argparse
import socket
import sys

charset = "UTF-8"
M = 5
Chords = 2 ** M


def bytes_to_int(bytes):
    return int.from_bytes(bytes, byteorder='big', signed=False) % Chords


def hex(x):
    h = hashlib.sha1()
    h.update(repr(x).encode(charset))
    res = h.hexdigest()
    return bytes_to_int(str.encode(res))


def hex_id(host, port):
    h = hashlib.sha1()
    h.update(repr(host).encode(charset))
    h.update(repr(port).encode(charset))
    res = h.hexdigest()
    return bytes_to_int(str.encode(res))


class Node:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.id = hex_id(host, port)
        self.kv = {}
        self.fingers = []
        self.fingers_built = False
    
    def put(self, key, val):
        if val == None or val == "":
            try:
                del self.kv[key]
            except KeyError:
                pass
        else:
            self.kv[key] = val
    
    def get(self, key):
        if key in self.kv:
            return self.kv[key]
        else:
            return ""
    
    # nodes are a list of Node objects, which are sorted by the attribute 'id'
    def build_finger(self, nodes):
        if self.fingers_built == True:
            return
        self.fingers_built = True
        def _next_node(id, nodes):
            for index, node in enumerate(nodes):
                if id <= node.id:
                    return node
            return nodes[0]
        for i in range(M):
            self.fingers.append(_next_node((self.id + 2 ** i) % (Chords), nodes))
    

    def get_successor(self, nodes):
        self.build_finger(nodes)
        return self.fingers[0]

    
    def closest_preceding_finger(self, key_id):
        for i in range(M - 1, -1, -1):
            if self.id < key_id:
                if self.id < self.fingers[i] and self.fingers[i] < key_id:
                    return self.fingers[i]
            else:
                if self.fingers[i].id > self.id or self.fingers[i].id < key_id:
                    return self.fingers[i]
        return self

    
    def __eq__(self, other):
        if isinstance(other, Node):
            return self.host == other.host and self.port == self.port
        return False


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("hostfile", type=str, nargs=1, help="the hostfile path")
    parser.add_argument("linenum", type=int, nargs=1, help="the line number in host file")
    args = parser.parse_args()
    return args.hostfile[0], args.linenum[0]


def _load_data_to_nodes(hostfile):
    nodes = []
    try:
        with open(hostfile, 'rb') as file:
            for line in file.readlines():
                host, port = line.split()
                port = int(port)
                nodes.append(Node(host, port))
    except FileNotFoundError as e:
        print (hostfile, 'not exist')
        print (repr(e))
        sys.exit()
    except Exception as e:
        print (repr(e))
        sys.exit()
    return nodes


def parse_request(r):
    r = r.split(',')
    is_found = int(r[0])
    host = str(r[1])
    port = int(r[2])
    hop = int(r[3])
    op = str(r[4])
    k = str(r[5])
    v = str(r[6])
    return is_found, host, port, hop, op, k, v


hostfile, linenum = _parse_args()
nodes = _load_data_to_nodes(hostfile)
myself = Node(nodes[linenum].host, nodes[linenum].port)
nodes.sort(key = lambda x : x.id)
myself.build_finger(nodes)

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((myself.host, myself.port))
while True:
    print ("[Node_ID: %d]%s is waiting" % (myself.id, (myself.host, myself.port)))
    req, cli_address = sock.recvfrom(1024)
    req = req.decode(charset)
    is_found, cli_host, cli_port, hops, op, key, value = parse_request(req)
    key_id = hex(id)
    print ("Received request [op = %s, key = %s, val = %s, hops = %d, key_ID = %d] from %s" % (op, key, value, hops, key_id, cli_address))
    
    if hops == 1:
        # store the origin client address and put it in the request always
        cli_host = cli_address[0]
        cli_port = cli_address[1]
    
    cur_node = myself
    is_key_assigned_to_mysuccessor = False
    # check if the key can be assigned to my successor
    if cur_node.id < cur_node.get_successor(nodes).id:
        is_key_assigned_to_mysuccessor = (key_id > cur_node.id and key_id <= cur_node.get_successor(nodes).id)
    elif cur_node.id == cur_node.get_successor(nodes).id:
        is_key_assigned_to_mysuccessor = (cur_node.id == key_id)
    else:
        is_key_assigned_to_mysuccessor = (key_id > cur_node.id or key_id <= cur_node.get_successor(nodes).id)

    if is_found == 1:
        resp = ""
        if op.lower() == "get":
            resp = myself.get(key)
        else:
            myself.put(key, value)
            resp = "put successful"
        sock.sendto(str.encode(resp), (cli_host, cli_port))
        print ('Done. Send response[%s] to origin client %s:%d\n' % (resp, cli_host, cli_port))
        continue
    
    if is_key_assigned_to_mysuccessor:
        # the key can be assigned to my successor, and will let the successor process this msg
        is_found = 1
        cur_node = cur_node.get_successor(nodes)
    else:
        #the key can't be assigned to my successor, forward to other node
        cur_node = cur_node.closest_preceding_finger(key_id)
    def build_req(is_found, host, port, hops, op, k, v):
        return str(is_found) + ',' + host + ',' + str(port) + ',' + str(hops) + ',' + op + ',' + k + ',' + v
    hops += 1
    msg = build_req(is_found, cli_host, cli_port, hops, op, key, value)
    print ("Forward the request[%s] to %s:%d\n" % (msg, cur_node.host, cur_node.port))
    sock.sendto(str.encode(msg), (cur_node.host, cur_node.port))
