import hashlib
import argparse
import socket
import sys

charset = "UTF-8"
M = 4
Chords = 2 ** M


def bytes_to_int(bytes):
    result = 0
    for b in bytes:
        result = result * 256 + int(b)
        result %= Chords
    return result


def hex(x):
    h = hashlib.sha1()
    h.update(repr(x).encode(charset))
    return bytes_to_int(h.hexdigest())


def hex_id(host, port):
    h = hashlib.sha1()
    h.update(repr(host).encode(charset))
    h.update(repr(port).encode(charset))
    return bytes_to_int(h.hexdigest())


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
        build_finger(nodes)
        return self.fingers[0]

    
    def closest_preceding_finger(self, key_id):
        for i in range(M - 1, -1, -1):
            if self.id < key_id:
                if self.id < self.fingers[i] and self.fingers[i] < key_id:
                    return self.fingers[i]
            else:
                if self.fingers[i] > self.id or self.fingers[i] < key_id:
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
                host, port = line.strip().split(' ')
                port = int(port)
                nodes.append(Node(host, port))
    except Exception as e:
        print ('loading file error')
        print (repr(e))
        sys.exit()
    return nodes


def parse_request(r):
    r = r.split(',')
    host = str(r[0])
    port = int(r[1])
    hop = int(r[2])
    op = str(r[3])
    k = str(r[4])
    v = str(r[5])
    return host, port, hop, op, k, v


hostfile, linenum = _parse_args()
nodes = _load_data_to_nodes(hostfile)
myself = Node(nodes[linenum].host, nodes[linenum].port)
nodes.sort(key = lambda x : x.id)
myself.build_finger(nodes)

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((myself.host, myself.port))
while True:
    print ("waiting")
    req, cli_address = sock.recvfrom(1024)
    req = req.decode(charset)
    print('request received:', req, ' from ', cli_address)
    cli_host, cli_port, hops, op, key, value = parse_request(req)
    if hops == 1:
        cli_host = cli_address[0]
        cli_port = cli_address[1]
    
    key_id = hex(id)
    cur_node = myself
    while key_id <= hex(cur_node.id) or key_id > hex(cur_node.get_successor(nodes).id):
        cur_node = cur_node.closest_preceding_finger(key_id)
    if cur_node == myself:
        resp = ""
        if op.lower() == "get":
            resp = myself.get(key)
        else:
            myself.put(key, value)
            resp = "put successful"
        sock.sendto(str.encode(resp), (cli_host, cli_port))
        print ('send response:', resp, ' to ', (cli_host, cli_port))
    else:
        #forward to other node
        def build_req(host, port, hops, op, k, v):
            return host + ',' + str(port) + ',' + str(hops) + ',' + op + ',' + k + ',' + v
        hops += 1
        msg = build_req(cli_host, cli_port, hops, op, key, value)
        print ('forward ', msg, ' to ', (target_host, target_port))
        sock.sendto(str.encode(msg), (cur_node.host, cur_node.port))
    
    # target_host, target_port = find_successor(nodes, key)
    # if target_host == myself.host and target_port == myself.port:
    #     resp = ""
    #     if op.lower() == "get":
    #         resp = myself.get(key)
    #     else:
    #         myself.put(key, value)
    #         resp = "put successful"
    #     sock.sendto(str.encode(resp), (cli_host, cli_port))
    #     print ('send response:', resp, ' to ', (cli_host, cli_port))
    # else:
    #     # forward to other node
    #     def build_req(host, port, hops, op, k, v):
    #         return host + ',' + str(port) + ',' + str(hops) + ',' + op + ',' + k + ',' + v
    #     hops += 1
    #     msg = build_req(cli_host, cli_port, hops, op, key, value)
    #     print ('forward ', msg, ' to ', (target_host, target_port))
    #     sock.sendto(str.encode(msg), (target_host, target_port))
