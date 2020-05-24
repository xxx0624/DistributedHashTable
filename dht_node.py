import hashlib
import argparse
import socket
import sys

charset = "UTF-8"


def hex(x):
    h = hashlib.sha1()
    h.update(repr(x).encode(charset))
    return h.hexdigest()


def hex_id(host, port):
    h = hashlib.sha1()
    h.update(repr(host).encode(charset))
    h.update(repr(port).encode(charset))
    return h.hexdigest()


class Node:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.id = hex_id(host, port)
        self.kv = {}
    
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
                nodes.append((host, port))
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


# return the successor(Assume there is one node at least in nodes list)
def find_successor(nodes, key):
    all_id = []
    for node in nodes:
        all_id.append(hex_id(node[0], node[1]))
    all_id.append(hex(key))
    all_id.sort()
    # find the index of key
    index_of_key = -1
    key_id = hex(key)
    for idx, val in enumerate(all_id):
        if val == key_id:
            index_of_key = idx
            break
    # check if the key id matches one node exactly
    def find_node_by_id(nodes, id):
        for node in nodes:
            if id == hex_id(node[0], node[1]):
                return node[0], node[1]
        return None, None
    pre_index_to_check = index_of_key - 1
    next_index_to_check = index_of_key + 1
    if pre_index_to_check < 0:
        pre_index_to_check = len(all_id) - 1
    if next_index_to_check >= len(all_id):
        next_index_to_check = 0
    if key_id == all_id[pre_index_to_check]:
        return find_node_by_id(nodes, all_id[pre_index_to_check])
    if hex(key) == all_id[next_index_to_check]:
        return find_node_by_id(nodes, all_id[next_index_to_check])
    # not exist the same id. try to find the next one node in the sorted list
    next_index = 1 + index_of_key
    if next_index >= len(all_id):
        next_index = 0
    return find_node_by_id(nodes, all_id[next_index])


hostfile, linenum = _parse_args()
nodes = _load_data_to_nodes(hostfile)
myself = Node(nodes[linenum][0], nodes[linenum][1])

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
    target_host, target_port = find_successor(nodes, key)
    if target_host == myself.host and target_port == myself.port:
        resp = ""
        if op.lower() == "get":
            resp = myself.get(key)
        else:
            myself.put(key, value)
            resp = "put successful"
        sock.sendto(str.encode(resp), (cli_host, cli_port))
        print ('send response:', resp, ' to ', (cli_host, cli_port))
    else:
        # forward to other node
        def build_req(host, port, hops, op, k, v):
            return host + ',' + str(port) + ',' + str(hops) + ',' + op + ',' + k + ',' + v
        hops += 1
        msg = build_req(cli_host, cli_port, hops, op, key, value)
        print ('forward ', msg, ' to ', (target_host, target_port))
        sock.sendto(str.encode(msg), (target_host, target_port))
