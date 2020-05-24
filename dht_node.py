import hashlib
import argparse
import socket
import sys

charset = "UTF-8"


def hex(x):
    h = hashlib.sha1()
    h.update(repr(x).encode(charset))
    return h.digest()


def hex_id(host, port):
    port = socket.htonl(port)
    h = hashlib.sha1()
    h.update(repr(host).encode(charset))
    h.update(repr(port).encode(charset))
    return h.hexdigest()


class Node:
    def __init__(self, host, port):
        self.host = host
        self.port = int(port)
        self.id = hex_id(host, port)
        self.kv = {}
    
    def put(key, val):
        if val == None or val == "":
            try:
                del self.kv[key]
            except KeyError:
                pass
        else:
            self.kv[key] = val
    
    def get(key):
        if key in self.kv:
            return self.kv[key]
        else:
            return None


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
        print (repr(e))
        sys.exit()
    return nodes


def parse_request(r):
    r = r.decode(charset).split(',')
    host = str(r[0])
    port = int(r[1])
    hop = int(r[2])
    op = str(r[3])
    k = str(r[4])
    v = str(r[5])
    return host, port, hop, op, k, v


hostfile, linenum = _parse_args()
nodes = _load_data_to_nodes(hostfile)
myself = Node(nodes[linenum][0], nodes[linenum][1])

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((myself.host, myself.port))
while True:
    print ("waiting")
    req, address = sock.recvfrom(1024)
    print('request received:', req, ' from ', address)
    cli_addr, cli_port, hops, operation, key, value = parse_request(req)
    resp = 'ok'
    sent = sock.sendto(str.encode(resp), address)
    print ('send response', resp)
