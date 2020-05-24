import hashlib
import argparse

charset = "UTF-8"


def hex(x):
    h = hashlib.sha1()
    h.update(repr(x).encode(charset))
    return h.digest()


def hex_id(h, p):
    p = socket.htonl(p)
    h = hashlib.sha1()
    h.update(repr(a).encode(charset))
    h.update(repr(p).encode(charset))
    return h.hexdigest()


class Node:
    def __init__(self, host, port):
        self.host = ip
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
    file = open(hostfile, 'r')
    for line in file.readlines():
        host, port = line.split()
        port = int(port)
        nodes.append((host, port))
    return nodes


def parse_request(r):
    a = str(r[0])
    p = int(r[1])
    h = int(r[2])
    op = str(r[3])
    k = str(r[4])
    v = str(r[5])
    return a, p, h, op, k, v


hostfile, linenum = _parse_args()
nodes = _load_data_to_nodes(hostfile)
myself = Node(nodes[linenum][0], nodes[linenum][1])

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((self.host, port))
while True:
    message, address = sock.recvfrom(1024)
    request = pickle.loads(message)
    print('\nreceived {} bytes from {}'.format(len(message), address))
    print('request received : ' + str(request))
    cli_addr, cli_port, hops, operation, key, value = parse_request(request)
