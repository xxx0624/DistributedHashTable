import hashlib
import argparse

charset = "UTF-8"


def hex(x):
    h = hashlib.sha1()
    h.update(repr(x).encode(charset))
    return h.digest()


class Node:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.id = hex(ip + str(port))
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


hostfile, linenum = _parse_args()
