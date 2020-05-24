import argparse
import socket

def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("host", type=str, nargs=1, help="the host")
    parser.add_argument("port", type=int, nargs=1, help="the port")
    parser.add_argument("op", type=str, nargs=1, help="the operation name GET/PUT")
    parser.add_argument("key", type=str, nargs=1, help="key")
    parser.add_argument("value", type=str, nargs='*', help="value", default=[""])
    args = parser.parse_args()
    return args.host[0], args.port[0], args.op[0], args.key[0], args.value[0]

'''
host & port is the client address, both of them will be speicified in the first server node
hops: total hops at some stage
op: operation GET / PUT
k: key
v: value
'''
def build_req(host, port, hops, op, k, v):
    return host + ',' + str(port) + ',' + str(hops) + ',' + op + ',' + k + ',' + v


host, port, op, k, v = _parse_args()
msg = build_req("", 0, 1, op, k, v)
print ('send request:', msg, ' to ', (host, port))
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.sendto(str.encode(msg), (host, port))
resp, server_address = sock.recvfrom(1024)
print ('response received:', resp, ' from ', server_address)