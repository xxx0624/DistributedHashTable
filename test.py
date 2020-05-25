import hashlib

charset = 'utf-8'

def bytes_to_int(bytes):
    result = 0
    for b in bytes:
        result = result * 256 + int(b)
    return result

def hex(x):
    h = hashlib.sha1()
    h.update(repr(x).encode(charset))
    return h.digest()


def hex_id(host, port):
    h = hashlib.sha1()
    h.update(repr(host).encode(charset))
    h.update(repr(port).encode(charset))
    return h.hexdigest()

x = hex("ab")
print (type(x))
print (x)
print (bytes_to_int(x))