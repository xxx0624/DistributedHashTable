M = 5
Chords = 2 ** M


def bytes_to_int(bytes):
    return int.from_bytes(bytes, byteorder='big', signed=False) % Chords

ss = ["1", "12", "123", "fsadfasd", "13241324fqef;k;l;l"]
for s in ss:
    print bytes_to_int(str.encode(s))