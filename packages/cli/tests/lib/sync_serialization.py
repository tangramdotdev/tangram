import http.client
import json
import socket
import sys

socket_path, group_id, id_hex, input_encoding, output_encoding = sys.argv[1:]


def varint(value):
    output = bytearray()
    while value >= 128:
        output.append((value & 127) | 128)
        value >>= 7
    output.append(value)
    return bytes(output)


def frame(data):
    return varint(len(data)) + data


def read_varint(reader):
    value = 0
    shift = 0
    while True:
        byte = reader.read(1)
        assert byte, "the response ended inside a length"
        value |= (byte[0] & 127) << shift
        if byte[0] < 128:
            return value
        shift += 7
        assert shift < 70, "invalid frame length"


def event(kind, value):
    return f"event: {kind}\ndata: {json.dumps(value)}\n\n".encode()


native_type = "application/vnd.tangram.sync"
content_types = {"json": "text/event-stream", "tangram": native_type}
headers = {
    "Accept": content_types[output_encoding],
    "Content-Type": content_types[input_encoding],
}
if input_encoding == "json":
    arg = json.dumps({"ancestors": "never", "get": "foo"}).encode()
    body = frame(arg)
    body += event("put", {"kind": "node", "value": {"kind": "group", "value": {
        "id": group_id, "name": "foo", "specifier": "foo",
    }}})
    body += event("put", {"kind": "end"})
    body += event("get", {"kind": "end"})
    body += event("end", None)
    headers["x-tg-arg-in-body"] = "true"
    path = "/sync"
else:
    node = bytes.fromhex("0b 01 0b 00 0b 00 0a 03 00 07 14")
    node += bytes.fromhex(id_hex)
    node += bytes.fromhex("01 06 03 66 6f 6f 03 06 03 66 6f 6f")
    body = frame(node)
    body += bytes.fromhex("05 0b 01 0b 03 00 05 0b 00 0b 03 00 03 0b 02 00")
    path = "/sync?ancestors=never&get=foo"

connection = http.client.HTTPConnection("localhost", timeout=20)
connection.sock = socket.socket(socket.AF_UNIX)
connection.sock.settimeout(20)
connection.sock.connect(socket_path)
connection.request("POST", path, body, headers)
response = connection.getresponse()
assert response.status == 200, (response.status, response.read())
assert response.getheader("Content-Type") == content_types[output_encoding]
header = response.read(read_varint(response))
if output_encoding == "json":
    assert "sync" in json.loads(header), header
    output = response.read().decode()
    assert "event: error" not in output, output
    assert "event: end\n" in output, output
else:
    assert header[0] == 10, header
    assert response.read().endswith(bytes.fromhex("03 0b 02 00"))
connection.close()
