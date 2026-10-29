import http.client
import json
import socket
import sys

from process_id import derived_sandbox_id, sandbox_id

# Open a sandbox control stream with the given token and arg, and print the response header, or exit with the error response.
socket_path, token, arg_path = sys.argv[1:]
with open(arg_path) as file:
    arg = json.load(file)
# An arg that names a deriving runner gets an ID that the runner derives with its proof, and any other arg without an ID gets a new one.
deriver = arg.pop("deriver", None)
if deriver is not None:
    arg["id"], arg["proof"] = derived_sandbox_id(deriver)
arg.setdefault("id", sandbox_id())
sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.settimeout(10)
sock.connect(socket_path)
headers = (
    "POST /sandboxes/control HTTP/1.1\r\nHost: localhost\r\n"
    "Accept: text/event-stream\r\nContent-Type: text/event-stream\r\n"
    "Transfer-Encoding: chunked\r\nX-Tg-Arg-In-Body: true\r\n"
    f"Authorization: Bearer {token}\r\n\r\n"
)
sock.sendall(headers.encode())


def chunk(content):
    sock.sendall(f"{len(content):x}\r\n".encode() + content + b"\r\n")


payload = json.dumps(arg).encode()
length = len(payload)
prefix = bytearray()
while length >= 128:
    prefix.append((length & 127) | 128)
    length >>= 7
prefix.append(length)
chunk(bytes(prefix) + payload)
response = http.client.HTTPResponse(sock)
response.begin()
if response.status != 200:
    sys.stderr.write(f"{response.status} {response.read().decode()}\nsandbox {arg['id']}\n")
    sys.exit(1)
length, shift = 0, 0
while True:
    byte = response.read(1)[0]
    length |= (byte & 127) << shift
    if byte < 128:
        break
    shift += 7
header = json.loads(response.read(length))
print(json.dumps(header))
