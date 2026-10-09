import http.client
import json
import socket
import sys

socket_path, credentials_path, sandbox, expected = sys.argv[1:]
with open(credentials_path) as file:
    credentials = json.load(file)
runner = credentials["data"]["id"]
token = credentials["token"]["token"]
sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.settimeout(10)
sock.connect(socket_path)
headers = (
    "POST /runners/control HTTP/1.1\r\nHost: localhost\r\n"
    "Accept: text/event-stream\r\nContent-Type: text/event-stream\r\n"
    "Transfer-Encoding: chunked\r\nX-Tg-Arg-In-Body: true\r\n"
    f"Authorization: Bearer {token}\r\n\r\n"
)
sock.sendall(headers.encode())


def chunk(content):
    sock.sendall(f"{len(content):x}\r\n".encode() + content + b"\r\n")


def send(event, value):
    chunk(f"event: {event}\ndata: {json.dumps(value)}\n\n".encode())


capacity = {
    "available": {"cpu": {"dedicated": 0, "shared": 0}, "memory": 0},
    "cpu_oversubscription": 4,
    "shared_cpu_limit": 0,
    "total": {"cpu": {"dedicated": 0, "shared": 0}, "memory": 0},
}
arg = {"heartbeat": {"capacity": capacity, "index": 0, "sandboxes": []}, "host": "test", "id": runner, "scheduler_ttl": 300}
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
assert response.status == 200, (response.status, response.read())
length, shift = 0, 0
while True:
    byte = response.read(1)[0]
    length |= (byte & 127) << shift
    if byte < 128:
        break
    shift += 7
output = json.loads(response.read(length))
assert (sandbox in output["sandboxes"]) == (expected == "true"), output


def destroy(id):
    send("request", {"id": id, "arg": {"kind": "destroy_sandbox", "value": {"sandbox": sandbox}}})
    event, data = None, None
    while True:
        line = response.readline().decode().strip()
        if line:
            key, value = line.split(":", 1)
            if key == "event":
                event = value.strip()
            elif key == "data":
                data = json.loads(value)
            continue
        assert event is not None, "the control stream ended"
        assert event != "error", data
        if event == "response" and data["id"] == id:
            assert data.get("error") is None, data
            send("ack", {"id": id})
            assert data["output"]["kind"] == "destroy_sandbox", data
            return data["output"]["value"]["destroyed"]
        event, data = None, None


assert destroy("destroy") == (expected == "true")
assert destroy("repeat") is False
response.close()
sock.close()
