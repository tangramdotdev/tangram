import base64
import http.client
import json
import socket
import subprocess
import sys
import time
import urllib.parse

socket_path, tangram, url, parent, data_path, compaction = sys.argv[1:]
with open(data_path) as file:
    finished = json.load(file)
finished.pop("log", None)
data = dict(finished)
for key in ("children", "error", "exit", "finished_at", "output"):
    data.pop(key, None)
data["status"] = "started"


def flatten(value, prefix=""):
    if isinstance(value, dict):
        return [pair for name, item in value.items() for pair in flatten(item, f"{prefix}[{name}]" if prefix else name)]
    if isinstance(value, list):
        return [pair for index, item in enumerate(value) for pair in flatten(item, f"{prefix}[{index}]")]
    if value is None:
        return []
    return [(prefix, str(value).lower() if isinstance(value, bool) else str(value))]


def connect(arg, token=None):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect(socket_path)
    path = "/processes/control?" + urllib.parse.urlencode(flatten(arg))
    headers = f"POST {path} HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n"
    if token:
        headers += f"Authorization: Bearer {token}\r\n"
    sock.sendall((headers + "\r\n").encode())
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
    return sock, response, json.loads(response.read(length))


def send(sock, event, value):
    content = ("event: " + event + "\ndata: " + json.dumps(value) + "\n\n").encode()
    sock.sendall(f"{len(content):x}\r\n".encode() + content + b"\r\n")


def read(response):
    event, data = None, None
    while True:
        line = response.readline().decode().strip()
        if not line:
            assert event is not None, "the response stream ended"
            return event, json.loads(data)
        key, value = line.split(":", 1)
        if key == "event":
            event = value.strip()
        elif key == "data":
            data = value.strip()


def receive(response, event, id):
    while True:
        kind, data = read(response)
        assert kind != "error", data
        if kind == event and data["id"] == id:
            return data


def request(sock, response, id, arg, acknowledge=True):
    send(sock, "request", {"id": id, "arg": arg})
    output = receive(response, "response", id)
    assert output.get("error") is None, output
    if acknowledge:
        send(sock, "ack", {"id": id})
    return output["output"]


def chunk(stream, position, value):
    return {"kind": "write", "value": {"kind": "chunk", "value": {
        "bytes": base64.b64encode(value).decode(), "combined_position": position,
        "stream": stream, "stream_position": 0, "timestamp": 0,
    }}}


def close(sock, response):
    response.close()
    sock.close()


sock, response, output = connect({"parent": parent, "lease": "test", "data": data})
id, token = output["id"], output["token"]
arg = {"id": id, "lease": "test"}
write = chunk("stdout", 0, b"hello\n")
send(sock, "request", {"id": "first", "arg": write})
receive(response, "ack", "first")
close(sock, response)

# Replay a write whose receipt was acknowledged but whose result was lost.
sock, response, _ = connect(arg, token)
assert request(sock, response, "first", write)["value"] == {"closed": False, "length": 6}
write = chunk("stderr", 6, b"world\n")
assert request(sock, response, "second", write, False)["value"] == {"closed": False, "length": 6}
close(sock, response)

# Replay a persisted write without duplicating it.
sock, response, _ = connect(arg, token)
assert request(sock, response, "second", write)["value"] == {"closed": False, "length": 6}
request(sock, response, "finish", {"kind": "finish", "value": {"data": finished}})
close(sock, response)

# The writer supplies the final positions on a fresh connection.
end = {"kind": "write", "value": {"kind": "end", "value": {
    "position": 12, "stderr_position": 6, "stdout_position": 6,
}}}
sock, response, _ = connect(arg, token)
assert request(sock, response, "end", end, False)["value"] == {"closed": True, "length": 0}
close(sock, response)

command = [tangram, "--url", url]
if compaction == "true":
    deadline = time.monotonic() + 10
    while True:
        process = json.loads(subprocess.check_output(command + ["get", id], timeout=10))
        if process.get("log") is not None:
            break
        assert time.monotonic() < deadline, "the log did not compact"
        time.sleep(0.05)

# Repeat End after losing its response, including after compaction.
sock, response, _ = connect(arg, token)
assert request(sock, response, "end", end)["value"] == {"closed": True, "length": 0}
close(sock, response)
result = subprocess.run(command + ["log", "--no-timeout", id], capture_output=True, timeout=10)
assert result.returncode == 0, result.stderr
assert result.stdout == b"hello\n", result.stdout
assert result.stderr == b"world\n", result.stderr
