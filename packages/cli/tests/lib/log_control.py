import base64
import concurrent.futures
import http.client
import json
import queue
import socket
import subprocess
import sys
import threading
import time
import urllib.parse

import token_body

from process_id import process_id

case, socket_path, tangram, url, parent, data_path = sys.argv[1:]
with open(data_path) as file:
    finished = json.load(file)
finished.pop("log", None)
data = dict(finished)
for key in ("children", "error", "exit", "finished_at", "output"):
    data.pop(key, None)
data["status"] = "started"
lease = "06d0000001r010000000000000"

# Name the parent in a shortcut proof, whose tokens the server does not check for the root.
shortcut = {"parent": {"id": parent, "token": ""}, "sandbox": {"token": ""}}


def open_stream(path, arg, token=None, messages=()):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect(socket_path)
    headers = f"POST {path} HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nX-Tg-Arg-In-Body: true\r\n"
    if token:
        headers += f"Authorization: Bearer {token}\r\n"
    sock.sendall((headers + "\r\n").encode())
    payload = json.dumps(arg).encode()
    length = len(payload)
    prefix = bytearray()
    while length >= 128:
        prefix.append((length & 127) | 128)
        length >>= 7
    prefix.append(length)
    content = bytes(prefix) + payload
    sock.sendall(f"{len(content):x}\r\n".encode() + content + b"\r\n")
    for event, value in messages:
        send(sock, event, value)
    response = http.client.HTTPResponse(sock)
    response.begin()
    assert response.status == 200, (response.status, response.read())
    return sock, response


def connect(arg, token=None, messages=()):
    value = {key: value for key, value in arg.items() if key not in ("id", "location")}
    mode = "start" if "data" in value else "resume"
    arg = {key: value for key, value in arg.items() if key in ("id", "location")}
    # The client names the process, so a start without an ID gets a new one.
    arg.setdefault("id", process_id())
    arg["mode"] = {"kind": mode, **value}
    sock, response = open_stream("/processes/control", arg, token, messages)
    length, shift = 0, 0
    while True:
        byte = response.read(1)[0]
        length |= (byte & 127) << shift
        if byte < 128:
            break
        shift += 7
    header = json.loads(response.read(length))
    header["process"] = {"node": arg["id"]}
    control = Control(sock, response, value.get("data", data))
    return control, header


class Control:
    """A process control stream that answers the server's get requests while the driver waits on other work."""

    def __init__(self, sock, response, data):
        self.data = data
        self.events = queue.Queue()
        self.lock = threading.Lock()
        self.response = response
        self.sock = sock
        threading.Thread(target=self.serve, daemon=True).start()

    def serve(self):
        try:
            while True:
                event, value = read(self.response)
                if event == "request" and value["arg"]["kind"] == "get":
                    output = {"kind": "get", "value": {"data": self.data}}
                    self.send("response", {"id": value["id"], "output": output})
                    continue
                self.events.put((event, value))
        except Exception as error:
            self.events.put(("closed", repr(error)))

    def send(self, event, value):
        with self.lock:
            send(self.sock, event, value)

    def receive(self, event, id):
        while True:
            kind, data = self.events.get(timeout=10)
            assert kind not in ("closed", "error"), data
            if kind == event and data["id"] == id:
                return data

    def request(self, id, arg, acknowledge=True):
        if arg["kind"] == "finish":
            self.data = arg["value"]["data"]
        self.send("request", {"id": id, "arg": arg})
        output = self.receive("response", id)
        assert output.get("error") is None, output
        if acknowledge:
            self.send("ack", {"id": id})
        return output["output"]

    def close(self):
        self.sock.shutdown(socket.SHUT_RDWR)
        close(self.sock, self.response)


def send(sock, event, value):
    content = ("event: " + event + "\ndata: " + json.dumps(value) + "\n\n").encode()
    sock.sendall(f"{len(content):x}\r\n".encode() + content + b"\r\n")


def read(response, positions=False):
    event, data = None, None
    while True:
        line = response.readline().decode().strip()
        if not line:
            assert event is not None, "the response stream ended"
            value = json.loads(data)
            if not positions and event == "notification" and value.get("kind") == "position":
                event, data = None, None
                continue
            return event, value
        key, value = line.split(":", 1)
        if key == "event":
            event = value.strip()
        elif key == "data":
            data = value.strip()


def chunk(stream, position, value, stream_position=0):
    return {"kind": "chunk", "value": {
        "bytes": base64.b64encode(value).decode(), "combined_position": position,
        "stream": stream, "stream_position": stream_position, "timestamp": 0,
    }}


def end(position, stderr, stdout):
    return {"kind": "end", "value": {
        "combined_position": position, "stream_positions": {"stderr": stderr, "stdout": stdout},
    }}


def open_writer(id, token):
    return open_stream(f"/processes/{id}/stdio/write", {"streams": "stdout,stderr"}, token)


def write(id, token, data):
    sock, response = open_writer(id, token)
    try:
        return write_with(sock, response, data)
    finally:
        close(sock, response)


def write_with(sock, response, data):
    send(sock, "data", data)
    while True:
        kind, value = read(response)
        assert kind != "error", value
        if kind == "output":
            return value


def close(sock, response):
    response.close()
    sock.close()


def early_finish():
    command = [tangram, "--url", url]

    def checkpoint(operation, name, *args):
        result = subprocess.run(command + ["checkpoint", operation, name, *map(str, args)], capture_output=True, timeout=10)
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout) if result.stdout else None

    watches = {name: checkpoint("watch", name)["watch"] for name in (
        "process.control.header", "process.control.finish",
    )}
    finish = {"kind": "finish", "value": {"data": finished}}
    request_id = "early-finish"
    messages = [
        ("request", {"id": request_id, "arg": finish}),
    ]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        pending = executor.submit(connect, {"shortcut": shortcut, "lease": lease, "data": data}, None, messages)
        try:
            # The request body reaches the server before the header is returned.
            for name in watches:
                checkpoint("wait", name, watches[name], 0)
            checkpoint("unwatch", "process.control.header", watches.pop("process.control.header"))
            control, output = pending.result(timeout=10)
            assert "token" in output, output
            control.receive("ack", request_id)
            control.close()
            # End the synthetic writer's empty log.
            write(output["process"]["node"], output["token"], end(0, 0, 0))
        finally:
            for name, watch in watches.items():
                checkpoint("unwatch", name, watch)

    # Reconnect after losing an acknowledged Finish; the server owns the sync.
    arg = {"id": output["process"]["node"], "lease": lease}
    # Resume must not enter process initialization again.
    watch = checkpoint("watch", "process.control.index.started")["watch"]
    try:
        control, reconnected = connect(arg, output["token"])
    finally:
        checkpoint("unwatch", "process.control.index.started", watch)
    control.request(request_id, finish)
    control.close()
    process = json.loads(subprocess.check_output(command + ["get", arg["id"]], timeout=10))
    value = process.get("output") or process["error"]
    if isinstance(value, dict):
        options = value.get("options") or {}
        tokens = [token for entry in (options.get("tokens") or {}).values() for token in entry]
    else:
        params = urllib.parse.parse_qs(urllib.parse.urlsplit(value).query)
        tokens = [token for name, values in params.items() if name.startswith("tokens[") for token in values]
    bodies = [token_body.body(token) for token in tokens]
    assert any(body["resource"].startswith("syn_") and "sync_read" in body["permissions"] for body in bodies), value
    assert process["status"] == "finished", process


def reconnect():
    control, output = connect({"shortcut": shortcut, "lease": lease, "data": data})
    id, token = output["process"]["node"], output["token"]
    arg = {"id": id, "lease": lease}
    first = chunk("stdout", 0, b"hello\n")
    assert write(id, token, first) == {"closed": False, "position": 6}

    # Rewrite a chunk after losing its position.
    assert write(id, token, first) == {"closed": False, "position": 6}

    # An unfinished log must not clip a reverse cursor to the currently persisted prefix.
    result = subprocess.run([tangram, "--url", url, "log", "--position", "12", "--length=-12", id], capture_output=True, timeout=10)
    assert result.returncode == 0 and result.stdout == b"" and result.stderr == b"", result

    second = chunk("stderr", 6, b"world\n")
    assert write(id, token, second) == {"closed": False, "position": 12}

    # Rewrite a persisted chunk without duplicating it.
    assert write(id, token, second) == {"closed": False, "position": 12}

    # The writer can supply EOF before Finish is processed.
    assert write(id, token, end(12, 6, 6)) == {"closed": True, "position": 12}

    control.request("finish", {"kind": "finish", "value": {"data": finished}})
    control.close()

    command = [tangram, "--url", url]
    # Repeat End after losing its response, after Finish.
    assert write(id, token, end(12, 6, 6)) == {"closed": True, "position": 12}
    result = subprocess.run(command + ["log", "--no-timeout", id], capture_output=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert result.stdout == b"hello\n", result.stdout
    assert result.stderr == b"world\n", result.stderr


def reordered():
    command = [tangram, "--url", url]
    for last_stream in ("stdout", "stderr"):
        control, output = connect({"shortcut": shortcut, "lease": lease, "data": data})
        id, token = output["process"]["node"], output["token"]
        reader = None
        try:
            # Replay the last chunk before the missing initial and middle chunks.
            write(id, token, chunk(last_stream, 2, b"C", 2 if last_stream == "stdout" else 0))
            result = subprocess.run(command + ["log", "--timeout", "0", id], capture_output=True, timeout=10)
            assert result.returncode == 0 and result.stdout == b"" and result.stderr == b"", result
            write(id, token, chunk("stdout", 0, b"A"))

            # Snapshot reads stop at the gap, including when both entries have the same stream.
            for streams in ([], ["--stream", "stdout"]):
                result = subprocess.run(command + ["log", "--timeout", "0", *streams, id], capture_output=True, timeout=10)
                assert result.returncode == 0 and result.stdout == b"A" and result.stderr == b"", result

            # A reverse read must not return a prefix separated from its cursor by a gap.
            result = subprocess.run(command + ["log", "--timeout", "0", "--position", "3", "--length=-3", id], capture_output=True, timeout=10)
            assert result.returncode == 0 and result.stdout == b"" and result.stderr == b"", result

            # A live reader retains its position and resumes when the middle chunk arrives.
            reader = open_stream(f"/processes/{id}/stdio/read", {"streams": "stdout,stderr"})
            _, reader_response = reader
            event, message = read(reader_response)
            assert event == "notification" and message["kind"] == "chunk", (event, message)
            assert base64.b64decode(message["value"]["bytes"]) == b"A", message
            write(id, token, chunk("stdout", 1, b"B", 1))
            control.request("finish", {"kind": "finish", "value": {"data": finished}})
            write(id, token, end(3, 0 if last_stream == "stdout" else 1, 3 if last_stream == "stdout" else 2))
            output = {"stdout": b"A", "stderr": b""}
            position = 1
            while True:
                event, message = read(reader_response)
                if event == "response" and message["kind"] in ("end", "limit", "timeout"):
                    break
                assert event == "notification" and message["kind"] == "chunk", (event, message)
                value = message["value"]
                assert value["combined_position"] == position, value
                assert value["stream_position"] == len(output[value["stream"]]), value
                value_bytes = base64.b64decode(value["bytes"])
                output[value["stream"]] += value_bytes
                position += len(value_bytes)
            expected_stdout = b"ABC" if last_stream == "stdout" else b"AB"
            expected_stderr = b"" if last_stream == "stdout" else b"C"
            assert output == {"stdout": expected_stdout, "stderr": expected_stderr}, output
            result = subprocess.run(command + ["log", "--no-timeout", id], capture_output=True, timeout=10)
            assert result.returncode == 0, result.stderr
            assert result.stdout == expected_stdout and result.stderr == expected_stderr, result
            result = subprocess.run(command + ["log", "--no-timeout", "--position", "3", "--length=-3", id], capture_output=True, timeout=10)
            assert result.returncode == 0, result.stderr
            expected_reverse_stdout = b"CBA" if last_stream == "stdout" else b"BA"
            assert result.stdout == expected_reverse_stdout and result.stderr == expected_stderr, result
        finally:
            if reader is not None:
                close(*reader)
            control.close()


def growing():
    for added in (46, 96):
        control, output = connect({"shortcut": shortcut, "lease": lease, "data": data})
        id, token = output["process"]["node"], output["token"]
        write(id, token, chunk("stdout", 0, b"abcd"))
        reader = open_stream(f"/processes/{id}/stdio/read", {"streams": "stdout", "position": "end.96", "length": -99, "size": 1})
        reader_response = reader[1]
        try:
            # Wait for seek resolution before changing the log, without depending on timing.
            event, message = read(reader_response, positions=True)
            assert event == "notification" and message == {"kind": "position", "value": {"length": -99, "position": 100}}, (event, message)
            write(id, token, chunk("stdout", 4, b"x" * added, 4))
            control.request("finish", {"kind": "finish", "value": {"data": finished}})
            write(id, token, end(4 + added, 0, 4 + added))
            if added < 96:
                event, message = read(reader_response, positions=True)
                assert event == "notification" and message == {"kind": "position", "value": {"length": -(3 + added), "position": 4 + added}}, (event, message)
            chunks = []
            while True:
                event, message = read(reader_response)
                if event == "response" and message["kind"] in ("end", "limit", "timeout"):
                    break
                assert event == "notification" and message["kind"] == "chunk", (event, message)
                chunks.append(base64.b64decode(message["value"]["bytes"]))
            assert b"".join(chunks) == b"x" * added + b"dcb", chunks
        finally:
            close(*reader)
            control.close()


if case == "early_finish":
    early_finish()
elif case == "growing":
    growing()
elif case == "reconnect":
    reconnect()
elif case == "reordered":
    reordered()
else:
    raise ValueError(f"unknown case: {case}")
