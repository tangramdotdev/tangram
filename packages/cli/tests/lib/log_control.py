import base64
import concurrent.futures
import http.client
import json
import socket
import subprocess
import sys
import time

case, socket_path, tangram, url, parent, data_path, compaction = sys.argv[1:]
with open(data_path) as file:
    finished = json.load(file)
finished.pop("log", None)
data = dict(finished)
for key in ("children", "error", "exit", "finished_at", "output"):
    data.pop(key, None)
data["status"] = "started"


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
    arg = {**arg, "start": True}
    sock, response = open_stream("/processes/control", arg, token, messages)
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


def chunk(stream, position, value, stream_position=0):
    return {"kind": "write", "value": {"kind": "chunk", "value": {
        "bytes": base64.b64encode(value).decode(), "combined_position": position,
        "stream": stream, "stream_position": stream_position, "timestamp": 0,
    }}}


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
        "process.control.output", "process.control.finish",
    )}
    finish = {"kind": "finish", "value": {"data": finished}}
    request_id = "early-finish"
    message = ("request", {"id": request_id, "arg": finish})
    with concurrent.futures.ThreadPoolExecutor() as executor:
        pending = executor.submit(connect, {"parent": parent, "lease": "test", "data": data}, None, [message])
        try:
            # The request body reaches the server before Output is returned.
            for name in watches:
                checkpoint("wait", name, watches[name], 0)
            checkpoint("unwatch", "process.control.output", watches.pop("process.control.output"))
            sock, response, output = pending.result(timeout=10)
            token = output["sync"]
            assert token, output
            receive(response, "ack", request_id)
            close(sock, response)
        finally:
            for name, watch in watches.items():
                checkpoint("unwatch", name, watch)

    # Reconnect after losing an acknowledged request, preserving the sync token for the eventual push.
    arg = {"id": output["process"]["node"], "lease": "test", "sync": token}
    sock, response, reconnected = connect(arg, output["token"])
    assert reconnected["sync"] == token, reconnected
    request(sock, response, request_id, finish)
    close(sock, response)
    process = json.loads(subprocess.check_output(command + ["get", arg["id"]], timeout=10))
    value = process.get("output") or process["error"]
    referent = value["value"] if isinstance(value, dict) else value
    assert "?" not in referent, value
    assert process["status"] == "finished", process


def reconnect():
    sock, response, output = connect({"parent": parent, "lease": "test", "data": data})
    id, token = output["process"]["node"], output["token"]
    arg = {"id": id, "lease": "test"}
    write = chunk("stdout", 0, b"hello\n")
    send(sock, "request", {"id": "first", "arg": write})
    receive(response, "ack", "first")
    close(sock, response)

    # Replay a write whose receipt was acknowledged but whose result was lost.
    sock, response, _ = connect(arg, token)
    assert request(sock, response, "first", write)["value"] == {"closed": False, "length": 6}

    # An unfinished log must not clip a reverse cursor to the currently persisted prefix.
    result = subprocess.run([tangram, "--url", url, "log", "--position", "12", "--length=-12", id], capture_output=True, timeout=10)
    assert result.returncode == 0 and result.stdout == b"" and result.stderr == b"", result

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
        "combined_position": 12, "stream_positions": {"stderr": 6, "stdout": 6},
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


def reordered():
    command = [tangram, "--url", url]
    for last_stream in ("stdout", "stderr"):
        sock, response, output = connect({"parent": parent, "lease": "test", "data": data})
        id, token = output["process"]["node"], output["token"]
        reader = None
        try:
            # Replay the last chunk before the missing initial and middle chunks.
            write = chunk(last_stream, 2, b"C", 2 if last_stream == "stdout" else 0)
            request(sock, response, "c", write)
            result = subprocess.run(command + ["log", "--timeout", "0", id], capture_output=True, timeout=10)
            assert result.returncode == 0 and result.stdout == b"" and result.stderr == b"", result
            request(sock, response, "a", chunk("stdout", 0, b"A"))

            # Snapshot reads stop at the gap, including when both entries have the same stream.
            for streams in ([], ["--stream", "stdout"]):
                result = subprocess.run(command + ["log", "--timeout", "0", *streams, id], capture_output=True, timeout=10)
                assert result.returncode == 0 and result.stdout == b"A" and result.stderr == b"", result

            # A reverse read must not return a prefix separated from its cursor by a gap.
            result = subprocess.run(command + ["log", "--timeout", "0", "--position", "3", "--length=-3", id], capture_output=True, timeout=10)
            assert result.returncode == 0 and result.stdout == b"" and result.stderr == b"", result

            # A live reader retains its position and resumes when the middle chunk arrives.
            reader = open_stream(f"/processes/{id}/stdio/read", {"streams": "stdout,stderr"})
            reader_sock, reader_response = reader
            event, message = read(reader_response)
            assert event == "notification" and message["kind"] == "chunk", (event, message)
            assert base64.b64decode(message["value"]["bytes"]) == b"A", message
            send(reader_sock, "notification", {"consumed": 1})
            close(sock, response)
            sock, response, _ = connect({"id": id, "lease": "test"}, token)
            request(sock, response, "b", chunk("stdout", 1, b"B", 1))
            request(sock, response, "finish", {"kind": "finish", "value": {"data": finished}})
            request(sock, response, "end", {"kind": "write", "value": {"kind": "end", "value": {
                "combined_position": 3, "stream_positions": {"stderr": 0 if last_stream == "stdout" else 1, "stdout": 3 if last_stream == "stdout" else 2},
            }}})
            output = {"stdout": b"A", "stderr": b""}
            position = 1
            while True:
                event, message = read(reader_response)
                if event == "response" and message["kind"] in ("end", "limit", "timeout"):
                    send(reader_sock, "ack", None)
                    break
                assert event == "notification" and message["kind"] == "chunk", (event, message)
                value = message["value"]
                assert value["combined_position"] == position, value
                assert value["stream_position"] == len(output[value["stream"]]), value
                value_bytes = base64.b64decode(value["bytes"])
                output[value["stream"]] += value_bytes
                position += len(value_bytes)
                send(reader_sock, "notification", {"consumed": position})
            expected_stdout = b"ABC" if last_stream == "stdout" else b"AB"
            expected_stderr = b"" if last_stream == "stdout" else b"C"
            assert output == {"stdout": expected_stdout, "stderr": expected_stderr}, output
            result = subprocess.run(command + ["log", "--no-timeout", id], capture_output=True, timeout=10)
            assert result.returncode == 0, result.stderr
            assert result.stdout == expected_stdout and result.stderr == expected_stderr, result
            result = subprocess.run(command + ["log", "--no-timeout", "--position", "3", "--length=-3", id], capture_output=True, timeout=10)
            assert result.returncode == 0, result.stderr
            assert result.stdout == expected_stdout and result.stderr == expected_stderr, result
        finally:
            if reader is not None:
                close(*reader)
            close(sock, response)


def growing():
    for added in (46, 96):
        sock, response, output = connect({"parent": parent, "lease": "test", "data": data})
        id = output["process"]["node"]
        request(sock, response, "prefix", chunk("stdout", 0, b"abcd"))
        reader_sock, reader_response = open_stream(f"/processes/{id}/stdio/read", {"streams": "stdout", "position": "end.96", "length": -99, "size": 1})
        try:
            # Wait for seek resolution before changing the log, without depending on timing.
            event, message = read(reader_response, positions=True)
            assert event == "notification" and message == {"kind": "position", "value": {"length": -99, "position": 100}}, (event, message)
            request(sock, response, "suffix", chunk("stdout", 4, b"x" * added, 4))
            request(sock, response, "finish", {"kind": "finish", "value": {"data": finished}})
            request(sock, response, "end", {"kind": "write", "value": {"kind": "end", "value": {
                "combined_position": 4 + added, "stream_positions": {"stderr": 0, "stdout": 4 + added},
            }}})
            if added < 96:
                event, message = read(reader_response, positions=True)
                assert event == "notification" and message == {"kind": "position", "value": {"length": -(3 + added), "position": 4 + added}}, (event, message)
            chunks = []
            while True:
                event, message = read(reader_response)
                if event == "response" and message["kind"] in ("end", "limit", "timeout"):
                    send(reader_sock, "ack", None)
                    break
                assert event == "notification" and message["kind"] == "chunk", (event, message)
                chunks.append(base64.b64decode(message["value"]["bytes"]))
                send(reader_sock, "notification", {"consumed": sum(map(len, chunks))})
            assert b"".join(chunks) == b"x" * added + b"dcb", chunks
        finally:
            close(reader_sock, reader_response)
            close(sock, response)


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
