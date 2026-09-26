import base64
import concurrent.futures
import dataclasses
import errno
import http.client
import io
import json
import queue
import socket
import subprocess
import sys
import threading
import time
import urllib.parse

case, socket_path, tangram, url, port, source_url, directory = sys.argv[1:]
TTL = 2


# The control and sync protocols use Tangram serialization, including numeric enum and field IDs.
@dataclasses.dataclass
class Variant:
    id: int
    value: object = None


def varint(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def read_varint(reader):
    value, shift = 0, 0
    while True:
        byte = reader.read(1)
        assert byte, "the stream ended inside a varint"
        value |= (byte[0] & 127) << shift
        if byte[0] < 128:
            return value
        shift += 7
        assert shift < 70, "invalid varint"


def encode(value):
    if value is None:
        return b"\x00"
    if isinstance(value, bool):
        return b"\x01" + bytes([value])
    if isinstance(value, int):
        return b"\x02" + varint(value)
    if isinstance(value, str):
        data = value.encode()
        return b"\x06" + varint(len(data)) + data
    if isinstance(value, bytes):
        return b"\x07" + varint(len(value)) + value
    if isinstance(value, tuple):
        return b"".join(map(encode, value))
    if isinstance(value, list):
        return b"\x08" + varint(len(value)) + b"".join(map(encode, value))
    if isinstance(value, dict):
        return b"\x0a" + varint(len(value)) + b"".join(bytes([key]) + encode(item) for key, item in sorted(value.items()))
    if isinstance(value, Variant):
        return b"\x0b" + bytes([value.id]) + encode(value.value)
    raise ValueError(value)


def decode(reader):
    kind = reader.read(1)[0]
    if kind == 0:
        return None
    if kind == 1:
        return bool(reader.read(1)[0])
    if kind == 2:
        return read_varint(reader)
    if kind == 3:
        value = read_varint(reader)
        return (value >> 1) ^ -(value & 1)
    if kind in (6, 7):
        value = reader.read(read_varint(reader))
        return value.decode() if kind == 6 else value
    if kind == 8:
        return [decode(reader) for _ in range(read_varint(reader))]
    if kind == 9:
        return {decode(reader): decode(reader) for _ in range(read_varint(reader))}
    if kind == 10:
        return {reader.read(1)[0]: decode(reader) for _ in range(read_varint(reader))}
    if kind == 11:
        return Variant(reader.read(1)[0], decode(reader))
    raise ValueError(f"unsupported wire kind: {kind}")


ALPHABET = "0123456789abcdefghjkmnpqrstvwxyz"
BASE32 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"


def node_bytes(id):
    body = id[6:].translate(str.maketrans(ALPHABET, BASE32))
    body = base64.b32decode(body + "=" * (-len(body) % 8))
    kind = {"blb": 0, "pcs": 8}[id[:3]]
    return bytes([0, 0, kind, int(id[5])]) + body


def missing_id(index=0):
    body = base64.b32encode(index.to_bytes(32, "little")).decode().rstrip("=")
    return "blb_01" + body.translate(str.maketrans(BASE32, ALPHABET))


def subject(token):
    body = token.split(".")[1]
    body = json.loads(base64.b64decode(body + "=" * (-len(body) % 4)))
    return f"syncs.{body['resource']}.control"


def command(*args):
    return subprocess.check_output([tangram, "--url", url, *map(str, args)], timeout=15).decode().strip()


def source_blob(value):
    return subprocess.check_output([tangram, "--url", source_url, "put", f"tg.blob({json.dumps(value)})"], timeout=15).decode().strip()


def watch(name, **params):
    return json.loads(command("checkpoint", "watch", name, "--params", json.dumps(params)))["watch"]


def reached(name, watch, index=0):
    return json.loads(command("checkpoint", "wait", name, watch, str(index)))


def release(name, watch):
    command("checkpoint", "unwatch", name, watch)


class Sync:
    def __init__(self, arg=None, status=200):
        self.requests = set()
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.settimeout(10)
        self.socket.connect(socket_path)
        headers = ("POST /sync HTTP/1.1\r\nHost: localhost\r\n"
                   "Accept: application/vnd.tangram.sync\r\nContent-Type: application/vnd.tangram.sync\r\n"
                   "x-tg-arg-in-body: true\r\nTransfer-Encoding: chunked\r\n\r\n")
        self.socket.sendall(headers.encode())
        arg = json.dumps(arg or {}).encode()
        self.chunk(varint(len(arg)) + arg)
        self.response = http.client.HTTPResponse(self.socket)
        self.response.begin()
        assert self.response.status == status, (self.response.status, self.response.read())
        if status == 200:
            assert self.response.getheader("Content-Type") == "application/vnd.tangram.sync"
            assert self.response.getheader("x-tg-output-in-body") == "true"
            self.output = json.loads(self.response.read(read_varint(self.response)))
            self.sync = self.output["sync"]
            self.token = self.sync["options"]["tokens"]["local"][0]

    def chunk(self, data):
        self.socket.sendall(f"{len(data):x}\r\n".encode() + data + b"\r\n")

    def send(self, message):
        data = encode(message)
        self.chunk(varint(len(data)) + data)

    def missing(self, id):
        self.send(Variant(1, Variant(1, {0: Variant(0, node_bytes(id))})))

    def pending(self, id):
        self.send(Variant(1, Variant(4, node_bytes(id))))

    def available(self, id):
        while True:
            message = self.receive()
            if message.id == 0 and message.value.id == 1:
                available = message.value.value
                if available.id == 0 and available.value[0] == node_bytes(id):
                    return

    def put_message(self):
        while True:
            message = self.receive()
            if message.id == 1 and message.value.id != 2:
                return message.value

    def receive(self):
        return decode(io.BytesIO(self.response.read(read_varint(self.response))))

    def finish(self):
        self.send(Variant(1, Variant(3)))
        self.send(Variant(0, Variant(3)))
        self.send(Variant(2))
        self.socket.sendall(b"0\r\n\r\n")

    def close(self):
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except OSError as error:
            if error.errno != errno.ENOTCONN:
                raise
        self.response.close()
        self.socket.close()

    def requested(self, id):
        id = node_bytes(id)
        while True:
            if id in self.requests:
                self.requests.remove(id)
                return
            message = self.receive()
            if message.id == 0 and message.value.id == 0:
                selector = message.value.value[0]
                assert selector.id == 0
                self.requests.add(selector.value)


class Messenger:
    def __init__(self):
        self.socket = socket.create_connection(("127.0.0.1", int(port)), timeout=10)
        self.socket.settimeout(None)
        self.reader = self.socket.makefile("rb")
        assert self.reader.readline().startswith(b"INFO ")
        self.socket.sendall(b'CONNECT {"verbose":false}\r\nSUB syncs.> 1\r\nPING\r\n')
        assert self.reader.readline() == b"PONG\r\n"
        self.queue = queue.Queue()
        self.backlog = []
        self.lock = threading.Lock()
        threading.Thread(target=self.read, daemon=True).start()

    def read(self):
        try:
            while True:
                line = self.reader.readline()
                assert line, "the messenger connection ended"
                if line == b"PING\r\n":
                    with self.lock:
                        self.socket.sendall(b"PONG\r\n")
                    continue
                assert line.startswith(b"MSG "), line
                fields = line.split()
                data = self.reader.read(int(fields[-1]))
                assert self.reader.read(2) == b"\r\n"
                path = fields[1].decode()
                reader = io.BytesIO(data)
                message = decode(reader)
                if ".client." in path and message.id == 1:
                    output = message.value[3]
                    if output is not None and output.id == 0:
                        # Duration is an unframed pair; its nanoseconds follow the final struct field.
                        output.value[0] = (output.value[0], decode(reader))
                assert not reader.read(), "unexpected trailing control data"
                self.queue.put((path, message))
        except Exception as error:
            self.queue.put(error)

    def publish(self, subject, message):
        data = encode(message)
        with self.lock:
            self.socket.sendall(f"PUB {subject} {len(data)}\r\n".encode() + data + b"\r\n")

    def receive(self, predicate, timeout=10):
        for index, item in enumerate(self.backlog):
            if predicate(*item):
                return self.backlog.pop(index)[1].value
        deadline = time.monotonic() + timeout
        while True:
            try:
                item = self.queue.get(timeout=max(0, deadline - time.monotonic()))
            except queue.Empty:
                raise TimeoutError("the expected control message did not arrive") from None
            if isinstance(item, Exception):
                raise item
            if predicate(*item):
                return item[1].value
            self.backlog.append(item)

    def absent(self, predicate, duration=0.25):
        try:
            message = self.receive(predicate, duration)
        except TimeoutError:
            return
        raise AssertionError(f"unexpected control message: {message}")


class Peer:
    def __init__(self, messenger, token):
        self.messenger = messenger
        self.subject = subject(token)

    def request(self, heartbeat=False, attempt=None, timeout=10):
        return self.messenger.receive(lambda path, message:
            path.startswith(self.subject + ".") and path.endswith(".server") and message.id == 1
            and (message.value[0].id == 0) == heartbeat
            and (attempt is None or message.value[3] == attempt), timeout)

    def ack(self, request):
        self.reply(request, Variant(0, {0: request[2], 1: request[3]}))

    def reply(self, request, message):
        self.messenger.publish(f"{self.subject}.client.{request[1]}", message)

    def heartbeat(self, request, attempt):
        self.reply(request, Variant(1, {0: None, 1: request[2], 2: attempt, 3: Variant(0, {0: (TTL, 0)})}))

    def respond(self, request, error=None, stored=True):
        storage = {0: True} if stored else None
        permissions = [Variant(0), Variant(1)] if stored else []
        kind = 1 if request[0].value[0][2] == 8 else 0
        output = None if error else Variant(1, Variant(kind, {0: storage, 1: permissions}) if stored else None)
        self.reply(request, Variant(1, {0: {3: error} if error else None, 1: request[2], 2: request[3], 3: output}))

    def cancel(self, id, attempt):
        self.messenger.publish(f"{self.subject}.attempts.{attempt}.server", Variant(2, {0: id, 1: attempt}))

    def cancelled(self, request):
        return self.messenger.receive(lambda path, message:
            path == f"{self.subject}.attempts.{request[3]}.server"
            and message == Variant(2, {0: request[2], 1: request[3]}))

    def acknowledged(self, id, attempt):
        return self.messenger.receive(lambda path, message:
            path == f"{self.subject}.attempts.{attempt}.server" and message == Variant(0, {0: id, 1: attempt}))

    def send(self, id, node=None, attempt=None, client="client", permissions=Variant(1, [Variant(0)]), storage=Variant(0, {})):
        arg = Variant(0, {}) if node is None else Variant(1, {0: node_bytes(node), 1: permissions, 2: storage})
        request = {0: arg, 1: client, 2: id, 3: attempt}
        path = f"{self.subject}.server" if attempt is None else f"{self.subject}.attempts.{attempt}.server"
        self.messenger.publish(path, Variant(1, request))
        return request

    def response(self, id, timeout=10, client="client"):
        return self.messenger.receive(lambda path, message:
            path == f"{self.subject}.client.{client}" and message.id == 1 and message.value[1] == id, timeout)

    def retained(self, id, client="client"):
        return self.messenger.receive(lambda path, message:
            path == f"{self.subject}.client.{client}" and message.id == 0 and message.value[0] == id)

    def acknowledge(self, response):
        self.messenger.publish(f"{self.subject}.attempts.{response[2]}.server", Variant(0, {0: response[1], 1: response[2]}))

    def connect(self, id="heartbeat", client="client"):
        deadline = time.monotonic() + 10
        while True:
            self.send(id, client=client)
            try:
                response = self.response(id, 0.1, client)
                self.acknowledge(response)
                return response[2]
            except TimeoutError:
                assert time.monotonic() < deadline, "the responder did not subscribe"


def read_object(id, token, collection="objects"):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect(socket_path)
    tokens = token if isinstance(token, list) else [token]
    query = urllib.parse.urlencode({f"tokens[local][{index}]": token for index, token in enumerate(tokens)})
    sock.sendall(f"GET /{collection}/{id}?{query} HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\nConnection: close\r\n\r\n".encode())
    response = http.client.HTTPResponse(sock)
    response.begin()
    result = response.status, response.read()
    response.close()
    sock.close()
    return result


def fake_peer(messenger, nodes=None):
    # Keep the real responder unsubscribed while the test controls its wire messages.
    subscribe = watch("sync.control.subscribe")
    sync = Sync({"get": ",".join(nodes or [missing_id()])})
    reached("sync.control.subscribe", subscribe)
    return sync, Peer(messenger, sync.token), subscribe


def test_output(messenger):
    sync = Sync({"get": missing_id()})
    referent = sync.sync
    token = sync.token
    assert f"syncs.{referent['node']}.control" == subject(token)
    sync.close()
    sync = Sync({"get": missing_id(), "sync": referent})
    assert sync.sync == referent, "retries must reuse the supplied sync referent"
    sync.close()

    # An ID alone, another sync's proof, and a forged proof must all be rejected.
    other = Sync()
    wrong_id = other.sync["node"]
    other.close()
    parts = token.split(".")
    body = json.loads(base64.b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
    body["expires_at"] += 1
    parts[1] = base64.b64encode(json.dumps(body).encode()).decode()
    invalid = [
        {"node": referent["node"]},
        {"node": wrong_id, "options": referent["options"]},
        {"node": referent["node"], "options": {"tokens": {"local": [".".join(parts)]}}},
    ]
    for referent in invalid:
        sync = Sync({"get": missing_id(), "sync": referent}, status=500)
        assert b"invalid sync authorization" in sync.response.read()
        sync.close()


def recover_attempt(messenger, fail):
    sync, peer, subscribe = fake_peer(messenger)
    id = missing_id(1) if fail else source_blob("old attempt")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        read = executor.submit(read_object, id, sync.token)
        heartbeat = peer.request(True)
        assert peer.request(True)[2] == heartbeat[2], "heartbeat retries must preserve their ID"
        peer.heartbeat(heartbeat, "old")
        request = peer.request()
        assert peer.request() == request, "node retries must preserve their ID and attempt"
        ack_watch = watch("sync.control.ack", node=id)
        peer.ack(request)
        reached("sync.control.ack", ack_watch)
        release("sync.control.ack", ack_watch)
        old_heartbeat = heartbeat
        heartbeat = peer.request(True)
        while heartbeat[2] == old_heartbeat[2]:
            heartbeat = peer.request(True)
        peer.heartbeat(heartbeat, "new")
        replacement = peer.request(attempt="new", timeout=1)
        assert replacement[2] == request[2], "attempt replacement must replay the outstanding request"
        peer.ack(request)
        assert peer.request(attempt="new")[2] == request[2], "an old ACK must not stop retries on the new attempt"
        peer.ack(replacement)
        if not fail:
            assert command("put", 'tg.blob("old attempt")') == id
        error = "the replacement was cancelled" if fail else None
        response_request = replacement if fail else request
        peer.respond(response_request, error)
        status, body = read.result(timeout=5)
        if fail:
            assert status == 404, (status, body)
        else:
            assert status == 200, (status, body)
            assert base64.b64decode(json.loads(body)["data"]["value"]["bytes"]) == b"old attempt"
        peer.acknowledged(request[2], response_request[3])
        peer.respond(request, error)
        peer.acknowledged(request[2], "old")
        peer.respond(replacement, error)
        peer.acknowledged(request[2], "new")
    sync.close()
    release("sync.control.subscribe", subscribe)


def test_attempts(messenger):
    recover_attempt(messenger, False)
    recover_attempt(messenger, True)


def test_stale_heartbeats(messenger):
    sync, peer, subscribe = fake_peer(messenger)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        read = executor.submit(read_object, missing_id(1), sync.token)
        old = peer.request(True)
        new = peer.request(True)
        while new[2] == old[2]:
            new = peer.request(True)
        peer.heartbeat(new, "new")
        request = peer.request()
        peer.ack(request)
        peer.heartbeat(old, "old")
        peer.acknowledged(old[2], "old")
        messenger.absent(lambda path, message:
            path == f"{peer.subject}.attempts.old.server" and message.id == 1)
        deadline = time.monotonic() + TTL + 1
        while time.monotonic() < deadline:
            peer.heartbeat(new, "new")
            time.sleep(0.05)
        status, body = read.result(timeout=5)
        assert status == 404, (status, body)
    sync.close()
    release("sync.control.subscribe", subscribe)


def test_final_read(messenger):
    sync, peer, subscribe = fake_peer(messenger)
    id = source_blob("final read")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        read = executor.submit(read_object, id, sync.token)
        peer.heartbeat(peer.request(True), "attempt")
        request = peer.request()
        ack_watch = watch("sync.control.ack", node=id)
        peer.ack(request)
        reached("sync.control.ack", ack_watch)
        assert command("put", 'tg.blob("final read")') == id
        assert not read.done(), "an acknowledged sync must wait for a response"
        release("sync.control.ack", ack_watch)
        peer.respond(request, "the transfer failed")
        status, body = read.result(timeout=5)
        assert status == 200, (status, body)
        assert base64.b64decode(json.loads(body)["data"]["value"]["bytes"]) == b"final read"
    sync.close()
    release("sync.control.subscribe", subscribe)


def test_pending_source(messenger):
    text = "source recovers after pending"
    id = source_blob(text)
    alternate, peer, subscribe = fake_peer(messenger)
    token = urllib.parse.quote(alternate.token, safe="")
    sync = Sync({"put": f"{id}?tokens[local][0]={token}"})
    started = time.monotonic()
    assert sync.put_message() == Variant(4, node_bytes(id))
    assert time.monotonic() - started < 1, "pending must precede the retry timeout"
    peer.heartbeat(peer.request(True), "pending")
    request = peer.request()
    peer.ack(request)
    assert command("put", f"tg.blob({json.dumps(text)})") == id
    peer.respond(request)
    message = sync.put_message()
    assert message.id == 0 and message.value.id == 1, message
    assert message.value.value[0] == node_bytes(id)
    sync.close()
    alternate.close()
    release("sync.control.subscribe", subscribe)

    # A local hit must not send pending or start alternate-sync requests.
    alternate, peer, subscribe = fake_peer(messenger)
    token = urllib.parse.quote(alternate.token, safe="")
    sync = Sync({"put": f"{id}?tokens[local][0]={token}"})
    assert sync.put_message().id == 0
    messenger.absent(lambda path, message:
        path.startswith(peer.subject + ".") and path.endswith(".server") and message.id == 1)
    sync.close()
    alternate.close()
    release("sync.control.subscribe", subscribe)


def test_pending_cancel(messenger):
    alternate, peer, subscribe = fake_peer(messenger)
    text = "the original source wins"
    id = source_blob(text)
    token = urllib.parse.quote(alternate.token, safe="")
    sync = Sync({"get": f"{id}?tokens[local][0]={token}"})
    sync.requested(id)
    sync.pending(id)
    peer.heartbeat(peer.request(True), "pending")
    request = peer.request()
    peer.ack(request)
    sync.send(Variant(1, Variant(0, Variant(1, {0: node_bytes(id), 1: b"\x00" + text.encode()}))))
    peer.cancelled(request)
    sync.finish()
    while sync.receive().id != 2:
        pass
    sync.close()
    alternate.close()
    release("sync.control.subscribe", subscribe)


def test_pending_destination(messenger):
    text = "destination recovers after pending"
    id = source_blob(text)
    alternate, peer, subscribe = fake_peer(messenger)
    token = urllib.parse.quote(alternate.token, safe="")
    sync = Sync({"get": f"{id}?tokens[local][0]={token}"})
    sync.requested(id)
    sync.pending(id)
    time.sleep(0.1)
    peer.heartbeat(peer.request(True), "pending")
    request = peer.request()
    peer.ack(request)
    assert command("put", f"tg.blob({json.dumps(text)})") == id
    peer.respond(request)
    command("index")
    sync.available(id)
    # The source's eventual failure must not defeat the successful fallback.
    sync.missing(id)
    sync.finish()
    while sync.receive().id != 2:
        pass
    sync.close()
    alternate.close()
    release("sync.control.subscribe", subscribe)


def test_pending_late_source(messenger):
    text = "source succeeds after destination fallback fails"
    id = source_blob(text)
    sync = Sync({"get": id})
    sync.requested(id)
    sync.pending(id)
    time.sleep(0.4)
    sync.send(Variant(1, Variant(0, Variant(1, {0: node_bytes(id), 1: b"\x00" + text.encode()}))))
    sync.finish()
    while sync.receive().id != 2:
        pass
    sync.close()
    status, body = read_object(id, [])
    assert status == 200, (status, body)


def test_pending_process(messenger):
    id = "pcs_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0"
    data = {
        "children": [],
        "command": "cmd_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0",
        "created_at": 0,
        "finished_at": 0,
        "host": "test",
        "output": 5,
        "sandbox": "sbx_00041061050r3gg28a1c60t3gf20",
        "status": "finished",
    }
    alternate, peer, subscribe = fake_peer(messenger)
    token = urllib.parse.quote(alternate.token, safe="")
    referent = f"{id}?tokens[local][0]={token}"
    sender = Sync({"put": referent})
    receiver = Sync({"get": referent})
    receiver.requested(id)
    receiver.pending(id)
    assert sender.put_message() == Variant(4, node_bytes(id))
    requests = {}
    while len(requests) < 2:
        request = messenger.receive(lambda path, message:
            path.startswith(peer.subject + ".") and path.endswith(".server") and message.id == 1)
        if request[0].id == 0:
            peer.heartbeat(request, "pending")
        else:
            requests[request[2]] = request
            peer.ack(request)
    command("process", "put", id, json.dumps(data))
    command("index")
    for request in requests.values():
        peer.respond(request)
    message = sender.put_message()
    assert message.id == 0 and message.value.id == 3, message
    while True:
        message = receiver.receive()
        if message.id == 0 and message.value.id == 1:
            available = message.value.value
            assert available.id == 1 and available.value[0] == node_bytes(id), available
            break
    receiver.missing(id)
    receiver.finish()
    while receiver.receive().id != 2:
        pass
    sender.close()
    receiver.close()


    alternate.close()
    release("sync.control.subscribe", subscribe)

def test_pending_missing(messenger):
    id = missing_id(100)
    sync = Sync({"put": id})
    started = time.monotonic()
    assert sync.put_message() == Variant(4, node_bytes(id))
    message = sync.put_message()
    elapsed = time.monotonic() - started
    assert message.id == 1 and elapsed < 1, (message, elapsed)
    sync.close()

    # A later missing reply observes the completed tokenless fallback.
    sync = Sync({"get": id})
    sync.requested(id)
    started = time.monotonic()
    sync.pending(id)
    time.sleep(0.6)
    sync.missing(id)
    try:
        while True:
            assert sync.receive().id != 2, "an absent node must not complete successfully"
    except AssertionError as error:
        assert str(error) == "the stream ended inside a varint", error
    elapsed = time.monotonic() - started
    assert 0.6 <= elapsed < 1.5, elapsed
    sync.close()


def test_no_polling(messenger):
    id = source_blob("no polling without tokens")
    started = time.monotonic()
    status, body = read_object(id, [])
    assert status == 404 and time.monotonic() - started < 1, (status, body)
    assert command("put", 'tg.blob("no polling without tokens")') == id
    status, body = read_object(id, [])
    assert status == 200, (status, body)
    process = "pcs_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0"
    started = time.monotonic()
    status, body = read_object(process, [], "processes")
    # Process gets also perform the existing runner-control discovery.
    assert status == 404 and time.monotonic() - started < 4, (status, body)


def test_failed_wait(messenger):
    sync, peer, subscribe = fake_peer(messenger)
    id = source_blob("polling after failure")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        read = executor.submit(read_object, id, sync.token)
        peer.heartbeat(peer.request(True), "attempt")
        request = peer.request()
        peer.ack(request)
        peer.respond(request, "the transfer failed")
        peer.acknowledged(request[2], request[3])
        status, body = read.result(timeout=2)
        assert status == 404, (status, body)
        assert command("put", 'tg.blob("polling after failure")') == id
        status, body = read_object(id, [])
        assert status == 200, (status, body)
    sync.close()
    release("sync.control.subscribe", subscribe)


def test_notification(messenger):
    sync, peer, subscribe = fake_peer(messenger)
    id = source_blob("immediate notification")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        read = executor.submit(read_object, id, sync.token)
        peer.heartbeat(peer.request(True), "attempt")
        request = peer.request()
        ack = watch("sync.control.ack", id=request[2])
        peer.ack(request)
        reached("sync.control.ack", ack)
        release("sync.control.ack", ack)
        time.sleep(0.1)
        assert command("put", 'tg.blob("immediate notification")') == id
        peer.respond(request)
        status, body = read.result(timeout=1)
        assert status == 200, (status, body)
    sync.close()
    release("sync.control.subscribe", subscribe)


def test_live_wait(messenger):
    sync, peer, subscribe = fake_peer(messenger)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        started = time.monotonic()
        read = executor.submit(read_object, missing_id(98), sync.token)
        peer.heartbeat(peer.request(True), "attempt")
        request = peer.request()
        peer.ack(request)
        while time.monotonic() - started < 1.5:
            assert not read.done(), "a live sync must outlast the former polling deadline"
            try:
                peer.heartbeat(peer.request(True, timeout=0.1), "attempt")
            except TimeoutError:
                pass
        peer.respond(request, "the transfer failed")
        status, body = read.result(timeout=2)
        assert status == 404, (status, body)
    sync.close()
    release("sync.control.subscribe", subscribe)


def test_shared_heartbeat(messenger):
    ids = [missing_id(index + 1) for index in range(1024)]
    values = ",".join('{"kind":"value","value":' + id + '}' for id in ids)
    parent = command("put", 'tg.command({"args":[' + values + '],"executable":"true","host":"builtin"})')
    sequential_ids = [source_blob(value) for value in ("first", "second")]
    sync, peer, subscribe = fake_peer(messenger, [parent] + sequential_ids)
    token = urllib.parse.quote(sync.token, safe="")
    outgoing = Sync({"eager": True, "put": f"{parent}?tokens[local][0]={token}"})
    clients, requests = set(), {}
    deadline = time.monotonic() + 15
    while len(requests) < len(ids):
        assert time.monotonic() < deadline, f"only {len(requests)} of {len(ids)} requests became outstanding"
        try:
            request = messenger.receive(lambda path, message:
                path.startswith(peer.subject + ".") and path.endswith(".server") and message.id == 1,
                max(0, deadline - time.monotonic()))
        except TimeoutError:
            raise AssertionError(f"only {len(requests)} of {len(ids)} requests became outstanding") from None
        clients.add(request[1])
        if request[0].id == 0:
            peer.heartbeat(request, "shared")
        else:
            requests[request[2]] = request
            peer.ack(request)
    assert len(clients) == 1, f"1024 outstanding requests started {len(clients)} heartbeat tasks"
    assert all(request[3] == "shared" for request in requests.values())
    # Every terminal response is acknowledged, including those arriving after the transfer fails.
    for request in requests.values():
        peer.respond(request, "the transfer failed")
    for request in requests.values():
        peer.acknowledged(request[2], "shared")
    outgoing.close()

    # Keep a second transfer open while its individual callers finish, then issue another request.
    ids = sequential_ids
    incoming = Sync({"get": ",".join(f"{id}?tokens[local][0]={token}" for id in ids)})
    client = None
    for id, value in zip(ids, ("first", "second")):
        incoming.requested(id)
        incoming.missing(id)
        while True:
            request = messenger.receive(lambda path, message:
                path.startswith(peer.subject + ".") and path.endswith(".server") and message.id == 1
                and message.value[1] not in clients)
            if request[0].id == 0:
                peer.heartbeat(request, "sequential")
            else:
                break
        if client is None:
            client = request[1]
        assert request[1] == client and request[3] == "sequential", "sequential requests must reuse the peer task"
        assert command("put", f"tg.blob({json.dumps(value)})") == id
        peer.respond(request)
        peer.acknowledged(request[2], "sequential")
    incoming.close()
    sync.close()
    release("sync.control.subscribe", subscribe)


def test_candidates(messenger):
    subscribe = watch("sync.control.subscribe")
    syncs = [Sync({"get": missing_id()}) for _ in range(4)]
    for index in range(len(syncs)):
        reached("sync.control.subscribe", subscribe, index)
    peers = [Peer(messenger, sync.token) for sync in syncs]
    id = source_blob("candidates")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        read = executor.submit(read_object, id, [sync.token for sync in syncs] + [syncs[0].token])
        requests = []
        for index, peer in enumerate(peers):
            peer.heartbeat(peer.request(True), f"candidate-{index}")
            request = peer.request()
            ack = watch("sync.control.ack", id=request[2])
            peer.ack(request)
            reached("sync.control.ack", ack)
            release("sync.control.ack", ack)
            requests.append(request)
        messenger.absent(lambda path, message:
            path.startswith(peers[0].subject + ".") and path.endswith(".server")
            and message.id == 1 and message.value[0].id == 1
            and message.value[2] != requests[0][2])
        peers[0].respond(requests[0], "this sync failed")
        peers[0].acknowledged(requests[0][2], requests[0][3])
        peers[1].respond(requests[1], stored=False)
        peers[1].acknowledged(requests[1][2], requests[1][3])
        assert not read.done(), "one failed or missing candidate must not end the search"
        assert command("put", 'tg.blob("candidates")') == id
        peers[3].respond(requests[3])
        status, body = read.result(timeout=5)
        assert status == 200, (status, body)
        peers[2].cancelled(requests[2])
        # A response racing with cancellation must still be acknowledged.
        peers[2].respond(requests[2])
        peers[2].acknowledged(requests[2][2], requests[2][3])
    for sync in syncs:
        sync.close()
    release("sync.control.subscribe", subscribe)


def test_candidate_local_fallback(messenger):
    for missing in (False, True):
        subscribe = watch("sync.control.subscribe")
        syncs = [Sync({"get": missing_id()}) for _ in range(2)]
        for index in range(len(syncs)):
            reached("sync.control.subscribe", subscribe, index)
        peers = [Peer(messenger, sync.token) for sync in syncs]
        text = f"local fallback after candidate ends: {missing}"
        id = source_blob(text)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            read = executor.submit(read_object, id, [sync.token for sync in syncs])
            requests = []
            for index, peer in enumerate(peers):
                peer.heartbeat(peer.request(True), f"fallback-{index}")
                request = peer.request()
                ack = watch("sync.control.ack", id=request[2])
                peer.ack(request)
                reached("sync.control.ack", ack)
                release("sync.control.ack", ack)
                requests.append(request)
            assert command("put", f"tg.blob({json.dumps(text)})") == id
            command("index")
            assert not read.done(), "local availability must not trigger polling"
            if missing:
                peers[0].respond(requests[0], stored=False)
            else:
                peers[0].respond(requests[0], "this sync failed")
            status, body = read.result(timeout=1)
            assert status == 200, (status, body)
            assert base64.b64decode(json.loads(body)["data"]["value"]["bytes"]) == text.encode()
            peers[1].cancelled(requests[1])
        for sync in syncs:
            sync.close()
        release("sync.control.subscribe", subscribe)


def test_client_cancel(messenger):
    subscribe = watch("sync.control.subscribe")
    syncs = [Sync({"get": missing_id()}) for _ in range(2)]
    for index in range(len(syncs)):
        reached("sync.control.subscribe", subscribe, index)
    peers = [Peer(messenger, sync.token) for sync in syncs]
    id = missing_id(1)
    parent = command("put", 'tg.command({"args":[{"kind":"value","value":' + id + '}],"executable":"true","host":"builtin"})')
    query = "&".join(f"tokens[local][{index}]={urllib.parse.quote(sync.token, safe='')}" for index, sync in enumerate(syncs))
    outgoing = Sync({"eager": True, "put": f"{parent}?{query}"})
    requests = []
    for index, peer in enumerate(peers):
        peer.heartbeat(peer.request(True), f"cancel-{index}")
        request = peer.request()
        peer.ack(request)
        requests.append(request)
    outgoing.close()
    for peer, request in zip(peers, requests):
        peer.cancelled(request)
    for sync in syncs:
        sync.close()
    release("sync.control.subscribe", subscribe)


def test_cancel(messenger):
    for mode in ("before", "pending", "response"):
        id = source_blob("cancelled")
        sync = Sync({"get": id})
        peer = Peer(messenger, sync.token)
        attempt = peer.connect()
        peer.send("other", missing_id(1), attempt)
        peer.retained("other")
        if mode != "before":
            peer.send("cancelled", id, attempt)
            peer.retained("cancelled")
        if mode == "response":
            sync.requested(id)
            sync.send(Variant(1, Variant(0, Variant(1, {0: node_bytes(id), 1: b"\x00cancelled"}))))
            peer.response("cancelled")
        cancel = watch("sync.control.cancel", id="cancelled", attempt=attempt)
        peer.cancel("cancelled", attempt)
        reached("sync.control.cancel", cancel)
        release("sync.control.cancel", cancel)
        while True:
            try:
                peer.response("cancelled", 0.01)
            except TimeoutError:
                break
        peer.cancel("cancelled", attempt)
        peer.send("cancelled", id, attempt)
        messenger.absent(lambda path, message:
            path == f"{peer.subject}.client.client" and
            ((message.id == 0 and message.value[0] == "cancelled") or
             (message.id == 1 and message.value[1] == "cancelled")))
        sync.close()
        assert peer.response("other")[0] is not None, "cancellation must not remove other requests"


def test_finish(messenger):
    for cancel in (False, True):
        sync = Sync()
        peer = Peer(messenger, sync.token)
        attempt = peer.connect()
        peer.send("missing", missing_id(1), attempt)
        peer.retained("missing")
        if cancel:
            sync.close()
        else:
            sync.finish()
        first = peer.response("missing")
        assert peer.response("missing") == first, "unacknowledged terminal responses must retry"
        if cancel:
            assert first[0] is not None and first[3] is None, first
        else:
            assert first[0] is None and first[3] == Variant(1, None), first
        ack_watch = watch("sync.control.response_ack", id="missing", attempt=attempt)
        peer.acknowledge(first)
        reached("sync.control.response_ack", ack_watch)
        # Drain messages already published before the acknowledgement was consumed.
        while True:
            try:
                peer.response("missing", 0.05)
            except TimeoutError:
                break
        release("sync.control.response_ack", ack_watch)
        messenger.absent(lambda path, message:
            path == f"{peer.subject}.client.client" and message.id == 1 and message.value[1] == "missing")
        time.sleep(TTL + 0.2)
        peer.send("after-finish")
        messenger.absent(lambda path, message:
            path == f"{peer.subject}.client.client" and message.id == 1 and message.value[1] == "after-finish")
        if not cancel:
            sync.close()


def test_index_handoff(messenger):
    for interruption in ("cancel", "failure", "enqueue"):
        text = "handoff " + interruption
        id = source_blob(text)
        filler = source_blob(text + " filler")
        missing = missing_id(85)
        nodes = f"{id},{filler}" if interruption == "enqueue" else f"{id},{filler},{missing}"
        sync = Sync({"get": nodes})
        peer = Peer(messenger, sync.token)
        attempt = peer.connect()
        peer.send("stored", id, attempt)
        peer.retained("stored")
        sync.requested(id)
        sync.send(Variant(1, Variant(0, Variant(1, {0: node_bytes(id), 1: b"\x00" + text.encode()}))))
        sync.send(Variant(1, Variant(0, Variant(1, {0: node_bytes(filler), 1: b"\x00" + (text + " filler").encode()}))))
        stored = peer.response("stored")
        assert stored[0] is None, stored
        peer.acknowledge(stored)
        peer.send("missing", missing, attempt)
        peer.retained("missing")
        enqueue = watch("sync.get.index.enqueue")
        if interruption == "enqueue":
            sync.finish()
            reached("sync.get.index.enqueue", enqueue)
            sync.close()
            # Cancelling the final enqueue must leave the partial-indexing fallback armed.
            reached("sync.get.index.enqueue", enqueue, 1)
        elif interruption == "cancel":
            sync.close()
            reached("sync.get.index.enqueue", enqueue)
        else:
            sync.send(Variant(1, Variant(0, Variant(1, {0: node_bytes(missing), 1: b"\xff"}))))
            reached("sync.get.index.enqueue", enqueue)
        messenger.absent(lambda path, message:
            path == f"{peer.subject}.client.client" and message.id == 1 and message.value[1] == "missing")
        # Control must still accept new readers while the partial grant batch is blocked.
        fresh_attempt = peer.connect("fresh", client="fresh")
        peer.send("read", id, fresh_attempt, client="fresh")
        response = peer.response("read", client="fresh")
        assert response[0] is None and response[3] == stored[3], response
        peer.acknowledge(response)
        batch = watch("index.batch")
        release("sync.get.index.enqueue", enqueue)
        reached("index.batch", batch)
        # Finishing control requires enqueueing, but does not require processing the batch.
        terminal = peer.response("missing")
        assert terminal[0] is not None, terminal
        peer.acknowledge(terminal)
        release("index.batch", batch)
        if interruption == "failure":
            sync.close()


def test_requirements(messenger):
    child = source_blob("child")
    data = b"\x01\x00" + encode({0: [{0: node_bytes(child), 1: 5}]})
    parent = subprocess.check_output(
        [tangram, "--url", source_url, "put", "--bytes", "--kind", "blob"],
        input=data,
        timeout=15,
    ).decode().strip()
    blocker = missing_id(77)
    sync = Sync({"get": f"{parent},{blocker}"})
    peer = Peer(messenger, sync.token)
    attempt = peer.connect()
    peer.send("node", parent, attempt)
    peer.send("permissions", parent, attempt, permissions=Variant(1, [Variant(1)]), storage=None)
    peer.send("storage", parent, attempt, permissions=Variant(1, []))
    peer.send("subtree", parent, attempt, permissions=Variant(1, [Variant(1)]), storage=Variant(0, {0: True}))
    for id in ("node", "storage", "subtree"):
        peer.retained(id)
    response = peer.response("permissions")
    assert response[0] is None and response[3].value.value[0] is None, response
    peer.acknowledge(response)
    sync.requested(parent)
    sync.send(Variant(1, Variant(0, Variant(1, {0: node_bytes(parent), 1: data}))))
    response = peer.response("node")
    assert response[0] is None and response[3].value.value[0] == {}, response
    peer.acknowledge(response)
    response = peer.response("storage")
    assert response[0] is None and response[3].value.value[0] == {}, response
    peer.acknowledge(response)
    messenger.absent(lambda path, message: path == f"{peer.subject}.client.client"
                     and message.id == 1 and message.value[1] == "subtree")
    sync.requested(child)
    sync.send(Variant(1, Variant(0, Variant(1, {0: node_bytes(child), 1: b"\x00child"}))))
    response = peer.response("subtree")
    assert response[0] is None and response[3].value.value[0] == {0: True}, response
    peer.acknowledge(response)
    peer.send("late", parent, attempt, permissions=Variant(1, [Variant(1)]), storage=Variant(0, {0: True}))
    response = peer.response("late")
    assert response[0] is None and response[3].value.value[0] == {0: True}, response
    peer.acknowledge(response)
    sync.close()


def test_shutdown(messenger):
    sync = Sync()
    peer = Peer(messenger, sync.token)
    attempt = peer.connect()
    peer.send("missing", missing_id(1), attempt)
    peer.retained("missing")
    sync.finish()
    response = peer.response("missing")
    sync.close()

    # Shutdown must retain and retry an unacknowledged terminal response.
    stop = subprocess.Popen([tangram, "--directory", directory, "server", "stop"])
    try:
        time.sleep(0.1)
        assert stop.poll() is None, "shutdown must retain an unacknowledged response"
        assert peer.response("missing") == response
        peer.acknowledge(response)
        # Once the response is acknowledged, shutdown must not wait for the two-second attempt.
        assert stop.wait(timeout=1) == 0
    finally:
        if stop.poll() is None:
            stop.kill()
            stop.wait()


def test_expiration(messenger):
    sync = Sync({"get": missing_id()})
    peer = Peer(messenger, sync.token)
    attempt = peer.connect()
    # Retransmit an already acknowledged heartbeat until the original attempt expires.
    deadline = time.monotonic() + TTL + 1
    while True:
        peer.send("heartbeat")
        response = peer.response("heartbeat")
        peer.acknowledge(response)
        if response[2] != attempt:
            break
        assert time.monotonic() < deadline, "heartbeat retries renewed the original attempt"
        time.sleep(0.05)
    peer.send("expired", missing_id(1), attempt)
    messenger.absent(lambda path, message:
        path == f"{peer.subject}.client.client" and
        ((message.id == 0 and message.value[0] == "expired") or (message.id == 1 and message.value[1] == "expired")))
    peer.send("fresh", missing_id(1), response[2])
    peer.retained("fresh")
    sync.close()
    assert peer.response("fresh")[0] is not None


def test_failed_transfer(messenger):
    id = source_blob("stored")
    filler = source_blob("filler")
    missing = missing_id(1)
    sync = Sync({"get": f"{id},{filler},{missing}"})
    peer = Peer(messenger, sync.token)
    attempt = peer.connect()
    peer.send("stored", id, attempt)
    peer.retained("stored")
    peer.send("missing", missing, attempt)
    peer.retained("missing")
    sync.requested(id)
    sync.send(Variant(1, Variant(0, Variant(1, {0: node_bytes(id), 1: b"\x00stored"}))))
    # A second object flushes the first storage batch while the input remains open.
    sync.send(Variant(1, Variant(0, Variant(1, {0: node_bytes(filler), 1: b"\x00filler"}))))
    stored = peer.response("stored")
    output = stored[3]
    assert stored[0] is None and output.id == 1 and output.value.id == 0, stored
    assert output.value.value == {0: {0: True}, 1: [Variant(0), Variant(1)]}, stored

    # A malformed object fails the remaining transfer while the stored response is still unacknowledged.
    sync.send(Variant(1, Variant(0, Variant(1, {0: node_bytes(missing), 1: b"\xff"}))))
    failure = peer.response("missing")
    assert failure[0] is not None and failure[3] is None, failure
    assert peer.response("stored") == stored
    peer.acknowledge(stored)
    peer.acknowledge(failure)
    peer.send("late", id, attempt)
    late = peer.response("late")
    assert late[0] is None and late[3] == stored[3], late
    peer.acknowledge(late)
    sync.close()


messenger = None if case == "output" else Messenger()
globals()["test_" + case](messenger)
