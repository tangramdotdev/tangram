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
    return f"syncs.{body['id']}.control"


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
            self.token = self.output["token"]

    def chunk(self, data):
        self.socket.sendall(f"{len(data):x}\r\n".encode() + data + b"\r\n")

    def send(self, message):
        data = encode(message)
        self.chunk(varint(len(data)) + data)

    def missing(self, id):
        self.send(Variant(1, Variant(1, {0: Variant(0, node_bytes(id))})))

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

    def request(self, heartbeat=False, lease=None, timeout=10):
        return self.messenger.receive(lambda path, message:
            path.startswith(self.subject + ".") and path.endswith(".server") and message.id == 1
            and (message.value[0].id == 0) == heartbeat
            and (lease is None or message.value[3] == lease), timeout)

    def ack(self, request):
        self.reply(request, Variant(0, {0: request[2], 1: request[3]}))

    def reply(self, request, message):
        self.messenger.publish(f"{self.subject}.client.{request[1]}", message)

    def heartbeat(self, request, lease):
        self.reply(request, Variant(1, {0: None, 1: request[2], 2: lease, 3: Variant(0, {0: (TTL, 0)})}))

    def respond(self, request, error=None):
        output = None if error else Variant(1, Variant(request[0].value.id, {0: {0: True}, 1: [Variant(0), Variant(1)]}))
        self.reply(request, Variant(1, {0: {3: error} if error else None, 1: request[2], 2: request[3], 3: output}))

    def acknowledged(self, id, lease):
        return self.messenger.receive(lambda path, message:
            path == f"{self.subject}.leases.{lease}.server" and message == Variant(0, {0: id, 1: lease}))

    def send(self, id, node=None, lease=None, client="client"):
        arg = Variant(0, {}) if node is None else Variant(1, Variant(0, {0: node_bytes(node)}))
        request = {0: arg, 1: client, 2: id, 3: lease}
        path = f"{self.subject}.server" if lease is None else f"{self.subject}.leases.{lease}.server"
        self.messenger.publish(path, Variant(1, request))
        return request

    def response(self, id, timeout=10, client="client"):
        return self.messenger.receive(lambda path, message:
            path == f"{self.subject}.client.{client}" and message.id == 1 and message.value[1] == id, timeout)

    def retained(self, id, client="client"):
        return self.messenger.receive(lambda path, message:
            path == f"{self.subject}.client.{client}" and message.id == 0 and message.value[0] == id)

    def acknowledge(self, response):
        self.messenger.publish(f"{self.subject}.leases.{response[2]}.server", Variant(0, {0: response[1], 1: response[2]}))

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


def read_object(id, token):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect(socket_path)
    query = urllib.parse.urlencode({"tokens[local][sync]": token})
    sock.sendall(f"GET /objects/{id}?{query} HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\nConnection: close\r\n\r\n".encode())
    response = http.client.HTTPResponse(sock)
    response.begin()
    result = response.status, response.read()
    response.close()
    sock.close()
    return result


def fake_peer(messenger):
    # Keep the real responder unsubscribed while the test controls its wire messages.
    subscribe = watch("sync.control.subscribe")
    sync = Sync({"get": missing_id()})
    reached("sync.control.subscribe", subscribe)
    return sync, Peer(messenger, sync.token), subscribe


def test_output(messenger):
    sync = Sync({"get": missing_id()})
    token = sync.token
    assert token
    sync.close()
    sync = Sync({"get": missing_id(), "token": token})
    assert sync.token == token, "retries must reuse the provided token"
    sync.close()
    parts = token.split(".")
    body = json.loads(base64.b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
    body["id"] += "x"
    parts[1] = base64.b64encode(json.dumps(body).encode()).decode().rstrip("=")
    sync = Sync({"get": missing_id(), "token": ".".join(parts)}, status=500)
    assert b"invalid sync token" in sync.response.read()
    sync.close()


def recover_lease(messenger, fail):
    sync, peer, subscribe = fake_peer(messenger)
    id = missing_id(1) if fail else source_blob("old lease")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        read = executor.submit(read_object, id, sync.token)
        heartbeat = peer.request(True)
        assert peer.request(True)[2] == heartbeat[2], "heartbeat retries must preserve their ID"
        peer.heartbeat(heartbeat, "old")
        request = peer.request()
        assert peer.request() == request, "node retries must preserve their ID and lease"
        ack_watch = watch("sync.control.ack", node=id)
        peer.ack(request)
        reached("sync.control.ack", ack_watch)
        release("sync.control.ack", ack_watch)
        old_heartbeat = heartbeat
        heartbeat = peer.request(True)
        while heartbeat[2] == old_heartbeat[2]:
            heartbeat = peer.request(True)
        peer.heartbeat(heartbeat, "new")
        replacement = peer.request(lease="new", timeout=1)
        assert replacement[2] == request[2], "lease replacement must replay the outstanding request"
        peer.ack(request)
        assert peer.request(lease="new")[2] == request[2], "an old ACK must not stop retries on the new lease"
        peer.ack(replacement)
        if not fail:
            assert command("put", 'tg.blob("old lease")') == id
        error = "the replacement was cancelled" if fail else None
        response_request = replacement if fail else request
        peer.respond(response_request, error)
        status, body = read.result(timeout=5)
        if fail:
            assert status >= 400 and b"the replacement was cancelled" in body, (status, body)
        else:
            assert status == 200, (status, body)
            assert base64.b64decode(json.loads(body)["data"]["value"]["bytes"]) == b"old lease"
        peer.acknowledged(request[2], response_request[3])
        peer.respond(request, error)
        peer.acknowledged(request[2], "old")
        peer.respond(replacement, error)
        peer.acknowledged(request[2], "new")
    sync.close()
    release("sync.control.subscribe", subscribe)


def test_leases(messenger):
    recover_lease(messenger, False)
    recover_lease(messenger, True)


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
            path == f"{peer.subject}.leases.old.server" and message.id == 1)
        deadline = time.monotonic() + TTL + 1
        while time.monotonic() < deadline:
            peer.heartbeat(new, "new")
            time.sleep(0.05)
        status, body = read.result(timeout=1)
        assert status >= 400 and b"failed to recover the sync request" in body, (status, body)
    sync.close()
    release("sync.control.subscribe", subscribe)


def test_final_read(messenger):
    sync, peer, subscribe = fake_peer(messenger)
    id = source_blob("final read")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        read = executor.submit(read_object, id, sync.token)
        peer.heartbeat(peer.request(True), "lease")
        request = peer.request()
        ack_watch = watch("sync.control.ack", node=id)
        peer.ack(request)
        reached("sync.control.ack", ack_watch)
        assert command("put", 'tg.blob("final read")') == id
        assert not read.done(), "an acknowledged request must wait for its response"
        release("sync.control.ack", ack_watch)
        peer.respond(request, "the transfer failed")
        status, body = read.result(timeout=5)
        assert status == 200, (status, body)
        assert base64.b64decode(json.loads(body)["data"]["value"]["bytes"]) == b"final read"
    sync.close()
    release("sync.control.subscribe", subscribe)


def test_shared_heartbeat(messenger):
    sync, peer, subscribe = fake_peer(messenger)
    ids = [missing_id(index + 1) for index in range(1024)]
    token = urllib.parse.quote(sync.token, safe="")
    outgoing = Sync({"eager": True, "put": ",".join(f"{id}?tokens[local][sync]={token}" for id in ids)})
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
    ids = [source_blob(value) for value in ("first", "second")]
    incoming = Sync({"get": ",".join(f"{id}?tokens[local][sync]={token}" for id in ids)})
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


def test_finish(messenger):
    for cancel in (False, True):
        sync = Sync()
        peer = Peer(messenger, sync.token)
        lease = peer.connect()
        peer.send("missing", missing_id(1), lease)
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
            assert first[0] is None and first[3] == Variant(1, Variant(0, {0: None, 1: []})), first
        ack_watch = watch("sync.control.response_ack", id="missing", lease=lease)
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


def test_shutdown(messenger):
    sync = Sync()
    peer = Peer(messenger, sync.token)
    lease = peer.connect()
    peer.send("missing", missing_id(1), lease)
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
        # Once the response is acknowledged, shutdown must not wait for the two-second lease.
        assert stop.wait(timeout=1) == 0
    finally:
        if stop.poll() is None:
            stop.kill()
            stop.wait()


def test_expiration(messenger):
    sync = Sync({"get": missing_id()})
    peer = Peer(messenger, sync.token)
    lease = peer.connect()
    # Retransmit an already acknowledged heartbeat until the original lease expires.
    deadline = time.monotonic() + TTL + 1
    while True:
        peer.send("heartbeat")
        response = peer.response("heartbeat")
        peer.acknowledge(response)
        if response[2] != lease:
            break
        assert time.monotonic() < deadline, "heartbeat retries renewed the original lease"
        time.sleep(0.05)
    peer.send("expired", missing_id(1), lease)
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
    lease = peer.connect()
    peer.send("stored", id, lease)
    peer.retained("stored")
    peer.send("missing", missing, lease)
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
    peer.send("late", id, lease)
    late = peer.response("late")
    assert late[0] is None and late[3] == stored[3], late
    peer.acknowledge(late)
    sync.close()


messenger = None if case == "output" else Messenger()
globals()["test_" + case](messenger)
