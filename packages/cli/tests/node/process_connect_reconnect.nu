use ../lib/test.nu *

# A node process reconnects without losing pending stdio.

const javascript_path = path self '../../../javascript'
cd $javascript_path

# Reopen the selected process, preserve stdio positions, and cancel unused reads.
let output = timeout 15 node --input-type=module -e '
	import assert from "node:assert/strict";
	import * as tg from "@tangramdotdev/client";
	tg.setEncoding({
		base64: {
			decode: value => new Uint8Array(Buffer.from(value, "base64")),
			encode: value => Buffer.from(value).toString("base64"),
		},
		utf8: {
			decode: value => new TextDecoder().decode(value),
			encode: value => new TextEncoder().encode(value),
		},
	});
	const id = "pcs_010000000000000000000000000000000000000000000000000000";
	const tick = () => new Promise(resolve => setImmediate(resolve));
	class Queue {
		values = [];
		waiters = [];
		push(value) {
			const waiter = this.waiters.shift();
			if (waiter) waiter(value);
			else this.values.push(value);
		}
		next() {
			return this.values.length ? Promise.resolve(this.values.shift()) : new Promise(resolve => this.waiters.push(resolve));
		}
	}
	let connections = new Queue();
	let requests = [];
	tg.client.send = async request => {
		requests.push(request.uri.path);
		assert.equal(request.uri.path, "/processes/connect");
		let input = request.body.sse();
        let query = new URLSearchParams(request.uri.query);
        let arg = { mode: query.get("mode"), process: query.get("process"), reads: {} };
        for (let [key, value] of query) {
            let match = key.match(/^reads\[(\d+)\]\[([^\]]+)\]$/);
            if (match) (arg.reads[match[1]] ??= {})[match[2]] = match[2] === "position" ? Number(value) : value;
        }
		let events = new Queue();
		let next = async () => {
			while (true) {
				let event = await input.next();
				assert.equal(event.done, false);
				if (event.value.event === "request") return JSON.parse(event.value.data);
			}
		};
		let emit = (event, value) => events.push({ event, data: JSON.stringify(value) });
		let response = (id, kind, value) => emit("response", { id, error: null, output: { kind, value } });
		connections.push({ arg, input, next, emit, response, end: (error = null) => events.push(error) });
		return new tg.Response(200, { "content-type": "text/event-stream" }, {
			[Symbol.asyncIterator]: async function* () {
                    yield new Uint8Array([2, 123, 125]);
				while (true) {
					let event = await events.next();
					if (event === null) return;
					if (event instanceof Error) throw event;
					yield new TextEncoder().encode("event: " + event.event + "\ndata: " + event.data + "\n\n");
				}
			},
		});
	};
	const selected = { cached: false, lease: null, location: null, process: id, tokens: {}, outcome: null };
	const accept = async () => {
		let connection = await connections.next();
        assert.equal(connection.arg.mode, "run");
        assert.equal(connection.arg.process, id);
        connection.emit("notification", { kind: "progress", value: { kind: "output", value: selected } });
        return { ...connection, reads: connection.arg.reads };
	};
	const chunk = (connection, id, position, text) => connection.emit("notification", {
		kind: "read", value: { id, event: { kind: "chunk", value: {
			bytes: Buffer.from(text).toString("base64"), combined_position: position, stream: "stdout", stream_position: position,
		} } },
	});
	const end = (connection, id, position, stream = "stdout") => connection.response(id, "read", {
		kind: "end", value: { combined_position: position, stream_positions: { [stream]: position } },
	});

	let opening = tg.Process.connect(id, { reads: [{ streams: ["stderr"] }] });
	let first = await accept();
	let process = await opening;
	await process.stderr.close();
	assert.deepEqual((await first.next()).arg, { kind: "close", value: 1 });
	first.end();
	await tick();

	// Concurrent operations share a single reopened stream.
	let stdout = process.readStdio({ streams: ["stdout"] });
	let stderr = process.readStdio({ streams: ["stderr"] });
	let second = await accept();
	assert.equal(second.reads[1].streams, "stdout");
	let extra = await second.next();
	assert.equal(extra.arg.kind, "read");
	assert.equal(extra.arg.value.streams, "stderr");
	assert.equal(requests.length, 2);
	let out = await stdout;
	let err = await stderr;
	let reading = out.next();
	chunk(second, 1, 0, "a");
	assert.equal(Buffer.from((await reading).value.bytes).toString(), "a");
	// Returning an iterator before its first next still releases its read request.
	await err.return();
	assert.deepEqual((await second.next()).arg, { kind: "close", value: extra.id });
	second.end(new Error("transport failed"));
	await tick();

	reading = out.next();
	let current = await accept();
	assert.equal(current.reads[1].position, 1);
	chunk(current, 1, 1, "b");
	end(current, 1, 2);
	assert.equal(Buffer.from((await reading).value.bytes).toString(), "b");
	assert.equal((await out.next()).done, true);

	// An idle in-progress read is also canceled immediately.
	let idle = await process.readStdio({ streams: ["stderr"] });
	let idleRequest = await current.next();
	let pendingRead = idle.next();
	await idle.return();
	assert.equal((await pendingRead).done, true);
	assert.deepEqual((await current.next()).arg, { kind: "close", value: idleRequest.id });

	// Canceling while the replacement connection is opening also releases its read.
	let interrupted = await process.readStdio({ streams: ["stderr"] });
	assert.equal((await current.next()).arg.kind, "read");
	let interruptedRead = interrupted.next();
	current.end();
	let replacement = await connections.next();
	assert.equal(replacement.arg.reads[1].streams, "stderr");
	let returning = interrupted.return();
	replacement.emit("notification", { kind: "progress", value: { kind: "output", value: selected } });
	assert.equal((await returning).done, true);
	assert.equal((await interruptedRead).done, true);
	assert.deepEqual((await replacement.next()).arg, { kind: "close", value: 1 });
	current = replacement;

	// A retried EOF or finite completion cannot hide a missing final chunk.
	for (let kind of ["end", "limit", "timeout"]) {
		let read = await process.readStdio({ streams: ["stdout"] });
		let request = await current.next();
		let pending = read.next();
		let rejected = assert.rejects(pending, /gap at the end/);
		if (kind === "end") end(current, request.id, 4);
		else current.response(request.id, "read", { kind, value: { position: 4 } });
		await rejected;
		assert.deepEqual((await current.next()).arg, { kind: "close", value: request.id });
	}

	// A receipt ACK does not complete a write; reconnect replays its unconfirmed bytes.
	let writing = process.stdin.write(new TextEncoder().encode("abc"));
	let write = await current.next();
	assert.equal(write.arg.kind, "write");
	current.emit("ack", { id: write.id });
	current.end();
	let last = await accept();
	let retry = await last.next();
	assert.deepEqual(retry.arg.value.data, write.arg.value.data);
	last.response(retry.id, "write", { closed: false, length: 3 });
	assert.equal(await writing, 3);
	let closing = process.stdin.close();
	let eof = await last.next();
	assert.deepEqual(eof.arg.value.data, { kind: "end", value: { combined_position: 3, stream_positions: { stdin: 3 } } });
	last.response(eof.id, "write", { closed: true, length: 0 });
	await closing;
	last.emit("notification", { kind: "outcome", value: { exit: 0 } });
	assert.equal((await process.wait()).exit, 0);
	let connection = process.connection;
	await process.detach();
	await assert.rejects(connection.read(id, { streams: ["stdout"] }), /closed/);
	assert.equal(process.connection, null);
	assert.equal(requests.length, 5);
' | complete
success $output
