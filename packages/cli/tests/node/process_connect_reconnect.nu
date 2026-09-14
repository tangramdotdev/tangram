use ../../test.nu *

const js_path = path self '../../../js'
cd $js_path

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
		connections.push({ input, next, emit, response, end: () => events.push(null) });
		return new tg.Response(200, { "content-type": "text/event-stream" }, {
			sse: async function* () {
				while (true) {
					let event = await events.next();
					if (event === null) return;
					yield event;
				}
			},
		});
	};
	const selected = { cached: false, lease: null, location: null, process: id, tokens: {}, wait: null };
	const accept = async () => {
		let connection = await connections.next();
		let opening = await connection.next();
		assert.equal(opening.arg.kind, "connect");
		assert.equal(opening.arg.value.target.kind, "existing");
		assert.equal(opening.arg.value.target.value.id, id);
		connection.response(0, "connect", selected);
		return { ...connection, reads: opening.arg.value.reads };
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
	second.end();
	await tick();

	reading = out.next();
	let third = await accept();
	assert.equal(third.reads[1].position, 1);
	chunk(third, 1, 1, "b");
	end(third, 1, 2);
	assert.equal(Buffer.from((await reading).value.bytes).toString(), "b");
	assert.equal((await out.next()).done, true);

	// An idle in-progress read is also canceled immediately.
	let idle = await process.readStdio({ streams: ["stderr"] });
	let idleRequest = await third.next();
	let pendingRead = idle.next();
	await idle.return();
	assert.equal((await pendingRead).done, true);
	assert.deepEqual((await third.next()).arg, { kind: "close", value: idleRequest.id });

	// A retried EOF or finite completion cannot hide a missing final chunk.
	for (let kind of ["end", "limit", "timeout"]) {
		let read = await process.readStdio({ streams: ["stdout"] });
		let request = await third.next();
		let pending = read.next();
		let rejected = assert.rejects(pending, /gap at the end/);
		if (kind === "end") end(third, request.id, 4);
		else third.response(request.id, "read", { kind, value: { position: 4 } });
		await rejected;
		assert.deepEqual((await third.next()).arg, { kind: "close", value: request.id });
	}

	// A receipt ACK does not complete a write; reconnect replays its unconfirmed bytes.
	let writing = process.stdin.write(new TextEncoder().encode("abc"));
	let write = await third.next();
	assert.equal(write.arg.kind, "write");
	third.emit("ack", { id: write.id });
	third.end();
	let fourth = await accept();
	let retry = await fourth.next();
	assert.deepEqual(retry.arg.value.data, write.arg.value.data);
	fourth.response(retry.id, "write", { closed: false, length: 3 });
	assert.equal(await writing, 3);
	let closing = process.stdin.close();
	let eof = await fourth.next();
	assert.deepEqual(eof.arg.value.data, { kind: "end", value: { combined_position: 3, stream_positions: { stdin: 3 } } });
	fourth.response(eof.id, "write", { closed: true, length: 0 });
	await closing;
	fourth.emit("notification", { kind: "wait", value: { exit: 0 } });
	assert.equal((await process.wait()).exit, 0);
	let connection = process.connection;
	await process.detach();
	await assert.rejects(connection.read(id, { streams: ["stdout"] }), /closed/);
	assert.equal(process.connection, null);
	assert.equal(requests.length, 4);
' | complete
success $output
