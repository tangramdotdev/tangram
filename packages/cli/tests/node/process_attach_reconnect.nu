use ../lib/test.nu *

# A node process reconnects without losing pending stdio.

const javascript_path = path self '../../../javascript'
cd $javascript_path

# Reopen the selected process once for concurrent operations, and resume stdio from its positions.
let output = timeout 15 node --input-type=module -e '
	import assert from "node:assert/strict";
	import * as tg from "@tangramdotdev/client";
	tg.setEncoding({
		base64: {
			decode: (value) => new Uint8Array(Buffer.from(value, "base64")),
			encode: (value) => Buffer.from(value).toString("base64"),
		},
		utf8: {
			decode: (value) => new TextDecoder().decode(value),
			encode: (value) => new TextEncoder().encode(value),
		},
	});
	const id = "pcs_010000000000000000000000000000000000000000000000000000";
	const tick = () => new Promise((resolve) => setImmediate(resolve));
	class Queue {
		values = [];
		waiters = [];
		push(value) {
			const waiter = this.waiters.shift();
			if (waiter) waiter(value);
			else this.values.push(value);
		}
		next() {
			return this.values.length ? Promise.resolve(this.values.shift()) : new Promise((resolve) => this.waiters.push(resolve));
		}
	}
	let connections = new Queue();
	let stdio = new Queue();
	let requests = [];
	tg.client.send = async (request) => {
		requests.push(request.uri.path);
		let input = request.body.sse();
		let query = new URLSearchParams(request.uri.query);
		let events = new Queue();
		let next = async () => {
			while (true) {
				let event = await input.next();
				assert.equal(event.done, false);
				if (event.value.event === "request" || event.value.event === "data") return JSON.parse(event.value.data);
			}
		};
		let emit = (event, value) => events.push({ event, data: JSON.stringify(value) });
		let connection = { emit, end: (error = null) => events.push(error), next, path: request.uri.path, query };
		let attach = request.uri.path === "/processes/attach";
		(attach ? connections : stdio).push(connection);
		let Body = request.body.constructor;
		return new tg.Response(200, { "content-type": "text/event-stream" }, new Body({
			[Symbol.asyncIterator]: async function* () {
				if (attach) yield new Uint8Array([2, 123, 125]);
				while (true) {
					let event = await events.next();
					if (event === null) return;
					if (event instanceof Error) throw event;
					yield new TextEncoder().encode("event: " + event.event + "\ndata: " + event.data + "\n\n");
				}
			},
		}));
	};
	const selected = { cached: false, lease: null, location: null, process: id, tokens: {}, outcome: null };
	const accept = async () => {
		let connection = await connections.next();
		assert.equal(connection.query.get("mode"), "run");
		assert.equal(connection.query.get("process"), id);
		connection.emit("notification", { kind: "progress", value: { kind: "output", value: selected } });
		return connection;
	};
	const chunk = (connection, position, text) => connection.emit("notification", { kind: "chunk", value: {
		bytes: Buffer.from(text).toString("base64"), combined_position: position, stream: "stdout", stream_position: position,
	} });
	const end = (connection, position) => connection.emit("response", {
		kind: "end", value: { combined_position: position, stream_positions: { stdout: position } },
	});

	let opening = tg.Process.attach(id);
	let first = await accept();
	let process = await opening;
	first.end();
	await tick();

	// Concurrent operations share a single reopened stream.
	let signaling = process.signal(tg.Process.Signal.TERM);
	let resizing = process.setTtySize({ cols: 80, rows: 24 });
	let current = await accept();
	let kinds = [];
	for (let index = 0; index < 2; index++) {
		let request = await current.next();
		kinds.push(request.arg.kind);
		current.emit("response", { id: request.id, error: null, output: { kind: request.arg.kind } });
	}
	assert.deepEqual(kinds.sort(), ["signal", "tty"]);
	await signaling;
	await resizing;
	assert.equal(requests.filter((path) => path === "/processes/attach").length, 2);

	// A failed read resumes from the position it reached.
	let out = await process.readStdio({ streams: ["stdout"] });
	let reading = out.next();
	let read = await stdio.next();
	assert.equal(read.path, `/processes/${id}/stdio/read`);
	chunk(read, 0, "a");
	assert.equal(Buffer.from((await reading).value.bytes).toString(), "a");
	reading = out.next();
	read.end(new Error("transport failed"));
	read = await stdio.next();
	assert.equal(read.query.get("position"), "1");
	chunk(read, 1, "b");
	end(read, 2);
	assert.equal(Buffer.from((await reading).value.bytes).toString(), "b");
	assert.equal((await out.next()).done, true);

	// A retried EOF or finite completion cannot hide a missing final chunk.
	for (let kind of ["end", "limit", "timeout"]) {
		let iterator = await process.readStdio({ streams: ["stdout"] });
		let pending = iterator.next();
		let rejected = assert.rejects(pending, /gap at the end/);
		let read = await stdio.next();
		if (kind === "end") end(read, 4);
		else read.emit("response", { kind, value: { position: 4 } });
		await rejected;
	}

	// A write completes only once its position is reported; reconnect resends its unreported bytes.
	let writing = process.stdin.write(new TextEncoder().encode("abc"));
	let writer = await stdio.next();
	assert.equal(writer.path, `/processes/${id}/stdio/write`);
	let write = await writer.next();
	assert.equal(write.kind, "chunk");
	writer.end(new Error("transport failed"));
	writer = await stdio.next();
	let retry = await writer.next();
	assert.deepEqual(retry, write);
	writer.emit("output", { closed: false, position: 3 });
	assert.equal(await writing, 3);
	let closing = process.stdin.close();
	let eof = await writer.next();
	assert.deepEqual(eof, { kind: "end", value: { combined_position: 3, stream_positions: { stdin: 3 } } });
	writer.emit("output", { closed: true, position: 3 });
	await closing;

	current.emit("notification", { kind: "outcome", value: { exit: 0 } });
	assert.equal((await process.wait()).exit, 0);
	await process.detach();
	assert.equal(process.connection, null);
	assert.equal(requests.filter((path) => path === "/processes/attach").length, 2);
' | complete
success $output
