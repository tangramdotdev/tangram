use ../lib/test.nu *

# A node process connects and receives its outcome.

const javascript_path = path self '../../../javascript'
cd $javascript_path

# Receipt, operation completion, process exit, and read EOF are independent handshakes, and stdio uses its own requests.
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
			if (waiter) waiter({ value, done: false });
			else this.values.push(value);
		}
		next() {
			return this.values.length
				? Promise.resolve({ value: this.values.shift(), done: false })
				: new Promise((resolve) => this.waiters.push(resolve));
		}
		[Symbol.asyncIterator]() {
			return this;
		}
	}
	const stream = (Body, events, header) => new Body({
		[Symbol.asyncIterator]: async function* () {
			if (header) yield new Uint8Array([2, 123, 125]);
			for await (let event of events) {
				if (event === null) return;
				if (event instanceof Error) throw event;
				yield new TextEncoder().encode("event: " + event.event + "\ndata: " + event.data + "\n\n");
			}
		},
	});
	const encode = (value) => ({ event: value[0], data: JSON.stringify(value[1]) });
	let requests = [];
	let input;
	let query;
	let events = new Queue();
	let stdio = new Queue();
	tg.client.send = async (request) => {
		requests.push(request.uri.path);
		if (request.uri.path === "/processes/connect") {
			input = request.body.sse();
			query = new URLSearchParams(request.uri.query);
			return new tg.Response(200, { "content-type": "text/event-stream" }, stream(request.body.constructor, events, true));
		}
		let output = new Queue();
		stdio.push({
			emit: (event, value) => output.push(encode([event, value])),
			end: () => output.push(null),
			input: request.body.sse(),
			path: request.uri.path,
			query: new URLSearchParams(request.uri.query),
		});
		return new tg.Response(200, { "content-type": "text/event-stream" }, stream(request.body.constructor, output, false));
	};
	const emit = (event, value) => events.push(encode([event, value]));
	const selected = (value) => emit("notification", { kind: "progress", value: { kind: "output", value } });
	const response = (id, kind, value) =>
		emit("response", {
			id,
			error: null,
			output: { kind, ...(value === undefined ? {} : { value }) },
		});
	const next = async () => {
		while (true) {
			let event = (await input.next()).value;
			if (event.event !== "ack" && !(event.event === "notification" && JSON.parse(event.data).kind === "ready"))
				return { kind: event.event, value: JSON.parse(event.data) };
		}
	};
	const data = async (connection) => {
		let event = (await connection.input.next()).value;
		assert.equal(event.event, "data");
		return JSON.parse(event.data);
	};
	const output = { cached: false, lease: null, location: null, process: id, tokens: {}, outcome: null };

	let connected = false;
	let opening = tg.Process.connect(id).then((value) => {
		connected = true;
		return value;
	});
	await tick();
	assert.equal(query.get("process"), id);
	assert.equal([...query.keys()].some((key) => key.startsWith("reads")), false);
	await tick();
	assert.equal(connected, false);
	selected(output);
	let process = await opening;

	// A write completes only once the server reports its position.
	let written = false;
	let writing = process.stdin
		.write(new TextEncoder().encode("abc"))
		.then((value) => {
			written = true;
			return value;
		});
	let writer = (await stdio.next()).value;
	assert.equal(writer.path, `/processes/${id}/stdio/write`);
	let write = await data(writer);
	assert.equal(write.kind, "chunk");
	await tick();
	assert.equal(written, false);
	writer.emit("output", { closed: false, position: 3 });
	assert.equal(await writing, 3);
	let closing = process.stdin.close();
	let end = await data(writer);
	assert.deepEqual(end, {
		kind: "end", value: { combined_position: 3, stream_positions: { stdin: 3 } },
	});
	writer.emit("output", { closed: true, position: 3 });
	await closing;
	writer.end();

	// The outcome arrives on the connection.
	emit("notification", { kind: "outcome", value: { exit: 0 } });
	assert.equal((await process.wait()).exit, 0);

	// A chunk does not end a read; its response does.
	let readDone = false;
	let reading = process.stdout.text().then((value) => {
		readDone = true;
		return value;
	});
	let reader = (await stdio.next()).value;
	assert.equal(reader.path, `/processes/${id}/stdio/read`);
	assert.equal(reader.query.get("streams"), "stdout");
	reader.emit("notification", { kind: "chunk", value: {
		bytes: "YWJj", combined_position: 0, stream: "stdout", stream_position: 0,
	} });
	await tick();
	assert.equal(readDone, false);
	reader.emit("response", { kind: "end", value: { combined_position: 3, stream_positions: { stdout: 3 } } });
	reader.end();
	assert.equal(await reading, "abc");
	assert.equal((await process.wait()).exit, 0);
	assert.deepEqual(requests, [
		"/processes/connect",
		`/processes/${id}/stdio/write`,
		`/processes/${id}/stdio/read`,
	]);

	// A transport failure after a receipt fails the operation, and the connection reopens to wait.
	requests = [];
	events = new Queue();
	let reopening = tg.Process.connect(id);
	await tick();
	selected(output);
	let failed = await reopening;
	let signaling = failed.signal(tg.Process.Signal.TERM);
	let waiting = failed.wait();
	let signalError = assert.rejects(signaling, /transport failed/);
	let signal = await next();
	assert.equal(signal.value.arg.kind, "signal");
	emit("ack", { id: signal.value.id });
	events.push(new Error("transport failed"));
	await signalError;
	await tick();
	assert.equal(query.get("mode"), "run");
	assert.equal(query.get("process"), id);
	selected(output);
	emit("notification", { kind: "outcome", value: { exit: 0 } });
	assert.equal((await waiting).exit, 0);
	assert.deepEqual(requests, Array(2).fill("/processes/connect"));
	await failed.detach();

	// Backpressure on requests must leave room for response acknowledgments.
	events = new Queue();
	let openingBusy = tg.Process.connect(id);
	await tick();
	selected(output);
	let busy = await openingBusy;
	assert.deepEqual(JSON.parse((await input.next()).value.data), { kind: "ready" });
	let pending = [busy.signal(tg.Process.Signal.TERM)];
	pending[0].catch(() => {});
	let first = await next();
	for (let index = 0; index < 63; index++) {
		let signaling = busy.signal(tg.Process.Signal.TERM);
		signaling.catch(() => {});
		pending.push(signaling);
	}
	await assert.rejects(busy.signal(tg.Process.Signal.TERM), /too many pending/);
	response(first.value.id, "signal");
	await pending[0];
	let acknowledgment = (await input.next()).value;
	assert.equal(acknowledgment.event, "ack");
	assert.deepEqual(JSON.parse(acknowledgment.data), { id: first.value.id });
	for (let index = 0; index < 63; index++) {
		let signal = await next();
		assert.equal(signal.value.arg.kind, "signal");
		response(signal.value.id, "signal");
	}
	await Promise.all(pending);
	emit("notification", { kind: "outcome", value: { exit: 0 } });
	assert.equal((await busy.wait()).exit, 0);
	await busy.detach();
' | complete
success $output
