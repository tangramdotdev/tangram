use ../../test.nu *

const js_path = path self '../../../js'
cd $js_path

# Receipt, operation completion, process exit, and read EOF are independent handshakes.
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
	let requests = [];
	let input;
	let events = new Queue();
	tg.client.send = async (request) => {
		requests.push(request.uri.path);
		input = request.body.sse();
		return new tg.Response(
			200,
			{ "content-type": "text/event-stream" },
			{
				sse: async function* () {
					for await (let event of events) {
						if (event instanceof Error) throw event;
						yield event;
					}
				},
			},
		);
	};
	const emit = (event, value) =>
		events.push({ event, data: JSON.stringify(value) });
	const response = (id, kind, value) =>
		emit("response", {
			id,
			error: null,
			output: { kind, ...(value === undefined ? {} : { value }) },
		});
	const next = async () => {
		while (true) {
			let event = (await input.next()).value;
			if (event.event !== "ack")
				return { kind: event.event, value: JSON.parse(event.data) };
		}
	};
	let connected = false;
	let opening = tg.Process.connect(id, { reads: [{ streams: ["stdout"] }] }).then(
		(value) => {
			connected = true;
			return value;
		},
	);
	await tick();
	let initial = await next();
	assert.equal(initial.value.arg.kind, "connect");
	emit("ack", { id: 0 });
	await tick();
	assert.equal(connected, false);
	response(0, "connect", {
		cached: false,
		lease: null,
		location: null,
		process: id,
		tokens: {},
		wait: null,
	});
	let process = await opening;
	let written = false;
	let writing = process.stdin
		.write(new TextEncoder().encode("abc"))
		.then((value) => {
			written = true;
			return value;
		});
	let write = await next();
	assert.equal(write.value.arg.kind, "write");
	emit("ack", { id: write.value.id });
	await tick();
	assert.equal(written, false);
    assert.equal(write.value.arg.value.data.kind, "chunk");
    response(write.value.id, "write", { closed: false, length: 3 });
    assert.equal(await writing, 3);
    let closing = process.stdin.close();
    let end = await next();
    assert.deepEqual(end.value.arg.value.data, {
        kind: "end", value: { combined_position: 3, stream_positions: { stdin: 3 } },
    });
    response(end.value.id, "write", { closed: true, length: 0 });
    await closing;
	emit("notification", { kind: "wait", value: { exit: 0 } });
	assert.equal((await process.wait()).exit, 0);
    let readDone = false;
    let reading = process.stdout.text().then(value => { readDone = true; return value; });
    emit("notification", {
        kind: "read", value: { id: 1, event: { kind: "chunk", value: {
            bytes: "YWJj", combined_position: 0, stream: "stdout", stream_position: 0,
        } } },
    });
    await tick();
    assert.equal(readDone, false);
    response(1, "read", { kind: "end" });
    assert.equal(await reading, "abc");
    while (true) {
        const event = (await input.next()).value;
        if (event.event === "ack" && JSON.parse(event.data).id === 1) break;
    }
	assert.equal((await process.wait()).exit, 0);
	assert.deepEqual(requests, ["/processes/connect"]);

	events = new Queue();
	let reopening = tg.Process.connect(id);
	await tick();
	await next();
	response(0, "connect", {
		cached: false,
		lease: null,
		location: null,
		process: id,
		tokens: {},
		wait: null,
	});
	let failed = await reopening;
	let signaling = failed.signal(tg.Process.Signal.TERM);
	let waiting = failed.wait();
	let signalError = assert.rejects(signaling, /transport failed/);
	let waitError = assert.rejects(waiting, /transport failed/);
	let signal = await next();
	assert.equal(signal.value.arg.kind, "signal");
	emit("ack", { id: signal.value.id });
	events.push(new Error("transport failed"));
	await Promise.all([signalError, waitError]);
	assert.deepEqual(requests, ["/processes/connect", "/processes/connect"]);

	// Backpressure on requests and stdio must leave room for response acknowledgments.
	events = new Queue();
	let openingBusy = tg.Process.connect(id, { reads: [{ streams: ["stdout"] }] });
	await tick();
	await next();
	response(0, "connect", {
		cached: false,
		lease: null,
		location: null,
		process: id,
		tokens: {},
		wait: null,
	});
	let busy = await openingBusy;
	assert.equal((await input.next()).value.event, "ack");
	let busyReading = busy.stdout.text();
	let pending = [busy.signal(tg.Process.Signal.TERM)];
	pending[0].catch(() => {});
	let first = await next();
	for (let index = 0; index < 63; index++) {
		let signaling = busy.signal(tg.Process.Signal.TERM);
		signaling.catch(() => {});
		pending.push(signaling);
	}
    response(1, "read", { kind: "end" });
    assert.equal(await busyReading, "");
    const readAck = (await input.next()).value;
    assert.equal(readAck.event, "ack");
    assert.deepEqual(JSON.parse(readAck.data), { id: 1 });
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
	emit("notification", { kind: "wait", value: { exit: 0 } });
	assert.equal((await busy.wait()).exit, 0);
	await busy.detach();
' | complete
success $output
