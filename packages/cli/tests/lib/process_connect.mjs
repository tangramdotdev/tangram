import assert from "node:assert/strict";
import http from "node:http";

const [socketPath, id, lease, mode, location = "remote", optionsJson = "{}"] =
	process.argv.slice(2);
const options = JSON.parse(optionsJson);
const timer = setTimeout(() => {
	throw new Error("the connect test timed out");
}, 10000);
const requests = new Map();
const notifications = [];
const chunks = [];
const consumed = new Map();
let nextId = mode === "cached" ? 2 : 1;
let closed = false;
let notify;
let error;
let connected = false;
const selected =
	mode === "cached"
		? {
				cached: true,
				command: JSON.parse(id),
				sandbox: {},
				stderr: "log",
				stdin: "null",
				stdout: "log",
			}
		: id;
const reads = mode === "cached" ? { 1: { streams: "stderr" } } : {};
const arg = {
	...(mode === "cached"
		? {}
		: { lease: lease === "none" ? null : lease, location }),
	mode: "run",
	process: selected,
	reads,
	tokens: options.tokens,
};
const request = http.request({
	socketPath,
	path: "/processes/connect",
	method: "POST",
	headers: {
		accept: "text/event-stream",
		"content-type": "text/event-stream",
		"x-tg-arg-in-body": "true",
		...(options.authorization
			? { authorization: `Bearer ${options.authorization}` }
			: {}),
	},
});
function finish(cause) {
	closed = true;
	error ??= cause;
	for (const { reject } of requests.values())
		reject(cause ?? new Error("the connection closed before the response"));
	requests.clear();
	notify?.();
}
function sendEvent(event, value) {
	request.write(`event: ${event}\ndata: ${JSON.stringify(value)}\n\n`);
}
function send(arg) {
	const id = nextId++;
	const result = new Promise((resolve, reject) => {
		requests.set(id, { resolve, reject });
	});
	sendEvent("request", { id, arg });
	return result;
}
async function until(predicate) {
	while (!predicate()) {
		if (error) throw error;
		if (closed)
			throw new Error("the connection closed before the notification");
		await new Promise((resolve) => {
			notify = resolve;
		});
	}
	if (error) throw error;
}
request.on("error", (cause) => {
	if (!connected || mode !== "disconnect") finish(cause);
});
request.on("response", (response) => {
	assert.equal(response.statusCode, 200);
	let buffer = "";
	let prefix = Buffer.alloc(0);
	let headerReceived = false;

	response.on("data", (chunk) => {
		if (!headerReceived) {
			prefix = Buffer.concat([prefix, chunk]);
			if (prefix.length < 3) return;
			assert.deepEqual(prefix.subarray(0, 3), Buffer.from([2, 123, 125]));
			headerReceived = true;
			chunk = prefix.subarray(3);
		}
		buffer += chunk.toString("utf8");
		while (buffer.includes("\n\n")) {
			const index = buffer.indexOf("\n\n");
			const frame = buffer.slice(0, index);
			buffer = buffer.slice(index + 2);
			const event = frame
				.split("\n")
				.find((line) => line.startsWith("event:"))
				?.slice(6)
				.trim();
			const value = JSON.parse(
				frame
					.split("\n")
					.find((line) => line.startsWith("data:"))
					?.slice(5),
			);
			if (event === "error") {
				finish(new Error(JSON.stringify(value)));
				return;
			}
			if (event === "response") {
				sendEvent("ack", { id: value.id });
				const pending = requests.get(value.id);
				requests.delete(value.id);
				if (value.error)
					pending?.reject(new Error(JSON.stringify(value.error)));
				else pending?.resolve(value.output);
			} else if (event === "notification") {
				notifications.push(value);
				if (
					(mode === "cached" || mode === "control") &&
					value.kind === "read"
				) {
					const { id, event } = value.value;
					if (event.kind === "chunk") {
						const bytes = Buffer.from(event.value.bytes, "base64");
						chunks.push(bytes);
						consumed.set(id, (consumed.get(id) ?? 0) + bytes.length);
						sendEvent("notification", {
							kind: "read",
							value: { id, progress: { consumed: consumed.get(id) } },
						});
					}
				}
				notify?.();
			}
		}
	});
	response.on("error", (cause) => {
		if (!connected || mode !== "disconnect") finish(cause);
	});
	response.on("end", () => finish());
});
try {
	const bytes = Buffer.from(JSON.stringify(arg));
	let length = bytes.length;
	const prefix = [];
	while (length >= 128) {
		prefix.push((length % 128) | 128);
		length = Math.floor(length / 128);
	}
	prefix.push(length);
	request.write(Buffer.concat([Buffer.from(prefix), bytes]));
	request.flushHeaders();
	await until(() =>
		notifications.some(
			(value) => value.kind === "progress" && value.value.kind === "output",
		),
	);
	const output = notifications.find(
		(value) => value.kind === "progress" && value.value.kind === "output",
	).value;
	sendEvent("notification", { kind: "ready" });
	connected = true;
	if (mode === "cached") {
		assert.equal(output.value.cached, true);
		await until(() => closed);
		assert.equal(Buffer.concat(chunks).toString(), "cached log\n");
		const outcome = notifications.find((value) => value.kind === "outcome");
		assert.equal(outcome.value.exit, 0);
		assert.equal(outcome.value.output, "cached output");
	} else if (mode === "disconnect") {
		request.destroy();
	} else if (mode === "write") {
		const write = await send({
			kind: "write",
			value: {
				data: {
					kind: "chunk",
					value: {
						bytes: "eA==",
						combined_position: 0,
						stream: "stdin",
						stream_position: 0,
					},
				},
			},
		});
		assert.equal(write.kind, "write");
		assert.equal(write.value.length, 1);
		const end = send({
			kind: "write",
			value: {
				data: {
					kind: "end",
					value: {
						combined_position: 1,
						stream_positions: { stdin: 1 },
					},
				},
			},
		});
		assert.equal(
			(await send({ kind: "cancel", value: { lease } })).kind,
			"cancel",
		);
		assert.equal((await end).kind, "write");
		await until(() => closed);
	} else if (mode === "control" || mode === "control_denied") {
		const operations = [
			{ kind: "read", value: { length: 6, streams: "stdout" } },
			{ kind: "signal", value: { signal: "TERM" } },
			{
				kind: "write",
				value: {
					data: {
						kind: "chunk",
						value: {
							bytes: Buffer.from("hello\n").toString("base64"),
							combined_position: 0,
							stream: "stdin",
							stream_position: 0,
						},
					},
				},
			},
			...["stdout", "stderr"].map((stream) => ({
				kind: "read",
				value: {
					length: 6,
					position: stream === "stdout" ? 6 : 0,
					streams: stream,
				},
			})),
		];
		for (const operation of operations) {
			if (mode === "control_denied") {
				await assert.rejects(
					send(operation),
					/unauthorized|not found|failed to find/i,
				);
			} else {
				assert.equal((await send(operation)).kind, operation.kind);
			}
		}
		if (mode === "control")
			assert.equal(Buffer.concat(chunks).toString(), "ready\nhello\nhello\n");
		assert.equal((await send({ kind: "detach" })).kind, "detach");
		await until(() => closed);
	} else {
		if (mode === "idle") {
			for (let index = 0; index < 70; index++) {
				const readId = nextId;
				const read = send({ kind: "read", value: { streams: "stdout" } });
				read.catch(() => {});
				requests.delete(readId);
				assert.equal(
					(await send({ kind: "close", value: readId })).kind,
					"close",
				);
			}
		}
		assert.equal((await send({ kind: "detach" })).kind, "detach");
		await until(() => closed);
	}
} finally {
	clearTimeout(timer);
	request.destroy();
}
