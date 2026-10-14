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
let nextId = 1;
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
const arg = {
	...(mode === "cached"
		? {}
		: { lease: lease === "none" ? null : lease, location }),
	mode: "run",
	process: selected,
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
		console.log(output.value.process);
		await until(() => closed);
		const outcome = notifications.find((value) => value.kind === "outcome");
		assert.equal(outcome.value.exit, 0);
		assert.equal(outcome.value.output, "cached output");
	} else if (mode === "disconnect") {
		request.destroy();
	} else if (mode === "control" || mode === "control_denied") {
		const operations = [{ kind: "signal", value: { signal: "TERM" } }];
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
		assert.equal((await send({ kind: "detach" })).kind, "detach");
		await until(() => closed);
	} else {
		assert.equal((await send({ kind: "detach" })).kind, "detach");
		await until(() => closed);
	}
} finally {
	clearTimeout(timer);
	request.destroy();
}
