import assert from "node:assert/strict";
import http from "node:http";

const [socketPath, id, lease, mode, location = "remote"] =
	process.argv.slice(2);
const timer = setTimeout(() => {
	throw new Error("the connect test timed out");
}, 10000);
const requests = new Map();
const notifications = [];
const chunks = [];
let nextId = 0;
let closed = false;
let notify;
let error;
let connected = false;
const request = http.request({
	socketPath,
	path: "/processes/connect",
	method: "POST",
	headers: { accept: "text/event-stream", "content-type": "text/event-stream" },
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
	response.setEncoding("utf8");
	response.on("data", (chunk) => {
		buffer += chunk;
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
				if (mode === "cached" && value.kind === "read") {
					const { id, message } = value.value;
					if (
						message.kind === "notification" &&
						message.value.kind === "chunk"
					) {
						const chunk = message.value.value;
						const bytes = Buffer.from(chunk.bytes, "base64");
						chunks.push(bytes);
						sendEvent("notification", {
							kind: "read",
							value: {
								id,
								message: {
									kind: "notification",
									value: {
										kind: "read",
										value: { position: chunk.stream_position + bytes.length },
									},
								},
							},
						});
					} else if (
						message.kind === "request" &&
						message.value.kind === "end"
					) {
						sendEvent("notification", {
							kind: "read",
							value: {
								id,
								message: {
									kind: "response",
									value: { kind: "end" },
								},
							},
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
	const target =
		mode === "cached"
			? {
					kind: "spawn",
					value: {
						arg: {
							cached: true,
							command: { node: id },
							sandbox: {},
							stdin: "null",
							stdout: "log",
							stderr: "log",
						},
						mode: "run",
					},
				}
			: {
					kind: "existing",
					value: { id, ...(lease === "none" ? {} : { lease }), location },
				};
	const reads = mode === "cached" ? { 1: { streams: "stderr" } } : {};
	const output = await send({ kind: "connect", value: { reads, target } });
	if (mode === "cached") nextId = 2;
	assert.equal(output.kind, "connect");
	connected = true;
	if (mode === "cached") {
		assert.equal(output.value.cached, true);
		await until(() => closed);
		assert.equal(Buffer.concat(chunks).toString(), "cached log\n");
		const wait = notifications.find((value) => value.kind === "wait");
		assert.equal(wait.value.exit, 0);
		assert.equal(wait.value.output, "cached output");
	} else if (mode === "disconnect") {
		request.destroy();
	} else if (mode === "write") {
		const writeId = nextId;
		assert.equal(
			(await send({ kind: "write", value: { streams: "stdin" } })).kind,
			"write",
		);
		sendEvent("notification", {
			kind: "write",
			value: {
				id: writeId,
				message: {
					kind: "request",
					value: {
						kind: "chunk",
						value: {
							bytes: "eA==",
							combined_position: 0,
							stream: "stdin",
							stream_position: 0,
						},
					},
				},
			},
		});
		await until(() =>
			notifications.some(
				(value) =>
					value.kind === "write" && value.value.message.value.kind === "write",
			),
		);
		assert.equal(
			(await send({ kind: "cancel", value: { lease } })).kind,
			"cancel",
		);
		await until(() => notifications.some((value) => value.kind === "wait"));
		sendEvent("notification", {
			kind: "write",
			value: {
				id: writeId,
				message: {
					kind: "request",
					value: { kind: "end", value: { position: 1 } },
				},
			},
		});
		await until(() =>
			notifications.some(
				(value) =>
					value.kind === "write" && value.value.message.value.kind === "end",
			),
		);
		assert(
			notifications.some(
				(value) =>
					value.kind === "write" && value.value.message.value.kind === "write",
			),
		);
		await until(() => closed);
	} else {
		if (mode === "idle") {
			for (let index = 0; index < 70; index++) {
				const readId = nextId;
				assert.equal(
					(await send({ kind: "read", value: { streams: "stdout" } })).kind,
					"read",
				);
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
