import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import http from "node:http";

const [socketPath, command, ready] = process.argv.slice(2);
const requestCount = 128;
const maxChunks = 64;
const chunkSize = 32 * 1024;
const timer = setTimeout(() => {
	throw new Error("the connect buffering test timed out");
}, 25000);
const arg = {
	location: "remote",
	mode: "run",
	process: {
		cached: false,
		command,
		sandbox: {},
		stderr: "null",
		stdin: "pipe",
		stdout: "null",
	},
};
const request = http.request({
	socketPath,
	path: "/processes/connect",
	method: "POST",
	headers: {
		accept: "text/event-stream",
		"content-type": "text/event-stream",
		"x-tg-arg-in-body": "true",
	},
});
const send = (event, value) =>
	request.write(`event: ${event}
data: ${JSON.stringify(value)}

`);
const header = Promise.withResolvers();
const completed = new Set();
let outcomeReceived = false;
const done = new Promise((resolve, reject) => {
	request.on("error", reject);
	request.on("response", (response) => {
		assert.equal(response.statusCode, 200);
		response.on("error", reject);
		let buffer = "";
		let prefix = Buffer.alloc(0);
		let headerReceived = false;

		response.on("data", (chunk) => {
			try {
				if (!headerReceived) {
					prefix = Buffer.concat([prefix, chunk]);
					if (prefix.length < 3) return;
					assert.deepEqual(prefix.subarray(0, 3), Buffer.from([2, 123, 125]));
					headerReceived = true;
					chunk = prefix.subarray(3);
					header.resolve({});
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
							.slice(5),
					);
					if (event === "error") throw new Error(JSON.stringify(value));
					if (
						event === "notification" &&
						value.kind === "progress" &&
						value.value.kind === "output"
					)
						send("notification", { kind: "ready" });
					if (event === "response") {
						assert.equal(value.error, null, JSON.stringify(value));
						assert(!completed.has(value.id));
						completed.add(value.id);
						send("ack", { id: value.id });
						if (value.id >= 1 && value.id <= maxChunks) {
							assert.equal(value.output.kind, "write");
							assert.equal(value.output.value.length, chunkSize);
						}
					}
					if (event === "notification" && value.kind === "outcome") {
						assert.equal(value.value.exit, 0);
						outcomeReceived = true;
					}
					if (outcomeReceived && completed.size === requestCount) resolve();
				}
			} catch (error) {
				reject(error);
			}
		});
		response.on("end", () => {
			if (!outcomeReceived || completed.size !== requestCount)
				reject(new Error("the connection closed early"));
		});
	});
});
try {
	const argBytes = Buffer.from(JSON.stringify(arg));
	let length = argBytes.length;
	const prefix = [];
	while (length >= 128) {
		prefix.push((length % 128) | 128);
		length = Math.floor(length / 128);
	}
	prefix.push(length);
	request.write(Buffer.concat([Buffer.from(prefix), argBytes]));

	await header.promise;
	const bytes = Buffer.alloc(chunkSize, 120);
	bytes[chunkSize - 1] = 10;
	for (let index = 0; index < maxChunks; index++) {
		send("request", {
			id: index + 1,
			arg: {
				kind: "write",
				value: {
					data: {
						kind: "chunk",
						value: {
							bytes: bytes.toString("base64"),
							combined_position: index * chunkSize,
							stream: "stdin",
							stream_position: index * chunkSize,
						},
					},
				},
			},
		});
	}
	send("request", {
		id: maxChunks + 1,
		arg: {
			kind: "write",
			value: {
				data: {
					kind: "end",
					value: {
						combined_position: maxChunks * chunkSize,
						stream_positions: { stdin: maxChunks * chunkSize },
					},
				},
			},
		},
	});
	for (let id = maxChunks + 2; id <= requestCount; id++) {
		send("request", { id, arg: { kind: "close", value: requestCount + id } });
	}
	writeFileSync(ready, "ready");
	await done;
	console.log("ok");
} finally {
	clearTimeout(timer);
	request.destroy();
}
