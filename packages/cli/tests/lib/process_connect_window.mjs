import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import http from "node:http";

const [socketPath, command, ready] = process.argv.slice(2);
const requestWindow = 128;
const maxChunks = 64;
const chunkSize = 32 * 1024;
const timer = setTimeout(() => {
	throw new Error("the connect window test timed out");
}, 25000);
const request = http.request({
	socketPath,
	path: "/processes/connect",
	method: "POST",
	headers: { accept: "text/event-stream", "content-type": "text/event-stream" },
});
const send = (event, value) =>
	request.write(`event: ${event}
data: ${JSON.stringify(value)}

`);
const completed = new Set();
let waited = false;
const done = new Promise((resolve, reject) => {
	request.on("error", reject);
	request.on("response", (response) => {
		assert.equal(response.statusCode, 200);
		response.on("error", reject);
		let buffer = "";
		response.setEncoding("utf8");
		response.on("data", (chunk) => {
			try {
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
							.slice(5),
					);
					if (event === "error") throw new Error(JSON.stringify(value));
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
					if (event === "notification" && value.kind === "wait") {
						assert.equal(value.value.exit, 0);
						waited = true;
					}
					if (waited && completed.size === requestWindow + 1) resolve();
				}
			} catch (error) {
				reject(error);
			}
		});
		response.on("end", () => {
			if (!waited || completed.size !== requestWindow + 1)
				reject(new Error("the connection closed early"));
		});
	});
});
try {
	send("request", {
		id: 0,
		arg: {
			kind: "connect",
			value: {
				mode: "run",
				location: "remote",
				process: {
					command,
					cached: false,
					sandbox: {},
					stdin: "pipe",
					stdout: "null",
					stderr: "null",
				},
			},
		},
	});
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
	for (let id = maxChunks + 2; id <= requestWindow; id++) {
		send("request", { id, arg: { kind: "close", value: requestWindow + id } });
	}
	writeFileSync(ready, "ready");
	await done;
	console.log("ok");
} finally {
	clearTimeout(timer);
	request.destroy();
}
