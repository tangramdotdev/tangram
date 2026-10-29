import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import http from "node:http";

const [socketPath, command, ready] = process.argv.slice(2);
const requestCount = 32;
const timer = setTimeout(() => {
	throw new Error("the attach buffering test timed out");
}, 25000);
const arg = {
	location: "remote",
	mode: "run",
	process: {
		cached: false,
		command,
		sandbox: {},
		stderr: "null",
		stdin: "null",
		stdout: "null",
	},
};
const request = http.request({
	socketPath,
	path: "/processes/attach",
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
					let length = 0;
					let shift = 0;
					let offset = 0;
					for (;;) {
						if (offset >= prefix.length) return;
						const byte = prefix[offset++];
						length += (byte & 127) * 2 ** shift;
						if (byte < 128) break;
						shift += 7;
					}
					if (prefix.length < offset + length) return;
					const value = JSON.parse(
						prefix.subarray(offset, offset + length).toString("utf8"),
					);
					assert(value.sync, "the header should carry the command sync");
					headerReceived = true;
					chunk = prefix.subarray(offset + length);
					header.resolve(value);
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
						assert(!completed.has(value.id));
						completed.add(value.id);
						send("ack", { id: value.id });
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
	for (let id = 1; id <= requestCount; id++) {
		send("request", {
			id,
			arg: { kind: "tty", value: { size: { cols: 80, rows: 24 } } },
		});
	}
	writeFileSync(ready, "ready");
	await done;
	console.log("ok");
} finally {
	clearTimeout(timer);
	request.destroy();
}
