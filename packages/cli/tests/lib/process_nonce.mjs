import assert from "node:assert/strict";
import crypto from "node:crypto";
import http from "node:http";

// Print a new nonce, wait for a process by a connect's nonce and print the outcome or the response status, or spawn a process through a connect with a nonce and print the outcome.
const [mode, socketPath, ...rest] = process.argv.slice(2);

// Print a new nonce, which is 16 random bytes written with the symbols of the ID encoding.
if (mode === "nonce") {
	const symbols = "0123456789abcdefghjkmnpqrstvwxyz";
	let output = "";
	let buffer = 0;
	let bits = 0;
	for (const byte of crypto.randomBytes(16)) {
		buffer = (buffer << 8) | byte;
		bits += 8;
		while (bits >= 5) {
			bits -= 5;
			output += symbols[(buffer >> bits) & 31];
		}
		buffer &= (1 << bits) - 1;
	}
	if (bits > 0) {
		output += symbols[(buffer << (5 - bits)) & 31];
	}
	console.log(output);
	process.exit(0);
}

const [command, nonce, token] = mode === "connect" ? rest : [null, ...rest];
const timer = setTimeout(() => {
	throw new Error("the nonce test timed out");
}, 25000);
const authorization = token ? { authorization: `Bearer ${token}` } : {};

// Read the server-sent events of a response.
async function* events(response) {
	let buffer = "";
	for await (const chunk of response) {
		buffer += chunk.toString("utf8");
		while (buffer.includes("\n\n")) {
			const index = buffer.indexOf("\n\n");
			const frame = buffer.slice(0, index);
			buffer = buffer.slice(index + 2);
			const lines = frame.split("\n");
			const event = lines
				.find((line) => line.startsWith("event:"))
				?.slice(6)
				.trim();
			const data = lines.find((line) => line.startsWith("data:"))?.slice(5);
			yield { data: data === undefined ? undefined : JSON.parse(data), event };
		}
	}
}

// Wait for the process by the nonce.
const wait = () => new Promise((resolve, reject) => {
	const request = http.request({
		socketPath,
		path: `/processes/${nonce}/wait`,
		method: "POST",
		headers: { accept: "text/event-stream", ...authorization },
	});
	request.on("error", reject);
	request.on("response", async (response) => {
		try {
			if (response.statusCode !== 200) {
				response.resume();
				resolve({ status: response.statusCode });
				return;
			}
			let outcome;
			for await (const { data, event } of events(response)) {
				if (event === "error") throw new Error(JSON.stringify(data));
				if (event === "outcome") outcome = data;
			}
			resolve(outcome);
		} catch (error) {
			reject(error);
		}
	});
	request.end();
});

// Spawn the process through a connect with the nonce, and read its messages until the outcome.
const connect = () => new Promise((resolve, reject) => {
	const request = http.request({
		socketPath,
		path: "/processes/connect",
		method: "POST",
		headers: {
			accept: "text/event-stream",
			"content-type": "text/event-stream",
			"x-tg-arg-in-body": "true",
			...authorization,
		},
	});
	const send = (event, value) =>
		request.write(`event: ${event}\ndata: ${JSON.stringify(value)}\n\n`);
	request.on("error", reject);
	request.on("response", async (response) => {
		try {
			assert.equal(response.statusCode, 200);
			let prefix = Buffer.alloc(0);
			let header = false;
			let buffer = "";
			for await (let chunk of response) {
				if (!header) {
					prefix = Buffer.concat([prefix, chunk]);
					let length = 0;
					let shift = 0;
					let offset = 0;
					let complete = false;
					while (offset < prefix.length) {
						const byte = prefix[offset++];
						length += (byte & 127) * 2 ** shift;
						if (byte < 128) {
							complete = true;
							break;
						}
						shift += 7;
					}
					if (!complete || prefix.length < offset + length) continue;
					header = true;
					chunk = prefix.subarray(offset + length);
				}
				buffer += chunk.toString("utf8");
				while (buffer.includes("\n\n")) {
					const index = buffer.indexOf("\n\n");
					const frame = buffer.slice(0, index);
					buffer = buffer.slice(index + 2);
					const lines = frame.split("\n");
					const event = lines
						.find((line) => line.startsWith("event:"))
						?.slice(6)
						.trim();
					const value = JSON.parse(
						lines.find((line) => line.startsWith("data:")).slice(5),
					);
					if (event === "error") throw new Error(JSON.stringify(value));
					if (
						event === "notification" &&
						value.kind === "progress" &&
						value.value.kind === "output"
					)
						send("notification", { kind: "ready" });
					if (event === "notification" && value.kind === "outcome") {
						resolve(value.value);
						request.destroy();
						return;
					}
				}
			}
			reject(new Error("the connection closed before the outcome"));
		} catch (error) {
			reject(error);
		}
	});
	const arg = {
		mode: "run",
		nonce,
		process: {
			cached: false,
			command,
			sandbox: {},
			stderr: "null",
			stdin: "null",
			stdout: "null",
		},
	};
	const bytes = Buffer.from(JSON.stringify(arg));
	const prefix = [];
	let length = bytes.length;
	while (length >= 128) {
		prefix.push((length % 128) | 128);
		length = Math.floor(length / 128);
	}
	prefix.push(length);
	request.write(Buffer.concat([Buffer.from(prefix), bytes]));
});

try {
	const output = mode === "connect" ? await connect() : await wait();
	console.log(JSON.stringify(output));
} finally {
	clearTimeout(timer);
}
