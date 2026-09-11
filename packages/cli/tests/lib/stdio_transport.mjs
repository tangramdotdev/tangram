import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import http from "node:http";
import http2 from "node:http2";

const [tangram, socketPath, id] = process.argv.slice(2);
const attempts = { read: 0, write: 0 };
const sessions = new Set();
const proxy = http2.createServer();
proxy.on("session", (session) => {
	sessions.add(session);
	session.on("close", () => sessions.delete(session));
	session.on("error", () => {});
});
proxy.on("stream", (stream, headers) => {
	stream.on("error", () => {});
	const path = headers[":path"];
	const kind = path.includes("/stdio/read")
		? "read"
		: path.includes("/stdio/write")
			? "write"
			: null;
	const drop = kind !== null && attempts[kind]++ === 0;
	const upstream = http.request(
		{
			headers: Object.fromEntries(
				Object.entries(headers).filter(([name]) => !name.startsWith(":")),
			),
			method: headers[":method"],
			path,
			socketPath,
		},
		(response) => {
			const headers = Object.fromEntries(
				Object.entries(response.headers).filter(
					([name]) =>
						!["connection", "keep-alive", "transfer-encoding"].includes(name),
				),
			);
			stream.respond({ ...headers, ":status": response.statusCode });
			if (drop) {
				response.once("data", (bytes) => {
					// Lose the connection halfway through a response, after the backend has handled the operation.
					stream.write(bytes.subarray(0, 1));
					response.pause();
					setTimeout(() => stream.session?.destroy(), 50);
				});
			} else {
				response.pipe(stream);
			}
			response.on("error", () => stream.destroy());
		},
	);
	upstream.on("error", () => stream.destroy());
	stream.on("close", () => upstream.destroy());
	stream.pipe(upstream);
	upstream.flushHeaders();
});
await new Promise((resolve) => proxy.listen(0, "127.0.0.1", resolve));
const url = `http://127.0.0.1:${proxy.address().port}`;
async function run(args, input = "") {
	const child = spawn(tangram, ["--mode", "client", "--url", url, ...args]);
	child.stdin.end(input);
	let stdout = "";
	let stderr = "";
	child.stdout.on("data", (bytes) => (stdout += bytes));
	child.stderr.on("data", (bytes) => (stderr += bytes));
	const timer = setTimeout(() => child.kill("SIGKILL"), 10000);
	const status = await new Promise((resolve) => child.on("close", resolve));
	clearTimeout(timer);
	assert.equal(status, 0, stderr);
	assert.equal(stderr, "");
	return stdout;
}
try {
	await run(["process", "stdio", "write", "--stream", "stdin", id], "hello\n");
	assert.equal(
		await run(["log", "--no-timeout", "--stream", "stdout", id]),
		"hello\n",
	);
	assert.deepEqual(attempts, { read: 2, write: 2 });
} finally {
	for (const session of sessions) session.destroy();
	proxy.close();
}
