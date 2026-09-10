use ../../test.nu *

const js_path = path self '../../../js'
cd $js_path

# A reverse reader resumes the clipped window after losing the connection.
let output = timeout 10 node --input-type=module -e '
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
	tg.setProcess({
		args: [], cwd: process.cwd(),
		env: Object.fromEntries(Object.entries(process.env).filter(([, value]) => value !== undefined)),
		executable: process.execPath,
	});
	let client = tg.client;
	for (let test of [
		...[100, "100", "start.100", "end.96"].map(position => ({ position, initial: {length: -3, position: 4}, end: 4, length: -2 })),
		{ position: "end.96", initial: {length: -99, position: 100}, end: 100, length: -98 },
		{ position: "end.96", initial: {length: -99, position: 100}, clipped: {length: -49, position: 50}, end: 50, length: -48 },
	]) {
		let requests = [];
		client.send = async (request) => {
			let connection = requests.length;
			let query = new URLSearchParams(request.uri.query);
			requests.push({position: query.get("position"), length: query.get("length")});
			let notification = (kind, value) => ({event: "notification", data: JSON.stringify({kind, value})});
			let output = async function* () {
				if (connection === 0) {
					yield notification("position", test.initial);
					if (test.clipped) yield notification("position", test.clipped);
					yield notification("chunk", {bytes: "eA==", combined_position: test.end - 1, stream: "stdout", stream_position: test.end - 1});
				} else {
					yield {event: "request", data: JSON.stringify({kind: "end"})};
				}
			};
			return new tg.Response(200, { "content-type": "text/event-stream" }, { sse: output });
		};
		let chunks = [];
		let stream = await client.tryReadProcessStdio("pcs_010000000000000000000000000000000000000000000000000000", { length: -99, position: test.position, streams: ["stdout"] });
		for await (let chunk of stream) chunks.push(Buffer.from(chunk.bytes).toString());
		assert.deepEqual(chunks, ["x"]);
		assert.deepEqual(requests, [
			{position: String(test.position), length: "-99"},
			{position: String(test.end - 1), length: String(test.length)},
		]);
	}
' | complete
success $output
