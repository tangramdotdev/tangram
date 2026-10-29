use ../../lib/test.nu *

# A standard input write succeeds after the stream reconnects.

const javascript_path = path self '../../../javascript'
cd $javascript_path

# The JavaScript writer resends from the last reported position after the response stream breaks.
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
	let connections = 0;
	client.send = async (request) => {
		let connection = connections++;
		let input = request.body.sse();
		let read = async () => {
			let event = await input.next();
			assert.equal(event.done, false);
			assert.equal(event.value.event, "data");
			return JSON.parse(event.value.data);
		};
		let output = (closed, position) => ({
			event: "output", data: JSON.stringify({ closed, position }),
		});
		let end = { kind: "end", value: { combined_position: 9, stream_positions: { stdin: 9 } } };
		let stream = async function* () {
			if (connection === 0) {
				let first = await read();
				assert.equal(first.value.stream_position, 0);
				assert.equal(first.value.bytes, "YWJjZGVm");
				let second = await read();
				assert.equal(second.value.stream_position, 6);
				assert.equal(second.value.bytes, "Z2hp");
				// Report a partial write, then break the response stream.
				yield output(false, 3);
			} else if (connection === 1) {
				let resent = await read();
				assert.equal(resent.value.stream_position, 3);
				assert.equal(resent.value.bytes, "ZGVm");
				let second = await read();
				assert.equal(second.value.stream_position, 6);
				assert.equal(second.value.bytes, "Z2hp");
				assert.deepEqual(await read(), end);
				yield output(false, 9);
			} else {
				assert.equal(connection, 2);
				assert.deepEqual(await read(), end);
				yield output(true, 9);
			}
		};
		return new tg.Response(200, { "content-type": "text/event-stream" }, { sse: stream });
	};
	let input = async function* () {
		yield {
			bytes: new TextEncoder().encode("abcdef"),
			combinedPosition: 0,
			stream: "stdin",
			streamPosition: 0,
		};
		yield {
			bytes: new TextEncoder().encode("ghi"),
			combinedPosition: 6,
			stream: "stdin",
			streamPosition: 6,
		};
	};
	await client.writeProcessStdio("pcs_010000000000000000000000000000000000000000000000000000", { streams: ["stdin"] }, input());
	assert.equal(connections, 3);
' | complete
success $output
