use ../../test.nu *

const js_path = path self '../../../js'
cd $js_path

# The JavaScript writer advances by the completed byte count and retains its position across lost write and end responses.
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
			assert.equal(event.value.event, "request");
			return JSON.parse(event.value.data);
		};
		let write = (length) => ({
			event: "response",
			data: JSON.stringify({ kind: "write", value: { closed: false, length } }),
		});
		let output = async function* () {
			if (connection === 0) {
				let first = await read();
				assert.equal(first.kind, "chunk");
				assert.equal(first.value.stream_position, 0);
				assert.equal(first.value.bytes, "YWJjZGVm");
				yield write(2);
				let remaining = await read();
				assert.equal(remaining.value.stream_position, 2);
				assert.equal(remaining.value.bytes, "Y2RlZg==");
			} else if (connection === 1) {
				let replay = await read();
				assert.equal(replay.value.stream_position, 2);
				assert.equal(replay.value.bytes, "Y2RlZg==");
				yield write(4);
				assert.deepEqual(await read(), { kind: "end", value: { position: 6 } });
			} else {
				assert.equal(connection, 2);
				assert.deepEqual(await read(), { kind: "end", value: { position: 6 } });
				yield { event: "response", data: JSON.stringify({ kind: "end" }) };
			}
		};
		return new tg.Response(200, { "content-type": "text/event-stream" }, { sse: output });
	};
	let input = async function* () {
		yield {
			bytes: new TextEncoder().encode("abcdef"),
			combinedPosition: 0,
			stream: "stdin",
			streamPosition: 0,
		};
	};
	await client.writeProcessStdio("pcs_010000000000000000000000000000000000000000000000000000", { streams: ["stdin"] }, input());
	assert.equal(connections, 3);
' | complete
success $output
