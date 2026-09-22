use ../lib/test.nu *

const js_path = path self '../../../js'
cd $js_path

# The JavaScript writer retains unconfirmed chunks and final positions across lost write and EOF responses.
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
			while (true) {
				let event = await input.next();
				if (event.value.event === "ack") continue;
				assert.equal(event.value.event, "request");
				return JSON.parse(event.value.data);
			}
		};
		let response = (id, length, closed = false) => ({
			event: "response", data: JSON.stringify({ id, error: null, output: { closed, length } }),
		});
		let output = async function* () {
			if (connection === 0) {
				let first = await read();
				assert.equal(first.arg.kind, "chunk");
				assert.equal(first.arg.value.stream_position, 0);
				assert.equal(first.arg.value.bytes, "YWJjZGVm");
				yield { event: "ack", data: JSON.stringify({ id: first.id }) };
			} else if (connection === 1) {
				let replay = await read();
				assert.equal(replay.arg.value.stream_position, 0);
				assert.equal(replay.arg.value.bytes, "YWJjZGVm");
				yield response(replay.id, 6);
				assert.deepEqual((await read()).arg, { kind: "end", value: { combined_position: 6, stream_positions: { stdin: 6 } } });
			} else {
				assert.equal(connection, 2);
				const end = await read();
				assert.deepEqual(end.arg, { kind: "end", value: { combined_position: 6, stream_positions: { stdin: 6 } } });
				yield response(end.id, 0, true);
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
