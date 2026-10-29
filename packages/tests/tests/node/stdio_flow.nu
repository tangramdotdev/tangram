use ../../util/test.nu *

# Node standard input and output flow through the process streams.

const javascript_path = path self '../../../javascript'
cd $javascript_path

# A 4 MiB write and a 4 MiB read complete without credit messages, and the read acknowledges every byte it receives.
let output = timeout 5 node --input-type=module -e '
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
	let id = "pcs_010000000000000000000000000000000000000000000000000000";
	let chunkSize = 32 * 1024;
	let count = 128;
	let sent = 0;
	let acked = null;
	let acks = null;
	tg.client.send = async (request) => {
	    let input = request.body.sse();
	    if (request.uri.path.endsWith("/stdio/read")) {
	        // A client read sends only acknowledgments.
	        acks = (async () => {
	            while (true) {
	                let event = await input.next();
	                if (event.done) return;
	                assert.equal(event.value.event, "ack");
	                acked = JSON.parse(event.value.data);
	            }
	        })();
	        let output = async function* () {
	            for (let index = 0; index < count; index++) {
	                let chunk = { bytes: Buffer.alloc(chunkSize, 120).toString("base64"), combined_position: index * chunkSize, stream: "stdout", stream_position: index * chunkSize };
	                yield { event: "notification", data: JSON.stringify({ kind: "chunk", value: chunk }) };
	            }
	            let end = { combined_position: count * chunkSize, stream_positions: { stdout: count * chunkSize } };
	            yield { event: "response", data: JSON.stringify({ kind: "end", value: end }) };
	        };
	        return new tg.Response(200, { "content-type": "text/event-stream" }, { sse: output });
	    }
	    let output = async function* () {
	        while (true) {
	            let event = await input.next();
	            assert.equal(event.done, false);
	            assert.equal(event.value.event, "data");
	            let data = JSON.parse(event.value.data);
	            if (data.kind === "end") {
	                assert.deepEqual(data.value, { combined_position: chunkSize * sent, stream_positions: { stdin: chunkSize * sent } });
	                yield { event: "output", data: JSON.stringify({ closed: true, position: chunkSize * sent }) };
	                return;
	            }
	            assert.equal(data.value.stream_position, sent * chunkSize);
	            assert.equal(Buffer.from(data.value.bytes, "base64").length, chunkSize);
	            sent++;
	            yield { event: "output", data: JSON.stringify({ closed: false, position: sent * chunkSize }) };
	        }
	    };
	    return new tg.Response(200, { "content-type": "text/event-stream" }, { sse: output });
	};
	let input = async function* () {
	    for (let index = 0; index < count; index++) {
	        yield { bytes: new Uint8Array(chunkSize), combinedPosition: index * chunkSize, stream: "stdin", streamPosition: index * chunkSize };
	    }
	};
	await tg.client.writeProcessStdio(id, { streams: ["stdin"] }, input());
	assert.equal(sent, count);
	let read = await tg.client.tryReadProcessStdio(id, { streams: ["stdout"] });
	let bytes = 0;
	for await (let chunk of read) bytes += chunk.bytes.length;
	assert.equal(bytes, count * chunkSize);
	await acks;
	assert.equal(acked, count * chunkSize);
	console.log("ok");
' | complete
success $output
