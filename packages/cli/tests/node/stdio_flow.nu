use ../../test.nu *

const js_path = path self '../../../js'
cd $js_path

# A 4 MiB write pipelines two windows, and receipt acknowledgments do not release either window.
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
	let chunkSize = 32 * 1024;
	let maxChunks = 64;
	let delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
	let sent = 0;
	let start = performance.now();
	tg.client.send = async (request) => {
	    let input = request.body.sse();
	    let pending = null;
	    let read = async () => {
	        while (true) {
	            let event = await (pending ?? input.next());
	            pending = null;
	            assert.equal(event.done, false);
	            if (event.value.event === "ack") continue;
	            assert.equal(event.value.event, "request");
	            return JSON.parse(event.value.data);
	        }
	    };
	    let response = (id, length, closed = false) => ({
	        event: "response", data: JSON.stringify({ id, error: null, output: { closed, length } }),
	    });
	    let output = async function* () {
	        for (let batch = 0; batch < 2; batch++) {
	            let requests = [];
	            for (let index = 0; index < maxChunks; index++) {
	                let request = await read();
	                assert.equal(request.arg.kind, "chunk");
	                assert.equal(request.arg.value.stream_position, sent * chunkSize);
	                assert.equal(Buffer.from(request.arg.value.bytes, "base64").length, chunkSize);
	                sent++;
	                requests.push(request);
	            }
	            // Receipt acknowledgments must not release the write window.
	            for (let request of requests) {
	                yield { event: "ack", data: JSON.stringify({ id: request.id }) };
	            }
	            let arrived = false;
	            pending = input.next().then((event) => { arrived = true; return event; });
	            await delay(100);
	            assert.equal(arrived, false);
	            for (let request of requests) yield response(request.id, chunkSize);
	        }
	        let end = await read();
	        assert.deepEqual(end.arg, {
	            kind: "end",
	            value: { combined_position: chunkSize * sent, stream_positions: { stdin: chunkSize * sent } },
	        });
	        await delay(100);
	        yield response(end.id, 0, true);
	    };
	    return new tg.Response(200, { "content-type": "text/event-stream" }, { sse: output });
	};
	let input = async function* () {
	    for (let index = 0; index < maxChunks * 2; index++) {
	        yield { bytes: new Uint8Array(chunkSize), combinedPosition: index * chunkSize, stream: "stdin", streamPosition: index * chunkSize };
	    }
	};
	await tg.client.writeProcessStdio("pcs_010000000000000000000000000000000000000000000000000000", { streams: ["stdin"] }, input());
	assert.equal(sent, maxChunks * 2);
	console.log(JSON.stringify({ chunks: sent, bytes: sent * chunkSize, latency_ms: 100, elapsed_ms: Math.round(performance.now() - start) }));
' | complete
success $output
