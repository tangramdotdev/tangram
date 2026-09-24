use ../lib/test.nu *

const js_path = path self '../../../js'
cd $js_path

# Token-bearing requests frame large arguments without consuming a streaming body.
let output = timeout 10 node --input-type=module -e '
	import assert from "node:assert/strict";
	import * as tg from "@tangramdotdev/client";

	tg.setEncoding({
		utf8: {
			decode: value => new TextDecoder().decode(value),
			encode: value => new TextEncoder().encode(value),
		},
	});
	const client = tg.client;
	const tokens = { local: ["a".repeat(70000)] };
	const id = "pcs_010000000000000000000000000000000000000000000000000000";
	const blob = "blb_01zby8hmr9wc7c8t2g8c7qt29cyt8hkeg2y6y1yahh585dx6hebf2g";
	const stopped = tg.error.sync("request inspected");
	const requests = [];
	const inspect = async request => {
		assert.equal(request.uri.query, undefined);
		assert.equal(request.headers.get("x-tg-arg-in-body"), "true");
		assert.equal(request.headers.get("cache-control"), "no-store");
		const streaming = request.uri.path.includes("/stdio/");
		assert.equal(request.body.replayable, !streaming);
		const iterator = request.body[Symbol.asyncIterator]();
		const { value: frame } = await iterator.next();
		let length = 0, offset = 0, factor = 1;
		while (true) {
			const byte = frame[offset++];
			length += (byte & 127) * factor;
			if (byte < 128) break;
			factor *= 128;
		}
		assert.equal(frame.length, offset + length);
		const arg = JSON.parse(new TextDecoder().decode(frame.subarray(offset)));
		assert.deepEqual(arg.tokens, tokens);
		if (streaming) assert.equal(arg.streams, "stdout");
		if (request.uri.path === `/processes/${id}`) assert.equal(arg.metadata, true);
		if (!streaming) {
			assert.equal((await iterator.next()).done, true);
			assert.deepEqual(await request.body.collect(), frame);
		}
		await iterator.return();
		requests.push(request.uri.path);
		throw stopped;
	};
	client.send = inspect;
	client.sendWithRetry = inspect;
	const operations = [
		() => client.tryRead({ blob, tokens }),
		() => client.tryGetProcess(id, { metadata: true, tokens }),
		async () => (await client.tryWaitProcessPromise(id, { tokens }))(),
		() => client.tryReadProcessStdio(id, { streams: ["stdout"], tokens }),
		() => client.tryWriteProcessStdio(id, { streams: ["stdout"], tokens }, (async function* () {})()),
	];
	for (const operation of operations) {
		await assert.rejects(operation, error => error === stopped);
	}
	assert.deepEqual(requests, [
		"/read",
		`/processes/${id}`,
		`/processes/${id}/wait`,
		`/processes/${id}/stdio/read`,
		`/processes/${id}/stdio/write`,
	]);
' | complete
success $output
