import { strict as assert } from "node:assert";
import { test } from "node:test";
import * as tg from "../src/index.ts";
import { Body } from "../src/http.ts";

test("response headers preserve the following body across chunk boundaries", async () => {
	const utf8 = tg.encoding.utf8;
	tg.encoding.utf8 = {
		decode: (value) => new TextDecoder().decode(value),
		encode: (value) => new TextEncoder().encode(value),
	};
	try {
		const expected = { value: "x".repeat(130) };
		const header = new TextEncoder().encode(JSON.stringify(expected));
		const prefix = new Uint8Array([
			(header.length & 127) | 128,
			header.length >> 7,
		]);
		const suffix = new TextEncoder().encode("event: ready\ndata: {}\n\n");
		const bytes = new Uint8Array([...prefix, ...header, ...suffix]);
		for (const fragmented of [false, true]) {
			const body = new Body({
				async *[Symbol.asyncIterator]() {
					if (fragmented) {
						for (const byte of bytes) yield new Uint8Array([byte]);
					} else {
						yield bytes;
					}
				},
			});
			const response = new tg.Response(200, {}, body);
			assert.deepEqual(await response.bodyHeader(), expected);
			assert.deepEqual(await response.collect(), suffix);
		}
		for (const bytes of [[], [128], [2, 123], [128, 128, 128, 1]]) {
			const response = new tg.Response(
				200,
				{},
				Body.bytes(new Uint8Array(bytes)),
			);
			await assert.rejects(response.bodyHeader());
		}
	} finally {
		tg.encoding.utf8 = utf8;
	}
});

test("JSON clients reject native body prefixes", async () => {
	const headers = { "content-type": "application/vnd.tangram.process-connect" };
	const response = new tg.Response(
		200,
		headers,
		Body.bytes(new Uint8Array([2, 123, 125])),
	);
	await assert.rejects(response.bodyHeader(), /does not support Tangram/);
	const request = new tg.Request({
		method: "POST",
		uri: "/processes/connect",
		headers: { ...headers, "x-tg-arg-in-body": "true" },
	});
	assert.throws(() => request.arg({}), /does not support Tangram/);
});
