import { strict as assert } from "node:assert";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import * as tg from "../src/index.ts";
import { Body } from "../src/http.ts";

test("SSE decoding matches the shared fixtures across chunk boundaries", async () => {
	let utf8 = tg.encoding.utf8;
	tg.encoding.utf8 = {
		encode: (value) => new TextEncoder().encode(value),
		decode: (value) => new TextDecoder().decode(value),
	};
	try {
		let cases = JSON.parse(
			readFileSync(
				new URL("../../../http/fixtures/sse.json", import.meta.url),
				"utf8",
			),
		) as Array<{ input: string; events: Body.SseEvent[] }>;
		for (let fixture of cases) {
			for (let size of [1, 2, 128]) {
				let bytes = new TextEncoder().encode(fixture.input);
				let body = new Body({
					async *[Symbol.asyncIterator]() {
						for (let offset = 0; offset < bytes.length; offset += size)
							yield bytes.subarray(offset, offset + size);
					},
				});
				let events = [];
				for await (let event of body.sse()) events.push(event);
				assert.deepEqual(events, fixture.events);
			}
		}
	} finally {
		tg.encoding.utf8 = utf8;
	}
});
