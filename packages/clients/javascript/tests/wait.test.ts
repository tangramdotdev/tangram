import { strict as assert } from "node:assert";
import { test } from "node:test";
import * as tg from "../src/index.ts";
import { Client } from "../src/client.ts";
import { Body } from "../src/http.ts";

const id = "pcs_010000000000000000000000000000000000000000000000000000";

const streamRetry = { backoff: 0, jitter: 0, maxDelay: 0, maxRetries: 2 };

// Run a function with JavaScript implementations of the encodings that the runtime host provides.
async function withEncodings(function_: () => Promise<void>): Promise<void> {
	let utf8 = tg.encoding.utf8;
	tg.encoding.utf8 = {
		decode: (value) => new TextDecoder().decode(value),
		encode: (value) => new TextEncoder().encode(value),
	};
	try {
		await function_();
	} finally {
		tg.encoding.utf8 = utf8;
	}
}

// Create a response whose body sends the given text, and then fails with the given error if there is one.
function response(text: string, error?: unknown): tg.Response {
	let body = new Body({
		async *[Symbol.asyncIterator]() {
			yield new TextEncoder().encode(text);
			if (error !== undefined) {
				throw error;
			}
		},
	});
	return new tg.Response(200, { "content-type": "text/event-stream" }, body);
}

test("a wait whose stream fails partway is sent again and returns the outcome", async () => {
	await withEncodings(async () => {
		let client = new Client({ streamRetry });
		let reset = Object.assign(new Error("reset"), { code: "ECONNRESET" });
		let responses = [
			response('event: outcome\ndata: {"exit":0}\n', reset),
			response('event: outcome\ndata: {"exit":0}\n\nevent: end\ndata: \n\n'),
		];
		let sends = 0;
		client.sendWithRetry = async () => responses[sends++]!;
		let outcome = await client.waitProcess(id, {});
		assert.equal(outcome.exit, 0);
		assert.equal(sends, 2);
	});
});

test("a wait whose server sends an error event fails without being sent again", async () => {
	await withEncodings(async () => {
		let client = new Client({ streamRetry });
		let sends = 0;
		client.sendWithRetry = async () => {
			sends++;
			return response('event: error\ndata: {"message":"failed"}\n\n');
		};
		await assert.rejects(client.waitProcess(id, {}));
		assert.equal(sends, 1);
	});
});
