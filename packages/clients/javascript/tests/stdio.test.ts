import { strict as assert } from "node:assert";
import { test } from "node:test";
import * as tg from "../src/index.ts";
import { Client } from "../src/client.ts";
import { Body } from "../src/http.ts";

const id = "pcs_010000000000000000000000000000000000000000000000000000";

const streamRetry = { backoff: 0, jitter: 0, maxDelay: 0, maxRetries: 2 };

// Run a function with JavaScript implementations of the encodings that the runtime host provides.
async function withEncodings(function_: () => Promise<void>): Promise<void> {
	let base64 = tg.encoding.base64;
	let utf8 = tg.encoding.utf8;
	tg.encoding.base64 = {
		decode: (value) => new Uint8Array(Buffer.from(value, "base64")),
		encode: (value) => Buffer.from(value).toString("base64"),
	};
	tg.encoding.utf8 = {
		decode: (value) => new TextDecoder().decode(value),
		encode: (value) => new TextEncoder().encode(value),
	};
	try {
		await function_();
	} finally {
		tg.encoding.base64 = base64;
		tg.encoding.utf8 = utf8;
	}
}

// Create a response whose event stream sends the given chunk notifications and then ends without a response.
function response(positions: Array<number>): tg.Response {
	let events = positions.map((position) => ({
		data: JSON.stringify({
			kind: "chunk",
			value: {
				bytes: "YQ==",
				combined_position: position,
				stream: "stdout",
				stream_position: position,
			},
		}),
		event: "notification",
	}));
	return new tg.Response(
		200,
		{ "content-type": "text/event-stream" },
		Body.sse(
			(async function* () {
				yield* events;
			})(),
		),
	);
}

test("a stdio read fails once consecutive reconnects exhaust the stream retry options", async () => {
	await withEncodings(async () => {
		let client = new Client({ streamRetry });
		let sends = 0;
		client.send = async () => {
			sends++;
			return response([]);
		};
		let output = await client.tryReadProcessStdio({
			process: id,
			streams: ["stdout"],
		});
		assert.notEqual(output, null);
		await assert.rejects(
			output!.next(),
			/the process stdio stream ended before completion/,
		);
		assert.equal(sends, 1 + streamRetry.maxRetries);
	});
});

test("a stdio read resets its reconnect attempts when a stream delivers a chunk", async () => {
	await withEncodings(async () => {
		let client = new Client({ streamRetry });
		let responses = [[0], [], [1], [], []];
		let sends = 0;
		client.send = async () => response(responses[sends++] ?? []);
		let output = await client.tryReadProcessStdio({
			process: id,
			streams: ["stdout"],
		});
		assert.notEqual(output, null);
		let chunks = [];
		await assert.rejects(async () => {
			for await (let chunk of output!) {
				chunks.push(chunk.streamPosition);
			}
		}, /the process stdio stream ended before completion/);
		assert.deepEqual(chunks, [0, 1]);
		assert.equal(sends, responses.length);
	});
});

test("a stdio write fails once consecutive reconnects exhaust the stream retry options", async () => {
	await withEncodings(async () => {
		let client = new Client({ streamRetry });
		let sends = 0;
		client.send = async () => {
			sends++;
			return new tg.Response(
				200,
				{ "content-type": "text/event-stream" },
				Body.empty(),
			);
		};
		let input =
			(async function* (): AsyncGenerator<tg.Process.Stdio.Chunk> {})();
		await assert.rejects(
			client.tryWriteProcessStdio({ process: id, streams: ["stdin"] }, input),
			/the process stdio stream ended before completion/,
		);
		assert.equal(sends, 1 + streamRetry.maxRetries);
	});
});
