import { strict as assert } from "node:assert";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import * as tg from "../src/index.ts";
import { isTransportError } from "../src/client.ts";
import { Http2 } from "../src/config.ts";
import { Body, Headers } from "../src/http.ts";

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

test("header lookup normalizes field names", () => {
	let headers = new Headers({
		Authorization: "explicit",
		"Content-Type": "application/json",
	});
	assert.equal(headers.get("authorization"), "explicit");
	assert.equal(headers.get("CONTENT-TYPE"), "application/json");
});

test("closing a response stream settles pending readers", async () => {
	const { EventEmitter } = await import("node:events");
	class Stream extends EventEmitter {
		pause() {
			return this;
		}
		resume() {
			return this;
		}
		close() {
			this.emit("close");
			return this;
		}
	}
	const stream = new Stream();
	const response = tg.Response.fromStream(
		stream as unknown as tg.Host.Http2.ClientHttp2Stream,
	);
	stream.close();
	await assert.rejects(response, /closed before/);
	const nextStream = new Stream();
	const nextResponse = tg.Response.fromStream(
		nextStream as unknown as tg.Host.Http2.ClientHttp2Stream,
	);
	nextStream.emit("response", { ":status": "200" });
	const opened = await nextResponse;
	const collected = opened.collect();
	opened.close();
	await assert.rejects(collected, /closed before/);
});

test("HTTP/2 configuration validation matches the Rust client", () => {
	Http2.validate(Http2.defaultValue());
	Http2.validate({ connectionWindowSize: 65535, streamWindowSize: 1 });
	assert.throws(
		() => Http2.validate({ connectionWindowSize: 65534, streamWindowSize: 1 }),
		/connection window must be between/,
	);
	assert.throws(
		() =>
			Http2.validate({ connectionWindowSize: 1 << 20, streamWindowSize: 0 }),
		/stream window must be between/,
	);
	assert.throws(
		() =>
			Http2.validate({
				connectionWindowSize: 1 << 20,
				maxConcurrentStreams: 0,
				streamWindowSize: 1,
			}),
		/max concurrent streams/,
	);
	assert.throws(
		() =>
			Http2.validate({
				connectionWindowSize: 1 << 20,
				streamWindowSize: 1 << 20,
			}),
		/at least twice the stream window/,
	);
});

test("transport errors are distinguished from configuration errors", () => {
	assert.equal(isTransportError(new Error("missing TANGRAM_URL")), false);
	assert.equal(
		isTransportError(Object.assign(new Error(), { code: "ECONNREFUSED" })),
		true,
	);
	assert.equal(
		isTransportError(Object.assign(new Error(), { retryable: false })),
		true,
	);
	assert.equal(
		isTransportError(
			tg.Error.fromData({ source: { node: { kind: "unavailable" } } }),
		),
		true,
	);
	assert.equal(
		isTransportError(tg.Error.fromData({ kind: "internal" })),
		false,
	);
});
