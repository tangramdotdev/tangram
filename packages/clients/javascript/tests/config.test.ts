import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Client } from "../src/client.ts";
import { Config, Http, ProcessConnect, Retry } from "../src/config.ts";

test("the default configuration is valid", () => {
	let config = Config.defaultValue();
	assert.deepEqual(config.http, {
		coalescingTargetSize: 16 * 1024,
		connectTimeout: 1,
		requestTimeout: 60,
	});
	assert.deepEqual(config.process, {
		connect: { maxMessages: 128, maxRequests: 64 },
	});
	assert.deepEqual(config.streamRetry, {
		backoff: 0.01,
		jitter: 0.01,
		maxDelay: 1,
		maxRetries: Number.MAX_SAFE_INTEGER,
	});
	let client = new Client(config);
	assert.equal(client.http.requestTimeout, 60);
	assert.equal(client.process.connect.maxMessages, 128);
	assert.equal(client.streamRetry.maxRetries, Number.MAX_SAFE_INTEGER);
});

test("HTTP timeouts must be positive and finite", () => {
	for (let value of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
		assert.throws(
			() => Http.validate({ ...Http.defaultValue(), connectTimeout: value }),
			/invalid HTTP connect timeout/,
		);
		assert.throws(
			() => Http.validate({ ...Http.defaultValue(), requestTimeout: value }),
			/invalid HTTP request timeout/,
		);
	}
	Http.validate({ ...Http.defaultValue(), connectTimeout: 0.5 });
});

test("process connect limits must be positive integers", () => {
	for (let value of [0, -1, 1.5, Number.NaN]) {
		assert.throws(
			() =>
				ProcessConnect.validate({
					...ProcessConnect.defaultValue(),
					maxMessages: value,
				}),
			/invalid process connect max messages/,
		);
		assert.throws(
			() =>
				ProcessConnect.validate({
					...ProcessConnect.defaultValue(),
					maxRequests: value,
				}),
			/invalid process connect max requests/,
		);
	}
	assert.throws(
		() =>
			new Client({
				process: { connect: { maxMessages: 0, maxRequests: 1 } },
			}),
		/invalid process connect max messages/,
	);
});

test("the stream retry options are validated", () => {
	assert.throws(
		() =>
			new Client({
				streamRetry: { ...Retry.streamDefaultValue(), maxRetries: -1 },
			}),
		/invalid retry count/,
	);
	assert.throws(
		() =>
			new Client({
				streamRetry: { ...Retry.streamDefaultValue(), backoff: -1 },
			}),
		/invalid retry duration/,
	);
});
