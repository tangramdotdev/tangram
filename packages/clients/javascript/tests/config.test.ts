import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Client } from "../src/client.ts";
import { Config, Http, ProcessAttach, Retry } from "../src/config.ts";

test("the default configuration is valid", () => {
	let config = Config.defaultValue();
	assert.deepEqual(config.http, {
		coalescingTargetSize: 16 * 1024,
		connectTimeout: 1,
		requestTimeout: 60,
	});
	assert.deepEqual(config.process, {
		attach: { maxMessages: 128, maxRequests: 64 },
	});
	assert.deepEqual(config.streamRetry, {
		backoff: 0.01,
		jitter: 0.01,
		maxDelay: 1,
		maxRetries: 16,
	});
	let client = new Client(config);
	assert.equal(client.http.requestTimeout, 60);
	assert.equal(client.process.attach.maxMessages, 128);
	assert.equal(client.streamRetry.maxRetries, 16);
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

test("process attach limits must be positive integers", () => {
	for (let value of [0, -1, 1.5, Number.NaN]) {
		assert.throws(
			() =>
				ProcessAttach.validate({
					...ProcessAttach.defaultValue(),
					maxMessages: value,
				}),
			/invalid process attach max messages/,
		);
		assert.throws(
			() =>
				ProcessAttach.validate({
					...ProcessAttach.defaultValue(),
					maxRequests: value,
				}),
			/invalid process attach max requests/,
		);
	}
	assert.throws(
		() =>
			new Client({
				process: { attach: { maxMessages: 0, maxRequests: 1 } },
			}),
		/invalid process attach max messages/,
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

test("pool lifetime aliases normalize to the canonical name", () => {
	for (let name of ["timeToLive", "ttl"]) {
		let client = new Client({
			pool: { max: 1, min: 0, shared: 1, [name]: 1.5 },
		});
		assert.deepEqual(client.pool, {
			max: 1,
			min: 0,
			shared: 1,
			timeToLive: 1.5,
		});
	}
	assert.throws(
		() =>
			new Client({
				pool: { max: 1, min: 0, shared: 1, timeToLive: 1, ttl: 2 },
			}),
		/may not both be set/,
	);
});
