import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Client } from "../src/client.ts";
import { Config, Http } from "../src/config.ts";

test("the default configuration is valid", () => {
	let config = Config.defaultValue();
	assert.deepEqual(config.http, {
		coalescingTargetSize: 16 * 1024,
		connectTimeout: 1,
		requestTimeout: 60,
	});
	let client = new Client(config);
	assert.equal(client.http.requestTimeout, 60);
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
