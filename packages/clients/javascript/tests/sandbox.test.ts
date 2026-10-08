import { strict as assert } from "node:assert";
import { test } from "node:test";
import * as tg from "../src/index.ts";
import { Client } from "../src/client.ts";
import { Body, Response } from "../src/http.ts";

test("sandbox reads send authorization tokens and the requested source", async () => {
	let utf8 = tg.encoding.utf8;
	tg.encoding.utf8 = {
		encode: (value) => new TextEncoder().encode(value),
		decode: (value) => new TextDecoder().decode(value),
	};
	try {
		let client = new Client();
		let tokens: tg.Authorization.Tokens = { local: ["sandbox-read-token"] };
		let requests = 0;
		client.sendWithRetry = async (request) => {
			requests++;
			let query = new URLSearchParams(request.uri.query);
			assert.equal(query.get("tokens[local][0]"), "sandbox-read-token");
			assert.equal(query.get("source"), "index");
			return new Response(404, {}, Body.empty());
		};
		let output = await client.tryGetSandbox(
			"sbx_0006gdjpbwe1rhb2rpq4hrpc0z34",
			{
				source: "index",
				tokens,
			},
		);
		assert.equal(output, null);
		assert.equal(requests, 1);
	} finally {
		tg.encoding.utf8 = utf8;
	}
});

test("CPU shorthand normalizes to shared and mixed requests retain both counts", () => {
	assert.deepEqual(tg.Sandbox.Arg.toData({ cpu: 2 }), { cpu: { shared: 2 } });
	assert.deepEqual(
		tg.Sandbox.Arg.toData({ cpu: { dedicated: 2, shared: 4 } }),
		{ cpu: { dedicated: 2, shared: 4 } },
	);
	assert.deepEqual(tg.Sandbox.Arg.toData({ cpu: { dedicated: 2 } }), {
		cpu: { dedicated: 2 },
	});
	for (let cpu of [
		0,
		-1,
		0.5,
		NaN,
		Infinity,
		{ dedicated: -1 },
		{},
		{ shared: Number.MAX_SAFE_INTEGER, dedicated: 1 },
	]) {
		assert.throws(() => tg.Sandbox.Arg.toData({ cpu }));
	}
});

test("sandbox CPU builders resolve the numeric shorthand and mixed objects", async () => {
	let createSandbox = tg.client.createSandbox;
	let requests: Array<tg.Sandbox.Create.Arg> = [];
	tg.client.createSandbox = async (arg) => {
		requests.push(arg);
		return { data: { id: "sbx_test", status: "started" } };
	};
	try {
		await tg.Sandbox.create().cpu(Promise.resolve(2));
		await tg.Sandbox.create().cpu({ dedicated: 2, shared: 4 });
		assert.deepEqual(
			requests.map((arg) => arg.cpu),
			[{ shared: 2 }, { dedicated: 2, shared: 4 }],
		);
	} finally {
		tg.client.createSandbox = createSandbox;
	}
});
