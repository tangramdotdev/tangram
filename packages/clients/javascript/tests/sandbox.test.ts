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

test("resource shorthand and omitted maxima normalize consistently", () => {
	for (let key of ["cpu", "memory", "storage"] as const) {
		for (let [value, expected] of [
			[2, { max: 2, min: 0 }],
			[
				{ max: 4, min: 2 },
				{ max: 4, min: 2 },
			],
			[{ min: 2 }, { min: 2 }],
		] as const)
			assert.deepEqual(tg.Sandbox.Arg.toData({ [key]: value })[key], expected);
	}
	assert.deepEqual(tg.Sandbox.Arg.toData({ cpu: { min: 0.25, max: 1 } }).cpu, {
		min: 0.25,
		max: 1,
	});
	for (let cpu of [0, -1, 0.0001, NaN, Infinity, { min: 2, max: 1 }]) {
		assert.throws(() => tg.Sandbox.Arg.toData({ cpu }));
	}
	assert.deepEqual(tg.Sandbox.Arg.toData({ cpu: {} }).cpu, { min: 0 });
});

test("resource builders preserve minima and fractional CPU values", async () => {
	let createSandbox = tg.client.createSandbox;
	let requests: Array<tg.Sandbox.Create.Arg> = [];
	tg.client.createSandbox = async (arg) => {
		requests.push(arg);
		return { data: { id: "sbx_test", status: "started" } };
	};
	try {
		await tg.Sandbox.create().cpu(Promise.resolve(2));
		await tg.Sandbox.create()
			.cpu({ min: 0.25, max: 1 })
			.memory({ min: 1024 })
			.storage(4096);
		assert.deepEqual(requests[0]?.cpu, { max: 2, min: 0 });
		let { cpu, memory, storage } = requests[1]!;
		assert.deepEqual(
			{ cpu, memory, storage },
			{
				cpu: { max: 1, min: 0.25 },
				memory: { min: 1024 },
				storage: { max: 4096, min: 0 },
			},
		);
	} finally {
		tg.client.createSandbox = createSandbox;
	}
});
