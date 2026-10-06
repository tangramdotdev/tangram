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
