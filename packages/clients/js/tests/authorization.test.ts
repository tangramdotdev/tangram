import { strict as assert } from "node:assert";
import { test } from "node:test";
import * as tg from "../src/index.ts";

test("sandbox permissions match the server", () => {
	tg.setEncoding({
		...tg.encoding,
		base64: {
			decode: (value) => Buffer.from(value, "base64"),
			encode: (value) => Buffer.from(value).toString("base64"),
		},
		utf8: {
			decode: (value) => Buffer.from(value).toString("utf8"),
			encode: (value) => Buffer.from(value, "utf8"),
		},
	});
	const token = (permission: string) =>
		[
			"0",
			Buffer.from(
				JSON.stringify({
					resource: "sandbox",
					permissions: [permission],
					expires_at: 1,
				}),
			).toString("base64"),
			Buffer.from(
				JSON.stringify({ algorithm: "ed25519", key: "test" }),
			).toString("base64"),
			"signature",
		].join(".");
	const parent = token("sandbox_parent");
	const node = token("sandbox_node");
	assert(tg.Authorization.Token.covers(parent, node));
	assert(!tg.Authorization.Token.covers(node, parent));
	const tokens = { local: [parent, node] };
	tg.Authorization.Tokens.normalize(tokens);
	assert.deepEqual(tokens, { local: [parent] });
	for (const permission of ["sandbox_read", "sandbox_write"]) {
		assert(!tg.Authorization.Token.authorizes(parent, "sandbox", permission));
	}
});
