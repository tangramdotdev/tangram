import { strict as assert } from "node:assert";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { Authorization } from "../src/authorization.ts";

let fixtures = (): Array<{
	permissions: Array<string>;
	resource: string;
	token: string;
}> =>
	JSON.parse(
		readFileSync(
			new URL("../../rust/fixtures/tokens.json", import.meta.url),
			"utf8",
		),
	);

test("token decoding matches the shared fixtures", () => {
	for (let fixture of fixtures()) {
		assert.equal(Authorization.Token.resource(fixture.token), fixture.resource);
		for (let permission of fixture.permissions) {
			assert.ok(
				Authorization.Token.authorizes(
					fixture.token,
					fixture.resource,
					permission,
				),
			);
		}
	}
});

test("malformed tokens are opaque", () => {
	let token = fixtures()[0]!.token;
	for (let malformed of [
		"",
		"1",
		token.slice(0, 40),
		`1${token.slice(1)}`,
		token.toUpperCase(),
	]) {
		assert.equal(Authorization.Token.resource(malformed), null);
	}
});
