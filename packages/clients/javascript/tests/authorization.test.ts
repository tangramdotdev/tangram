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

declare const Bun: { gc: (force: boolean) => void };

// Encode a distinct, well-formed object token for an index.
let token = (index: number): string => {
	let id = Array.from({ length: 16 }, (_, position) =>
		position < 4 ? (index >> (position * 8)) & 0xff : 0,
	);
	let signature = Array.from({ length: 64 }, () => 0);
	let bytes = [0, 1, 0b10, 0, 0, 1, 0, ...id, 0, ...signature];
	let symbols = "0123456789abcdefghjkmnpqrstvwxyz";
	let output = "";
	let buffer = 0;
	let bits = 0;
	for (let byte of bytes) {
		buffer = ((buffer << 8) | byte) & 0xfff;
		bits += 8;
		while (bits >= 5) {
			bits -= 5;
			output += symbols[(buffer >> bits) & 31];
		}
	}
	if (bits > 0) {
		output += symbols[(buffer << (5 - bits)) & 31];
	}
	return output;
};

// Measure the heap that remains after running a callback and collecting garbage.
let retained = (callback: () => void): number => {
	Bun.gc(true);
	let before = process.memoryUsage().heapUsed;
	callback();
	Bun.gc(true);
	return process.memoryUsage().heapUsed - before;
};

test("decoding many distinct tokens retains a bounded amount of memory", () => {
	assert.notEqual(Authorization.Token.resource(token(0)), null);
	let bytes = retained(() => {
		for (let index = 0; index < 100_000; index++) {
			Authorization.Token.resource(token(index));
		}
	});
	assert.ok(bytes < 32 * 2 ** 20, `retained ${bytes} bytes`);
});

test("tokens decoded with their entries are released with the entries", () => {
	let bytes = retained(() => {
		for (let index = 100_000; index < 200_000; index++) {
			let entry = [token(index)];
			Authorization.Token.resource(entry[0]!, entry);
		}
	});
	assert.ok(bytes < 32 * 2 ** 20, `retained ${bytes} bytes`);
});
