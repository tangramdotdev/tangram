import { strict as assert } from "node:assert";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import * as tg from "../src/index.ts";

const { Authorization } = tg;

let fixtures = (): Array<{
	expires_at: string;
	permissions: Array<string>;
	resource: string;
	secret_id: number;
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
		let token = Authorization.Token.fromData(fixture.token);
		assert.deepEqual(token.body, {
			expiresAt: BigInt(fixture.expires_at),
			permissions: fixture.permissions,
			resource: fixture.resource,
		});
		assert.equal(token.secretId, BigInt(fixture.secret_id));
		assert.ok(token.signature.length > 0);
		assert.equal(token.toData(), fixture.token);
		for (let permission of fixture.permissions) {
			assert.ok(token.authorizes(fixture.resource, permission));
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
		let token = Authorization.Token.fromData(malformed);
		assert.equal(token.body, null);
		assert.equal(token.toData(), malformed);
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
	assert.notEqual(Authorization.Token.fromData(token(0)).body, null);
	let bytes = retained(() => {
		for (let index = 0; index < 100_000; index++) {
			Authorization.Token.fromData(token(index));
		}
	});
	assert.ok(bytes < 32 * 2 ** 20, `retained ${bytes} bytes`);
});

test("parsed tokens are released with their entries", () => {
	let bytes = retained(() => {
		for (let index = 100_000; index < 200_000; index++) {
			Authorization.Tokens.fromData({ local: [token(index)] });
		}
	});
	assert.ok(bytes < 32 * 2 ** 20, `retained ${bytes} bytes`);
});

test("structured entries round trip token data and isolate cloned entries", () => {
	let data = {
		local: fixtures().map((fixture) => fixture.token),
		remote: ["opaque-token"],
	};
	let tokens = Authorization.Tokens.fromData(data);
	assert.deepEqual(tokens.toData(), data);
	let entry = tokens.get("local")!;
	let cloned = tokens.clone();
	assert.equal(cloned.get("local")!.authorization[0], entry.authorization[0]);
	cloned.get("local")!.authorization.length = 0;
	assert.deepEqual(tokens.toData(), data);
});

test("object handles expose structured tokens and collect current token data", () => {
	let fixture = fixtures()[0]!;
	let file = tg.File.withId(fixture.resource as tg.File.Id);
	let data = { local: [fixture.token] };
	file.state.tokens = data;
	assert.equal(
		file.state.tokens.get("local")!.authorization[0]!.body!.resource,
		fixture.resource,
	);
	assert.deepEqual(file.state.collectTokens().toData(), data);
	file.state.tokens = { remote: ["opaque-token"] };
	assert.deepEqual(file.state.collectTokens().toData(), {
		remote: ["opaque-token"],
	});
});
