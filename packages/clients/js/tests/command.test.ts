import { strict as assert } from "node:assert";
import { test } from "node:test";
import { encodeJsArgs } from "../src/command.ts";
import * as tg from "../src/index.ts";

test("flag-like user arguments stay distinct through builder handoffs", async () => {
	let existing = await tg.Command.new({ executable: "tg", args: ["js"] });
	for (let flag of ["-a", "-A"]) {
		let value =
			flag === "-a"
				? tg.Command.Value.string("raw")
				: tg.Command.Value.value(42);
		let raw = [tg.Command.Value.string(flag), value];
		let expected = [
			["string", "-a"],
			["string", flag],
			["string", flag],
			[value.kind, value.value],
		];
		let encoded = encodeJsArgs(raw);
		assert.deepEqual(
			Array.from(encoded, (arg) => [arg.kind, arg.value]),
			expected,
		);
		assert.equal(encodeJsArgs(await tg.resolve(encoded)), encoded);
		let command = await tg.command(existing).arg(...raw);
		assert.deepEqual(
			(await command.args).slice(1).map((arg) => [arg.kind, arg.value]),
			expected,
		);
	}
});
