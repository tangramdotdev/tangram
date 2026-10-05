import { strict as assert } from "node:assert";
import { test } from "node:test";
import { encodeModuleArgs } from "../src/command.ts";
import * as tg from "../src/index.ts";

for (const language of ["js", "py"]) {
	test(`${language} flag-like user arguments stay distinct through builder handoffs`, async () => {
		let existing = await tg.Command.new({ executable: "tg", args: [language] });
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
			let encoded = encodeModuleArgs(raw);
			assert.deepEqual(
				Array.from(encoded, (arg) => [arg.kind, arg.value]),
				expected,
			);
			assert.equal(encodeModuleArgs(await tg.resolve(encoded)), encoded);
			let command = await tg.command(existing).arg(...raw);
			assert.deepEqual(
				(await command.args).slice(1).map((arg) => [arg.kind, arg.value]),
				expected,
			);
		}
	});
}

for (const kind of ["ts", "py"] as const) {
	test(`${kind} command modules exclude referent metadata`, async () => {
		const file = tg.File.withId("fil_example");
		const options = {
			id: "dir_example",
			name: "alias",
			path: "task.tg.py",
			tag: "tools/^1",
		};
		const module = new tg.Module({ kind, referent: { node: file, options } });
		const command = await tg.Command.jsArg(
			tg.Command.function(module, "default"),
			[],
		);
		const stored = command.node.args?.[3];
		assert(stored instanceof tg.Command.Value);
		assert(stored.value instanceof tg.Module);
		for (const field of ["id", "name", "path", "tag"] as const) {
			assert.equal(stored.value.referent.options?.[field], undefined);
			assert.equal(command.options?.[field], options[field]);
		}
		assert.deepEqual(module.referent.options, options);
	});
}
