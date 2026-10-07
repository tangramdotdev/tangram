import { strict as assert } from "node:assert";
import { execFileSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { readError, readOutcome, readOutput } from "../src/file/xattrs.ts";
import * as tg from "../src/index.ts";

test("the Node host lists and reads raw xattrs", async () => {
	let directory = await mkdtemp(join(tmpdir(), "tangram-xattrs-"));
	let path = join(directory, "file");
	try {
		await writeFile(path, "");
		for (let [name, value] of [
			["user.tangram.output.0", "first"],
			["user.tangram.output.1", "second"],
		] as const) {
			if (process.platform === "darwin") {
				execFileSync("xattr", ["-w", name, value, path]);
			} else {
				execFileSync("setfattr", ["--name", name, "--value", value, path]);
			}
		}
		await writeFile(path, "serialized contents");
		if (process.platform === "darwin") {
			execFileSync("xattr", ["-w", "user.tangram.outcome", "", path]);
		} else {
			execFileSync("setfattr", [
				"--name",
				"user.tangram.outcome",
				"--value",
				"",
				path,
			]);
		}
		let value = await tg.host.getxattr(path, "user.tangram.outcome");
		assert(value instanceof Uint8Array);
		assert.equal(value.byteLength, 0);
		assert.equal(
			new TextDecoder().decode(await readOutcome(path)),
			"serialized contents",
		);
		let names = await tg.host.listxattr(path);
		assert(names.includes("user.tangram.output.0"));
		assert(names.includes("user.tangram.output.1"));
		assert(!names.includes("user.tangram.output"));
		assert.equal(await tg.host.getxattr(path, "user.tangram.output"), null);
		assert.equal(
			new TextDecoder().decode(await readOutput(path)),
			"firstsecond",
		);
	} finally {
		await rm(directory, { recursive: true });
	}
});

test("process xattr readers validate and assemble shards", async () => {
	let getxattr = tg.host.getxattr;
	let listxattr = tg.host.listxattr;
	let readFile = tg.host.readFile;
	try {
		for (let [name, read] of [
			["user.tangram.output", readOutput],
			["user.tangram.error", readError],
			["user.tangram.outcome", readOutcome],
		] as const) {
			let values = new Map<string, Uint8Array>();
			tg.host.getxattr = async (_path, key) => values.get(key) ?? null;
			tg.host.listxattr = async () => [...values.keys()];
			assert.equal(await read("file"), null);
			values.set(name, new Uint8Array());
			let contents = new TextEncoder().encode("serialized contents");
			tg.host.readFile = async () => contents;
			assert.deepEqual(await read("file"), contents);
			tg.host.readFile = async () => {
				throw new Error("read failed");
			};
			await assert.rejects(() => read("file"), /read failed/);
			values.clear();

			// Split multibyte text and insert more than ten shards in reverse order.
			let bytes = new TextEncoder().encode("hello 🌍 hello 🌍");
			for (let index = bytes.length - 1; index >= 0; index--) {
				values.set(`${name}.${index}`, bytes.slice(index, index + 1));
			}
			assert.deepEqual(await read("file"), bytes);

			for (let suffixes of [
				["", ".0"],
				[".0", ".2"],
				[".1"],
				[".00"],
				[".+0"],
				[".invalid"],
				["."],
				[".18446744073709551616"],
			]) {
				values.clear();
				for (let suffix of suffixes) {
					values.set(`${name}${suffix}`, new TextEncoder().encode("1"));
				}
				await assert.rejects(() => read("file"));
			}
			values.clear();
			tg.host.listxattr = async () => [`${name}.0`];
			await assert.rejects(() => read("file"), /disappeared/);
		}
	} finally {
		tg.host.getxattr = getxattr;
		tg.host.listxattr = listxattr;
		tg.host.readFile = readFile;
	}
});
