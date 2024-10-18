import { expect, test } from "bun:test";
import Server from "../server.ts";
import { directory } from "../util.ts";

test("hello world", async () => {
	await using server = await Server.start();
	let dir = await directory({
		foo: {
			"tangram.ts": `
				export default tg.target(() => "Hello, World");
			`,
		},
	});
	const output = await server.tg`build ${dir}/foo`.text().then((t) => t.trim());
	expect(output).toBe('"Hello, World"');
});

test("path dependency", async () => {
	await using server = await Server.start();
	let dir = await directory({
		foo: {
			"tangram.ts": `
				import bar from "../bar";
				export default tg.target(() => bar());
			`,
		},
		bar: {
			"tangram.ts": `
				export default tg.target(() => "Hello, World");
			`,
		},
	});
	const output = await server.tg`build ${dir}/foo`.text().then((t) => t.trim());
	expect(output).toBe('"Hello, World"');
});

test("tagged dependency", async () => {
	await using remote = await Server.start();
	await using server = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	let dir = await directory({
		foo: {
			"tangram.ts": `
				import bar from "bar";
				export default tg.target(() => bar());
			`,
		},
		bar: {
			"tangram.ts": `
				export default tg.target(() => "Hello, World");
			`,
		},
	});
	await server.tg`tag bar ${dir}/bar`.quiet();
	await server.tg`push bar`.quiet();
	await server.tg`tag foo ${dir}/foo`.quiet();
	await server.tg`push foo`.quiet();
	await using newServer = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	const output = await newServer.tg`build foo`.text().then((t) => t.trim());
	expect(output).toBe('"Hello, World"');
});

test("build from pushed tag name", async () => {
	await using remote = await Server.start();
	await using server = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	let dir = await directory({
		foo: {
			"tangram.ts": `
				export default tg.target(() => "Hello, World");
			`,
		},
	});
	await server.tg`tag foo ${dir}/foo`.quiet();
	await server.tg`push foo`.quiet();
	await using newServer = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	const output = await newServer.tg`build foo`.text().then((t) => t.trim());
	expect(output).toBe('"Hello, World"');
});

test("cache hit after push", async () => {
	await using remote = await Server.start();
	await using server = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	let dir = await directory({
		foo: {
			"tangram.ts": `
				export default tg.target(() => "Hello, World");
			`,
		},
	});
	const build = await server.tg`build ${dir}/foo -d`
		.text()
		.then((t) => t.trim());
	await server.tg`build output ${build}`.quiet();
	await server.tg`push ${build}`.quiet();
	await using newServer = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	await newServer.tg`get ${build}`.quiet();
});

test("cycle detection", async () => {
	await using server = await Server.start();
	let dir = await directory({
		foo: {
			"tangram.ts": `
				export let x = tg.target(() => x());
			`,
		},
		bar: {
			"tangram.ts": `
			  import baz from "baz";
				export default tg.target(() => baz());
			`,
		},
		baz: {
			"tangram.ts": `
				import bar from "bar";
				export default tg.target(() => bar());
			`,
		},
	});
	const foo = server.tg`build ${dir}/foo`.quiet();
	expect((async () => await foo)()).rejects.toThrow();
	const bar = server.tg`build ${dir}/bar`.quiet();
	expect((async () => await bar)()).rejects.toThrow();
});

test("value cycle detection", async () => {
	await using server = await Server.start();
	let dir = await directory({
		foo: {
			"tangram.ts": `
				export default tg.target(async () => {
					let x = {};
					return { a: x, b: x };
				});
			`,
		},
		object: {
			"tangram.ts": `
				export default tg.target(async () => {
					let x = {};
					x.a = x;
					return x;
				});
			`,
		},
		array: {
			"tangram.ts": `
				export default tg.target(async () => {
					let x = [];
					x[0] = x;
					return x;
				});
			`,
		},
	});
	await server.tg`build ${dir}/foo`.quiet();
	const object = server.tg`build ${dir}/object`.quiet();
	await expect((async () => await object)()).rejects.toThrow();
	const array = server.tg`build ${dir}/array`.quiet();
	await expect((async () => await array)()).rejects.toThrow();
});
