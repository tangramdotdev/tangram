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
