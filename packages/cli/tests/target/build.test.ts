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
	await using remote = await Server.start();
	await using server = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	let dir = await directory({
		notACycle: {
			"tangram.ts": `
				export default tg.target(async () => {
				  let x = {};
				  return { a: x, b: x };
				});
			`,
		},
		selfRef: {
			"tangram.ts": `
				export default tg.target(async () => {
				  let x = {};
				  x.a = x;
				  return x;
				});
			`,
		},
		selfRefArray: {
			"tangram.ts": `
				export default tg.target(async () => {
				  let x = {};
				  x.a = [x];
				  return x;
				});
			`,
		},
		selfRefNoResolve: {
			"tangram.ts": `
				export default tg.target(async() => {
					let env: any = {};
					env.x = env;
					let target = await tg.target({
						env
					});
					return await target.id();
				});
			`,
		},
		doubleRecursionFoo: {
			"tangram.ts": `
				import bar from "bar";
				export default tg.target(() => bar());
			`,
		},
		doubleRecursionBar: {
			"tangram.ts": `
			  import foo from "foo";
				export default tg.target(() => foo());
			`,
		},
	});
	const notACycle = await server.tg`build ${dir}/notACycle`
		.text()
		.then((t) => t.trim());
	const selfRef = server.tg`build ${dir}/selfRef`.text().then((t) => t.trim());
	expect(selfRef).rejects.toThrow();
	const selfRefArray = server.tg`build ${dir}/selfRefArray`
		.text()
		.then((t) => t.trim());
	expect(selfRefArray).rejects.toThrow();
	const selfRefNoResolve = server.tg`build ${dir}/selfRefNoResolve`
		.text()
		.then((t) => t.trim());
	expect(selfRefNoResolve).rejects.toThrow();
	const doubleRecursion = server.tg`build ${dir}/doubleRecursionFoo`
		.text()
		.then((t) => t.trim());
	expect(doubleRecursion).rejects.toThrow();
});
