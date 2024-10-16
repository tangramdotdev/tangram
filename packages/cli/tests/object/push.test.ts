import { test } from "bun:test";
import Server from "../server.ts";
import { directory } from "../util.ts";

test("push object to remote", async () => {
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
	const id = await server.tg`checkin ${dir}/foo`.text().then((t) => t.trim());
	await server.tg`push ${id}`.quiet();
	await remote.tg`get ${id}`.quiet();
});

test("push same object to remote twice", async () => {
	await using remote = await Server.start();
	await using server = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	const dir = await directory({
		foo: {
			"tangram.ts": `
				export default tg.target(() => "Hello, World");
			`,
		},
	});
	const id = await server.tg`checkin ${dir}/foo`.text().then((t) => t.trim());
	await server.tg`push ${id}`.quiet();
	await remote.tg`get ${id}`.quiet();
	await server.tg`push ${id}`.quiet();
	await remote.tg`get ${id}`.quiet();
});

test("many concurrent pushes of one object", async () => {
	await using remote = await Server.start();
	await using server = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	const dir = await directory({
		foo: {
			"tangram.ts": `
				export default tg.target(() => "Hello, World");
			`,
		},
	});
	const id = await server.tg`checkin ${dir}/foo`.text().then((t) => t.trim());
	await Promise.all(
		Array.from(Array(100).keys()).map(async (_) => {
			await server.tg`push ${id}`.quiet();
		}),
	);
	await remote.tg`get ${id}`.quiet();
});

test("many concurrent pushes of different objects", async () => {
	await using remote = await Server.start();
	await using server = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	const ids = await Promise.all(
		Array.from(Array(100).keys()).map(async (i) => {
			const dir = await directory({
				[`${i}`]: {
					"tangram.ts": `
					export default tg.target(() => ${i});
				`,
				},
			});
			const id = await server.tg`checkin ${dir}/${i}`
				.text()
				.then((t) => t.trim());
			await server.tg`push ${id}`.quiet();
			return id;
		}),
	);
	await Promise.all(
		ids.map(async (id) => {
			await remote.tg`get ${id}`.quiet();
		}),
	);
});
