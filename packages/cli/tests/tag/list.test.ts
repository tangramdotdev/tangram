import { expect, test } from "bun:test";
import Server from "../server.ts";
import { directory } from "../util.ts";

test("list tags with no result", async () => {
	await using server = await Server.start();
	const output = await server.tg`tag list test`.text().then((t) => t.trim());
	expect(output).toBeEmpty();
});

test("list tags with one result", async () => {
	await using server = await Server.start();
	let dir = await directory({
		hello: {
			"tangram.ts": `
				export default tg.target(() => "Hello, World");
			`,
		},
	});
	await server.tg`tag hello ${dir}/hello`.quiet();
	const output = await server.tg`tag list hello`.text().then((t) => t.trim());
	expect(output).toBe("hello");
});

test("list tags with no result with remote", async () => {
	await using remote = await Server.start();
	await using server = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	const output = await server.tg`tag list test`.text().then((t) => t.trim());
	expect(output).toBeEmpty();
});

test("list tags with one result with remote", async () => {
	await using remote = await Server.start();
	await using server = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	let dir = await directory({
		hello: {
			"tangram.ts": `
				export default tg.target(() => "Hello, World");
			`,
		},
	});
	await server.tg`tag hello ${dir}/hello`.quiet();
	await server.tg`push hello`.quiet();
	const serverOutput = await server.tg`tag list hello`
		.text()
		.then((t) => t.trim());
	expect(serverOutput).toBe("hello");
	const remoteOutput = await remote.tg`tag list hello`
		.text()
		.then((t) => t.trim());
	expect(remoteOutput).toBe("hello");
});
