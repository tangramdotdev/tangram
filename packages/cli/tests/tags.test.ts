import { expect, test } from "bun:test";
import Server from "./server.ts";
import { directory } from "./util.ts";

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
	await server.tg`tag hello ${dir}/hello`;
	const output = await server.tg`tag list hello`.text().then((t) => t.trim());
	expect(output).toBe("hello");
});

test("list tags with no result with registry", async () => {
	await using remote = await Server.start({ registry: true });
	await using server = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	const output = await server.tg`tag list test`.text().then((t) => t.trim());
	expect(output).toBeEmpty();
});

test("list tags with one result with registry", async () => {
	await using remote = await Server.start({ registry: true });
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
	await server.tg`tag hello ${dir}/hello`;
	await server.tg`push hello`;
	const serverOutput = await server.tg`tag list hello`
		.text()
		.then((t) => t.trim());
	expect(serverOutput).toBe("hello");
	const remoteOutput = await remote.tg`tag list hello`
		.text()
		.then((t) => t.trim());
	expect(remoteOutput).toBe("hello");
});

test("build tagged package with dependency", async () => {
	await using remote = await Server.start({ registry: true });
	await using server = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	let dir = await directory({
		driver: {
			"tangram.ts": `
				import message from "message";
				export default tg.target(() => message());
			`,
		},
		message: {
			"tangram.ts": `
				export default tg.target(() => "Hello, World");
			`,
		},
	});
	await server.tg`tag message ${dir}/message`;
	await server.tg`push message`;
	await server.tg`tag driver ${dir}/driver`;
	await server.tg`push driver`;
	await using newServer = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	const output = await newServer.tg`build driver`.text().then((t) => t.trim());
	expect(output).toBe('"Hello, World"');
});
