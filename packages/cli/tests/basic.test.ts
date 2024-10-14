import { expect, test } from "bun:test";
import { $ } from "bun";
import Server from "./server.ts";
import { directory } from "./util.ts";

test("build a basic package", async () => {
	await using server = await Server.start();
	let dir = await directory({
		hello: {
			"tangram.ts": `
				export default tg.target(() => "Hello, World");
			`,
		},
	});
	const buildOutput = await server.tg`build ${dir}/hello`
		.text()
		.then((t) => t.trim());
	expect(buildOutput).toBe('"Hello, World"');
});

test("build a package with a path dependency", async () => {
	await using server = await Server.start();
	let dir = await directory({
		driver: {
			"tangram.ts": `
				import message from "../message";
				export default tg.target(() => message());
			`,
		},
		message: {
			"tangram.ts": `
				export default tg.target(() => "Hello, World");
			`,
		},
	});
	const buildOutput = await server.tg`build ${dir}/driver`
		.text()
		.then((t) => t.trim());
	expect(buildOutput).toBe('"Hello, World"');
});

test("cache hit after push", async () => {
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
	const buildId = await server.tg`build ${dir}/hello -d`
		.text()
		.then((t) => t.trim());
	await server.tg`build output ${buildId}`.quiet();
	await server.tg`push ${buildId}`;
	await server.stop();
	await using freshServer = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	await freshServer.tg`build ${dir}/hello`.text();
	const getBuildIdOutput = await freshServer.tg`get ${buildId}`.text();
	expect(getBuildIdOutput).toMatch(buildId);
});

test("push object to remote", async () => {
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
	const packageId = await server.tg`checkin ${dir}/hello`
		.text()
		.then((t) => t.trim());
	const localGetOutput = await server.tg`get ${packageId}`
		.text()
		.then((t) => t.trim());
	await server.tg`push ${packageId}`;
	await server.stop();
	const remoteGetOutput = await remote.tg`get ${packageId}`
		.text()
		.then((t) => t.trim());
	expect(localGetOutput).toBe(remoteGetOutput);
});

test("build from pushed tag name", async () => {
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
	await server.stop();
	await using freshServer = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	const buildOutput = await freshServer.tg`build hello`
		.text()
		.then((t) => t.trim());
	expect(buildOutput).toBe('"Hello, World"');
});

test("build alternate target after pushing build", async () => {
	await using remote = await Server.start({ registry: true });
	await using server = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	let dir = await directory({
		twoTargets: {
			"tangram.ts": `
				export const five = tg.target(() => 5);
				export const six = tg.target(() => 6);
			`,
		},
	});
	await server.tg`tag twoTargets ${dir}/twoTargets`;
	await server.tg`push twoTargets`;
	const buildId = await server.tg`build twoTargets#five -d`
		.text()
		.then((t) => t.trim());
	await server.tg`build output ${buildId}`.quiet();
	await server.tg`push ${buildId}`;
	await server.stop();
	await using freshServer = await Server.start({
		remotePath: remote.dataPath,
	});
	let otherTargetOutput = await freshServer.tg`build twoTargets#six`
		.text()
		.then((t) => t.trim());
	expect(otherTargetOutput).toBe("6");
});
