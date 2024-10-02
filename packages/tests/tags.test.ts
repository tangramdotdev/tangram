import { describe, expect, test } from "bun:test";
import { getTestPackage, startServer } from "./setup.ts";

describe("no remote", () => {
	test("list tags with no result", async () => {
		await using server = await startServer();

		const listOutput = await server.tg`tag list test`.text().then((t) => t.trim());

		expect(listOutput).toBeEmpty();
	});

	test("list tags with one result", async () => {
		await using server = await startServer();

		const five = getTestPackage("five");

		await server.tg`tag five ${five}`.quiet();

		const listOutput = await server.tg`tag list five`.text().then((t) => t.trim());

		expect(listOutput).toBe("five");
	});

	test("list tags with nproc set to 1", async () => {
	  await using server = await startServer({ concurrency: 1, dbConnections: 1 });

		const five = getTestPackage("five");

		await server.tg`tag five ${five}`.quiet();

		const listOutput = await server.tg`tag list five`.text().then((t) => t.trim());

		expect(listOutput).toBe("five");
	});
});

describe("single remote", () => {
	test("list tags with no result", async () => {
    await using remote = await startServer({ registry: true });

		await using server = await startServer({ remotePath: remote.serverPath });

		const listOutput = await server.tg`tag list test`.text().then((t) => t.trim());

		expect(listOutput).toBeEmpty();
	});

	test("list tags with one result", async () => {
	  await using remote = await startServer({ registry: true });

		await using server = await startServer({ remotePath: remote.serverPath });

		const five = getTestPackage("five");

		await server.tg`tag five ${five}`.quiet();

		await server.tg`push five`.quiet();

		const serverListOutput = await server.tg`tag list five`.text().then((t) => t.trim());
		expect(serverListOutput).toBe("five");

		const remoteListOutput = await remote.tg`tag list five`.text().then((t) => t.trim());
		expect(remoteListOutput).toBe("five");
	});

	test("list tags with nproc set to 1", async () => {
	  await using remote = await startServer({ concurrency: 1, dbConnections: 1, registry: true });

	  await using server = await startServer({ concurrency: 1, dbConnections: 1, remotePath: remote.serverPath });

		const five = getTestPackage("five");

		await server.tg`tag five ${five}`.quiet();

		await server.tg`push five`.quiet();

		const listOutput = await server.tg`tag list five`.text().then((t) => t.trim());
		expect(listOutput).toBe("five");

		const remoteListOutput = await remote.tg`tag list five`.text().then((t) => t.trim());
		expect(remoteListOutput).toBe("five");
	});

	test("build tagged package with path dependency", async () => {
		await using remote = await startServer({ registry: true });

		await using originalServer = await startServer({ remotePath: remote.serverPath });

    const five = getTestPackage("five");

		const importsFiveFromPath = getTestPackage("import_five_by_path");

		// Tag and push both packages.
		await originalServer.tg`tag five ${five}`.quiet();
		await originalServer.tg`push five`.quiet();
		await originalServer.tg`tag importsFive ${importsFiveFromPath}`.quiet();
    await originalServer.tg`push importsFive`.quiet();

		// Stop the server.
		await originalServer.stop();

		// Start a fresh server.
		await using freshServer = await startServer({ remotePath: remote.serverPath });

		// Build using just the tag.
		const buildOutput = await freshServer.tg`build importsFive --quiet`.text().then((t) => t.trim());

		// We should have received the correct output without ever passing either path to the server.
		expect(buildOutput).toBe("6");
	})
});
