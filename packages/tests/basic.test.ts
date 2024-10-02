import { describe, expect, test } from "bun:test";
import { $ } from "bun";
import { extractBuildId, getTestPackage, startServer } from "./setup.ts";

describe("server", () => {
	test("reports health", async () => {
		await using server = await startServer({ dbConnections: 2 });

		const server_health = await server.tg`server health`.text();
		expect(server_health).toMatchSnapshot();
	});
	
	test("builds a basic package", async () => {
		await using server = await startServer();

		const five = getTestPackage("five");

		const buildOutput = await server.tg`build ${five} --quiet`.text().then((t) => t.trim());

		expect(buildOutput).toBe("5");
	});
	
	test("builds a package with a path dependency", async () => {
		await using server = await startServer();

		const importsFiveByPath = getTestPackage("import_five_by_path");

		 const buildOutput = await server.tg`build ${importsFiveByPath} --quiet`.text().then((t) => t.trim());

		 expect(buildOutput).toBe("6");
	});
});

describe("remote", () => {
	test("cache hit after push", async () => {
		await using remote = await startServer({ registry: true });

		await using originalServer = await startServer({ remotePath: remote.serverPath });

		const five = getTestPackage("five");

		// Build the package.
		const originalBuildOutput = await originalServer.tg`build ${five}`.text();
		const originalBuildId = extractBuildId(originalBuildOutput);
		expect(originalBuildId).toBeDefined();

		// Push the build.
		await originalServer.tg`push ${originalBuildId}`.quiet();

		// Stop the server.
		await originalServer.stop();

		// Start a new server.
		await using freshServer = await startServer({ remotePath: remote.serverPath });

		// Build the package on the new server.
		await freshServer.tg`build ${five}`.text();

		// Get the original build ID.
		const getOrigBuildId = await freshServer.tg`get ${originalBuildId}`.text();

		// The returned output should include the build ID.
		expect(getOrigBuildId).toMatch(originalBuildId as string);
	});

	test("push object to remote", async () => {
		await using remote = await startServer({ registry: true });

		await using server = await startServer({ remotePath: remote.serverPath });

		const five = getTestPackage("five");

		// Check in the test package.
		const fivePackageId = await server.tg`checkin ${five}`.text().then((t) => t.trim());

		// Store the output of `tg get` on that ID.
		const localGetFiveOutput = await server.tg`get ${fivePackageId}`.text().then((t) => t.trim());

		// Push the test package
		await server.tg`push ${fivePackageId}`.quiet();

		// Stop the local server.
		await server.stop();
		
		// Store the output of `tg get` on the remote using that ID.
		const remoteGetFiveOutput = await remote.tg`get ${fivePackageId}`.text().then((t) => t.trim());

		// They should match.
		expect(localGetFiveOutput).toBe(remoteGetFiveOutput);
	});

	test("builds from pushed tag name", async () => {
		await using remote = await startServer({ registry: true });

		await using originalServer = await startServer({ remotePath: remote.serverPath });

		const five = getTestPackage("five");

		// Tag the package.
		await originalServer.tg`tag five ${five}`.quiet();
		
		// Push the tag.
		await originalServer.tg`push five`.quiet();

		// Stop the server.
		await originalServer.stop();

		// Start a fresh server.
		await using freshServer = await startServer({ remotePath: remote.serverPath });

		// Build using just the tag.
		const buildOutput = await freshServer.tg`build five --quiet`.text().then((t) => t.trim());
		
		// We should have received the correct output without ever passing the path to this server.
		expect(buildOutput).toBe("5");
	});
	
	test("build alternate target after pushing build", async () => {
		await using remote = await startServer({ registry: true });

		await using originalServer = await startServer({ remotePath: remote.serverPath });

		const twoTargets = getTestPackage("two_targets");

		// Tag the package.
		await originalServer.tg`tag twoTargets ${twoTargets}`.quiet();
		
		// Push the tag.
		await originalServer.tg`push twoTargets`.quiet();

		// Build the package.
		const originalBuildOutput = await originalServer.tg`build twoTargets#five`.text();
		const originalBuildId = extractBuildId(originalBuildOutput);
		expect(originalBuildId).toBeDefined();

		// Push the build.
		await originalServer.tg`push ${originalBuildId}`.quiet();

		// Stop the server.
		await originalServer.stop();

		// Start a new server.
		await using freshServer = await startServer({ remotePath: remote.serverPath });

		// Build the other target package on the new server. The tag should retrieve the package, but start a new build.
		let otherTargetOutput = await freshServer.tg`build twoTargets#six --quiet`.text().then((t) => t.trim());

		// The build should succeed.
		expect(otherTargetOutput).toBe("6");
	});
})
