import { expect, test } from "bun:test";
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

// test("build a package with a path dependency", async () => {
// 	await using server = await Server.start();
// 	const buildOutput =
// 		await server.tg`build ${packages.path("import_five_by_path")}`
// 			.text()
// 			.then((t) => t.trim());
// 	expect(buildOutput).toBe("6");
// });

// test("cache hit after push", async () => {
// 	// Start a remote server.
// 	await using remote = await Server.start();

// 	// Start a server.
// 	await using server = await Server.start({
// 		remotes: [{ url: remote.url }],
// 	});

// 	// Build the package.
// 	const output = await server.tg`build ${packages.path("five")}`.text();
// 	expect(output).toBeDefined();

// 	// Push the build.
// 	await server.tg`push ${output}`;

// 	// Stop the server.
// 	await server.stop();

// 	// Start a new server.
// 	await using freshServer = await Server.start({
// 		remotePath: remote.dataPath,
// 	});

// 	// Build the package on the new server.
// 	await freshServer.tg`build ${packages.path("five")}`.text();

// 	// Get the original build ID.
// 	const getOrigBuildId = await freshServer.tg`get ${output}`.text();

// 	// The returned output should include the build ID.
// 	expect(getOrigBuildId).toMatch(originalBuildId as string);
// });

// test("push object to remote", async () => {
// 	// Start a remote server.
// 	await using remote = await Server.start({ registry: true });

// 	// Start a server.
// 	await using server = await Server.start({ remotes: [{ url: remote.url }] });

// 	// Check in the test package.
// 	const fivePackageId = await server.tg`checkin ${packages.path("five")}`
// 		.text()
// 		.then((t) => t.trim());

// 	// Store the output of `tg get` on that ID.
// 	const localGetFiveOutput = await server.tg`get ${fivePackageId}`
// 		.text()
// 		.then((t) => t.trim());

// 	// Push the test package
// 	await server.tg`push ${fivePackageId}`;

// 	// Stop the local server.
// 	await server.stop();

// 	// Store the output of `tg get` on the remote using that ID.
// 	const remoteGetFiveOutput = await remote.tg`get ${fivePackageId}`
// 		.text()
// 		.then((t) => t.trim());

// 	// They should match.
// 	expect(localGetFiveOutput).toBe(remoteGetFiveOutput);
// });

// test("build from pushed tag name", async () => {
// 	// Start a remote server.
// 	await using remote = await Server.start({ registry: true });

// 	// Start a server.
// 	await using server = await Server.start({
// 		remotes: [{ url: remote.url }],
// 	});

// 	// Tag the package.
// 	await server.tg`tag five ${packages.path("five")}`;

// 	// Push the tag.
// 	await server.tg`push five`;

// 	// Stop the server.
// 	await server.stop();

// 	// Start a fresh server.
// 	await using freshServer = await Server.start({
// 		remotePath: remote.dataPath,
// 	});

// 	// Build using just the tag.
// 	const buildOutput = await freshServer.tg`build five`
// 		.text()
// 		.then((t) => t.trim());

// 	// We should have received the correct output without ever passing the path to this server.
// 	expect(buildOutput).toBe("5");
// });

// test("build alternate target after pushing build", async () => {
// 	// Start a remote server.
// 	await using remote = await Server.start({ registry: true });

// 	// Start a server.
// 	await using server = await Server.start({
// 		remotePath: remote.dataPath,
// 	});

// 	// Tag the package.
// 	await server.tg`tag twoTargets ${packages.path("two_targets")}`;

// 	// Push the tag.
// 	await server.tg`push twoTargets`;

// 	// Build the package.
// 	const originalBuildOutput = await server.tg`build twoTargets#five`.text();
// 	const originalBuildId = extractBuildId(originalBuildOutput);
// 	expect(originalBuildId).toBeDefined();

// 	// Push the build.
// 	await server.tg`push ${originalBuildId}`;

// 	// Stop the server.
// 	await server.stop();

// 	// Start a new server.
// 	await using freshServer = await Server.start({
// 		remotePath: remote.dataPath,
// 	});

// 	// Build the other target package on the new server. The should retrieve the package by tag, but start a new build.
// 	let otherTargetOutput = await freshServer.tg`build twoTargets#six`
// 		.text()
// 		.then((t) => t.trim());

// 	// The build should succeed.
// 	expect(otherTargetOutput).toBe("6");
// });
