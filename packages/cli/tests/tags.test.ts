// import { expect, test } from "bun:test";
// import * as packages from "./packages.ts";
// import Server from "./server.ts";

// test("list tags with no result", async () => {
// 	await using server = await Server.start();
// 	const output = await server.tg`tag list test`.text().then((t) => t.trim());
// 	expect(output).toBeEmpty();
// });

// test("list tags with one result", async () => {
// 	await using server = await Server.start();
// 	await server.tg`tag five ${packages.path("five")}`;
// 	const output = await server.tg`tag list five`.text().then((t) => t.trim());
// 	expect(output).toBe("five");
// });

// test("list tags with no result", async () => {
// 	await using remote = await Server.start({ registry: true });
// 	await using server = await Server.start({ remotePath: remote.dataPath });
// 	const output = await server.tg`tag list test`.text().then((t) => t.trim());
// 	expect(output).toBeEmpty();
// });

// test("list tags with one result", async () => {
// 	await using remote = await Server.start({ registry: true });
// 	await using server = await Server.start({ remotePath: remote.dataPath });
// 	await server.tg`tag five ${packages.path("five")}`;
// 	await server.tg`push five`;
// 	const serverOutput = await server.tg`tag list five`
// 		.text()
// 		.then((t) => t.trim());
// 	expect(serverOutput).toBe("five");
// 	const remoteOutput = await remote.tg`tag list five`
// 		.text()
// 		.then((t) => t.trim());
// 	expect(remoteOutput).toBe("five");
// });

// test("build tagged package with path dependency", async () => {
// 	// Create a remote server.
// 	await using remote = await Server.start({ registry: true });

// 	// Create a server.
// 	await using server = await Server.start({
// 		remotePath: remote.dataPath,
// 	});

// 	// Tag and push both packages.
// 	await server.tg`tag five ${packages.path("five")}`;
// 	await server.tg`push five`;
// 	await server.tg`tag importsFive ${packages.path("import_five_by_path")}`;
// 	await server.tg`push importsFive`;

// 	// Stop the server.
// 	await server.stop();

// 	// Start a new server.
// 	await using newServer = await Server.start({
// 		remotePath: remote.dataPath,
// 	});

// 	// Build using just the tag.
// 	const output = await newServer.tg`build importsFive`
// 		.text()
// 		.then((t) => t.trim());

// 	// We should have received the correct output without ever passing either path to the server.
// 	expect(output).toBe("6");
// });
