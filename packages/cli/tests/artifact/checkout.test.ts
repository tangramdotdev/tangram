import { expect, test } from "bun:test";
import { $ } from "bun";
import Server from "../server.ts";
import { directory, symlink } from "../util.ts";

test("file roundtrip", async () => {
	await using server = await Server.start();

	// Check in the file.
	let dir = await directory({
		"hello.txt": "hello, world!",
	});
	let id0 = await server.tg`checkin ${dir}/hello.txt`
		.text()
		.then((t) => t.trim());

	// Check it back out.
	const path = await $`mktemp`.text().then((t) => t.trim());
	await server.tg`checkout --force ${id0} ${path}`;

	// Validate the ids.
	let id1 = await server.tg`checkin ${path}`.text().then((t) => t.trim());
	expect(id0 === id1).toBeTrue();
});

test("directory roundtrip", async () => {
	await using server = await Server.start();

	// Check in the directory.
	let dir = await directory({
		directory: {
			file: "",
		},
	});
	let id0 = await server.tg`checkin ${dir}/directory`
		.text()
		.then((t) => t.trim());

	// Check it back out.
	const path = await $`mktemp -d`.text().then((t) => t.trim());
	await server.tg`checkout --force ${id0} ${path}`;

	// Check the artifact back in.
	let id1 = await server.tg`checkin ${path}`.text().then((t) => t.trim());

	// Verify that the ids match.
	expect(id0 === id1).toBeTrue();
});

test("symlink roundtrip", async () => {
	await using server = await Server.start();

	// Check in the directory.
	let dir = await directory({
		directory: {
			file: "",
			link: symlink("file"),
		},
	});

	let id0 = await server.tg`checkin ${dir}/directory`
		.text()
		.then((t) => t.trim());

	// Check it back out.
	const path = await $`mktemp -d`.text().then((t) => t.trim());
	await server.tg`checkout --force ${id0} ${path}`;

	// Check the artifact back in.
	let id1 = await server.tg`checkin ${path}`.text().then((t) => t.trim());

	// Verify that the ids match.
	expect(id0 === id1).toBeTrue();
});

test("symlink creating a cycle", async () => {
	await using server = await Server.start();

	// Check in the directory.
	let dir = await directory({
		directory: {
			link: symlink("."),
		},
	});

	let id0 = await server.tg`checkin ${dir}/directory`
		.text()
		.then((t) => t.trim());

	// Check it back out.
	const path = await $`mktemp -d`.text().then((t) => t.trim());
	await server.tg`checkout --force ${id0} ${path}`;

	// Check the artifact back in.
	let id1 = await server.tg`checkin ${path}`.text().then((t) => t.trim());

	// Verify that the ids match.
	expect(id0 === id1).toBeTrue();
});

test("package", async () => {
	await using server = await Server.start();

	// Create and tag a dependency.
	let dep = await directory({
		"tangram.ts": "",
	});
	await server.tg`tag dependency ${dep}`;

	// Create a package that imports it via tag.
	let pkg = await directory({
		"tangram.ts": 'import * as dep from "dependency"',
	});
	let pkgId = await server.tg`checkin ${pkg}`.text().then((t) => t.trim());

	// Check it out.
	const path = await $`mktemp -d`.text().then((t) => t.trim());
	await server.tg`checkout --force ${pkgId} ${path}`;

	// Check it in again.
	let id = await server.tg`checkin ${path}`.text().then((t) => t.trim());
	expect(id === pkgId).toBeTrue();
});

test("lockfile generation", async () => {
	await using server = await Server.start();

	// Create and tag a dependency.
	let dep = await directory({
		"tangram.ts": "",
	});
	await server.tg`tag dependency ${dep}`;

	// Create a package that imports it via tag.
	let pkg = await directory({
		child: await directory({
			"tangram.ts": "",
		}),
		"tangram.ts":
			'import * as dep from "dependency"\nimport * as child from "./child"',
	});
	let pkgId = await server.tg`checkin ${pkg}`.text().then((t) => t.trim());

	// Check it out.
	const path = await $`mktemp -d`.text().then((t) => t.trim());
	await server.tg`checkout --force ${pkgId} ${path}`;

	// Check it in again.
	let id = await server.tg`checkin ${path}`.text().then((t) => t.trim());
	expect(id === pkgId).toBeTrue();

	// Verify there is only one lockfile.
	let result = await $`find ${path} -name tangram.lock -type f | wc -l`
		.text()
		.then((t) => t.trim());
	expect(Number.parseInt(result) === 1).toBeTrue();
});
