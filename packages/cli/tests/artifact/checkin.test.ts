import { expect, test } from "bun:test";
import Server from "../server.ts";
import { compare, directory, file, symlink } from "../util.ts";

test("directory", async () => {
	await using server = await Server.start();
	console.log("path", server.path);
	let dir = await directory({
		"hello.txt": "hello, world!",
		link: await symlink("hello.txt"),
		"child/link": await symlink("../link"),
	});
	let id = await server.tg`checkin ${dir}`.text().then((t) => t.trim());
	let data = await server.tg`get ${id}`.text().then((t) => t.trim());
	let metadata = await server.tg`object metadata ${id}`
		.text()
		.then((t) => t.trim());

	expect(id).toMatchSnapshot();
	expect(data).toMatchSnapshot();
	expect(metadata).toMatchSnapshot();
});

test("file", async () => {
	await using server = await Server.start();
	let dir = await directory({
		"hello.txt": "Hello, World!",
	});
	let id = await server.tg`checkin ${dir}/hello.txt`
		.text()
		.then((t) => t.trim());
	let data = await server.tg`get ${id}`.text().then((t) => t.trim());
	let metadata = await server.tg`object metadata ${id}`
		.text()
		.then((t) => t.trim());

	expect(id).toMatchSnapshot();
	expect(data).toMatchSnapshot();
	expect(metadata).toMatchSnapshot();
});

test("symlink", async () => {
	await using server = await Server.start();
	let dir = await directory({
		file: "text",
		link: symlink("file"),
	});
	let id = await server.tg`checkin ${dir}/link`.text().then((t) => t.trim());
	let data = await server.tg`get ${id}`.text().then((t) => t.trim());
	let metadata = await server.tg`object metadata ${id}`
		.text()
		.then((t) => t.trim());

	expect(id).toMatchSnapshot();
	expect(data).toMatchSnapshot();
	expect(metadata).toMatchSnapshot();
});

test("cycle", async () => {
	await using server = await Server.start();
	let dir = await directory({
		link: await symlink("."),
	});
	let id = await server.tg`checkin ${dir}`.text().then((t) => t.trim());
	let data = await server.tg`get ${id}`.text().then((t) => t.trim());
	let metadata = await server.tg`object metadata ${id}`
		.text()
		.then((t) => t.trim());

	expect(id).toMatchSnapshot();
	expect(data).toMatchSnapshot();
	expect(metadata).toMatchSnapshot();
});

test("cyclic-path-dependencies", async () => {
	await using server = await Server.start();
	let dir = await directory({
		"tangram.ts": 'import * as dependency from "./dependency.tg.ts"',
		"dependency.tg.ts": 'import * as root from "./tangram.ts"',
	});
	let id = await server.tg`checkin ${dir}`.text().then((t) => t.trim());
	let data = await server.tg`get ${id}`.text().then((t) => t.trim());
	let metadata = await server.tg`object metadata ${id}`
		.text()
		.then((t) => t.trim());

	expect(id).toMatchSnapshot();
	expect(data).toMatchSnapshot();
	expect(metadata).toMatchSnapshot();
});

test("executable", async () => {
	await using server = await Server.start();
	let dir = await directory({
		executable: file({ contents: "", executable: true }),
	});
	let id = await server.tg`checkin ${dir}/executable`
		.text()
		.then((t) => t.trim());
	let data = await server.tg`get ${id}`.text().then((t) => t.trim());
	let metadata = await server.tg`object metadata ${id}`
		.text()
		.then((t) => t.trim());

	expect(id).toMatchSnapshot();
	expect(data).toMatchSnapshot();
	expect(metadata).toMatchSnapshot();
});

test("roundtrip directory", async () => {
	await using server = await Server.start();
	let dir = await directory({
		"hello.txt": "Hello, World!",
	});
	let id = await server.tg`checkin ${dir}`.text().then((t) => t.trim());
	let path = await server.tg`checkout ${id}`.text().then((t) => t.trim());
	const equal = await compare(path, dir);
	expect(equal).toBeTrue();
});
