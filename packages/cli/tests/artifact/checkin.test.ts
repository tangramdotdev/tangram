import { expect, test } from "bun:test";
import Server from "../server.ts";
import { compare, directory, file } from "../util.ts";

test("file", async () => {
	await using server = await Server.start();
	let dir = await directory({
		"hello.txt": "Hello, World!",
	});
	let id = await server.tg`checkin ${dir}/hello.txt`
		.text()
		.then((t) => t.trim());
	expect(id).toMatchSnapshot();
});

test("executable", async () => {
	await using server = await Server.start();
	let dir = await directory({
		executable: file({ contents: "", executable: true }),
	});
	let id = await server.tg`checkin ${dir}/executable`
		.text()
		.then((t) => t.trim());
	expect(id).toMatchSnapshot();
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
