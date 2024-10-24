import { describe, expect, test } from "bun:test";
import Server from "../server.ts";
import { compare, directory, symlink } from "../util.ts";

describe("constructor", () => {
	test("empty", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
				export default tg.target(async () => tg.directory());
			`,
		});
		const result = await server.tg`build ${dir}`.text().then((t) => t.trim());
		const actual = await server.tg`checkout ${result}`
			.text()
			.then((t) => t.trim());
		const expected = await directory();
		const equal = await compare(actual, expected);
		expect(equal).toBeTrue();
	});

	test("basic", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
				export default tg.target(async () => {
					let d = await tg.directory({
						hello: tg.file("hello!"),
						inner: {
							goodbye: tg.file("goodbye!"),
						},
						link: tg.symlink("hello"),
					});
					return d;
				});
			`,
		});
		const result = await server.tg`build ${dir}`.text().then((t) => t.trim());
		const actual = await server.tg`checkout ${result}`
			.text()
			.then((t) => t.trim());
		const expected = await directory({
			hello: "hello!",
			inner: {
				goodbye: "goodbye!",
			},
			link: symlink("hello"),
		});
		const equal = await compare(actual, expected);
		expect(equal).toBeTrue();
	});

	test("variadic append", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
				export default tg.target(async () => {
					let d = await tg.directory({
						hello: tg.file("hello!"),
					}, {
						goodbye: tg.file("goodbye!")
					});
					return d;
				});
			`,
		});
		const result = await server.tg`build ${dir}`.text().then((t) => t.trim());
		const actual = await server.tg`checkout ${result}`
			.text()
			.then((t) => t.trim());
		const expected = await directory({
			hello: "hello!",
			goodbye: "goodbye!",
		});
		const equal = await compare(actual, expected);
		expect(equal).toBeTrue();
	});

	test("variadic remove", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
				export default tg.target(async () => {
					let d = await tg.directory({
						hello: tg.file("hello!"),
						goodbye: tg.file("goodbye!")
					}, {
						goodbye: undefined
					});
					return d;
				});
			`,
		});
		const result = await server.tg`build ${dir}`.text().then((t) => t.trim());
		const actual = await server.tg`checkout ${result}`
			.text()
			.then((t) => t.trim());
		const expected = await directory({
			hello: "hello!",
		});
		const equal = await compare(actual, expected);
		expect(equal).toBeTrue();
	});

	test("graph", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
				export default tg.target(async () => {
					const f = await tg.file("hello from inside a graph");
					const graph = await tg.graph({
						nodes: [
							{ kind: "directory", entries: { "hello": f } },
						]
					});
					const d = await tg.directory({ graph, node: 0 });
					return d;
				});
			`,
		});
		const result = await server.tg`build ${dir}`.text().then((t) => t.trim());
		const actual = await server.tg`checkout ${result}`
			.text()
			.then((t) => t.trim());
		const expected = await directory({
			hello: "hello from inside a graph",
		});
		const equal = await compare(actual, expected);
		expect(equal).toBeTrue();
	});

	test("rejects mixed graph and regular objects", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
				export default tg.target(async () => {
					const f = await tg.file("hello from inside a graph");
					const graph = await tg.graph({
						nodes: [
							{ kind: "directory", entries: { "hello": f } },
						]
					});
					const f2 = await tg.file("hello from outside a graph");
					const d = await tg.directory({ graph, node: 0 }, { newFile: f2 });
					return d;
				});
			`,
		});
		const result = server.tg`build ${dir}`.quiet();
		await expect((async () => await result)()).rejects.toThrow();
	});

	test("rejects multiple graph objects", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
				export default tg.target(async () => {
					const f = await tg.file("hello from inside a graph");
					const graphA = await tg.graph({
						nodes: [
							{ kind: "directory", entries: { "hello": f } },
						],
					});
					const f2 = await tg.file("hello from outside a graph");
					const graphB = await tg.graph({
						nodes: [
							{ kind: "directory", entries: { "newFile": f2 } },
						],
					});
					const d = await tg.directory({ graph: graphA, node: 0 }, { graph: graphB: node: 0 });
					return d;
				});
			`,
		});
		const result = server.tg`build ${dir}`.quiet();
		await expect((async () => await result)()).rejects.toThrow();
	});
});
