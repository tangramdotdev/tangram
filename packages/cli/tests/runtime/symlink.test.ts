import { describe, expect, test } from "bun:test";
import Server from "../server.ts";
import { directory } from "../util.ts";

describe("constructor", () => {
	test("string", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
        export default tg.target(async () => {
        	let s = await tg.symlink("hello");
        	return await s.object();
        });
      `,
		});
		const result = await server.tg`build ${dir}`.text().then((t) => t.trim());
		expect(result).toMatchSnapshot();
	});
	test("artifact", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
        export default tg.target(async () => {
        	let f = await tg.file({ contents: "hello" });
        	let s = await tg.symlink(f);
        	return await s.object();
        });
      `,
		});
		const result = await server.tg`build ${dir}`.text().then((t) => t.trim());
		expect(result).toMatchSnapshot();
	});
	test("object", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
        export default tg.target(async () => {
        	let f = await tg.file({ contents: "hello" });
        	let d = await tg.directory({
        		hello: tg.file("hello")
        	});
        	let s = await tg.symlink({ artifact: d, path: "hello" });
        	return await s.object();
        });
      `,
		});
		const result = await server.tg`build ${dir}`.text().then((t) => t.trim());
		expect(result).toMatchSnapshot();
	});
	test("variadic overwrites completely", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
        export default tg.target(async () => {
        	let f = await tg.file({ contents: "hello" });
        	let d1 = await tg.directory({
        		hello: tg.file("hello from d1"),
        	});
        	let d2 = await tg.directory({
        		hello: tg.file("hello from d2"),
        	});
        	let s = await tg.symlink({ artifact: d1, path: "hello" }, { artifact: d2 });
        	return [await s.artifact(), await s.path()];
        });
      `,
		});
		const result = await server.tg`build ${dir}`.text().then((t) => t.trim());
		expect(result).toMatchSnapshot();
	});
	test("graph", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
        export default tg.target(async () => {
        	let f = await tg.file({ contents: "hello" });
        	let d1 = await tg.directory({
        		hello: tg.file("hello from d1"),
        	});
        	const graph = tg.graph({ nodes: [{ kind: "symlink", artifact: d1, path: "hello" }] });
          const s = await tg.symlink({ graph, node: 0 });
          return [await s.artifact(), await s.path()];
        });
      `,
		});
		const result = await server.tg`build ${dir}`.text().then((t) => t.trim());
		expect(result).toMatchSnapshot();
	});
	test("rejects mixed graph and regular objects", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
		      export default tg.target(async () => {
		      	const graph = tg.graph({ nodes: [{ kind: "file", contents: "hello from inside a graph" }] });
		        const s = await tg.symlink({ graph, node: 0 }, { path: "world" });
          	return [await s.artifact(), await s.path()];
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
		      	const graphA = tg.graph({ nodes: [{ kind: "file", contents: "hello from inside a graph" }] });
		      	const graphB = tg.graph({ nodes: [{ kind: "file", contents: "hello from a different graph" }] });
		        const s = await tg.symlink({ graph: graphA, node: 0 }, { graph: graphB, node: 0 });
          	return [await s.artifact(), await s.path()];
		      });
		    `,
		});
		const result = server.tg`build ${dir}`.quiet();
		await expect((async () => await result)()).rejects.toThrow();
	});
});
