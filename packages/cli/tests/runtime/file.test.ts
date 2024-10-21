import { describe, expect, test } from "bun:test";
import Server from "../server.ts";
import { directory } from "../util.ts";

describe("constructor", () => {
	test("string", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
        export default tg.target(async () => {
        	let f = await tg.file("hello");
        	return await f.text();
        });
      `,
		});
		const result = await server.tg`build ${dir}`.text().then((t) => t.trim());
		expect(result).toBe(`"hello"`);
	});
	test("object", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
        export default tg.target(async () => {
        	let f = await tg.file({ contents: "hello" });
        	return await f.text();
        });
      `,
		});
		const result = await server.tg`build ${dir}`.text().then((t) => t.trim());
		expect(result).toBe(`"hello"`);
	});
	test("uint8array", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
        export default tg.target(async () => {
        	let f = await tg.file(new Uint8Array([0x68, 0x65, 0x6C, 0x6C, 0x6F]));
        	return await f.text();
        });
      `,
		});
		const result = await server.tg`build ${dir}`.text().then((t) => t.trim());
		expect(result).toBe(`"hello"`);
	});
	test("variadic appends contents", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
        export default tg.target(async () => {
          const f = await tg.file({ contents: "hello, " }, { contents: "world" });
          return await f.text();
        });
      `,
		});
		const result = await server.tg`build ${dir}`.text().then((t) => t.trim());
		expect(result).toBe(`"hello, world"`);
	});
	test("variadic overwrites executable", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
        export default tg.target(async () => {
          const f = await tg.file({ contents: "hello", executable: true }, { executable: false });
          return [await f.text(), await f.executable()];
        });
      `,
		});
		const result = await server.tg`build ${dir}`.text().then((t) => t.trim());
		expect(result).toBe(`["hello",false,]`);
	});
	test("graph", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
        export default tg.target(async () => {
        	const graph = await tg.graph({ nodes: [{ kind: "file", contents: "hello from inside a graph" }] });
          const f = await tg.file({ graph, node: 0 });
          return await f.text();
        });
      `,
		});
		const result = await server.tg`build ${dir}`.text().then((t) => t.trim());
		expect(result).toBe(`"hello from inside a graph"`);
	});
	test("rejects mixed graph and regular objects", async () => {
		await using server = await Server.start();
		const dir = await directory({
			"tangram.ts": `
		      export default tg.target(async () => {
		      	const graph = await tg.graph({ nodes: [{ kind: "file", contents: "hello from inside a graph" }] });
		        const f = await tg.file({ graph, node: 0 }, { contents: "world" });
		        return await f.text();
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
		      	const graphA = await tg.graph({ nodes: [{ kind: "file", contents: "hello from inside a graph" }] });
		      	const graphB = await tg.graph({ nodes: [{ kind: "file", contents: "hello from a different graph" }] });
		        const f = await tg.file({ graph: graphA, node: 0 }, { graph: graphB, node: 0 });
		        return await f.text();
		      });
		    `,
		});
		const result = server.tg`build ${dir}`.quiet();
		await expect((async () => await result)()).rejects.toThrow();
	});
});
