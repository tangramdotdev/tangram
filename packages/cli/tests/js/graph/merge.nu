use ../../lib/test.nu *

# tg.graph concatenates multiple graphs, offsetting the second graph's node indices.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let a = await tg.graph({
				nodes: [
					{ kind: "directory", entries: { "f": 1 } },
					{ kind: "file", contents: "a" },
				],
			});
			let b = await tg.graph({
				nodes: [
					{ kind: "directory", entries: { local: 1, external: { graph: a, index: 1, kind: "file" } } },
					{ kind: "file", contents: "b", dependencies: { local: 0, external: { graph: a, index: 1, kind: "file" } } },
					{ kind: "symlink", artifact: 1 },
					{ kind: "symlink", artifact: { graph: a, index: 1, kind: "file" } },
				],
			});
			let merged = await tg.graph(a, b);
			return [
				(await merged.nodes).length,
				await ((await ((await merged.get(0)) as tg.Directory).get("f")) as tg.File).text,
				await ((await ((await merged.get(2)) as tg.Directory).get("local")) as tg.File).text,
				await ((await ((await merged.get(2)) as tg.Directory).get("external")) as tg.File).text,
				await ((await ((await merged.get(4)) as tg.Symlink).artifact) as tg.File).text,
				await ((await ((await merged.get(5)) as tg.Symlink).artifact) as tg.File).text,
				(await ((await merged.get(3)) as tg.File).dependencies)["local"]!.node instanceof tg.Directory,
				await ((await ((await merged.get(3)) as tg.File).dependencies)["external"]!.node as tg.File).text,
			];
		}
	'
}

let output = tg build $path
snapshot $output '[6,"a","b","a","b","a",true,"a"]'
