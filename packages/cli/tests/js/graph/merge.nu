use ../../../test.nu *

# tg.graph concatenates multiple graphs, offsetting the second graph's node indices.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let a = await tg.graph({
				nodes: [
					{ kind: "directory", entries: { "f": 1 } },
					{ kind: "file", contents: "a" },
				],
			});
			let b = await tg.graph({ nodes: [{ kind: "file", contents: "b" }] });
			let merged = await tg.graph(a, b);
			return [
				(await merged.nodes).length,
				await ((await ((await merged.get(0)) as tg.Directory).get("f")) as tg.File).text,
				await ((await merged.get(2)) as tg.File).text,
			];
		}
	'
}

let output = tg build $path
snapshot $output '[3,"a","b"]'
