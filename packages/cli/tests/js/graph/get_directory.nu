use ../../../test.nu *

# A directory node's entry edge resolves to the node at that index.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let graph = await tg.graph({
				nodes: [
					{ kind: "directory", entries: { "f": 1 } },
					{ kind: "file", contents: "child" },
				],
			});
			let directory = (await graph.get(0)) as tg.Directory;
			let file = await directory.get("f");
			return await (file as tg.File).text;
		}
	'
}

let output = tg build $path
snapshot $output '"child"'
