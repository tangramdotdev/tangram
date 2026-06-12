use ../../../test.nu *

# A file node's dependency edge resolves to the node at that index.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let graph = await tg.graph({
				nodes: [
					{ kind: "file", contents: "main", dependencies: { "./dep": 1 } },
					{ kind: "file", contents: "dependency contents" },
				],
			});
			let file = (await graph.get(0)) as tg.File;
			let dependencies = await file.dependencies;
			return await dependencies["./dep"].item.text;
		};
	'
}

let output = tg build $path
snapshot $output '"dependency contents"'
