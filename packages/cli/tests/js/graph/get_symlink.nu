use ../../../test.nu *

# A symlink node's artifact edge resolves to the node at that index.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let graph = await tg.graph({
				nodes: [
					{ kind: "symlink", artifact: 1, path: "p" },
					{ kind: "file", contents: "target" },
				],
			});
			let symlink = (await graph.get(0)) as tg.Symlink;
			return [await symlink.path, await ((await symlink.artifact) as tg.File).text];
		};
	'
}

let output = tg build $path
snapshot $output '["p","target"]'
