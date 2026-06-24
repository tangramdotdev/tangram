use ../../../test.nu *

# A directory node whose entry edge points to itself resolves back to the same directory.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let graph = await tg.graph({
				nodes: [{ kind: "directory", entries: { "self": 0 } }],
			});
			let directory = (await graph.get(0)) as tg.Directory;
			let self = await directory.get("self");
			return [self instanceof tg.Directory, self.id === directory.id];
		}
	'
}

let output = tg build $path
snapshot $output '[true,true]'
