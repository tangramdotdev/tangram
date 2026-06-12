use ../../../test.nu *

# The builder's node method appends a single node.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let graph = await tg.graph().node({ kind: "file", contents: "viaBuilder" });
			return await ((await graph.get(0)) as tg.File).text;
		};
	'
}

let output = tg build $path
snapshot $output '"viaBuilder"'
