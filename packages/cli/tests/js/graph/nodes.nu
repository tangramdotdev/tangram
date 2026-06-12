use ../../../test.nu *

# A graph's nodes accessor returns its node array.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let graph = await tg.graph({ nodes: [{ kind: "file", contents: "hello" }] });
			let nodes = await graph.nodes;
			return [nodes.length, nodes[0].kind];
		};
	'
}

let output = tg build $path
snapshot $output '[1,"file"]'
