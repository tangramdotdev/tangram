use ../../../test.nu *

# tg.Graph.withId returns a graph that preserves the given id.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let graph = await tg.graph({ nodes: [{ kind: "file", contents: "hello" }] });
			return tg.Graph.withId(graph.id).id === graph.id;
		};
	'
}

let output = tg build $path
snapshot $output 'true'
