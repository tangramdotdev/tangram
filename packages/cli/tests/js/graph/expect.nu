use ../../../test.nu *

# tg.Graph.expect returns the value unchanged when it is a graph.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return tg.Graph.expect(await tg.graph({ nodes: [{ kind: "file", contents: "hello" }] })) instanceof tg.Graph; }'
}

let output = tg build $path
snapshot $output 'true'
