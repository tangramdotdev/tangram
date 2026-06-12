use ../../../test.nu *

# A graph's get method returns the file at the given node index.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let graph = await tg.graph({ nodes: [{ kind: "file", contents: "hello" }] });
			let file = await graph.get(0);
			return await (file as tg.File).text;
		};
	'
}

let output = tg build $path
snapshot $output '"hello"'
