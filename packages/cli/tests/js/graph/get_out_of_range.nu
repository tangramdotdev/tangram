use ../../../test.nu *

# A graph's get method throws when the node index is out of range.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let graph = await tg.graph({ nodes: [{ kind: "file", contents: "hello" }] });
			try {
				await graph.get(5);
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"invalid graph index"'
