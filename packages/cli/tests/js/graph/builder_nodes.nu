use ../../../test.nu *

# The builder's nodes method appends an array of nodes.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let graph = await tg.graph().nodes([
				{ kind: "file", contents: "one" },
				{ kind: "file", contents: "two" },
			]);
			return [
				await ((await graph.get(0)) as tg.File).text,
				await ((await graph.get(1)) as tg.File).text,
			];
		};
	'
}

let output = tg build $path
snapshot $output '["one","two"]'
