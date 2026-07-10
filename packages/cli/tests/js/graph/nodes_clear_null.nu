use ../../../test.nu *

# A null nodes override clears inherited graph nodes, and the object and fluent forms are equivalent.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let base = await tg.graph({
				nodes: [{ kind: "file", contents: "contents" }],
			});
			let viaObject = await tg.graph(base, { nodes: null });
			let viaFluent = await tg.graph(base).nodes(null);
			return (await viaObject.nodes).length === 0 &&
				(await viaFluent.nodes).length === 0;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
