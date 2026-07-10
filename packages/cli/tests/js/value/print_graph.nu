use ../../../test.nu *

# tg.Value.print renders a graph whose file node has no module and whose pointer edge has no graph, without crashing and omitting those fields.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let graph = await tg.graph({
				nodes: [
					{ kind: "file", contents: "x" },
					{ kind: "symlink", artifact: 0 },
				],
			});
			let output = tg.Value.print(graph);
			return !output.includes(`"module":`) && !output.includes(`"graph":`);
		}
	'
}

let output = tg build $path
snapshot $output 'true'
