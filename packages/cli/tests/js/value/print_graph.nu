use ../../lib/test.nu *

# tg.Value.print renders internal graph edges as indices.

let local = server spawn

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
			return !output.includes(`"module":`) && !output.includes(`"graph":`) && /"artifact":\s*0/.test(output);
		}
	'
}

let output = tg build $path
snapshot $output 'true'
