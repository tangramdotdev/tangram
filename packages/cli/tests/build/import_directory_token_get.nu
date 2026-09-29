use ../lib/test.nu *

# Importing a token-bearing directory reference resolves its dependency with a get path.
let local = server spawn
let dependency_path = artifact {
	tangram.ts: '
		export default async function () {
			const graph = await tg.graph({ nodes: [
				{ kind: "directory", entries: { "module.tg.ts": 1 } },
				{ kind: "file", contents: `export default function () { return "graph target"; }`, module: "ts" },
			] });
			return graph.get(0);
		}
	'
}
let dependency = tg build $dependency_path | str trim
assert ($dependency | str contains 'tokens[') "the import reference should contain an authorization token"

# Exercise both standalone and graph-backed referrer files.
for cycle in ['', 'import source from "." with { type: "directory" };'] {
	let path = artifact {
		tangram.ts: $'
			($cycle)
			import target from "($dependency)" with { get: "module.tg.ts" };
			export default target;
		'
	}
	let output = tg build $path | from json
	assert equal $output "graph target"
}
