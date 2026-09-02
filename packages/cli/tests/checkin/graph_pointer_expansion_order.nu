use ../../test.nu *
use ../lib/checkin.nu checkin-output

# An opaque permission-only graph pointer does not prevent a later normal reference from expanding the same node.

let server = server spawn
let dependency = tg put 'tg.file("old dependency")' | str trim
let graph_module = r#'
	export default async function () {
		const dependency = tg.File.withId("<dependency>");
		const graph = await tg.graph({
			nodes: [{
				kind: "file",
				contents: "graph node",
				dependencies: {
					"dependency/^1": {
						node: dependency,
						options: {
							id: dependency.id,
							tag: "dependency/1.0.0",
						},
					},
				},
			}],
		});
		return graph;
	}
'# | str replace '<dependency>' $dependency
let graph_path = artifact {
	tangram.ts: $graph_module
}
let graph = tg build $graph_path | str trim
let pointer_module = r#'
	export default function () {
		const graph = tg.Graph.withId("<graph>");
		return tg.file({ graph, index: 0, kind: "file" });
	}
'# | str replace '<graph>' $graph
let pointer_path = artifact {
	tangram.ts: $pointer_module
}
let pointer = tg build $pointer_path | str trim
tg tag -p dependency/1.0.0 $dependency
tg tag -p pointer/1.0.0 $pointer
tg index

let metadata = tg object metadata $graph | from json
assert equal $metadata.node.solvable true "the graph should be solvable"
assert equal $metadata.subtree.solved true "the graph should already be solved"

let replacement = tg put 'tg.file("new dependency")' | str trim
tg tag put --force dependency/1.0.0 $replacement
let reference = $'graph=($graph)&index=0&kind=file'
let dependencies = [$reference] | to json

let permission_only_first = artifact {
	a: (file --xattrs { "user.tangram.dependencies": $dependencies } explicit)
	b.tg.ts: 'import "pointer/^1";'
}
let output = checkin-output $server $permission_only_first
let object = tg get --blobs --depth=inf --pretty $output.reference
assert ($object | str contains "new dependency") "the later normal reference should expand and solve the graph node"

let normal_first = artifact {
	a.tg.ts: 'import "pointer/^1";'
	b: (file --xattrs { "user.tangram.dependencies": $dependencies } explicit)
}
let output = checkin-output $server $normal_first
let object = tg get --blobs --depth=inf --pretty $output.reference
assert ($object | str contains "new dependency") "the earlier normal reference should expand and solve the graph node"
