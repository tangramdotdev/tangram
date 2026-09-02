use ../../test.nu *
use ../lib/checkin.nu checkin-output

# Solving a branch directory preserves the graph context of an internal pointer in a child.

let server = server spawn
let dependency_path = artifact {
	tangram.ts: '
		export default async function () {
			const graph = await tg.graph({
				nodes: [
					{ kind: "directory", entries: { "module.tg.ts": 1 } },
					{
						kind: "file",
						contents: `import "dependency/^1";`,
						dependencies: { "dependency/^1": null },
						module: "ts",
					},
				],
			});
			return tg.directory({
				children: [{
					directory: { graph, index: 0, kind: "directory" },
					count: 1,
					last: "module.tg.ts",
				}],
			});
		}
	'
}
let dependency = tg build $dependency_path | str trim
let replacement = tg put 'tg.file("replacement")' | str trim
tg tag -p dependency/1.0.0 $replacement
tg index

let metadata = tg metadata $dependency | from json
assert equal $metadata.subtree.solvable true "the branch dependency should be solvable"
assert equal $metadata.subtree.solved false "the branch dependency should be unsolved"

let dependencies = [$dependency] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } branch)
}
let path = $directory | path join input
let output = checkin-output $server $path
assert equal $output.permissions [object_subtree] "the branch permissions should propagate"
let object = tg get --blobs --depth=inf --pretty $output.reference
assert ($object | str contains "replacement") "the internal graph pointer should be solved"
