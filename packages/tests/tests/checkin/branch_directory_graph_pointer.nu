use ../../util/test.nu *

# Solving a branch directory preserves the graph context of an internal pointer in a child, and the reference grants another user read access to the solved subtree.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json
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
						module: "typescript",
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
let dependency = tg --token $alice.token build $dependency_path | str trim
let replacement = tg --token $alice.token put --no-tokens 'tg.file("replacement")' | referent node
tg --token $alice.token tag -p dependency/1.0.0 $replacement
tg --token $alice.token index

let metadata = tg --token $alice.token metadata $dependency | from json
assert equal $metadata.subtree.solvable true "the branch dependency should be solvable"
assert equal $metadata.subtree.solved false "the branch dependency should be unsolved"

let dependencies = [$dependency] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } branch)
}
let path = $directory | path join input
let reference = tg --token $alice.token checkin --no-lock --root $path | str trim
let object = tg --token $eve.token get --blobs --depth=inf --no-tokens --pretty $reference
assert ($object | str contains "replacement") "the internal graph pointer should be solved"
