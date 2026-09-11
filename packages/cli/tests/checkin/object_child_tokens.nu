use ../../test.nu *
use ../lib/checkin.nu checkin-output

# Both solver reads and prefetches use exact child tokens through branches, graphs, symlinks, and dependencies.

let root_token = random chars
let server = server spawn --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let replacement = tg --token $alice.token put 'tg.file("replacement")' | str trim
tg --token $alice.token tag -p replacement/1.0.0 $replacement
let fixture = artifact {
	tangram.ts: '
		export default async function () {
			const b = await tg.file({
				contents: "import replacement from \"replacement/^1\";",
				dependencies: { "replacement/^1": null },
				module: "ts",
			});
			const a = await tg.file({
				contents: "import b from \"./b\";",
				dependencies: { "./b": b },
				module: "ts",
			});
			const graph = await tg.graph({ nodes: [
				{ kind: "directory", entries: { a: 1 } },
				{
					kind: "file",
					contents: "import a from \"./a\";",
					dependencies: { "./a": a },
					module: "ts",
				},
			] });
			const symlink = await tg.symlink({ artifact: a });
			const leaf = await tg.directory({ z: symlink });
			const directory = await tg.directory({ children: [
				{ directory: { graph, index: 0, kind: "directory" }, count: 1, last: "a" },
				{ directory: leaf, count: 1, last: "z" },
			] });
			await directory.store();
			return {
				children: [graph.id, leaf.id, symlink.id, a.id, b.id],
				directory: directory.id,
			};
		}
	'
}
let case = tg --token $alice.token build $fixture | from json
tg --token $alice.token index
let metadata = tg --token $alice.token metadata $case.directory | from json
assert equal $metadata.subtree.solvable true
assert equal $metadata.subtree.solved false

# Any descendant authorization search would block the traversal or leave a recorded checkpoint hit.
let watches = $case.children | each { |id|
	let params = { resource: $id } | to json --raw
	tg --token $root_token checkpoint watch authorization.index --params $params | from json | get watch
}
let dependencies = [$case.directory] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } tokens)
}
let path = $directory | path join input
let job = job spawn {
	let job_id = job id
	let output = checkin-output $server $path --token $alice.token
	$output | job send --tag $job_id 0
}
let output = job recv --tag $job --timeout 15sec
assert equal $output.permissions [object_subtree]
for watch in $watches {
	let hit = timeout 1s tg --token $root_token checkpoint wait authorization.index $watch 0 | complete
	assert equal $hit.exit_code 124 "neither prefetch nor solver reads should search the index for descendants."
	tg --token $root_token checkpoint unwatch authorization.index $watch
}
let object = tg --token $alice.token get --blobs --depth inf $output.reference
assert ($object | str contains "replacement") "the descendant dependency should be solved."
