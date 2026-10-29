use ../../util/test.nu *

# Solving a dependency resolves a descendant dependency reached through branches, graphs, symlinks, and dependencies.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let replacement = tg --token $alice.token put --no-tokens 'tg.file("replacement")' | referent node
tg --token $alice.token tag -p replacement/1.0.0 $replacement
let fixture = artifact {
	tangram.ts: '
		export default async function () {
			const b = await tg.file({
				contents: "import replacement from \"replacement/^1\";",
				dependencies: { "replacement/^1": null },
				module: "typescript",
			});
			const a = await tg.file({
				contents: "import b from \"./b\";",
				dependencies: { "./b": b },
				module: "typescript",
			});
			const graph = await tg.graph({ nodes: [
				{ kind: "directory", entries: { a: 1 } },
				{
					kind: "file",
					contents: "import a from \"./a\";",
					dependencies: { "./a": a },
					module: "typescript",
				},
			] });
			const symlink = await tg.symlink({ artifact: a });
			const leaf = await tg.directory({ z: symlink });
			const directory = await tg.directory({ children: [
				{ directory: { graph, index: 0, kind: "directory" }, count: 1, last: "a" },
				{ directory: leaf, count: 1, last: "z" },
			] });
			await directory.store();
			return directory.id;
		}
	'
}
let directory_id = tg --token $alice.token build $fixture | from json
tg --token $alice.token index
let dependencies = [$directory_id] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } tokens)
}
let path = $directory | path join input
let reference = tg --token $alice.token checkin --no-lock --root $path | str trim
let object = tg --token $alice.token get --blobs --depth inf $reference
assert ($object | str contains "replacement") "the descendant dependency should be solved."
