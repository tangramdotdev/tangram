use ../lib/test.nu *
use ../lib/checkin.nu checkin-output

# A dependency on an artifact in a graph that is split into leaves and a branch receives subtree permission from a trusted remote and node permission from an untrusted remote, as an artifact in an unsplit graph does.

let remote = server spawn --name remote --config { remotes: {} }

# Create a package with 2,000 files that is in a cycle with another package, so that its graph is split into leaves and a branch.
let files = 0..<2_000 | each { |i| { name: $'file_($i | into string | fill --alignment right --character "0" --width 4).txt', contents: $'File ($i)' } } | transpose --header-row --as-record
let dependency_path = artifact {
	a: ($files | merge {
		tangram.ts: '
			import b from "b" with { source: "../b" };
			export default b;
		'
	})
	b: {
		tangram.ts: '
			import a from "a" with { source: "../a" };
			export default a;
		'
	}
}
let dependency = tg --url $remote.url checkin --no-tokens --no-lock ($dependency_path | path join a) | referent node
tg --url $remote.url index

# The dependency is a pointer into a branched graph.
let object = tg --url $remote.url object get --no-tokens $dependency
let graph = $object | parse --regex '(?<graph>gph_[0-9a-z]+)' | get graph | first
let graph_object = tg --url $remote.url object get --no-tokens $graph
assert ($graph_object | str contains '"children"') 'the dependency graph must be a branch'

def dependency_file [dependency: string, contents: string] {
	let dependencies = [$dependency] | to json
	let directory = artifact {
		input: (file --xattrs { "user.tangram.dependencies": $dependencies } $contents)
	}
	$directory | path join input
}

# A trusted remote proves subtree permission.
let remote_trusted = server spawn --name remote-trusted --config {
	remotes: { default: { trusted: true, url: $remote.url } }
}
let path = dependency_file $dependency trusted
let output = checkin-output $remote_trusted $path
assert equal $output.permissions [object_subtree] "a trusted remote token should produce a subtree checkin token"

# An untrusted remote cannot prove local subtree permission.
let remote_untrusted = server spawn --name remote-untrusted --config {
	remotes: { default: { url: $remote.url } }
}
let path = dependency_file $dependency untrusted
let output = checkin-output $remote_untrusted $path
assert equal $output.permissions [object_node] "an untrusted remote token should produce a node-only checkin token"
