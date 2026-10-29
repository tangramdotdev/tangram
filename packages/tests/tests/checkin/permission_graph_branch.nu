use ../../lib/test.nu *

# A dependency on an artifact in a graph that is split into leaves and a branch is readable by another user through the checked-in file's reference with a trusted remote but not with an untrusted remote, as an artifact in an unsplit graph is.

let root_token = random chars
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

# A trusted remote lets the reference grant the dependency.
let remote_trusted = server spawn --name remote-trusted --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	remotes: { default: { trusted: true, url: $remote.url } }
}
let eve = tg --url $remote_trusted.url login --verbose --name eve | from json
let path = dependency_file $dependency trusted
let reference = tg --url $remote_trusted.url --token $root_token checkin --no-lock --root $path | str trim

# Store the dependency locally, then read it as another user through the reference.
tg --url $remote_trusted.url --token $root_token get --depth=inf $reference | ignore
success (tg --url $remote_trusted.url --token $eve.token get --depth=inf $reference | complete) "a trusted remote should make the reference grant the dependency"

# An untrusted remote does not let the reference grant the dependency.
let remote_untrusted = server spawn --name remote-untrusted --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	remotes: { default: { url: $remote.url } }
}
let eve = tg --url $remote_untrusted.url login --verbose --name eve | from json
let path = dependency_file $dependency untrusted
let reference = tg --url $remote_untrusted.url --token $root_token checkin --no-lock --root $path | str trim

# Store the dependency locally, then read it as another user through the reference.
tg --url $remote_untrusted.url --token $root_token get --depth=inf $reference | ignore
success (tg --url $remote_untrusted.url --token $eve.token get $reference | complete) "an untrusted remote should make the reference grant the checked in file"
failure (tg --url $remote_untrusted.url --token $eve.token get --depth=inf $reference | complete) "an untrusted remote should not make the reference grant the dependency"
