use ../../test.nu *
use ../lib/checkin.nu checkin-output

# A trusted remote localizes its permission token, while an untrusted remote does not.

let remote = server spawn --name remote --config { remotes: {} }
let dependency_path = artifact { value: remote }
let dependency = tg --url $remote.url checkin --no-lock --root $dependency_path | str trim
let remote_metadata = tg --url $remote.url metadata $dependency | from json
assert ($remote_metadata.subtree.count > 1) "the remote fixture should have subtree permission"

def dependency_file [dependency: string, contents: string] {
	let dependencies = [$dependency] | to json
	let directory = artifact {
		input: (file --xattrs { "user.tangram.dependencies": $dependencies } $contents)
	}
	$directory | path join input
}

# A trusted remote proves subtree permission from the fetched root token without fetching descendants for permission discovery.
let trusted = server spawn --name trusted --config {
	remotes: { default: { trusted: true, url: $remote.url } }
}
let path = dependency_file $dependency trusted
let output = checkin-output $trusted $path
assert equal $output.permissions [object_subtree] "a trusted remote token should produce a subtree checkin token"

# An untrusted remote can provide the dependency bytes but cannot prove local subtree permission.
let untrusted = server spawn --name untrusted --config {
	remotes: { default: { url: $remote.url } }
}
let path = dependency_file $dependency untrusted
let output = checkin-output $untrusted $path
assert equal $output.permissions [object_node] "an untrusted remote token should produce a node-only checkin token"
