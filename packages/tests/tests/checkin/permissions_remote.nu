use ../../util/test.nu *

# A trusted remote lets a checked-in file's reference grant another user read access to its dependency, while an untrusted remote does not.

let root_token = random chars
let remote = server spawn --name remote --config { remotes: {} }
let dependency_path = artifact { value: remote }
let dependency = tg --url $remote.url checkin --no-tokens --no-lock --root $dependency_path | referent node
tg --url $remote.url index
let remote_metadata = tg --url $remote.url metadata $dependency | from json
assert ($remote_metadata.subtree.count > 1) "the remote fixture should have subtree permission"

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

# An untrusted remote can provide the dependency bytes, but the reference does not grant the dependency.
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
