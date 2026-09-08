use ../../test.nu *
use ../lib/vfs.nu

# The Rust client recovers checkout tokens to load unrendered file and directory paths.

const repository_path = path self '../../../..'

let server = server spawn --preserve-keys --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let file = tg --token $alice.token put 'tg.file("library")' | str trim
let source = 'tg.directory({"lib":tg.directory({"libexample.so":FILE})})' | str replace FILE $file
let directory = tg --token $alice.token put $source | str trim
let source = 'tg.file({"contents":"wrapper","dependencies":{"DIRECTORY":{"node":DIRECTORY}}})'
	| str replace --all DIRECTORY $directory
let wrapper = tg --token $alice.token put $source | str trim

# Allow the VFS provider to access the checkouts.
tg --token $alice.token grant public object_subtree $wrapper | ignore
for name in [one two] {
	let source = 'tg.directory({"NAME":DIRECTORY})'
		| str replace NAME $name
		| str replace DIRECTORY $directory
	tg --token $alice.token put $source | ignore
}
tg --token $alice.token index
let wrapper_path = tg --token $alice.token checkout $wrapper | str trim
let directory_path = tg --token $alice.token checkout $directory | str trim
let file_path = tg --token $alice.token checkout $file | str trim
if (($env.TANGRAM_TEST_VFS? | default '') | str length) > 0 {
	vfs assert_mounted $server.directory
	assert equal ($directory_path | path expand) (vfs root $server.directory $directory)
}

# Exercise the capacity fallback that retains only the file token.
let fallback = artifact (file --xattrs {
	'user.tangram.dependencies': ([$directory] | to json --raw)
	'user.tangram.token': (xattr_read user.tangram.token $wrapper_path)
} wrapper)

# A parent token needs one edge, while an unguided search exhausts the budget.
let config = $server.config | merge deep {
	authorization: {
		final: {
			ancestor: { max_depth: 1, max_edges: 1, max_nodes: 2 }
			descendant: { max_depth: 0, max_edges: 0, max_nodes: 0 }
			subtree: { max_objects: 0 }
		}
		index: { delay: null }
		initial: false
	}
}
$config | to json | save --force $server.config_path
let server = server restart ($server | upsert config $config)
let output = tg --token $bob.token get $directory | complete
failure $output 'the bare directory ID must exhaust the authorization search'
assert ($output.stderr | str contains 'authorization_search_exhausted = true')

let fixture = {
	directory: $directory_path
	file: $file
	file_path: $file_path
	wrapper: $wrapper_path
	wrapper_without_dependency_tokens: $fallback
}
let output = with-env { TANGRAM_TEST_CHECKOUT: ($fixture | to json), TANGRAM_TOKEN: $bob.token } {
	cargo test --quiet --manifest-path ($repository_path | path join Cargo.toml) --package tangram_client --test checkout -- --ignored | complete
}
success $output 'the client should load the unrendered paths using checkout tokens'
