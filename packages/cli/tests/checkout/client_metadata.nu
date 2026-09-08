use ../../test.nu *
use ../lib/vfs.nu

# A fresh client recovers authorization from a real checkout and loads a dependency object when the bare-ID authorization search exhausts.
# The same test runs against regular checkouts, FSKit on macOS, and FUSE on Linux with --vfs.

const repository_path = path self '../../../..'

let build = cargo build --quiet --manifest-path ($repository_path | path join Cargo.toml) --package tangram_client --example checkout_metadata | complete
success $build 'the checkout metadata example should build'
let reader = $repository_path | path join target debug examples checkout_metadata

let server = server spawn --preserve-keys --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let dependency = tg --token $alice.token put 'tg.file("private dependency")' | str trim
let source = 'tg.file({"contents":"wrapper","dependencies":{"DEPENDENCY":{"node":DEPENDENCY}}})'
	| str replace --all DEPENDENCY $dependency
let file = tg --token $alice.token put $source | str trim
let objects = { dependency: $dependency, file: $file }
# The mount can access the wrapper, while the dependency has no direct public grant.
tg --token $alice.token grant public object_subtree $file | ignore
for name in [one two] {
	let source = 'tg.directory({"NAME":DEPENDENCY})'
		| str replace NAME $name
		| str replace DEPENDENCY $dependency
	tg --token $alice.token put $source | ignore
}
tg --token $alice.token index
let path = tg --token $alice.token checkout $objects.file | str trim
if (($env.TANGRAM_TEST_VFS? | default '') | str length) > 0 {
	vfs assert_mounted $server.directory
	assert equal ($path | path expand) (vfs root $server.directory $objects.file)
}
let link = (mktemp -d) | path join wrapper
ln -s $path $link

# The Rust process has no client initialized and cannot reach a Tangram API endpoint.
let output = with-env { TANGRAM_URL: 'http+unix://%2Fnonexistent-checkout-reader.sock' } {
	^$reader $link | complete
}
success $output 'the client should recover metadata through the checkout symlink'
let metadata = $output.stdout | from json
assert equal ($metadata.dependencies | length) 1
assert not ($metadata.token | is-empty) 'the file token must be retained separately'

# Restrict the search to one edge so a retained parent token works but unrelated parents exhaust it.
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
let output = tg --token $bob.token get $objects.dependency | complete
failure $output 'Bob must not load the bare dependency ID'
snapshot --normalize $output.stderr '
	error an error occurred
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to get the object
	   id = fil_01feh1m5xszrzz642vac8kh2ttcb54rj1jgctwpj7p1y12rwetrmf0
	-> the authorization search exhausted
	   authorization_search_exhausted = true

'

let reference = $metadata.dependencies | first
let params = $'http://localhost/($reference)' | url parse | get params
let reference = if ($params | where key == 'tokens[local]' | is-empty) {
	# FSKit may retain authorization on the parent file rather than on each reference.
	$'($reference)?tokens[local]=($metadata.token | url encode --all)'
} else {
	$reference
}
let output = tg --token $bob.token get $reference | complete
success $output 'the recovered dependency reference must authorize the object load'
snapshot $output.stdout '
	tg.file({"contents":blb_0127zqzx4qqj6wvq150k0vssrchztd59hp0qz1gcygn4cz84px3vv0})

'

# The separately returned file token also authorizes the wrapper itself.
let reference = $'($objects.file)?tokens[local]=($metadata.token | url encode --all)'
let output = tg --token $bob.token get $reference | complete
success $output 'the recovered file token must authorize the wrapper object load'
snapshot --normalize $output.stdout '
	tg.file({"contents":blb_01t8w8s8g8m93cp78g8rdc5hvhsqx8h6fke8tqx7s5ecpy808ccw10,"dependencies":{"fil_01feh1m5xszrzz642vac8kh2ttcb54rj1jgctwpj7p1y12rwetrmf0":{"node":fil_01feh1m5xszrzz642vac8kh2ttcb54rj1jgctwpj7p1y12rwetrmf0}}})

'
