use ../../test.nu *

# Dependency tokens recovered from a reused checkout should authorize object storage after restart.

for preserve_keys in [true false] {
	# A zero-edge budget makes a tiny fixture reproduce the fallback search exhaustion.
	let root_token = random chars
	let server = server spawn --preserve-keys=$preserve_keys --config {
		authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
		authorization: {
			final: {
				ancestor: { max_depth: 0, max_edges: 0, max_nodes: 0 }
				descendant: { max_depth: 0, max_edges: 0, max_nodes: 0 }
				subtree: { max_objects: 0 }
			}
			index: { delay: null }
			initial: false
		}
		vfs: false
	}
	let bob = tg --token $root_token login --verbose --name bob | from json
	let user = $bob.user.id
	let library = tg --token $root_token put 'tg.file("library")' | str trim
	let source = 'tg.file({"contents":"wrapper","dependencies":{"LIBRARY":{"node":LIBRARY}}})'
		| str replace --all LIBRARY $library
	let wrapper = tg --token $root_token put $source | str trim
	tg --token $root_token grant $user object_subtree $wrapper | ignore
	tg --token $root_token index

	# Recover the dependency referent directly from a checkout, as a wrapper consumer would.
	let path = tg --token $root_token checkout $wrapper | str trim
	let reference = xattr_read user.tangram.dependencies $path | from json | first
	let token = $'http://localhost/($reference)' | url parse | get params
		| where key == 'tokens[local][authorization][0]' | first | get value
	let body = $token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
	assert equal $body.resource $library
	assert equal $body.permissions [object_subtree]
	assert equal $body.expires_at 9223372036854775807

	# Store a new parent with the recovered child token before the restart.
	let source = 'tg.file({"contents":"before restart","dependencies":{"library":{"node":REFERENCE}}})'
		| str replace REFERENCE $reference
	let output = tg --token $bob.token put $source | complete
	success $output 'the checkout dependency token should authorize the object batch before restart'

	let server = server restart $server
	let warm_path = tg --token $root_token checkout $wrapper | str trim
	assert equal $warm_path $path
	let warm_reference = xattr_read user.tangram.dependencies $warm_path | from json | first
	assert equal $warm_reference $reference

	# Store another parent with the dependency token recovered from the reused checkout.
	let source = 'tg.file({"contents":"after restart","dependencies":{"library":{"node":REFERENCE}}})'
		| str replace REFERENCE $warm_reference
	let warm_output = tg --token $bob.token put $source | complete

	# A separately materialized file receives a current token for the same dependency.
	let fresh_path = (mktemp --directory) | path join library
	tg --token $root_token checkout --dependencies=false --path $fresh_path $library | ignore
	let fresh_token = xattr_read user.tangram.token $fresh_path
	let query = { 'tokens[local][authorization][0]': $fresh_token } | url build-query
	let fresh_reference = $'($library)?($query)'
	let source = 'tg.file({"contents":"after restart","dependencies":{"library":{"node":REFERENCE}}})'
		| str replace REFERENCE $fresh_reference
	let output = tg --token $bob.token put $source | complete
	success $output 'a fresh token should authorize the same object batch without changing the search budget'

	server stop $server
	success $warm_output 'a dependency token recovered from the reused checkout should authorize the object batch after restart'
}
