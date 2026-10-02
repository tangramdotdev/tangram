use ../lib/test.nu *

# Eagerly pushing a built file object to a remote makes the object and its metadata identical on the local and remote servers.

# Create a remote server.
let remote = server spawn --cloud --name remote

# Create a local server.
let local = server spawn --name local

# Add the remote to the local server.
let output = tg remote put default $remote.url | complete
success $output

let path = artifact {
	tangram.ts: '
		export default function () {
			return tg.file("Hello, World!")
		}
	'
}

# Build the module.
let id = tg build --no-tokens $path

# Push the object.
let output = tg --no-quiet push $id --eager | complete
success $output
assert equal ($output.stdout | referent node) $id
let local_tokens = $output.stdout | referent tokens local
let remote_tokens = $output.stdout | referent tokens remote
assert not ($local_tokens | is-empty)
assert ($remote_tokens | any {|token| ($token | token body | get permissions) == [object_subtree] })
for token in $local_tokens {
	let body = $token | token body
	assert equal $body.resource $id
	assert equal $body.permissions [object_subtree]
}
assert ($output.stderr | lines | any {|line| ($line starts-with $'info ($id)') and ($line | str contains 'tokens[remote]') })

# Quiet pushes suppress the header while preserving the final authorization referent.
let quiet = tg --quiet push $id --eager | complete
success $quiet
assert equal ($quiet.stdout | referent node) $id
assert not ($quiet.stdout | referent tokens remote | is-empty)
assert not ($quiet.stderr | str contains 'tokens[')

# Disabling tokens affects both the header and the final output.
let without_tokens = tg --no-quiet push --no-tokens $id --eager | complete
success $without_tokens
assert equal ($without_tokens.stdout | str trim) $id
assert not ($without_tokens.stderr | str contains 'tokens[')

# Confirm the object is on the remote and the same.
let local_object = tg get $id --blobs --depth=inf --no-tokens --pretty
let remote_object = tg --url $remote.url get $id --blobs --depth=inf --no-tokens --pretty
assert equal $local_object $remote_object

# Index.
tg index
tg --url $remote.url index

# Get the metadata.
let local_metadata = tg object metadata $id --pretty
let remote_metadata = tg --url $remote.url object metadata $id --pretty

assert equal $local_metadata $remote_metadata
