use ../lib/test.nu *

# Internal and external directory checkouts retain exact permanent tokens on roots and nested directories.

let server = server spawn --config { vfs: false }
let nested = tg put --no-tokens 'tg.directory({})' | referent node
let directory = tg put --no-tokens 'tg.directory({ "nested": tg.directory({}) })' | referent node

let internal = tg checkout $directory | str trim
let external = (mktemp --directory) | path join checkout
tg checkout $directory --path $external | ignore

for path in [$internal $external] {
	for entry in [
		{ path: $path, id: $directory },
		{ path: ($path | path join nested), id: $nested },
	] {
		let token = xattr_read user.tangram.token $entry.path
		let body = $token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
		assert equal $body.resource $entry.id
		assert equal $body.permissions [object_subtree]
		assert equal $body.expires_at 9223372036854775807
	}
}

# Reusing the physical checkout preserves the token without rewriting the read-only directory.
let token = xattr_read user.tangram.token $internal
let reused = tg checkout $directory | str trim
assert equal $reused $internal
assert equal (xattr_read user.tangram.token $reused) $token
