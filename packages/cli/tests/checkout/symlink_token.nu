use ../lib/test.nu *

# macOS checkouts retain exact authorization tokens on symlinks without following their targets.

if $nu.os-info.name != 'macos' {
	skip_test 'this test requires macOS symlink xattrs'
}

let server = server spawn --config { vfs: false }
let symlink = tg put --no-tokens 'tg.symlink({ "path": "missing" })' | referent node
let directory = tg put --no-tokens 'tg.directory({ "link": tg.symlink({ "path": "missing" }) })' | referent node

let internal = tg checkout $symlink | str trim
let internal_directory = tg checkout $directory | str trim
let external = (mktemp --directory) | path join link
tg checkout $symlink --path $external | ignore
let external_directory = (mktemp --directory) | path join directory
tg checkout $directory --path $external_directory | ignore

for path in [
	$internal,
	($internal_directory | path join link),
	$external,
	($external_directory | path join link),
] {
	let token = xattr -s -p user.tangram.token $path | str trim
	let body = $token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
	assert equal $body.resource $symlink
	assert equal $body.permissions [object_subtree]
	assert equal $body.expires_at 9223372036854775807
}

let token = xattr -s -p user.tangram.token $internal | str trim
let reused = tg checkout $symlink | str trim
assert equal $reused $internal
assert equal (xattr -s -p user.tangram.token $reused | str trim) $token
