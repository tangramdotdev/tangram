use ../lib/test.nu *
use ../lib/checkin.nu checkin-output

# Store subpaths retain their root and carry exact tokens for both artifacts.

def token-body [token: string] {
	$token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
}

let root_token = random chars
let server = server spawn --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	object: { grant_time_to_live: 60 }
	remotes: {}
	vfs: false
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let socket = $server.url | str replace 'http+unix://' '' | url decode
let directory = tg --token $alice.token put 'tg.directory({
	"artifact_link": tg.symlink({ "artifact": tg.directory({ "program": tg.file("contents") }) }),
	"artifact_path_link": tg.symlink({
		"artifact": tg.directory({ "bin": tg.directory({ "program": tg.file("contents") }) }),
		"path": "bin"
	}),
	"bin": tg.directory({ "program": tg.file("contents") }),
	"cycle": tg.symlink({ "path": "cycle" }),
	"external": tg.symlink({ "path": "../outside" }),
	"final_link": tg.symlink({ "path": "artifact_link/program" }),
	"link": tg.symlink({ "path": "bin" })
})' | str trim
tg --token $alice.token index
let first = http get --headers { Accept: 'application/json', Authorization: $'Bearer ($alice.token)' } --unix-socket $socket $'http://localhost/objects/($directory)'
let proof = $first.tokens.local.0
let expiration = (token-body $proof).expires_at

# Canonicalize physical parents, then use the root proof to resolve and authorize the artifact.
let root = tg --token $alice.token checkout $directory | str trim
chmod u+w $root
xattr_write user.tangram.token $proof $root
let alias = mktemp --directory
ln -s $root ($alias | path join root)
ln -s $root $'($root).tg.ts'
sleep 1sec
let watch = tg --token $root_token checkpoint watch authorization.index | from json | get watch
mut resolved = ''
for case in [
	{ input: $'($root)/bin/program', path: 'bin/program' },
	{ input: $'($root)/bin/../bin/program', path: 'bin/program' },
	{ input: $'($root)/link/program', path: 'bin/program' },
	{ input: $'($alias)/root/bin/program', path: 'bin/program' },
	{ input: $'($root)/final_link', path: 'final_link' },
] {
	let output = checkin-output $server $case.input --token $bob.token
	let uri = $'http://localhost/($output.reference)' | url parse
	assert equal ($uri.params | where key == id | first | get value) $directory
	assert equal ($uri.params | where key == path | first | get value) $case.path
	let tokens = $uri.params | where key starts-with 'tokens[local]' | get value
	assert equal ($tokens | length) 2
	assert equal ($tokens | each { token-body $in | get resource } | sort) ([$output.artifact $directory] | sort)
	for token in $tokens {
		let body = token-body $token
		assert equal $body.permissions [object_subtree]
		assert equal $body.expires_at $expiration
		let reference = $'($body.resource)?tokens[local][0]=($token | url encode --all)'
		success (tg --token $bob.token object get --bytes $reference | complete)
	}
	if $resolved != '' {
		assert equal $output.artifact $resolved
	}
	$resolved = $output.artifact
}

# The root itself has no containing root or subpath and needs only one token.
for path in [$root $'($root).tg.ts'] {
	let output = checkin-output $server $path --token $bob.token
	assert equal $output.artifact $directory
	let params = $'http://localhost/($output.reference)' | url parse | get params
	assert ($params | where key in [id path] | is-empty)
	assert equal ($params | where key starts-with 'tokens[local]' | length) 1
}
tg --token $root_token checkpoint unwatch authorization.index $watch

# Following an intermediate artifact symlink changes the containing root.
let target = tg --token $root_token put 'tg.directory({ "program": tg.file("contents") })' | str trim
let target_parent = tg --token $root_token put 'tg.directory({ "bin": tg.directory({ "program": tg.file("contents") }) })' | str trim
for case in [
	{ input: 'artifact_link/program', id: $target, path: 'program' },
	{ input: 'artifact_path_link/program', id: $target_parent, path: 'bin/program' },
] {
	let output = checkin-output $server $'($root)/($case.input)' --token $alice.token
	let params = $'http://localhost/($output.reference)' | url parse | get params
	assert equal $output.artifact $resolved
	assert equal ($params | where key == id | first | get value) $case.id
	assert equal ($params | where key == path | first | get value) $case.path
}

# A token for a child cannot authorize its parent, and invalid signatures grant nothing.
let child = http get --headers { Accept: 'application/json', Authorization: $'Bearer ($root_token)' } --unix-socket $socket $'http://localhost/objects/($resolved)'
let wrong = $child.tokens.local.0
xattr_write user.tangram.token $wrong $root
failure (tg --token $bob.token checkin $root | complete)
let parts = $proof | split row '.'
let signature = $parts.3 | decode base64 | bytes reverse | encode base64
let forged = [$parts.0 $parts.1 $parts.2 $signature] | str join '.'
xattr_write user.tangram.token $forged $root
failure (tg --token $bob.token checkin $root | complete)

# Normal authorization still works when the xattr does not provide a valid proof.
success (tg --token $alice.token checkin $'($root)/bin/program' | complete)
xattr_write user.tangram.token $proof $root
success (tg --token $bob.token checkin $'($root)/../($directory)/bin/program' | complete)
failure (tg --token $bob.token checkin $'($root)/missing/program' | complete)
failure (tg --token $bob.token checkin $'($root)/external/program' | complete)
failure (tg --token $bob.token checkin $'($root)/cycle/program' | complete)
failure (tg --token $bob.token checkin $'($root)/cycle' | complete)
