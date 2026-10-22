use ../lib/test.nu *

# Store subpaths retain their root, and their references grant another user read access to both the artifact and its root.

let root_token = random chars
let local = server spawn --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	remotes: {}
	vfs: false
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let eve = tg login --verbose --name eve | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let directory = tg --token $alice.token put --no-tokens 'tg.directory({
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
})' | referent node
tg --token $alice.token index
let first = http get --headers { Accept: 'application/json', Authorization: $'Bearer ($alice.token)' } --unix-socket $socket $'http://localhost/objects/($directory)'
let proof = $first.tokens.local.0

# Canonicalize physical parents, then use the root proof to resolve and authorize the artifact.
let root = tg --token $alice.token checkout $directory | str trim
chmod u+w $root
xattr_write user.tangram.token $proof $root
let alias = mktemp --directory
ln -s $root ($alias | path join root)
ln -s $root $'($root).tg.ts'
mut resolved = ''
for case in [
	{ input: $'($root)/bin/program', path: 'bin/program' },
	{ input: $'($root)/bin/../bin/program', path: 'bin/program' },
	{ input: $'($root)/link/program', path: 'bin/program' },
	{ input: $'($alias)/root/bin/program', path: 'bin/program' },
	{ input: $'($root)/final_link', path: 'final_link' },
] {
	let reference = tg --token $bob.token checkin $case.input | str trim
	let artifact = $reference | referent node
	let uri = $'http://localhost/($reference)' | url parse
	assert equal ($uri.params | where key == id | first | get value) $directory
	assert equal ($uri.params | where key == path | first | get value) $case.path

	# The reference's tokens grant another user read access to both the artifact and its root.
	let tokens = $uri.params | where key starts-with 'tokens[local]' | each { |param| $'($param.key)=($param.value | url encode --all)' } | str join '&'
	for id in [$artifact $directory] {
		success (tg --token $eve.token get --depth=inf $'($id)?($tokens)' | complete)
	}
	if $resolved != '' {
		assert equal $artifact $resolved
	}
	$resolved = $artifact
}

# The root itself has no containing root or subpath, and its reference grants another user read access to it.
for path in [$root $'($root).tg.ts'] {
	let reference = tg --token $bob.token checkin $path | str trim
	assert equal ($reference | referent node) $directory
	let params = $'http://localhost/($reference)' | url parse | get params
	assert ($params | where key in [id path] | is-empty)
	success (tg --token $eve.token get --depth=inf $reference | complete)
}

# Following an intermediate artifact symlink changes the containing root.
let target = tg --token $root_token put --no-tokens 'tg.directory({ "program": tg.file("contents") })' | referent node
let target_parent = tg --token $root_token put --no-tokens 'tg.directory({ "bin": tg.directory({ "program": tg.file("contents") }) })' | referent node
for case in [
	{ input: 'artifact_link/program', id: $target, path: 'program' },
	{ input: 'artifact_path_link/program', id: $target_parent, path: 'bin/program' },
] {
	let reference = tg --token $alice.token checkin $'($root)/($case.input)' | str trim
	let params = $'http://localhost/($reference)' | url parse | get params
	assert equal ($reference | referent node) $resolved
	assert equal ($params | where key == id | first | get value) $case.id
	assert equal ($params | where key == path | first | get value) $case.path
}

# A token for a child cannot authorize its parent, and invalid signatures grant nothing.
let child = http get --headers { Accept: 'application/json', Authorization: $'Bearer ($root_token)' } --unix-socket $socket $'http://localhost/objects/($resolved)'
let wrong = $child.tokens.local.0
xattr_write user.tangram.token $wrong $root
failure (tg --token $bob.token checkin --no-tokens $root | complete)
let parts = $proof | split row '.'
let signature = $parts.3 | decode base64 | bytes reverse | encode base64
let forged = [$parts.0 $parts.1 $parts.2 $signature] | str join '.'
xattr_write user.tangram.token $forged $root
failure (tg --token $bob.token checkin --no-tokens $root | complete)

# Normal authorization still works when the xattr does not provide a valid proof.
success (tg --token $alice.token checkin --no-tokens $'($root)/bin/program' | complete)
xattr_write user.tangram.token $proof $root
success (tg --token $bob.token checkin --no-tokens $'($root)/../($directory)/bin/program' | complete)
failure (tg --token $bob.token checkin --no-tokens $'($root)/missing/program' | complete)
failure (tg --token $bob.token checkin --no-tokens $'($root)/external/program' | complete)
failure (tg --token $bob.token checkin --no-tokens $'($root)/cycle/program' | complete)
failure (tg --token $bob.token checkin --no-tokens $'($root)/cycle' | complete)
