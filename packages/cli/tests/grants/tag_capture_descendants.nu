use ../lib/test.nu *

# Capture preserves node permissions on both a directory and its file without granting their unavailable dependency.

def token-body [token: string] {
	$token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
}

let root_token = random chars
let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	indexer: { permission_capture: { delegation_time_to_live: 60 } }
	object: { permission_time_to_live: 60 }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let missing = 'fil_010000000000000000000000000000000000000000000000000000'
let expression = 'tg.directory({"file":tg.file({"contents":"capture","dependencies":{"missing":{"node":MISSING}}})})'
	| str replace MISSING $missing
let directory = tg --token $alice.token put $expression | str trim
let file = tg --token $alice.token children $directory | from json | get 0 | split row '?' | first
tg --token $root_token index

# Neither stored node has a subtree proof because F2 is unavailable.
for id in [$directory $file] {
	let object = http get --headers { Accept: application/json, Authorization: $'Bearer ($alice.token)' } --unix-socket $socket $'http://localhost/objects/($id)'
	assert equal (token-body $object.tokens.local.0).permissions [object_node]
	failure (tg --token $bob.token get --bytes $id | complete) "the reader must have no access before tagging."
}
failure (tg --token $root_token get --bytes $missing | complete) "the dependency must be absent even for root."

# Pause the descendant write so completion is observed independently of tg index.
let params = { resource: $file } | to json --raw
let advance = tg --token $root_token checkpoint watch permission_capture.advance --params $params | from json | get watch
let advanced = tg --token $root_token checkpoint watch permission_capture.advanced --params $params | from json | get watch
tg --token $alice.token tag put captured $directory --public
tg --token $root_token index
let hit = timeout 30s tg --token $root_token checkpoint wait permission_capture.advance $advance 0 | from json
let tag = tg --token $alice.token tag get captured | from json
assert equal $hit.params.tag $tag.id
tg --token $root_token checkpoint continue permission_capture.advance $advance 0
timeout 30s tg --token $root_token checkpoint wait permission_capture.advanced $advanced 0 | ignore
tg --token $root_token checkpoint unwatch permission_capture.advance $advance
tg --token $root_token checkpoint unwatch permission_capture.advanced $advanced

# Expire every source proof and delegation, then use raw ids without inherited tokens.
advance_time $local 2min
tg --token $root_token clean
for id in [$directory $file] {
	success (tg --token $bob.token get --bytes $id | complete) "the tag must permanently preserve node access to each captured object."
}
failure (tg --token $bob.token get --bytes $missing | complete) "capture must not invent permission on the unavailable dependency."
failure (tg --token $bob.token get $directory --depth inf | complete) "capturing two node permissions must not create a subtree permission."
