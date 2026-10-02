use ../lib/test.nu *

# An exact subtree token writes permanent tag permissions without searching the target or creating capture work.

def token-body [token: string] {
	$token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
}

let root_token = random chars
let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	indexer: { permission_capture: { delegation_time_to_live: 60, poll_interval: 0.05 } }
	object: { permission_time_to_live: 60 }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let directory = tg --token $alice.token put --no-tokens 'tg.directory({"file":tg.file("fast")})' | referent node
tg --token $root_token index
let object = http get --headers { Accept: application/json, Authorization: $'Bearer ($alice.token)' } --unix-socket $socket $'http://localhost/objects/($directory)'
let token = $object.tokens.local.0
assert equal (token-body $token).resource $directory
assert ('object_subtree' in (token-body $token).permissions)
let reference = $'($directory)?tokens[local][0]=($token | url encode --all)'

let params = { resource: $directory } | to json --raw
let authorization = tg --token $root_token checkpoint watch verification.index --params $params | from json | get watch
let capture = tg --token $root_token checkpoint watch permission_capture.started --params $params | from json | get watch
let tagged = timeout 10s tg --token $alice.token tag put fast $reference --public | complete
success $tagged "tagging with an exact subtree proof must not enter the target authorization search."
tg --token $root_token index

# The worker is polling; any accidental job for this root would hit and remain held here.
let unexpected = timeout 5s tg --token $root_token checkpoint wait permission_capture.started $capture 0 | complete
assert equal $unexpected.exit_code 124 "the subtree-token path must not enqueue capture."
tg --token $root_token checkpoint unwatch verification.index $authorization
tg --token $root_token checkpoint unwatch permission_capture.started $capture

# Access must survive expiration even though no background capture ran.
advance_time $local 2min
tg --token $root_token clean
success (tg --token $bob.token get $directory --depth inf | complete) "the immediate permission write must preserve the entire subtree."
