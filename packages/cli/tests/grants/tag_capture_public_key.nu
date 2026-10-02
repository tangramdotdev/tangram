use ../lib/test.nu *

# An accepted subtree proof remains a capture fast path when only its public key is configured.

let root_token = random chars
let local = server spawn --preserve-keys --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	indexer: { permission_capture: { delegation_time_to_live: 60, poll_interval: 0.05 } }
	object: { permission_time_to_live: 60 }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let reader = tg login --verbose --name reader | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let directory = tg --token $alice.token put --no-tokens 'tg.directory({"file":tg.file("public key")})' | referent node
tg --token $root_token index
let object = http get --headers { Accept: application/json, Authorization: $'Bearer ($alice.token)' } --unix-socket $socket $'http://localhost/objects/($directory)'
let token = $object.tokens.local.0
let body = $token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
assert equal $body.resource $directory
assert ('object_subtree' in $body.permissions)
let reference = $'($directory)?tokens[local][0]=($token | url encode --all)'

# Retain the accepted public key and the authentication keys, but remove the authorization signer.
let config = open $local.config_path | upsert verification.tokens.private_key null
$config | to json | save --force $local.config_path
let server = server restart $local
failure (tg --token $bob.token get --bytes $directory | complete) "Bob must have no indexed source permission on the target."
failure (tg --token $reader.token get --bytes $directory | complete) "the reader must have no access before tagging."

let params = { resource: $directory } | to json --raw
let authorization = tg --token $root_token checkpoint watch verification.index --params $params | from json | get watch
let capture = tg --token $root_token checkpoint watch permission_capture.started --params $params | from json | get watch
let tagged = timeout 10s tg --token $bob.token tag put accepted $reference --public | complete
success $tagged "a public-key-only subtree proof must be captured without a target authorization search."
tg --token $root_token index
let unexpected = timeout 5s tg --token $root_token checkpoint wait permission_capture.started $capture 0 | complete
assert equal $unexpected.exit_code 124 "an accepted subtree proof must not enqueue capture."
tg --token $root_token checkpoint unwatch verification.index $authorization
tg --token $root_token checkpoint unwatch permission_capture.started $capture

# Neither a signing key nor an unexpired source proof is needed to read the captured subtree.
advance_time $server 2min
tg --token $root_token clean
success (tg --token $reader.token get --bytes $directory | complete) "the tag must retain its immediately captured permissions."
success (tg --token $reader.token get $directory --depth inf | complete) "the entire captured subtree must remain readable without tokens."
