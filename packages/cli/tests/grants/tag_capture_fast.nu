use ../lib/test.nu *

# Tagging with an exact subtree token confers permanent access to the target subtree that outlives the token.

let root_token = random chars
let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	indexer: { permission_capture: { delegation_time_to_live: 60 } }
	object: { permission_time_to_live: 60 }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let directory = tg --token $alice.token put --no-tokens 'tg.directory({"file":tg.file("fast")})' | referent node
tg --token $root_token index
let object = http get --headers { Accept: application/json, Authorization: $'Bearer ($alice.token)' } --unix-socket $socket $'http://localhost/objects/($directory)'
let token = $object.tokens.local.0
let reference = $'($directory)?tokens[local][0]=($token | url encode --all)'

let tagged = tg --token $alice.token tag put fast $reference --public | complete
success $tagged "tagging with an exact subtree proof must succeed."
tg --token $root_token index

# Access must survive the expiration of the token.
advance_time $local 2min
tg --token $root_token clean
success (tg --token $bob.token get $directory --depth inf | complete) "the tag must preserve access to the entire subtree."
