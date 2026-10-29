use ../lib/test.nu *

# Tagging with a subtree proof that is verified only by a previous secret confers permanent access to the target subtree.

let root_token = random chars
let local = server spawn --preserve-keys --now '2026-01-01T00:00:00Z' --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	indexer: { permission_capture: { delegation_time_to_live: 60 } }
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
let reference = $'($directory)?tokens[local][0]=($token | url encode --all)'

# Keep the authorization secret only as a previous secret, so the server verifies existing tokens without signing new ones.
let secret = { algorithm: blake3, id: 0, path: ($local.directory | path join secret) }
let config = open $local.config_path | upsert authorization.tokens { secret: false, previous_secrets: [$secret] }
$config | to json | save --force $local.config_path
let server = server restart $local
failure (tg --token $bob.token get --bytes $directory | complete) "Bob must have no indexed source permission on the target."
failure (tg --token $reader.token get --bytes $directory | complete) "the reader must have no access before tagging."

let tagged = tg --token $bob.token tag put accepted $reference --public | complete
success $tagged "a subtree proof verified only by a previous secret must authorize tagging."
tg --token $root_token index

# Neither a signing secret nor an unexpired source proof is needed to read the captured subtree.
advance_time $server 2min
tg --token $root_token clean
success (tg --token $reader.token get --bytes $directory | complete) "the tag must retain its immediately captured permissions."
success (tg --token $reader.token get $directory --depth inf | complete) "the entire captured subtree must remain readable without tokens."
