use ../lib/test.nu *

# Indexing with --capture returns only after a tag's permissions are captured, so another user's access outlives the delegation.

let root_token = random chars
let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	indexer: { permission_capture: { delegation_time_to_live: 60 } }
	object: { permission_time_to_live: 60 }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let object = tg --token $alice.token put --no-tokens 'tg.file("captured")' | referent node
tg --token $alice.token tag put captured $object --public
tg --token $root_token index --capture

# Expire every delegation and source proof, so that only the captured permission grants access.
advance_time $local 2min
tg --token $root_token clean
success (tg --token $bob.token get --bytes $object | complete) "the captured permission must grant access to the object"
