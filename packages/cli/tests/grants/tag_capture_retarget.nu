use ../lib/test.nu *

# A capture write prepared for an old version must do nothing after a tag changes targets.

let root_token = random chars
let local = server spawn --config {
	advanced: { checkpoints: true }
	database: { kind: sqlite, path: "database.sqlite3" }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let missing = 'fil_010000000000000000000000000000000000000000000000000000'
let expression = 'tg.file({"contents":"old","dependencies":{"missing":{"node":MISSING}}})'
	| str replace MISSING $missing
let old = tg --token $alice.token put $expression | str trim
let replacement = tg --token $alice.token put 'tg.file("new")' | str trim
tg --token $root_token index

let params = { resource: $old } | to json --raw
let advance = tg --token $root_token checkpoint watch permission_capture.advance --params $params | from json | get watch
let advanced = tg --token $root_token checkpoint watch permission_capture.advanced --params $params | from json | get watch
tg --token $alice.token tag put race $old --public
tg --token $root_token index
let hit = timeout 30s tg --token $root_token checkpoint wait permission_capture.advance $advance 0 | from json
let before = tg --token $alice.token tag get race | from json
assert equal $hit.params.tag $before.id
let database = $local.directory | path join database.sqlite3
let before_version = open $database | query db "select version from tags" | get version.0
assert equal $hit.params.version $before_version

# Replace the target while the old job has already computed the permissions it wants to write.
tg --token $alice.token tag put --force race $replacement --public
tg --token $root_token index
let after = tg --token $alice.token tag get race | from json
assert equal $after.id $before.id
let after_version = open $database | query db "select version from tags" | get version.0
assert ($after_version != $before_version)
assert equal $after.target.id $replacement
failure (tg --token $bob.token get $old | complete) "retargeting must clear the old delegation and permissions."

tg --token $root_token checkpoint continue permission_capture.advance $advance 0
timeout 30s tg --token $root_token checkpoint wait permission_capture.advanced $advanced 0 | ignore
tg --token $root_token checkpoint unwatch permission_capture.advance $advance
tg --token $root_token checkpoint unwatch permission_capture.advanced $advanced

failure (tg --token $bob.token get $old | complete) "the old capture must not restore permission on the old target."
success (tg --token $bob.token get $replacement | complete) "the current tag must confer access to its replacement target."
