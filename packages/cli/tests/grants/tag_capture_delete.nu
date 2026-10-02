use ../lib/test.nu *

# A capture write prepared before deletion must not restore a deleted tag's target permissions.

def token-body [token: string] {
	$token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
}

let root_token = random chars
let local = server spawn --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let missing = 'fil_010000000000000000000000000000000000000000000000000000'
let expression = 'tg.file({"contents":"deleted","dependencies":{"missing":{"node":MISSING}}})'
	| str replace MISSING $missing
let target = tg --token $alice.token put $expression | str trim
tg --token $root_token index

let params = { resource: $target } | to json --raw
let advance = tg --token $root_token checkpoint watch permission_capture.advance --params $params | from json | get watch
let advanced = tg --token $root_token checkpoint watch permission_capture.advanced --params $params | from json | get watch
tg --token $alice.token tag put deleting $target --public
tg --token $root_token index
let hit = timeout 30s tg --token $root_token checkpoint wait permission_capture.advance $advance 0 | from json
let tag = http get --headers { Accept: application/json, Authorization: $'Bearer ($bob.token)' } --unix-socket $socket 'http://localhost/tags/deleting'
let token = $tag.tokens.local.0
let body = token-body $token
assert equal $body.resource $tag.data.id
assert equal $body.permissions [tag_read]
assert equal $hit.params.tag $tag.data.id
let reference = $'($target)?tokens[local][0]=($token | url encode --all)'
success (tg --token $bob.token get $reference | complete) "the retained tag-read proof must initially authorize the delegated target."

# Retain only tag_read, so this cannot pass because of an already issued target token.
tg --token $alice.token tag delete deleting | ignore
tg --token $root_token index
failure (tg --token $bob.token get $reference | complete) "deletion must remove the tag's existing target access."
tg --token $root_token checkpoint continue permission_capture.advance $advance 0
timeout 30s tg --token $root_token checkpoint wait permission_capture.advanced $advanced 0 | ignore
tg --token $root_token checkpoint unwatch permission_capture.advance $advance
tg --token $root_token checkpoint unwatch permission_capture.advanced $advanced

failure (tg --token $bob.token get $reference | complete) "a stale capture must not restore access through the deleted tag-read proof."
failure (tg --token $bob.token tag get deleting | complete) "the tag must remain deleted."
