use ../lib/test.nu *

# Commands that create resources return usable authorization referents by default.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } },
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let path = artifact { hello: (file "private contents") }

let checked_in = tg --token $alice.token checkin $path | str trim
let written = tg --token $alice.token write "private blob" | str trim
let put = tg --token $alice.token put 'tg.file("private file")' | str trim
let raw = tg --token $alice.token get --bytes $put
let put_bytes = $raw | tg --token $alice.token put --bytes --kind fil | str trim
let bundled = tg --token $alice.token bundle $checked_in | str trim

for entry in [
	{ referent: $checked_in, permission: object_subtree }
	{ referent: $written, permission: object_subtree }
	{ referent: $put, permission: object_subtree }
	{ referent: $put_bytes, permission: object_node }
	{ referent: $bundled, permission: object_subtree }
] {
	let referent = $entry.referent
	let id = $referent | referent node
	let tokens = $referent | referent tokens local
	assert not ($tokens | is-empty) "the output should carry local authorization"
	assert ($tokens | any {|token| $entry.permission in ($token | token body | get permissions) })
	for token in $tokens {
		let body = $token | token body
		assert equal $body.resource $id
		assert ($body.permissions | all {|permission| $permission in [object_node object_subtree] })
	}
	if not ($id starts-with 'blb_') {
		failure (tg --token $bob.token get $id | complete) $'the bare ID ($id) should not authorize another user'
	}
	success (tg --token $bob.token get $referent | complete) "the printed capability should authorize another user"
}

# Token suppression keeps the identity stable and also applies to get's stderr referent.
for entry in [
	{ actual: (tg --token $alice.token checkin --no-tokens $path | str trim), expected: $checked_in }
	{ actual: (tg --token $alice.token write --no-tokens "private blob" | str trim), expected: $written }
	{ actual: (tg --token $alice.token put --no-tokens 'tg.file("private file")' | str trim), expected: $put }
	{ actual: (tg --token $alice.token bundle --no-tokens $checked_in | str trim), expected: $bundled }
] {
	assert not ($entry.actual | str contains 'tokens[')
	assert equal ($entry.actual | referent node) ($entry.expected | referent node)
}
let get = tg --no-quiet --token $alice.token get --no-tokens $put | complete
success $get
assert not ($get.stderr | str contains 'tokens[')

let module = artifact { tangram.ts: 'export default () => "done";' }
for referent in [
	(tg --token $alice.token spawn $module | str trim)
	(tg --token $alice.token build --detach $module | str trim)
	(tg --token $alice.token run --sandbox --detach $module | str trim)
] {
	let id = $referent | referent node
	let tokens = $referent | referent tokens local
	assert not ($tokens | is-empty) "the detached process should carry authorization"
	for token in $tokens {
		assert equal ($token | token body | get resource) $id
	}
	failure (tg --token $bob.token process get $id | complete)
	success (tg --token $bob.token process get $referent | complete)
	success (tg --token $alice.token wait $referent | complete)
}

# Suppressing tokens must retain a sandbox's location option.
let sandbox = tg --token $alice.token sandbox create --no-network | str trim
let tokens = $sandbox | referent tokens local
assert equal ($tokens | length) 1
assert equal ($tokens.0 | token body | get resource) ($sandbox | referent node)
assert equal ($tokens.0 | token body | get permissions) [sandbox_read]
failure (tg --token $bob.token sandbox get ($sandbox | referent node) | complete)
success (tg --token $bob.token sandbox get $sandbox | complete)
tg --token $alice.token sandbox destroy ($sandbox | referent node)

let sandbox = tg --token $alice.token sandbox create --no-network --no-tokens | str trim
assert not ($sandbox | str contains 'tokens[')
assert ($sandbox | str contains 'location=local')
tg --token $alice.token sandbox destroy ($sandbox | referent node)
