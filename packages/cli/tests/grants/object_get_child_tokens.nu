use ../../test.nu *

# Object get returns exact child tokens using the authorization it already performed.

def get-object [socket: string, bearer: string, id: string, --token: string] {
	let query = if $token == null { '' } else { $'?tokens[local]=($token | url encode --all)' }
	http get --max-time 10sec --headers { Accept: 'application/json', Authorization: $'Bearer ($bearer)' } --unix-socket $socket $'http://localhost/objects/($id)($query)'
}

def token-body [token: string] {
	$token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
}

let root_token = random chars
let server = server spawn --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let socket = $server.url | str replace 'http+unix://' '' | url decode
let directory = tg --token $alice.token put 'tg.directory({ "nested": tg.directory({ "a": tg.file("hello"), "b": tg.file("hello") }) })' | str trim
tg --token $alice.token index

# Authorization through the existing grants also produces child tokens.
let first = get-object $socket $alice.token $directory
let child = $first.children | columns | first
let source = token-body $first.tokens.local
assert equal (token-body ($first.children | get $child | get tokens.local)).permissions [object_subtree]
let bytes = tg --token $alice.token object get --bytes $directory | into binary
sleep 1sec

# An exact subtree token must avoid every authorization index lookup, including for children.
let watch = tg --token $root_token checkpoint watch authorization.index | from json | get watch
let output = get-object $socket $bob.token $directory --token $first.tokens.local
assert equal $output.data $first.data
assert equal (token-body $output.tokens.local).expires_at $source.expires_at
assert equal ($output.children | columns) [$child]
let token = $output.children | get $child | get tokens.local
let body = token-body $token
assert equal $body.resource $child
assert equal $body.permissions [object_subtree]
assert equal $body.expires_at $source.expires_at

# The child token authorizes its subtree and supplies an exact token for the next object.
let child_output = get-object $socket $bob.token $child --token $token
assert equal ($child_output.children | columns | length) 1 "repeated children should share one token."
let file = $child_output.children | columns | first
let file_token = $child_output.children | get $file | get tokens.local
assert equal (token-body $file_token).resource $file
assert equal (token-body $file_token).expires_at $source.expires_at
get-object $socket $bob.token $file --token $file_token | ignore

# The Rust client consumes the child tokens when loading the whole directory.
let reference = $'($directory)?tokens[local]=($first.tokens.local | url encode --all)'
let job = job spawn {
	let job_id = job id
	let output = tg --token $bob.token get $reference --depth inf | complete
	$output | job send --tag $job_id 0
}
let loaded = job recv --tag $job --timeout 10sec
success $loaded "loading descendants with the returned tokens should not consult the index."
assert ($loaded.stdout | str contains '"contents"')
assert equal (tg --token $bob.token object get --bytes $reference | into binary) $bytes "the object bytes must be unchanged."
tg --token $root_token checkpoint unwatch authorization.index $watch

# A node grant allows the object get but must not mint child subtree tokens.
tg --token $alice.token grant $bob.user.id object_node $directory
let output = get-object $socket $bob.token $directory
assert equal ($output.children? | default {}) {}
assert equal (token-body $output.tokens.local).permissions [object_node]
failure (tg --token $bob.token get $child | complete) "a node-only read must not grant access to the child."

# An invalid subtree token must not add permissions to the node grant.
let parts = $first.tokens.local | split row '.'
let signature = $parts.3 | decode base64 | bytes reverse | encode base64
let forged = [$parts.0 $parts.1 $parts.2 $signature] | str join '.'
let output = get-object $socket $bob.token $directory --token $forged
assert equal ($output.children? | default {}) {}

# Large child-token maps belong in the body so they do not exceed HTTP header limits.
let entries = 0..127 | each { |i|
	let n = $i | into string
	['"' $n '": tg.file("' $n '")'] | str join
} | str join ', '
let directory = tg --token $alice.token put (['tg.directory({' $entries '})'] | str join) | str trim
tg --token $alice.token index
let output = get-object $socket $alice.token $directory
assert equal ($output.children | columns | length) 128
success (tg --token $alice.token object get --bytes $directory | complete)
