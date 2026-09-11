use ../../test.nu *

# Only a token accepted by authorization should limit the returned tokens' expiration.

def get-object [socket: string, bearer: string, id: string, --token: string] {
	let query = if $token == null { '' } else { $'?tokens[local][0]=($token | url encode --all)' }
	http get --max-time 10sec --headers { Accept: 'application/json', Authorization: $'Bearer ($bearer)' } --unix-socket $socket $'http://localhost/objects/($id)($query)'
}

def token-body [token: string] {
	$token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
}

let root_token = random chars
let server = server spawn --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	object: { grant_time_to_live: 2 }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let socket = $server.url | str replace 'http+unix://' '' | url decode
let directory = tg --token $root_token put 'tg.directory({ "child": tg.file("hello") })' | str trim
tg --token $root_token index
tg --token $root_token grant $alice.user.id object_subtree $directory

let first = get-object $socket $alice.token $directory
let expiration = (token-body $first.tokens.local.0).expires_at

# A proof accepted before it expires must still bound tokens returned after it expires.
let child = $first.children | columns | first
let params = { resource: $child } | to json --raw
let watch = tg --token $root_token checkpoint watch authorization.index --params $params | from json | get watch
let job = job spawn {
	let job_id = job id
	let output = get-object $socket $bob.token $child --token $first.tokens.local.0
	$output | job send --tag $job_id 0
}
let hit = timeout 10s tg --token $root_token checkpoint wait authorization.index $watch 0 | from json
assert equal $hit.params.token_resource $directory
sleep 3sec
tg --token $root_token checkpoint continue authorization.index $watch 0
let delayed = job recv --tag $job --timeout 10sec
assert equal (token-body $delayed.tokens.local.0).expires_at $expiration
for child in ($delayed.children | values) {
	assert equal (token-body $child.tokens.local.0).expires_at $expiration
}
tg --token $root_token checkpoint unwatch authorization.index $watch

# The expired token is ignored, and Alice's explicit grant authorizes fresh tokens.
let refreshed = get-object $socket $alice.token $directory --token $first.tokens.local.0
let refreshed_expiration = (token-body $refreshed.tokens.local.0).expires_at
assert ($refreshed_expiration > $expiration) "an expired token must not prevent a grant-authorized refresh."
let child = $refreshed.children | columns | first
let child_token = $refreshed.children | get $child | get tokens.local.0
assert equal (token-body $child_token).expires_at $refreshed_expiration

# Both refreshed tokens must work for Bob without an index authorization search.
let watch = tg --token $root_token checkpoint watch authorization.index | from json | get watch
get-object $socket $bob.token $directory --token $refreshed.tokens.local.0 | ignore
get-object $socket $bob.token $child --token $child_token | ignore
tg --token $root_token checkpoint unwatch authorization.index $watch

# An invalid signature with an expired body must not limit a grant-authorized refresh either.
let parts = $first.tokens.local.0 | split row '.'
let signature = $parts.3 | decode base64 | bytes reverse | encode base64
let forged = [$parts.0 $parts.1 $parts.2 $signature] | str join '.'
let output = get-object $socket $alice.token $directory --token $forged
assert ((token-body $output.tokens.local.0).expires_at > $expiration)
assert ((token-body ($output.children | get $child | get tokens.local.0)).expires_at > $expiration)
