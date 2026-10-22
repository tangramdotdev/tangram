use ../lib/test.nu *

# Only a token accepted by authorization limits how long the returned tokens grant access.

def get-object [socket: string, bearer: string, id: string, --token: string] {
	let query = if $token == null { '' } else { $'?tokens[local][0]=($token | url encode --all)' }
	http get --max-time 10sec --headers { Accept: 'application/json', Authorization: $'Bearer ($bearer)' } --unix-socket $socket $'http://localhost/objects/($id)($query)'
}

def reference [id: string, token: string] {
	$'($id)?tokens[local][0]=($token | url encode --all)'
}

let root_token = random chars
let local = server spawn --now 2026-01-01T00:00:00Z --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	object: { permission_time_to_live: 2 }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let directory = tg --token $root_token put --no-tokens 'tg.directory({ "child": tg.file("hello") })' | referent node
tg --token $root_token index
tg --token $root_token grant $alice.user.id object_subtree $directory

let first = get-object $socket $alice.token $directory

# A proof accepted before it expires must still bound tokens returned after it expires, so they no longer grant access.
let child = $first.children | columns | first
let params = { resource: $child } | to json --raw
let watch = tg --token $root_token checkpoint watch verification.index --params $params | from json | get watch
let job = job spawn {
	let job_id = job id
	let output = get-object $socket $bob.token $child --token $first.tokens.local.0
	$output | job send --tag $job_id 0
}
tg --token $root_token checkpoint wait --timeout 10s verification.index $watch 0 | ignore
advance_time $local 3sec
tg --token $root_token checkpoint continue verification.index $watch 0
let delayed = job recv --tag $job --timeout 10sec
tg --token $root_token checkpoint unwatch verification.index $watch
failure (tg --token $bob.token get --bytes (reference $child $delayed.tokens.local.0) | complete) "a token returned under an expired proof must not grant access."

# The expired token is ignored, and Alice's explicit grant authorizes fresh tokens.
let refreshed = get-object $socket $alice.token $directory --token $first.tokens.local.0
let child = $refreshed.children | columns | first
let child_token = $refreshed.children | get $child | get tokens.local.0
success (tg --token $bob.token get --bytes (reference $directory $refreshed.tokens.local.0) | complete) "an expired token must not prevent a grant-authorized refresh."
success (tg --token $bob.token get --bytes (reference $child $child_token) | complete) "the refreshed child token must grant access."

# An invalid signature with an expired body must not limit a grant-authorized refresh either.
let parts = $first.tokens.local.0 | split row '.'
let signature = $parts.3 | decode base64 | bytes reverse | encode base64
let forged = [$parts.0 $parts.1 $parts.2 $signature] | str join '.'
let output = get-object $socket $alice.token $directory --token $forged
success (tg --token $bob.token get --bytes (reference $directory $output.tokens.local.0) | complete) "a forged token must not limit the refreshed token."
success (tg --token $bob.token get --bytes (reference $child ($output.children | get $child | get tokens.local.0)) | complete) "a forged token must not limit the refreshed child token."
