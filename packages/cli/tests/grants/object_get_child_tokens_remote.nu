use ../../test.nu *

# Child tokens follow the same remote trust and location rules as the object's own token.

def token-body [token: string] {
	$token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
}

let root_token = random chars
let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json
let directory = tg --url $remote.url --token $alice.token put 'tg.directory({ "file": tg.file("hello") })' | str trim
tg --url $remote.url --token $alice.token index
let socket = $remote.url | str replace 'http+unix://' '' | url decode
let first = http get --headers { Accept: 'application/json', Authorization: $'Bearer ($alice.token)' } --unix-socket $socket $'http://localhost/objects/($directory)'
let child = $first.children | columns | first
let child_token = $first.children | get $child | get tokens.local.authorization.0
let child_body = token-body $child_token
let watch = tg --url $remote.url --token $root_token checkpoint watch authorization.index | from json | get watch

for trusted in [false true] {
	let local = server spawn --name $'local-($trusted)' --config {
		remotes: { default: { token: $bob.token, trusted: $trusted, url: $remote.url } }
	}
	let socket = $local.url | str replace 'http+unix://' '' | url decode
	let query = { location: remote, 'tokens[remote][authorization][0]': $child_token, 'tokens[remote][authorization][1]': $first.tokens.local.authorization.0 } | url build-query
	let output = http get --max-time 10sec --headers { Accept: 'application/json' } --unix-socket $socket $'http://localhost/objects/($directory)?($query)'
	let tokens = $output.children | get $child | get tokens
	assert equal (token-body $tokens.remote.authorization.0) $child_body
	assert equal (token-body $output.tokens.remote.authorization.0) (token-body $first.tokens.local.authorization.0)
	if $trusted {
		assert equal (token-body $tokens.local.authorization.0) $child_body
		assert equal (token-body $output.tokens.local.authorization.0) (token-body $output.tokens.remote.authorization.0)
		assert ($tokens.local.authorization.0 != $tokens.remote.authorization.0) "the local token must be signed with the local key."
	} else {
		assert equal ($tokens | columns) [remote]
		assert equal ($output.tokens | columns) [remote]
	}

	# The child token must authorize another remote object get without an index lookup.
	let query = { location: remote, 'tokens[remote][authorization][0]': $first.tokens.local.authorization.0, 'tokens[remote][authorization][1]': $tokens.remote.authorization.0 } | url build-query
	let output = http get --max-time 10sec --headers { Accept: 'application/json' } --unix-socket $socket $'http://localhost/objects/($child)?($query)'
	assert equal $output.data.kind file

	# The Rust client must also send the returned child tokens to the remote.
	let reference = $'($directory)?location=remote&tokens[remote][authorization][0]=($child_token | url encode --all)&tokens[remote][authorization][1]=($first.tokens.local.authorization.0 | url encode --all)'
	let job = job spawn {
		let job_id = job id
		let output = tg --url $local.url get --depth inf $reference | complete
		$output | job send --tag $job_id 0
	}
	let output = job recv --tag $job --timeout 10sec
	success $output "the client should load the remote children without an authorization index lookup."
	assert ($output.stdout | str contains '"contents"')
}

tg --url $remote.url --token $root_token checkpoint unwatch authorization.index $watch
