use ../../util/test.nu *

# Child tokens follow the same remote trust and location rules as the object's own token.

let root_token = random chars
let remote = server spawn --cloud --name remote --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json
let directory = tg --url $remote.url --token $alice.token put --no-tokens 'tg.directory({ "file": tg.file("hello") })' | referent node
tg --url $remote.url --token $alice.token index
let socket = $remote.url | str replace 'http+unix://' '' | url decode
let first = http get --headers { Accept: 'application/json', Authorization: $'Bearer ($alice.token)' } --unix-socket $socket $'http://localhost/objects/($directory)'
let child = $first.children | columns | first
let child_token = $first.children | get $child | get tokens.local.0

for trusted in [false true] {
	let local = server spawn --name $'local-($trusted)' --config {
		remotes: { default: { token: $bob.token, trusted: $trusted, url: $remote.url } }
	}
	let socket = $local.url | str replace 'http+unix://' '' | url decode
	let query = { location: remote, 'tokens[remote][0]': $child_token, 'tokens[remote][1]': $first.tokens.local.0 } | url build-query
	let output = http get --max-time 10sec --headers { Accept: 'application/json' } --unix-socket $socket $'http://localhost/objects/($directory)?($query)'
	let tokens = $output.children | get $child | get tokens
	if $trusted {
		assert equal ($tokens | columns | sort) [local remote]
		assert equal ($output.tokens | columns | sort) [local remote]
	} else {
		assert equal ($tokens | columns) [remote]
		assert equal ($output.tokens | columns) [remote]
	}

	# The child token must authorize another remote object get.
	let query = { location: remote, 'tokens[remote][0]': $first.tokens.local.0, 'tokens[remote][1]': $tokens.remote.0 } | url build-query
	let output = http get --max-time 10sec --headers { Accept: 'application/json' } --unix-socket $socket $'http://localhost/objects/($child)?($query)'
	assert equal $output.data.kind file

	# The Rust client must also send the returned child tokens to the remote.
	let reference = $'($directory)?location=remote&tokens[remote][0]=($child_token | url encode --all)&tokens[remote][1]=($first.tokens.local.0 | url encode --all)'
	let output = tg --url $local.url get --depth inf $reference | complete
	success $output "the client should load the remote children with the returned tokens."
	assert ($output.stdout | str contains '"contents"')
}
