use ../lib/test.nu *

# Putting duplicate parent references preserves authorization for the stored directory.

let root_token = random chars
let local = server spawn --name local --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	verification: {
		permissions: {
			final: false
		}
	}
}
let alice = tg login --verbose --name alice | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
for reverse in [false true] {
	let file = tg --token $root_token put --no-tokens (['tg.file("duplicate parent ' ($reverse | into string) '")'] | str join) | referent node
	let output = http get --headers { Accept: application/json, Authorization: $'Bearer ($root_token)' } --unix-socket $socket $'http://localhost/objects/($file)'
	let token = $output.tokens.local.0
	let referent = $'($file)?tokens[local][0]=($token | url encode --all)'
	let with_tokens = (['tg.directory({ "file": ' $referent ' })'] | str join)
	let without_tokens = (['tg.directory({ "file": ' $file ' })'] | str join)
	let entries = if $reverse { [$with_tokens $without_tokens] } else { [$without_tokens $with_tokens] }
	let directory = tg --token $alice.token put --no-tokens (['tg.directory({ "a": ' $entries.0 ', "b": ' $entries.1 ' })'] | str join) | referent node
	tg --token $alice.token index
	let output = http get --headers { Accept: application/json, Authorization: $'Bearer ($alice.token)' } --unix-socket $socket $'http://localhost/objects/($directory)'
	assert equal ($output.children | columns | length) 1
}
