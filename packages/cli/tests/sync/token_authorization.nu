use ../../test.nu *

# A sync token and unrelated authorization proofs do not authorize an object.
let root_token = random chars
let remote = server spawn --cloud --name remote --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	sync: { item_get_timeout: 1 },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json
let alice_local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let bob_local = server spawn --name bob-local --config {
	remotes: { default: { token: $bob.token, url: $remote.url } },
}

# Obtain a valid sync token from Alice's push and two proofs for Bob's unrelated objects.
let private = tg --url $alice_local.url put 'tg.file("private")' | str trim
let referent = tg --url $alice_local.url push $private | str trim
let uri = $'http://localhost/($referent)' | url parse
let sync = $uri.params | where key == 'tokens[remote][sync]' | first | get value
let unrelated = [one two] | each {|name|
	let value = ['tg.file("' $name '")'] | str join
	tg --url $remote.url --token $bob.token put $value | str trim
}
tg --url $remote.url --token $root_token index
let socket = $remote.url | str replace 'http+unix://' '' | url decode
let tokens = $unrelated | each {|id|
	http get --headers { Accept: 'application/json', Authorization: $'Bearer ($bob.token)' } --unix-socket $socket $'http://localhost/objects/($id)'
	| get tokens.local.authorization.0
}

# The sync hint must not turn either unrelated resource into a parent of Alice's object.
let query = {
	'tokens[remote][authorization][0]': ($tokens | get 0),
	'tokens[remote][authorization][1]': ($tokens | get 1),
	'tokens[remote][sync]': $sync,
} | url build-query
let output = timeout 10s tg --url $bob_local.url pull $'($private)?($query)' | complete
assert ($output.exit_code != 124) "the unauthorized pull should finish"
failure $output "the unrelated proofs must not authorize the private object"
failure (tg --url $bob_local.url get --local $private | complete) "the private object must not be copied"
