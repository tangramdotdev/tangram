use ../../test.nu *

# Multiple proofs reach authorization without an unrelated or invalid proof displacing an exact token.

def get-object [socket: string, bearer: string, id: string, tokens: list<string>] {
	let query = $tokens | enumerate | each { |entry|
		$'tokens[local][($entry.index)]=($entry.item | url encode --all)'
	} | str join '&'
	http get --max-time 10sec --headers { Accept: 'application/json', Authorization: $'Bearer ($bearer)' } --unix-socket $socket $'http://localhost/objects/($id)?($query)'
}

let root_token = random chars
let server = server spawn --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let socket = $server.url | str replace 'http+unix://' '' | url decode
let directory = tg --token $alice.token put 'tg.directory({ "file": tg.file("hello") })' | str trim
let unrelated = tg --token $alice.token put 'tg.file("unrelated")' | str trim
tg --token $alice.token index
let subtree = (get-object $socket $alice.token $directory []).tokens.local.0
let other = (get-object $socket $alice.token $unrelated []).tokens.local.0
tg --token $alice.token grant $bob.user.id object_node $directory
let node = (get-object $socket $bob.token $directory []).tokens.local.0
let parts = $subtree | split row '.'
let signature = $parts.3 | decode base64 | bytes reverse | encode base64
let forged = [$parts.0 $parts.1 $parts.2 $signature] | str join '.'

let watch = tg --token $root_token checkpoint watch authorization.index | from json | get watch
for tokens in [[$node $other $forged $subtree] [$subtree $forged $other $node]] {
	let output = get-object $socket $bob.token $directory $tokens
	assert equal ($output.children | columns | length) 1
}
tg --token $root_token checkpoint unwatch authorization.index $watch

let output = get-object $socket $bob.token $directory [$node $other $forged]
assert equal ($output.children? | default {}) {}

# A large collection of distinct proofs uses the framed arg through the Rust client.
let objects = 0..<16 | each { |i|
	let value = ['tg.file("proof ' ($i | into string) '")'] | str join
	tg --token $alice.token put $value | str trim
}
tg --token $alice.token index
let tokens = $objects | each { |object|
	(get-object $socket $alice.token $object []).tokens.local.0
} | append $subtree
let query = $tokens | enumerate | each { |entry|
	$'tokens[local][($entry.index)]=($entry.item | url encode --all)'
} | str join '&'
assert (($query | str length) > 4096)
let reference = $'($directory)?($query)'
let expected = tg --token $alice.token object get --bytes $directory | into binary
assert equal (tg --token $bob.token object get --bytes $reference | into binary) $expected
let loaded = tg --token $bob.token get $reference --depth inf | complete
success $loaded
assert ($loaded.stdout | str contains '"contents"')
