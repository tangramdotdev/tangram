use ../../util/test.nu *

# Object get returns child tokens that let a user without grants read the object's descendants.

def get-object [socket: string, bearer: string, id: string, --token: string] {
	let query = if $token == null { '' } else { $'?tokens[local][0]=($token | url encode --all)' }
	http get --max-time 10sec --headers { Accept: 'application/json', Authorization: $'Bearer ($bearer)' } --unix-socket $socket $'http://localhost/objects/($id)($query)'
}

let root_token = random chars
let local = server spawn --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let directory = tg --token $alice.token put --no-tokens 'tg.directory({ "nested": tg.directory({ "a": tg.file("hello"), "b": tg.file("hello") }) })' | referent node
tg --token $alice.token index

# Authorization through the existing grants also produces child tokens.
let first = get-object $socket $alice.token $directory
let child = $first.children | columns | first
let bytes = tg --token $alice.token object get --bytes $directory | into binary

# An exact subtree token lets Bob read the object and returns a token for its child.
let output = get-object $socket $bob.token $directory --token $first.tokens.local.0
assert equal $output.data $first.data
assert equal ($output.children | columns) [$child]
let token = $output.children | get $child | get tokens.local.0

# The child token authorizes its subtree and supplies an exact token for the next object.
let child_output = get-object $socket $bob.token $child --token $token
assert equal ($child_output.children | columns | length) 1 "repeated children should share one token."
let file = $child_output.children | columns | first
let file_token = $child_output.children | get $file | get tokens.local.0
get-object $socket $bob.token $file --token $file_token | ignore

# The Rust client consumes the child tokens when loading the whole directory.
let reference = $'($directory)?tokens[local][0]=($first.tokens.local.0 | url encode --all)'
let loaded = tg --token $bob.token get $reference --depth inf | complete
success $loaded "loading descendants with the returned tokens should succeed."
assert ($loaded.stdout | str contains '"contents"')
assert equal (tg --token $bob.token object get --bytes $reference | into binary) $bytes "the object bytes must be unchanged."

# A node grant allows the object get but must not mint child subtree tokens.
tg --token $alice.token grant $bob.user.id object_node $directory
let output = get-object $socket $bob.token $directory
assert equal ($output.children? | default {}) {}
failure (tg --token $bob.token get $child | complete) "a node-only read must not grant access to the child."

# An invalid subtree token must not add permissions to the node grant.
let token = $first.tokens.local.0
let index = ($token | str length) - 2
let symbol = if ($token | str substring $index..$index) == '0' { '1' } else { '0' }
let forged = ($token | str substring ..<$index) + $symbol + ($token | str substring ($index + 1)..)
let output = get-object $socket $bob.token $directory --token $forged
assert equal ($output.children? | default {}) {}

# Large child-token maps belong in the body so they do not exceed HTTP header limits.
let entries = 0..127 | each { |i|
	let n = $i | into string
	['"' $n '": tg.file("' $n '")'] | str join
} | str join ', '
let directory = tg --token $alice.token put --no-tokens (['tg.directory({' $entries '})'] | str join) | referent node
tg --token $alice.token index
let output = get-object $socket $alice.token $directory
assert equal ($output.children | columns | length) 128
success (tg --token $alice.token object get --bytes $directory | complete)
