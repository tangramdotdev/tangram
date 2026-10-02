use ../lib/test.nu *

# Reading a nested blob propagates an exact root token through every branch.

let root = random chars
let local = server spawn --config {
	authentication: { root: { token: $root }, users: { providers: { insecure: true } } }
	verification: { permissions: { final: false, initial: false } }
}
let bob = tg login --verbose --name bob | from json
let module = artifact {
	tangram.ts: '
		export default () => tg.Blob.branch(tg.Blob.branch("a", "b"), tg.Blob.branch("c", "d"));
	'
}
let blob = tg --token $root build $module | str trim | split row '?' | first
let socket = $local.url | str replace 'http+unix://' '' | url decode
let output = http get --headers { Accept: 'application/json', Authorization: $'Bearer ($root)' } --unix-socket $socket $'http://localhost/objects/($blob)'
let token = $output.tokens.local.0
let reference = $'($blob)?tokens[local][0]=($token | url encode --all)'

failure (tg --token $bob.token read $blob | complete) 'the reader must not access the blob without its token'
let output = tg --token $bob.token read $reference | complete
success $output 'the reader must propagate tokens through nested branches without authorization search'
assert equal $output.stdout 'abcd'
