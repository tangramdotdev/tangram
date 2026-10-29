use ../../util/test.nu *
use ../../util/checkin.nu reference-access

# An embedded graph token lets a checked-in file's reference grant another user read access to a pointer dependency with solving disabled.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	remotes: {}
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let eve = tg login --verbose --name eve | from json

let graph = (
	tg --token $alice.token put --no-tokens '
		tg.graph({
			"nodes": [
				{
					"kind": "file",
					"contents": tg.blob("pointer")
				}
			]
		})
	' | referent node
	| str trim
)
tg --token $alice.token index
tg --token $alice.token grant $bob.user.id object_subtree $graph | ignore

let socket = $local.url | str replace 'http+unix://' '' | url decode
let response = (
	http get
		--headers { Accept: 'application/json', Authorization: $'Bearer ($bob.token)' }
		--unix-socket $socket
		$'http://localhost/objects/($graph)?metadata=true'
)
let tokens = $response.tokens
let token = $tokens.local.0 | url encode --all
let reference = $'graph=($graph)&index=0&kind=file?tokens[local][0]=($token)'
let dependencies = [$reference] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } pointer)
}
let path = $directory | path join input
let reference = tg --token $bob.token checkin --no-lock --root --no-solve $path | str trim
assert equal (reference-access $reference --token $eve.token) subtree "the embedded graph token should make the reference grant the subtree"
