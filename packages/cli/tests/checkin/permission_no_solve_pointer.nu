use ../../test.nu *
use ../lib/checkin.nu checkin-output

# An embedded graph token proves subtree permission for a pointer with solving disabled.

let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	remotes: {}
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

let graph = (
	tg --token $alice.token put '
		tg.graph({
			"nodes": [
				{
					"kind": "file",
					"contents": tg.blob("pointer")
				}
			]
		})
	'
	| str trim
)
tg --token $alice.token index
tg --token $alice.token grant $bob.user.id object_subtree $graph | ignore

let socket = $server.url | str replace 'http+unix://' '' | url decode
let response = (
	http get
		--full
		--headers { Authorization: $'Bearer ($bob.token)' }
		--unix-socket $socket
		$'http://localhost/objects/($graph)?metadata=true'
)
let tokens = (
	$response.headers.response
	| where name == 'x-tg-object-tokens'
	| first
	| get value
	| from json
)
let token = $tokens.local | url encode --all
let reference = $'graph=($graph)&index=0&kind=file?tokens[local]=($token)'
let dependencies = [$reference] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } pointer)
}
let path = $directory | path join input
let output = checkin-output $server $path --no-solve --token $bob.token
assert equal $output.permissions [object_subtree] "the embedded graph token should prove subtree permission"
