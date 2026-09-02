use ../../test.nu *

# A signed subtree token and an indexed direct edge authorize a child without a graph search.

let root_token = random chars
let server = server spawn --config {
	authentication: {
		root: { token: $root_token }
		users: { providers: { insecure: true } }
	}
	authorization: { initial: false, final: false }
}
let bob = tg login --verbose --name bob | from json

let child = tg --token $root_token put 'tg.file("child")' | str trim
let parent = tg --token $root_token put 'tg.directory({ "child": tg.file("child") })' | str trim
tg --token $root_token index

failure (tg --token $bob.token object get --bytes $child | complete) 'the child should require proof'

let socket = $server.url | str replace 'http+unix://' '' | url decode
let response = http get --full --headers { Authorization: $'Bearer ($root_token)' } --unix-socket $socket $'http://localhost/objects/($parent)'
let token = (
	$response.headers.response
	| where name == 'x-tg-object-tokens'
	| first
	| get value
	| from json
	| get local
	| url encode --all
)

http get --headers { Authorization: $'Bearer ($bob.token)' } --unix-socket $socket $'http://localhost/objects/($child)?tokens[local]=($token)' | ignore
