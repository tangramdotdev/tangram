use ../../test.nu *

# Writing bytes returns subtree authorization for the blob.

let server = server spawn
let socket = $server.url | str replace 'http+unix://' '' | url decode

let output = (
	'hello'
	| into binary
	| http post
		--headers { 'Content-Type': 'application/octet-stream' }
		--unix-socket $socket
		'http://localhost/write'
)
let uri = $'http://localhost/($output.blob)' | url parse
let tokens = $uri.params | where key == 'tokens[local][authorization][0]'
assert equal ($tokens | length) 1 'the write should return an authorization token'
let body = $tokens.0.value | split row '.' | get 1 | decode base64 | decode utf-8 | from json
assert equal $body.resource ($uri.path | str substring 1..)
assert equal $body.permissions [object_subtree]
