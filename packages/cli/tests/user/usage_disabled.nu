use ../../test.nu *

# Usage tracking is disabled by default.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose alice | from json
tg --token $alice.token put 'tg.file("hello")'
tg --token $alice.token index

let output = tg --token $alice.token usage | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> the request failed
	   status = 500 Internal Server Error
	-> usage tracking is disabled

'
