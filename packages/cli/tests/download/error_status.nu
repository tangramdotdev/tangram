use ../lib/test.nu *
use ../lib/http.nu *

# Downloading a URL that responds with an error status fails with the reason on the CLI.

let http = spawn_http_server { '/does-not-exist': { status: 404 } }
let local = server spawn

let output = tg download $'($http.url)/does-not-exist' --checksum sha256:any | complete
failure $output
snapshot --normalize --redact $http.url $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> expected a success status
	   url = <redacted>/does-not-exist
	-> HTTP status client error (404 Not Found) for url (<redacted>/does-not-exist)

'
