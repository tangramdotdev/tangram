use ../lib/test.nu *
use ../lib/http.nu *

# Downloading a URL without a checksum fails, because the default checksum matches nothing and the caller must opt in with a wildcard.

let http = spawn_http_server { '/': { body: "hello, world!\n" } }
let local = server spawn

let output = tg download $http.url | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> checksum mismatch
	   actual = sha256:4dca0fd5f424a31b03ab807cbae77eb32bf2d089eed1cee154b3afed458de0dc
	   expected = sha256:none

'
