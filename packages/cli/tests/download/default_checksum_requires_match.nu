use ../lib/test.nu *
use ../lib/http.nu *

# Downloading a URL without a checksum fails, because the default checksum matches nothing and the caller must opt in with a wildcard.

let local = server spawn
let http = spawn_http_server { '/': { body: "hello, world!\n" } }

let output = tg download $http.url | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> checksum mismatch
	   actual = sha512:8b79c03ccb15265150cab1a7f3f61e0abb397c977fad9e51b4a0b0d8d6b9d881ac8ab33007ea6e632fda8f2c4ba0ee9ada322d6cb18be2d87948d4f9b1faf1a2
	   expected = sha512:none

'
