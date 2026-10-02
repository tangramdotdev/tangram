use ../lib/test.nu *
use ../lib/http.nu *

# Downloading a redirect returns the contents of its target.

let http = spawn_http_server {
	'/file.txt': { body: "hello, world!\n" },
	'/redirect': { headers: { location: '/file.txt' }, status: 302 },
}
let local = server spawn

let output = tg download $'($http.url)/redirect' --checksum sha256:any | complete
success $output
let output = tg read ($output.stdout | str trim) | complete
success $output
assert equal $output.stdout "hello, world!\n"
