use ../lib/test.nu *
use ../lib/http.nu *

# Downloading a URL with a wildcard checksum returns a blob with the downloaded contents.

let local = server spawn
let http = spawn_http_server { '/': { body: "hello, world!\n" } }

let output = tg download $http.url --checksum sha256:any | complete
success $output
assert ($output.stdout | str trim | str starts-with "blb_") "the download should return a blob id"

let contents = tg read ($output.stdout | str trim)
snapshot $contents "hello, world!\n"
